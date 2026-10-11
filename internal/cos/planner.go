package cos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The Opus advisor splits a task into independent Haiku 5.5 worker briefs and hands each to delegate_task.
type PlanItem struct {
	ID      string   `json:"id"`                // [a-z0-9-]{1,32}
	Goal    string   `json:"goal"`              // 1..2000 bytes
	Files   []string `json:"files,omitempty"`   // paths/globs the worker owns exclusively
	After   []string `json:"after,omitempty"`   // item IDs that must finish first
	Checks  string   `json:"checks,omitempty"`  // command(s) that prove it is done
	Minutes int      `json:"minutes,omitempty"` // estimate, default 10
}

type PlanInput struct {
	Task  string     `json:"task"`
	Repo  string     `json:"repo,omitempty"`
	Items []PlanItem `json:"items"`
}

type WorkerBrief struct {
	ID     string
	Wave   int
	Target map[string]any
	Brief  string
}

type Plan struct {
	Task    string
	Waves   [][]string
	Workers []WorkerBrief
	// Estimated wall clock: SerialMinutes is every item in turn, ParallelMinutes is each wave's longest item summed.
	SerialMinutes, ParallelMinutes int
}

// PlanResult is what the advisor measured after running a plan.
type PlanResult struct {
	Task    string `json:"task"`
	Workers []struct {
		ID      string `json:"id"`
		Seconds int    `json:"seconds"`
		Tokens  int    `json:"tokens"`
	} `json:"workers"`
	WallSeconds   int `json:"wallSeconds"`   // measured wall clock of the whole plan
	AdvisorTokens int `json:"advisorTokens"` // Opus advisor tokens spent coordinating
}

type planDone struct {
	WallSaved     int     `json:"wallSaved"`
	Speedup       float64 `json:"speedup"`
	WorkerTokens  int     `json:"workerTokens"`
	AdvisorTokens int     `json:"advisorTokens"`
}

const (
	planMaxItems    = 12
	planMaxParallel = 4
	planMaxGoal     = 2000
	planMinutes     = 10
)

var planID = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

func init() {
	cliCommands["plan"] = cliCommand{Usage: "plan '<json PlanInput>' | done '<json PlanResult>'", Run: func(ctx context.Context, cfg Config, args []string, w io.Writer) error {
		if len(args) == 2 && args[0] == "done" {
			bus, err := ConnectBus(ctx, cfg)
			if err != nil {
				return err
			}
			defer bus.Close()
			kv, err := openKV(ctx, bus, savingsBucket)
			if err != nil {
				return err
			}
			return planCommand(ctx, kv, args, w)
		}
		return planCommand(ctx, nil, args, w)
	}}
}

func planCommand(ctx context.Context, savings jetstream.KeyValue, args []string, w io.Writer) error {
	usage := errors.New("usage: cos plan '<json PlanInput>' | done '<json PlanResult>'")
	switch {
	case len(args) == 1 && args[0] != "done":
		var in PlanInput
		if err := json.Unmarshal([]byte(args[0]), &in); err != nil {
			return fmt.Errorf("plan: bad JSON: %w", err)
		}
		p, err := PlanWorkers(in)
		if err != nil {
			return err
		}
		return output(w, p)
	case len(args) == 2 && args[0] == "done":
		var r PlanResult
		if err := json.Unmarshal([]byte(args[1]), &r); err != nil {
			return fmt.Errorf("plan done: bad JSON: %w", err)
		}
		if err := r.validate(); err != nil {
			return err
		}
		saved, speedup := r.Effect()
		workerTokens := 0
		for _, wk := range r.Workers {
			workerTokens += wk.Tokens
		}
		if savings != nil {
			// Token savings of Haiku workers come from real usage (tokens page); the ledger adds wall-clock saved.
			if err := RecordSaving(ctx, savings, SavingParallel, 0, saved, time.Now()); err != nil {
				return err
			}
		}
		return output(w, planDone{WallSaved: saved, Speedup: speedup, WorkerTokens: workerTokens, AdvisorTokens: r.AdvisorTokens})
	}
	return usage
}

func PlanWorkers(in PlanInput) (Plan, error) {
	if strings.TrimSpace(in.Task) == "" {
		return Plan{}, errors.New("plan task is empty")
	}
	if len(in.Items) == 0 || len(in.Items) > planMaxItems {
		return Plan{}, fmt.Errorf("plan needs 1 to %d items, got %d", planMaxItems, len(in.Items))
	}
	byID := make(map[string]int, len(in.Items))
	minutes := make([]int, len(in.Items))
	for i, it := range in.Items {
		if !planID.MatchString(it.ID) {
			return Plan{}, fmt.Errorf("item id %q must be 1-32 lowercase letters, digits or dashes", it.ID)
		}
		if _, dup := byID[it.ID]; dup {
			return Plan{}, fmt.Errorf("duplicate item id %q", it.ID)
		}
		byID[it.ID] = i
		if strings.TrimSpace(it.Goal) == "" || len(it.Goal) > planMaxGoal {
			return Plan{}, fmt.Errorf("item %s goal must be 1 to %d bytes", it.ID, planMaxGoal)
		}
		if it.Minutes < 0 {
			return Plan{}, fmt.Errorf("item %s minutes must not be negative", it.ID)
		}
		minutes[i] = planMinutes
		if it.Minutes > 0 {
			minutes[i] = it.Minutes
		}
	}
	for _, it := range in.Items {
		for _, dep := range it.After {
			if _, ok := byID[dep]; !ok {
				return Plan{}, fmt.Errorf("item %s waits on unknown item %q", it.ID, dep)
			}
		}
	}

	// level is the earliest layer an item can run in; DFS also rejects cycles.
	level := make([]int, len(in.Items))
	state := make([]int, len(in.Items)) // 1 visiting, 2 done
	var stack []string
	var visit func(i int) error
	visit = func(i int) error {
		switch state[i] {
		case 1:
			cycle := append(slices.Clone(stack[slices.Index(stack, in.Items[i].ID):]), in.Items[i].ID)
			return fmt.Errorf("plan items form a cycle: %s", strings.Join(cycle, " -> "))
		case 2:
			return nil
		}
		state[i] = 1
		stack = append(stack, in.Items[i].ID)
		for _, dep := range in.Items[i].After {
			j := byID[dep]
			if err := visit(j); err != nil {
				return err
			}
			level[i] = max(level[i], level[j]+1)
		}
		stack = stack[:len(stack)-1]
		state[i] = 2
		return nil
	}
	for i := range in.Items {
		if err := visit(i); err != nil {
			return Plan{}, err
		}
	}

	// Layers run in order; inside a layer, an item that clashes with the open wave starts the next one.
	var waves [][]int
	for l := 0; l <= slices.Max(level); l++ {
		var cur []int
		for i := range in.Items {
			if level[i] != l {
				continue
			}
			if len(cur) == planMaxParallel || planClash(in.Items, cur, i) {
				waves = append(waves, cur)
				cur = nil
			}
			cur = append(cur, i)
		}
		if len(cur) > 0 {
			waves = append(waves, cur)
		}
	}

	plan := Plan{Task: in.Task}
	for w, wave := range waves {
		var ids []string
		longest := 0
		for _, i := range wave {
			it := in.Items[i]
			ids = append(ids, it.ID)
			longest = max(longest, minutes[i])
			plan.SerialMinutes += minutes[i]
			plan.Workers = append(plan.Workers, WorkerBrief{ID: it.ID, Wave: w + 1, Target: planTarget(), Brief: planBrief(in, it, w+1)})
		}
		plan.Waves = append(plan.Waves, ids)
		plan.ParallelMinutes += longest
	}
	return plan, nil
}

// planTarget returns a fresh map so callers cannot mutate a shared one.
func planTarget() map[string]any {
	pol := CurrentBehavior().Models
	return pol.WorkerTarget(pol.Leader)
}

func planBrief(in PlanInput, it PlanItem, wave int) string {
	repo, files, checks := "n/a", "no files (read-only)", "report what you did"
	if in.Repo != "" {
		repo = in.Repo
	}
	if len(it.Files) > 0 {
		files = strings.Join(it.Files, ", ")
	}
	if it.Checks != "" {
		checks = it.Checks
	}
	return fmt.Sprintf("You are a Haiku 5.5 worker under an Opus 5.5 advisor (COS). Task: %s\n"+
		"Your part (%s, wave %d): %s\n"+
		"Repo: %s\n"+
		"You own only: %s. Do not edit anything else; other workers run in parallel.\n"+
		"Done when: %s.\n"+
		"Report: what changed (files, lines), check output, anything you could not do. No commits or pushes unless the goal says so.",
		in.Task, it.ID, wave, it.Goal, repo, files, checks)
}

// planClash reports whether item i owns a file that an item already in cur also owns.
func planClash(items []PlanItem, cur []int, i int) bool {
	for _, j := range cur {
		for _, a := range items[j].Files {
			for _, b := range items[i].Files {
				if planOverlap(a, b) {
					return true
				}
			}
		}
	}
	return false
}

// planOverlap reports whether two owned paths or globs can name the same file.
func planOverlap(a, b string) bool {
	a, b = path.Clean(a), path.Clean(b)
	if a == b || planNested(a, b) || planNested(b, a) {
		return true
	}
	// path.Match cannot bound recursive globs; assume they overlap.
	if strings.Contains(a+b, "**") {
		return true
	}
	if strings.ContainsRune(a+b, '*') {
		da, db := path.Dir(a), path.Dir(b)
		ma, _ := path.Match(da, db)
		mb, _ := path.Match(db, da)
		return ma || mb
	}
	return false
}

// planNested reports whether p sits inside directory dir, matching at a "/" boundary.
func planNested(dir, p string) bool {
	return strings.HasPrefix(p, dir+"/")
}

func (r PlanResult) validate() error {
	if n := len(r.Workers); n < 1 || n > planMaxItems {
		return fmt.Errorf("plan result needs 1 to %d workers, got %d", planMaxItems, n)
	}
	for _, wk := range r.Workers {
		if wk.Seconds < 0 || wk.Tokens < 0 {
			return errors.New("worker seconds and tokens must not be negative")
		}
	}
	if r.WallSeconds <= 0 {
		return errors.New("wallSeconds must be positive")
	}
	if r.AdvisorTokens < 0 {
		return errors.New("advisorTokens must not be negative")
	}
	return nil
}

// Effect returns seconds saved versus running the workers one after another, and the speedup ratio.
func (r PlanResult) Effect() (wallSaved int, speedup float64) {
	sum := 0
	for _, wk := range r.Workers {
		sum += wk.Seconds
	}
	wallSaved = max(sum-r.WallSeconds, 0)
	if r.WallSeconds > 0 {
		speedup = math.Round(float64(sum)/float64(r.WallSeconds)*100) / 100
	}
	return wallSaved, speedup
}
