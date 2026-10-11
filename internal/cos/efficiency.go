package cos

import (
	"cmp"
	"encoding/json"
	"math"
	"slices"
	"time"
)

// savingsTargetPercent is the share of tokens the savings ledger should avoid.
const savingsTargetPercent = 30

const (
	efficiencyLowShare = 50     // a thread under this worker share is listed
	efficiencyLowFloor = 10_000 // a thread with less output is too small to judge
	efficiencyListMax  = 5
	efficiencyTopMax   = 3
)

// Efficiency is how far the same accounts stretch: worker share, rework, output per merged PR, savings.
type Efficiency struct {
	Range         string           `json:"range"`
	Tokens        int64            `json:"tokens"` // usageTokens: input + cache writes + output
	Output        int64            `json:"output"`
	WorkerShare   int              `json:"workerShare"` // % of output on worker models
	LowShare      []ThreadShare    `json:"lowShare"`    // threads under 50%, most output first, at most 5
	Waste         map[string]int64 `json:"waste"`       // gate | rebase | retry
	WastePct      int              `json:"wastePct"`    // of Tokens
	Merged        []MergedPR       `json:"merged"`      // PRs merged in range, newest first
	OutputPerPR   int64            `json:"outputPerPR"` // median output of Merged; 0 when none
	Saved         int64            `json:"saved"`
	SavingsPct    int              `json:"savingsPct"` // Saved / (Tokens + Saved)
	SavingsTarget int              `json:"savingsTarget"`
	Top           []TokenSink      `json:"top"` // where tokens go, at most 3
}

type ThreadShare struct {
	Thread      string `json:"thread"`
	Output      int64  `json:"output"`
	WorkerShare int    `json:"workerShare"`
}

type MergedPR struct {
	Repo   string    `json:"repo"`
	PR     int       `json:"pr"`
	Ticket string    `json:"ticket,omitempty"`
	Branch string    `json:"branch"`
	Output int64     `json:"output"`
	Time   time.Time `json:"time"`
}

type TokenSink struct {
	Label  string `json:"label"`
	Tokens int64  `json:"tokens"`
	Pct    int    `json:"pct"`
}

// EfficiencyOf builds the view. allRows span the whole keep window, because a PR's
// work can start before the range.
func EfficiencyOf(rng string, rows, allRows []UsageRow, merged []Event, saved int64) Efficiency {
	e := Efficiency{Range: rng, LowShare: []ThreadShare{}, Merged: []MergedPR{}, Top: []TokenSink{}, Saved: saved, SavingsTarget: savingsTargetPercent}
	total := usageTotal("total", rows)
	e.Tokens, e.Output, e.WorkerShare = usageTokens(total), total.Output, total.HaikuShare
	e.Waste = total.Waste
	var waste int64
	for _, n := range total.Waste {
		waste += n
	}
	e.WastePct = efficiencyPct(waste, e.Tokens)

	for _, t := range SummarizeUsage(rows, Live{}, "thread") {
		if t.Output >= efficiencyLowFloor && t.HaikuShare < efficiencyLowShare {
			e.LowShare = append(e.LowShare, ThreadShare{Thread: t.Key, Output: t.Output, WorkerShare: t.HaikuShare})
		}
	}
	slices.SortFunc(e.LowShare, func(a, b ThreadShare) int {
		return cmp.Or(cmp.Compare(b.Output, a.Output), cmp.Compare(a.Thread, b.Thread))
	})
	e.LowShare = e.LowShare[:min(len(e.LowShare), efficiencyListMax)]

	outputs := map[string]int64{}
	for _, r := range allRows {
		outputs[r.Branch] += r.Output
	}
	for _, ev := range merged {
		var d struct {
			Branch string `json:"branch"`
		}
		if json.Unmarshal(ev.Data, &d) != nil || d.Branch == "" {
			continue
		}
		e.Merged = append(e.Merged, MergedPR{Repo: ev.Repo, PR: ev.PR, Ticket: ev.Ticket, Branch: d.Branch, Output: outputs[d.Branch], Time: ev.Time})
	}
	slices.SortFunc(e.Merged, func(a, b MergedPR) int { return b.Time.Compare(a.Time) })
	e.OutputPerPR = efficiencyMedian(e.Merged)

	e.SavingsPct = efficiencyPct(saved, e.Tokens+saved)

	groups := map[string][]UsageRow{}
	for _, r := range rows {
		role := "advisor"
		if usageWorker(r.Model) {
			role = "workers"
		}
		label := cmp.Or(r.Ticket, "(no ticket)") + " " + role
		groups[label] = append(groups[label], r)
	}
	for label, g := range groups {
		tokens := usageTokens(usageTotal(label, g))
		e.Top = append(e.Top, TokenSink{Label: label, Tokens: tokens, Pct: efficiencyPct(tokens, e.Tokens)})
	}
	slices.SortFunc(e.Top, func(a, b TokenSink) int {
		return cmp.Or(cmp.Compare(b.Tokens, a.Tokens), cmp.Compare(a.Label, b.Label))
	})
	e.Top = e.Top[:min(len(e.Top), efficiencyTopMax)]
	return e
}

// efficiencyMedian is the middle output of the PRs; an even count takes the lower middle.
func efficiencyMedian(prs []MergedPR) int64 {
	if len(prs) == 0 {
		return 0
	}
	outs := make([]int64, len(prs))
	for i, p := range prs {
		outs[i] = p.Output
	}
	slices.Sort(outs)
	return outs[(len(outs)-1)/2]
}

// efficiencyPct is part of whole as a rounded integer percent; 0 when whole is 0.
func efficiencyPct(part, whole int64) int {
	if whole <= 0 {
		return 0
	}
	return int(math.Round(100 * float64(part) / float64(whole)))
}
