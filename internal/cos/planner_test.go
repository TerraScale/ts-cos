package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func planItem(id, goal string, files ...string) PlanItem {
	return PlanItem{ID: id, Goal: goal, Files: files}
}

func TestPlanDisjointFitsOneWave(t *testing.T) {
	p, err := PlanWorkers(PlanInput{Task: "rename", Items: []PlanItem{
		planItem("a", "do a", "internal/a/a.go"),
		planItem("b", "do b", "internal/b/b.go"),
		planItem("c", "do c", "internal/c/c.go"),
		planItem("d", "do d", "internal/d/d.go"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Waves) != 1 || len(p.Waves[0]) != 4 {
		t.Fatalf("waves = %v, want one wave of 4", p.Waves)
	}
	if p.SerialMinutes != 40 || p.ParallelMinutes != 10 {
		t.Fatalf("minutes serial=%d parallel=%d, want 40 and 10", p.SerialMinutes, p.ParallelMinutes)
	}
}

func TestPlanSixItemsSplitAtFour(t *testing.T) {
	var items []PlanItem
	for i := range 6 {
		items = append(items, planItem(fmt.Sprintf("w%d", i), "do it"))
	}
	p, err := PlanWorkers(PlanInput{Task: "t", Items: items})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Waves) != 2 || len(p.Waves[0]) != 4 || len(p.Waves[1]) != 2 {
		t.Fatalf("waves = %v, want sizes 4 and 2", p.Waves)
	}
}

func TestPlanAfterChainRunsInOrder(t *testing.T) {
	p, err := PlanWorkers(PlanInput{Task: "t", Items: []PlanItem{
		planItem("a", "first"),
		{ID: "b", Goal: "second", After: []string{"a"}},
		{ID: "c", Goal: "third", After: []string{"b"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Waves) != 3 || p.Waves[0][0] != "a" || p.Waves[1][0] != "b" || p.Waves[2][0] != "c" {
		t.Fatalf("waves = %v, want a, b, c in order", p.Waves)
	}
}

func TestPlanFileOverlap(t *testing.T) {
	cases := []struct {
		a, b  string
		waves int
	}{
		{"internal/cos/a.go", "internal/cos/a.go", 2},
		{"internal/cos", "internal/cos/x.go", 2},
		{"internal/cos/*.go", "internal/cos/y.go", 2},
		{"internal/cos/a.go", "internal/cos/ab.go", 1},
		{"internal/cos/*.go", "internal/other/y.go", 1},
		{"./src/a.go", "src/a.go", 2},
		{"src/**", "src/deep/x.go", 2},
		{"internal/cos/", "internal/cos/x.go", 2},
		{"src/a.go", "lib/b.go", 1},
	}
	for _, c := range cases {
		p, err := PlanWorkers(PlanInput{Task: "t", Items: []PlanItem{planItem("a", "g", c.a), planItem("b", "g", c.b)}})
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Waves) != c.waves {
			t.Errorf("%s vs %s: %d waves, want %d", c.a, c.b, len(p.Waves), c.waves)
		}
	}
}

func TestPlanRejectsBadInput(t *testing.T) {
	var thirteen []PlanItem
	for i := range 13 {
		thirteen = append(thirteen, planItem(fmt.Sprintf("w%d", i), "do it"))
	}
	cases := []struct {
		name string
		in   PlanInput
		want string
	}{
		{"cycle", PlanInput{Task: "t", Items: []PlanItem{
			{ID: "a", Goal: "g", After: []string{"b"}},
			{ID: "b", Goal: "g", After: []string{"a"}},
		}}, "a -> b -> a"},
		{"unknown after", PlanInput{Task: "t", Items: []PlanItem{{ID: "a", Goal: "g", After: []string{"zz"}}}}, `unknown item "zz"`},
		{"too many items", PlanInput{Task: "t", Items: thirteen}, "1 to 12 items"},
		{"duplicate id", PlanInput{Task: "t", Items: []PlanItem{planItem("a", "g"), planItem("a", "g")}}, `duplicate item id "a"`},
		{"bad id", PlanInput{Task: "t", Items: []PlanItem{planItem("Bad_ID", "g")}}, "lowercase"},
		{"empty task", PlanInput{Items: []PlanItem{planItem("a", "g")}}, "task is empty"},
	}
	for _, c := range cases {
		_, err := PlanWorkers(c.in)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want containing %q", c.name, err, c.want)
		}
	}
}

func TestPlanBriefsTargetHaikuAndOwnFiles(t *testing.T) {
	p, err := PlanWorkers(PlanInput{Task: "t", Repo: "ts-cos", Items: []PlanItem{
		planItem("a", "g", "internal/cos/a.go"),
		planItem("b", "g"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range p.Workers {
		if w.Target["model"] != "claude-haiku-5-5" || w.Target["providerInstanceId"] != "claudeAgent" {
			t.Errorf("%s target = %v", w.ID, w.Target)
		}
		if !strings.Contains(w.Brief, "Opus 5.5 advisor") {
			t.Errorf("%s brief missing advisor line: %s", w.ID, w.Brief)
		}
	}
	if !strings.Contains(p.Workers[0].Brief, "internal/cos/a.go") {
		t.Errorf("brief for a does not name its file: %s", p.Workers[0].Brief)
	}
	if !strings.Contains(p.Workers[1].Brief, "no files (read-only)") {
		t.Errorf("brief for b does not say read-only: %s", p.Workers[1].Brief)
	}
}

func TestPlanEffect(t *testing.T) {
	var r PlanResult
	in := `{"task":"t","workers":[{"id":"a","seconds":60,"tokens":100},{"id":"b","seconds":60,"tokens":100},{"id":"c","seconds":60,"tokens":100}],"wallSeconds":70,"advisorTokens":500}`
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	saved, speedup := r.Effect()
	if saved != 110 || speedup != 2.57 {
		t.Fatalf("effect = %d, %v; want 110, 2.57", saved, speedup)
	}
}

func TestPlanCommandRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := planCommand(context.Background(), nil, []string{`{"task":"t","items":[{"id":"a","goal":"do a"}]}`}, &buf); err != nil {
		t.Fatal(err)
	}
	var p Plan
	if err := json.Unmarshal(buf.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Workers) != 1 || p.ParallelMinutes != 10 {
		t.Fatalf("plan = %+v", p)
	}

	buf.Reset()
	done := `{"task":"t","workers":[{"id":"a","seconds":60,"tokens":100},{"id":"b","seconds":60,"tokens":100},{"id":"c","seconds":60,"tokens":100}],"wallSeconds":70,"advisorTokens":500}`
	if err := planCommand(context.Background(), nil, []string{"done", done}, &buf); err != nil {
		t.Fatal(err)
	}
	var d planDone
	if err := json.Unmarshal(buf.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d != (planDone{WallSaved: 110, Speedup: 2.57, WorkerTokens: 300, AdvisorTokens: 500}) {
		t.Fatalf("done = %+v", d)
	}
}

func TestPlanDoneRecordsSavings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	kv := memoryTestBuckets(t, ctx, savingsBucket)[0]
	done := `{"task":"t","workers":[{"id":"a","seconds":60,"tokens":100},{"id":"b","seconds":60,"tokens":100},{"id":"c","seconds":60,"tokens":100}],"wallSeconds":70,"advisorTokens":500}`
	if err := planCommand(ctx, kv, []string{"done", done}, io.Discard); err != nil {
		t.Fatal(err)
	}
	report, err := SavingsReport(ctx, kv, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	today := report[0]
	if c := today.Kinds[SavingParallel]; c.Events != 1 || c.Tokens != 0 || c.WallSeconds != 110 {
		t.Fatalf("parallel counter: %+v", c)
	}
	if today.Avoided != 0 || today.WallSeconds != 110 {
		t.Fatalf("today: %+v", today)
	}
}

func TestPlanCommandBadInput(t *testing.T) {
	for _, args := range [][]string{nil, {"{"}, {"done"}, {"done", "{"}, {"done", `{"task":"t","workers":[{"id":"a","seconds":60,"tokens":100}],"wallSeconds":0}`}} {
		if err := planCommand(context.Background(), nil, args, io.Discard); err == nil {
			t.Errorf("planCommand(%q) returned nil error", args)
		}
	}
}
