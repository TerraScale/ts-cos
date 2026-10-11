package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"slices"
	"strings"
	"testing"
)

// withPriority makes p the live model policy for the test, and restores the embedded one after.
func withPriority(t *testing.T, p ModelPolicy) {
	t.Helper()
	behaviors.cur.Store(&Behavior{Models: p})
	t.Cleanup(func() { behaviors.cur.Store(nil) })
}

func TestApplyModelsPatch(t *testing.T) {
	tests := []struct {
		name  string
		patch string
		check func(t *testing.T, p ModelPolicy)
		err   string
	}{
		{name: "replace priority", patch: `{"priority":["TS-819","TS-813"]}`, check: func(t *testing.T, p ModelPolicy) {
			if strings.Join(p.Priority, ",") != "TS-819,TS-813" {
				t.Fatalf("priority = %v", p.Priority)
			}
		}},
		{name: "edit route text", patch: `{"route":{"sol":"routine work only"}}`, check: func(t *testing.T, p ModelPolicy) {
			if p.Route["sol"] != "routine work only" || p.Route["opus"] == "" {
				t.Fatalf("route = %v", p.Route)
			}
		}},
		{name: "change sol advisor model", patch: `{"pairs":{"sol":{"advisor":{"model":"gpt-6.2-sol"}}}}`, check: func(t *testing.T, p ModelPolicy) {
			if p.Pairs["sol"].Advisor.Model != "gpt-6.2-sol" || p.Pairs["sol"].Advisor.InstanceID != "codex" {
				t.Fatalf("sol advisor = %+v", p.Pairs["sol"].Advisor)
			}
		}},
		{name: "null deletes priority", patch: `{"priority":null}`, check: func(t *testing.T, p ModelPolicy) {
			if len(p.Priority) != 0 {
				t.Fatalf("priority = %v", p.Priority)
			}
		}},
		{name: "upper-case priority keys", patch: `{"priority":["ts-819"]}`, check: func(t *testing.T, p ModelPolicy) {
			if strings.Join(p.Priority, ",") != "TS-819" {
				t.Fatalf("priority = %v", p.Priority)
			}
		}},
		{name: "unknown field", patch: `{"bogus":1}`, err: "unknown field"},
		{name: "worker-only advisor", patch: `{"pairs":{"opus":{"advisor":{"model":"claude-haiku-5-5"}}}}`, err: "worker-only"},
		{name: "no-op", patch: `{"leader":"opus"}`, err: "changes nothing"},
		{name: "not an object", patch: `[1]`, err: "must be a JSON object"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next, file, err := applyModelsPatch(freshPolicy(t), []byte(tc.patch))
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasSuffix(file, []byte("}\n")) {
				t.Fatalf("file lacks trailing newline: %q", file[len(file)-5:])
			}
			tc.check(t, next)
		})
	}
}

func TestApplyModelsPatchWritesModelsJSONShape(t *testing.T) {
	_, file, err := applyModelsPatch(freshPolicy(t), []byte(`{"priority":["TS-819"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(file), `"id": "effort"`) || !strings.Contains(string(file), `"value": "high"`) {
		t.Fatalf("options not written as id/value:\n%s", file)
	}
	if _, err := parseModelPolicy(string(file)); err != nil {
		t.Fatalf("written file does not parse: %v", err)
	}
}

func TestModelsSummaryEmbedded(t *testing.T) {
	want := "leader opus · opus claude-opus-5-5 (high) + claude-haiku-5-5 · sol gpt-6.1-sol (high) + gpt-6-luna · priority (none)"
	if got := modelsSummary(freshPolicy(t)); got != want {
		t.Fatalf("summary = %q", got)
	}
	p := freshPolicy(t)
	p.Priority = []string{"TS-819", "TS-813"}
	if got := modelsSummary(p); !strings.HasSuffix(got, " · priority TS-819, TS-813") {
		t.Fatalf("summary with priority = %q", got)
	}
}

func TestIsP0(t *testing.T) {
	if !IsP0("TS-1", "FULL SEND: anything") || IsP0("TS-1", "plain title") {
		t.Fatal("FULL SEND title must be P0, a plain title must not")
	}
	p := freshPolicy(t)
	p.Priority = []string{"TS-819", "TS-813"}
	withPriority(t, p)
	if !IsP0("TS-819", "plain") || IsP0("TS-813", "plain") || !IsP0("ts-819", "plain") {
		t.Fatal("only priority[0] is P0 on its own")
	}
}

func TestCompareTasksPriorityList(t *testing.T) {
	tasks := []Task{
		{Ticket: "TS-1", Priority: "P2"},
		{Ticket: "TS-5", Priority: "P2"},
		{Ticket: "TS-9", Priority: "P2"},
		{Ticket: "TS-7", Priority: "FULL SEND"},
	}
	priority := []string{"TS-9", "TS-5"}
	slices.SortStableFunc(tasks, func(a, b Task) int { return compareTasks(a, b, priority) })
	var got []string
	for _, task := range tasks {
		got = append(got, task.Ticket)
	}
	if strings.Join(got, " ") != "TS-7 TS-9 TS-5 TS-1" {
		t.Fatalf("order = %v", got)
	}
}

func TestModelsFormPatchOnlyChangedFields(t *testing.T) {
	cur := freshPolicy(t)
	form := url.Values{
		"priority":     {"TS-819, TS-813"},
		"route_opus":   {cur.Route["opus"]},
		"route_sol":    {cur.Route["sol"]},
		"advisor_opus": {cur.Pairs["opus"].Advisor.Model},
		"worker_opus":  {cur.Pairs["opus"].Worker.Model},
		"advisor_sol":  {"gpt-6.2-sol"},
		"worker_sol":   {cur.Pairs["sol"].Worker.Model},
		"leader":       {"opus"},
	}
	raw, err := json.Marshal(modelsFormPatch(cur, form))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"pairs":{"sol":{"advisor":{"model":"gpt-6.2-sol"}}},"priority":["TS-819","TS-813"]}`
	if string(raw) != want {
		t.Fatalf("patch = %s, want %s", raw, want)
	}
	if _, _, err := applyModelsPatch(cur, raw); err != nil {
		t.Fatal(err)
	}
}

func TestModelsCommandRejectsBadJSON(t *testing.T) {
	var buf bytes.Buffer
	err := modelsCommand(context.Background(), Config{}, []string{"set", "{bad"}, &buf)
	if err == nil || !strings.Contains(err.Error(), "must be a JSON object") {
		t.Fatalf("err = %v", err)
	}
	if err := modelsCommand(context.Background(), Config{}, []string{"set", "[1]"}, &buf); err == nil {
		t.Fatal("array patch accepted")
	}
	if err := modelsCommand(context.Background(), Config{}, []string{"nope"}, &buf); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("err = %v", err)
	}
}

func TestModelsHashChangesOnRouteOnlyEdit(t *testing.T) {
	cur := freshPolicy(t)
	next, _, err := applyModelsPatch(cur, []byte(`{"route":{"sol":"routine work only"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if modelsHash(cur) == modelsHash(next) || len(modelsHash(cur)) != 8 {
		t.Fatalf("hash %s vs %s", modelsHash(cur), modelsHash(next))
	}
}
