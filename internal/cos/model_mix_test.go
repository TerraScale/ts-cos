package cos

import (
	"encoding/json"
	"strings"
	"testing"

	"git.terrascale.tech/TerraScale/ts-cos/behavior"
)

// freshPolicy parses the embedded models.json anew, so a test can break its own copy.
func freshPolicy(t *testing.T) ModelPolicy {
	t.Helper()
	raw, err := behavior.Files.ReadFile("models.json")
	if err != nil {
		t.Fatal(err)
	}
	var p ModelPolicy
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestModelPolicyEmbeddedIsValid(t *testing.T) {
	raw, err := behavior.Files.ReadFile("models.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseModelPolicy(string(raw)); err != nil {
		t.Fatal(err)
	}
}

func TestModelPolicyRejectsBrokenCopies(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ModelPolicy)
		want   string
	}{
		{"route without pair", func(p *ModelPolicy) { delete(p.Route, "opus") }, "same pairs"},
		{"route text empty", func(p *ModelPolicy) { p.Route["sol"] = "  " }, "needs text"},
		{"unknown leader", func(p *ModelPolicy) { p.Leader = "haiku" }, "is not a pair"},
		{"worker-only advisor", func(p *ModelPolicy) {
			pair := p.Pairs["opus"]
			pair.Advisor.Model = "claude-haiku-5-5"
			p.Pairs["opus"] = pair
		}, "worker-only"},
		{"worker not worker-only", func(p *ModelPolicy) {
			pair := p.Pairs["opus"]
			pair.Worker.Model = "claude-sonnet-5-5"
			p.Pairs["opus"] = pair
		}, "listed in workerOnly"},
		{"codex advisor fast", func(p *ModelPolicy) { p.Pairs["sol"].Advisor.Options[1].Value = "fast" }, "serviceTier default"},
		{"codex worker fast", func(p *ModelPolicy) { p.Pairs["sol"].Worker.Options["serviceTier"] = "fast" }, "serviceTier default"},
		{"empty high-stakes effort", func(p *ModelPolicy) {
			pair := p.Pairs["opus"]
			pair.HighStakesEffort = ""
			p.Pairs["opus"] = pair
		}, "highStakesEffort"},
		{"bad priority key", func(p *ModelPolicy) { p.Priority = []string{"ts-819"} }, "not a ticket key"},
		{"duplicate priority key", func(p *ModelPolicy) { p.Priority = []string{"TS-819", "TS-819"} }, "listed twice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := freshPolicy(t)
			tt.mutate(&p)
			err := p.validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.HasPrefix(err.Error(), "behavior models.json: ") {
				t.Fatalf("got %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestModelPolicyAdvisor(t *testing.T) {
	p := freshPolicy(t)
	tests := []struct {
		pair       string
		highStakes bool
		want       string
	}{
		{"sol", false, `{"instanceId":"codex","model":"gpt-6.1-sol","options":[{"id":"reasoningEffort","value":"high"},{"id":"serviceTier","value":"default"}]}`},
		{"sol", true, `{"instanceId":"codex","model":"gpt-6.1-sol","options":[{"id":"reasoningEffort","value":"xhigh"},{"id":"serviceTier","value":"default"}]}`},
		{"opus", false, `{"instanceId":"claudeAgent","model":"claude-opus-5-5","options":[{"id":"effort","value":"high"}]}`},
		{"opus", true, `{"instanceId":"claudeAgent","model":"claude-opus-5-5","options":[{"id":"effort","value":"xhigh"}]}`},
	}
	for _, tt := range tests {
		if got := mustJSON(t, p.Advisor(tt.pair, tt.highStakes)); got != tt.want {
			t.Errorf("Advisor(%s, %v) = %s, want %s", tt.pair, tt.highStakes, got, tt.want)
		}
	}
	first := p.Advisor("sol", false)
	first["model"] = "changed"
	if p.Advisor("sol", false)["model"] != "gpt-6.1-sol" {
		t.Fatal("Advisor shares its map between calls")
	}
}

func TestModelPolicyWorkerTarget(t *testing.T) {
	p := freshPolicy(t)
	tests := []struct {
		pair string
		want string
	}{
		{"sol", `{"model":"gpt-6-luna","options":{"reasoningEffort":"medium","serviceTier":"default"},"providerInstanceId":"codex"}`},
		{"opus", `{"model":"claude-haiku-5-5","providerInstanceId":"claudeAgent"}`},
	}
	for _, tt := range tests {
		if got := mustJSON(t, p.WorkerTarget(tt.pair)); got != tt.want {
			t.Errorf("WorkerTarget(%s) = %s, want %s", tt.pair, got, tt.want)
		}
	}
}

func TestModelPolicyPairOfAndWorkerOnly(t *testing.T) {
	p := freshPolicy(t)
	pairs := []struct {
		model, pair string
		ok          bool
	}{
		{"claude-opus-5-5[1m]", "opus", true},
		{" GPT-6.1-SOL ", "sol", true},
		{"gpt-6-luna", "sol", true},
		{"claude-haiku-5-5", "opus", true},
		{"claude-sonnet-5-5", "", false},
	}
	for _, tt := range pairs {
		if pair, ok := p.PairOf(tt.model); pair != tt.pair || ok != tt.ok {
			t.Errorf("PairOf(%q) = %q, %v; want %q, %v", tt.model, pair, ok, tt.pair, tt.ok)
		}
	}
	workerOnly := map[string]bool{
		"claude-haiku-5-5": true, "claude-haiku-4-5": true, "claude-haiku-5-5-20251001": true, "gpt-6-luna": true,
		"gpt-6.1-sol": false, "claude-opus-5-5": false,
	}
	for model, want := range workerOnly {
		if got := p.WorkerOnlyModel(model); got != want {
			t.Errorf("WorkerOnlyModel(%q) = %v, want %v", model, got, want)
		}
	}
}

func TestModelPolicyLeader(t *testing.T) {
	if got := freshPolicy(t).Leader; got != "opus" {
		t.Fatalf("Leader = %q, want opus", got)
	}
}

func TestModelPolicyBriefLine(t *testing.T) {
	p := freshPolicy(t)
	want := `ADVISOR decides, WORKERS execute: you are the advisor on gpt-6.1-sol (high); your workers are gpt-6-luna via delegate_task target {"model":"gpt-6-luna","options":{"reasoningEffort":"medium","serviceTier":"default"},"providerInstanceId":"codex"}. Never run a worker model as a thread.`
	if got := p.BriefLine("sol"); got != want {
		t.Fatalf("BriefLine(sol) =\n%s\nwant\n%s", got, want)
	}
}

func TestModelPolicyPairFor(t *testing.T) {
	p := freshPolicy(t)
	routes := `state the pair: "pair": "opus" (` + p.Route["opus"] + `) or "pair": "sol" (` + p.Route["sol"] + `)`
	tests := []struct {
		name  string
		input map[string]any
		pair  string
		err   string // refusal text; empty means accepted
	}{
		{"pair given", map[string]any{"pair": "opus"}, "opus", ""},
		{"modelSelection given", map[string]any{"modelSelection": map[string]any{"model": "gpt-6.1-sol"}}, "sol", ""},
		{"neither names a pair", map[string]any{"message": "hi"}, "", routes},
		{"unknown pair", map[string]any{"pair": "haiku"}, "", routes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pair, err := p.PairFor(tt.input)
			if tt.err != "" {
				if err == nil || err.Error() != tt.err {
					t.Fatalf("err = %v, want %q", err, tt.err)
				}
			} else if err != nil || pair != tt.pair {
				t.Fatalf("PairFor = %q, %v; want %q", pair, err, tt.pair)
			}
			if _, ok := tt.input["pair"]; ok {
				t.Fatal("the pair key is still sent to T3")
			}
		})
	}
}

func TestLaunchInputSetsModelAndBrief(t *testing.T) {
	p := freshPolicy(t)
	tests := []struct {
		name    string
		input   map[string]any
		model   string // model the launch ends on
		brief   string // pair whose brief line the message ends with; empty means none
		message string // message before the brief
	}{
		{"pair key takes its advisor", map[string]any{"pair": "opus", "message": "hi"}, "claude-opus-5-5", "opus", "hi"},
		{"named model gets the brief", map[string]any{"message": "hi", "modelSelection": map[string]any{"model": "gpt-6.1-sol"}}, "gpt-6.1-sol", "sol", "hi"},
		{"brief not repeated", map[string]any{"message": "ADVISOR decides already", "modelSelection": map[string]any{"model": "gpt-6.1-sol"}}, "gpt-6.1-sol", "", "ADVISOR decides already"},
		{"empty message gets only the brief", map[string]any{"pair": "sol"}, "gpt-6.1-sol", "sol", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := launchInput(p, tt.input)
			if err != nil {
				t.Fatal(err)
			}
			sel, _ := got["modelSelection"].(map[string]any)
			if sel["model"] != tt.model {
				t.Fatalf("model = %v, want %s", sel["model"], tt.model)
			}
			want := tt.message
			if tt.brief != "" {
				if want != "" {
					want += "\n\n"
				}
				want += p.BriefLine(tt.brief)
			}
			if got["message"] != want {
				t.Fatalf("message = %q, want %q", got["message"], want)
			}
		})
	}
}

func TestLaunchInputRefusesUnnamedPair(t *testing.T) {
	if _, err := launchInput(freshPolicy(t), map[string]any{"message": "hi"}); err == nil || !strings.Contains(err.Error(), `state the pair`) {
		t.Fatalf("err = %v, want a state-the-pair refusal", err)
	}
}
