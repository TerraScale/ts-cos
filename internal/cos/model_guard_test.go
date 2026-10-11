package cos

import (
	"context"
	"strings"
	"testing"
)

func TestThreadModelGuard(t *testing.T) {
	tests := []struct {
		name    string
		command string
		args    []string
		reject  string // substring of the refusal; empty means accepted
	}{
		{"launch haiku 5.5", "launch", []string{`{"modelSelection":{"instanceId":"claudeAgent","model":"claude-haiku-5-5"}}`}, "worker-only model"},
		{"launch luna", "launch", []string{`{"modelSelection":{"instanceId":"codex","model":"gpt-6-luna"}}`}, "worker-only model"},
		{"model switch to haiku 4.5", "model", []string{"t1", `{"instanceId":"claudeAgent","model":"claude-haiku-4-5"}`}, "worker-only model"},
		{"handoff to uppercase haiku", "handoff", []string{"t1", `{"model":"CLAUDE-HAIKU-5-5"}`, "brief"}, "worker-only model"},
		{"dispatch thread.create haiku", "dispatch", []string{`{"type":"thread.create","modelSelection":{"model":"claude-haiku-5-5"}}`}, "worker-only model"},
		{"dispatch nested haiku", "dispatch", []string{`{"type":"x","items":[{"modelSelection":{"model":"claude-haiku-5-5"}}]}`}, "worker-only model"},
		{"launch capitalized Model key", "launch", []string{`{"modelSelection":{"Model":"claude-haiku-5-5"}}`}, "worker-only model"},
		{"launch model list haiku", "launch", []string{`{"modelSelection":{"model":["claude-haiku-5-5"]}}`}, "worker-only model"},
		{"launch model object haiku", "launch", []string{`{"modelSelection":{"model":{"id":"Claude-Haiku-5-5"}}}`}, "worker-only model"},
		{"launch sol standard tier", "launch", []string{`{"modelSelection":{"instanceId":"codex","model":"gpt-6.1-sol","options":[{"id":"reasoningEffort","value":"high"},{"id":"serviceTier","value":"default"}]}}`}, ""},
		{"launch sol fast tier", "launch", []string{`{"modelSelection":{"instanceId":"codex","model":"gpt-6.1-sol","options":[{"id":"serviceTier","value":"fast"}]}}`}, "Standard tier"},
		{"launch fast tier key", "launch", []string{`{"modelSelection":{"model":"gpt-6.1-sol","serviceTier":"fast"}}`}, "Standard tier"},
		{"launch opus 5.5 high", "launch", []string{`{"modelSelection":{"instanceId":"claudeAgent","model":"claude-opus-5-5","options":[{"id":"effort","value":"high"}]}}`}, ""},
		{"model switch to opus 5.5", "model", []string{"t1", `{"model":"claude-opus-5-5"}`}, ""},
		{"send mentions haiku subagents", "send", []string{"t1", "use haiku subagents"}, ""},
		{"dispatch archive", "dispatch", []string{`{"type":"thread.archive","threadId":"t1"}`}, ""},
		{"launch invalid json left to command", "launch", []string{`{`}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := threadModelGuard(tt.command, tt.args)
			if tt.reject != "" {
				if err == nil || !strings.Contains(err.Error(), tt.reject) {
					t.Fatalf("got %v, want refusal containing %q", err, tt.reject)
				}
				return
			}
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}

func TestLocalCommandRejectsWorkerOnlyThreadBeforeT3(t *testing.T) {
	_, err := LocalCommand(context.Background(), Config{}, "launch", []string{`{"modelSelection":{"model":"claude-haiku-5-5"}}`})
	if err == nil || !strings.Contains(err.Error(), "worker-only model") {
		t.Fatalf("got %v, want a worker-only refusal", err)
	}
}
