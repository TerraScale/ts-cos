package cos

import (
	"context"
	"testing"
)

func TestThreadModelGuard(t *testing.T) {
	tests := []struct {
		name    string
		command string
		args    []string
		reject  bool
	}{
		{"launch haiku 5.5", "launch", []string{`{"modelSelection":{"instanceId":"claudeAgent","model":"claude-haiku-5-5"}}`}, true},
		{"model switch to haiku 4.5", "model", []string{"t1", `{"instanceId":"claudeAgent","model":"claude-haiku-4-5"}`}, true},
		{"handoff to uppercase haiku", "handoff", []string{"t1", `{"model":"CLAUDE-HAIKU-5-5"}`, "brief"}, true},
		{"dispatch thread.create haiku", "dispatch", []string{`{"type":"thread.create","modelSelection":{"model":"claude-haiku-5-5"}}`}, true},
		{"dispatch nested haiku", "dispatch", []string{`{"type":"x","items":[{"modelSelection":{"model":"claude-haiku-5-5"}}]}`}, true},
		{"launch capitalized Model key", "launch", []string{`{"modelSelection":{"Model":"claude-haiku-5-5"}}`}, true},
		{"launch model list haiku", "launch", []string{`{"modelSelection":{"model":["claude-haiku-5-5"]}}`}, true},
		{"launch model object haiku", "launch", []string{`{"modelSelection":{"model":{"id":"Claude-Haiku-5-5"}}}`}, true},
		{"launch opus 5.5 high", "launch", []string{`{"modelSelection":{"instanceId":"claudeAgent","model":"claude-opus-5-5","options":[{"id":"effort","value":"high"}]}}`}, false},
		{"model switch to opus 5.5", "model", []string{"t1", `{"model":"claude-opus-5-5"}`}, false},
		{"send mentions haiku subagents", "send", []string{"t1", "use haiku subagents"}, false},
		{"dispatch archive", "dispatch", []string{`{"type":"thread.archive","threadId":"t1"}`}, false},
		{"launch invalid json left to command", "launch", []string{`{`}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := threadModelGuard(tt.command, tt.args)
			if tt.reject {
				if err == nil || err.Error() != haikuThreadError {
					t.Fatalf("got %v, want %q", err, haikuThreadError)
				}
				return
			}
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}

func TestLocalCommandRejectsHaikuThreadBeforeT3(t *testing.T) {
	_, err := LocalCommand(context.Background(), Config{}, "launch", []string{`{"modelSelection":{"model":"claude-haiku-5-5"}}`})
	if err == nil || err.Error() != haikuThreadError {
		t.Fatalf("got %v, want %q", err, haikuThreadError)
	}
}
