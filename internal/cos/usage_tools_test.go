package cos

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestShellCategory(t *testing.T) {
	cases := map[string]string{
		"cd x && nice -n 19 go test ./...": "build",
		"make check":                       "build",
		"npm run lint":                     "build",
		"fj pr status 19":                  "ci",
		"bash .cos/gate.sh":                "ci",
		"git rebase origin/main":           "git",
		"git log -3":                       "gitread",
		"ssh beta uptime":                  "live",
		"cos thread send x":                "live",
		"cat go.mod":                       "read",
		"ls -la && sed -n 1,5p a.go":       "read",
		"echo hi":                          "other",
		"":                                 "other",
	}
	for cmd, want := range cases {
		if got := shellCategory(cmd); got != want {
			t.Errorf("shellCategory(%q) = %q, want %q", cmd, got, want)
		}
	}
}

func TestToolCategory(t *testing.T) {
	bash := []byte(`{"command":"go vet ./..."}`)
	cases := []struct {
		name  string
		input []byte
		want  string
	}{
		{"Read", nil, "read"},
		{"Grep", nil, "read"},
		{"Glob", nil, "read"},
		{"Bash", bash, "build"},
		{"Edit", nil, "other"},
		{"mcp__t3-code__delegate_task", nil, ""},
	}
	for _, tt := range cases {
		if got := toolCategory(tt.name, tt.input); got != tt.want {
			t.Errorf("toolCategory(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

// Advisor tool calls count once per block; Haiku workers and delegate_task calls are not advisor work.
func TestScanTranscriptCountsAdvisorTools(t *testing.T) {
	line := func(id, model, block string) string {
		return `{"type":"assistant","timestamp":"2026-10-10T09:00:00.000Z","sessionId":"s","message":{"id":"` + id + `","model":"` + model +
			`","content":[` + block + `],"usage":{"input_tokens":1,"output_tokens":5}}}` + "\n"
	}
	bash := `{"type":"tool_use","id":"tu1","name":"Bash","input":{"command":"go test ./..."}}`
	read := `{"type":"tool_use","id":"tu2","name":"Read","input":{}}`
	delegate := `{"type":"tool_use","id":"tu3","name":"mcp__t3-code__delegate_task","input":{}}`
	path := filepath.Join(t.TempDir(), "s.jsonl")
	body := line("m1", "claude-opus-5-5", bash) + line("m1", "claude-opus-5-5", read) + line("m1", "claude-opus-5-5", bash) +
		line("m1", "claude-opus-5-5", delegate) + line("m2", "claude-haiku-5-5", bash)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, err := scanTranscript(path, "p", "beta", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	byModel := map[string]UsageRow{}
	for _, r := range rows {
		byModel[r.Model] = r
	}
	if got := byModel["claude-opus-5-5"].Tools; got["build"] != 1 || got["read"] != 1 || len(got) != 2 {
		t.Errorf("advisor tools = %v, want build 1 and read 1 (the repeated block and delegate_task are not counted)", got)
	}
	if got := byModel["claude-haiku-5-5"].Tools; got != nil {
		t.Errorf("haiku worker tools = %v, want none", got)
	}
}
