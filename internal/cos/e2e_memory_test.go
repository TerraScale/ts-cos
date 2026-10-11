package cos

import (
	"strings"
	"testing"
)

func TestMemoryProbeResult(t *testing.T) {
	tests := []struct {
		name    string
		out     string
		wantErr string
	}{
		{"ok", `{"entries":[{"id":"other"},{"id":"m-probe"}],"tokens":120,"fullTokens":900,"omitted":3}`, ""},
		{"ok with trailing newline", "{\"entries\":[{\"id\":\"m-probe\"}],\"tokens\":10,\"fullTokens\":10}\n", ""},
		{"probe missing", `{"entries":[{"id":"other"}],"tokens":10,"fullTokens":20}`, "probe m-probe missing from 1 recalled entries"},
		{"no entries", `{"entries":[],"tokens":0,"fullTokens":0}`, "probe m-probe missing from 0 recalled entries"},
		{"over cap", `{"entries":[{"id":"m-probe"}],"tokens":1501,"fullTokens":4000}`, "recall spends 1501 tokens, over the 1500 cap"},
		{"bad json", "cos: not json", "recall prints no JSON: cos: not json"},
		{"full below tokens", `{"entries":[{"id":"m-probe"}],"tokens":50,"fullTokens":40}`, "full tokens 40 below recalled tokens 50"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := memoryProbeResult(tc.out, "m-probe")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				if len(r.Entries) == 0 {
					t.Fatalf("entries = %v, want the parsed recall", r.Entries)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
