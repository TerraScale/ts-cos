package cos

import (
	"strings"
	"testing"
)

func TestT3SchemaVerdict(t *testing.T) {
	ok := []HostFleet{{Host: "mariogk-dev"}, {Host: "beta"}, {Host: "delta"}}
	tests := []struct {
		name    string
		fleet   []HostFleet
		wantErr string // empty: every host answers with the schema COS understands
	}{
		{"every host answers", ok, ""},
		{"schema changed on one host", []HostFleet{{Host: "beta"}, {Host: "delta", Error: "T3 schema changed on delta: /api/orchestration/shell: schemaVersion 3, COS understands 2"}, {Host: "mariogk-dev"}}, "T3 schema changed on delta"},
		{"other host error", []HostFleet{{Host: "beta", Error: "COS request failed; local node unavailable"}, {Host: "delta"}, {Host: "mariogk-dev"}}, "beta: COS request failed"},
		{"host missing from fleet", []HostFleet{{Host: "beta"}, {Host: "delta"}}, "fleet answers for [beta delta]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := t3SchemaVerdict(tc.fleet)
			if tc.wantErr == "" {
				if want := "T3 shell schema 2 on beta, delta, mariogk-dev"; err != nil || got != want {
					t.Fatalf("t3SchemaVerdict = %q, %v; want %q", got, err, want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("t3SchemaVerdict error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}
