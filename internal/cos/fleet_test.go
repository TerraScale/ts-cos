package cos

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestParseAsks(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []Ask
	}{
		{
			name: "plain markers and trimmed options",
			text: "NEEDS-USER: Choose a host?\nOPTIONS: local | remote ||  \nWHY: Keep the work near its data.",
			want: []Ask{{Question: "Choose a host?", Options: []string{"local", "remote"}, Why: "Keep the work near its data."}},
		},
		{
			name: "bold label with colon outside",
			text: "**NEEDS-USER**: Ship now?\n**OPTIONS**: yes | wait\n**WHY**: Tests passed.",
			want: []Ask{{Question: "Ship now?", Options: []string{"yes", "wait"}, Why: "Tests passed."}},
		},
		{
			name: "bold label with colon inside and bullet",
			text: "  - **NEEDS-USER:** Ship now?  \n  - **OPTIONS:** yes | wait\n  - **WHY:** Tests passed.  ",
			want: []Ask{{Question: "Ship now?", Options: []string{"yes", "wait"}, Why: "Tests passed."}},
		},
		{
			name: "asterisk bullets and latest options replace earlier ones",
			text: "* NEEDS-USER: Which model?\n* OPTIONS: fast | precise\n* OPTIONS: cheap\n* WHY: Latency matters.",
			want: []Ask{{Question: "Which model?", Options: []string{"cheap"}, Why: "Latency matters."}},
		},
		{
			name: "options and why belong to the latest question",
			text: "OPTIONS: orphan\nWHY: orphan\nNEEDS-USER: First?\nOPTIONS: a | b\nWHY: first reason\nNEEDS-USER: Second?\nOPTIONS: c | d\nWHY: second reason",
			want: []Ask{
				{Question: "First?", Options: []string{"a", "b"}, Why: "first reason"},
				{Question: "Second?", Options: []string{"c", "d"}, Why: "second reason"},
			},
		},
		{
			name: "question without optional fields",
			text: "NEEDS-USER: Confirm?",
			want: []Ask{{Question: "Confirm?", Options: []string{}}},
		},
		{
			name: "prose and orphan metadata are ignored",
			text: "This mentions NEEDS-USER: without asking.\nOPTIONS: orphan\nWHY: orphan\nneeds-user: lowercase prose",
			want: []Ask{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseAsks(tt.text); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseAsks() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestContextOf(t *testing.T) {
	tests := []struct {
		name string
		json string
		want ContextUsage
	}{
		{
			name: "used then maximum",
			json: `{"providerTurns":[{"providerInstanceId":"codex","usage":{"usedTokens":125,"maxTokens":1000}}]}`,
			want: ContextUsage{Provider: "codex", UsedTokens: 125, MaxTokens: 1000, Pct: 13},
		},
		{
			name: "maximum then used",
			json: `{"providerTurns":[{"usage":{"maxTokens":1000,"usedTokens":125},"providerInstanceId":"codex"}]}`,
			want: ContextUsage{Provider: "codex", UsedTokens: 125, MaxTokens: 1000, Pct: 13},
		},
		{
			name: "latest provider turn wins",
			json: `{"providerTurns":[{"providerInstanceId":"old","usage":{"usedTokens":900,"maxTokens":1000}},{"providerInstanceId":"new","response":{"contextUsage":{"maxTokens":2000,"usedTokens":400}}}]}`,
			want: ContextUsage{Provider: "new", UsedTokens: 400, MaxTokens: 2000, Pct: 20},
		},
		{
			name: "latest turn without usage does not erase known usage",
			json: `{"providerTurns":[{"providerInstanceId":"codex","data":{"usage":{"usedTokens":40,"maxTokens":100}}},{"providerInstanceId":"new","result":{"other":1}}]}`,
			want: ContextUsage{Provider: "codex", UsedTokens: 40, MaxTokens: 100, Pct: 40},
		},
		{
			name: "native driver and latest nested usage",
			json: `{"providerTurns":[{"providerInstanceId":"custom-instance","nativeTurnRef":{"driver":"claude"},"result":[{"usage":{"usedTokens":10,"maxTokens":100}},{"usage":{"maxTokens":100,"usedTokens":80}}]}]}`,
			want: ContextUsage{Provider: "claude", UsedTokens: 80, MaxTokens: 100, Pct: 80},
		},
		{
			name: "zero maximum retains count without dividing",
			json: `{"providerTurns":[{"providerInstanceId":"codex","usage":{"maxTokens":0,"usedTokens":42}}]}`,
			want: ContextUsage{Provider: "codex", UsedTokens: 42, MaxTokens: 0, Pct: 0},
		},
		{
			name: "absent usage",
			json: `{"providerTurns":[{"providerInstanceId":"codex"}]}`,
			want: ContextUsage{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p Projection
			if err := json.Unmarshal([]byte(tt.json), &p); err != nil {
				t.Fatal(err)
			}
			if got := ContextOf(p); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ContextOf() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
