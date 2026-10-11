package cos

import (
	"context"
	"io"
	"strings"
	"testing"
)

// Empty Config: any request would fail on missing credentials, so each error must come from parsing.
func TestMergeCommandRejectsBadFlagsBeforeNetwork(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "unknown flag", args: []string{"test", "1", "--yolo", "--reason", "x"}, want: `unknown flag "--yolo"`},
		{name: "duplicate flag", args: []string{"test", "1", "--adopt", "--adopt", "--reason", "x"}, want: "duplicate --adopt"},
		{name: "missing reason value", args: []string{"test", "1", "--force", "--reason"}, want: "--reason needs a value"},
		{name: "force without reason", args: []string{"test", "1", "--force"}, want: "--reason"},
		{name: "reason without order", args: []string{"test", "1", "--reason", "x"}, want: "--reason needs --full-send"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := mergeCommand(context.Background(), Config{}, test.args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("mergeCommand(%v) error = %v, want %q", test.args, err, test.want)
			}
		})
	}
}
