package cos

import (
	"encoding/json"
	"regexp"
	"strings"
)

// toolCategory names what one advisor tool call is for; "" means delegate_task, which is not counted.
func toolCategory(name string, input json.RawMessage) string {
	switch {
	case strings.HasSuffix(name, "delegate_task"):
		return ""
	case name == "Read" || name == "Grep" || name == "Glob":
		return "read"
	case name == "Bash":
		var in struct {
			Command string `json:"command"`
		}
		_ = json.Unmarshal(input, &in)
		return shellCategory(in.Command)
	}
	return "other"
}

// shellCategories are checked in order; the first match wins.
var shellCategories = []struct {
	name string
	re   *regexp.Regexp
}{
	{"build", regexp.MustCompile(`\bgo (build|vet|test)\b|templ generate|\bmake\b|\bnpm (run|test)\b`)},
	{"ci", regexp.MustCompile(`\bfj\b|actions/(runs|jobs)|gate\.sh`)},
	{"git", regexp.MustCompile(`\bgit (rebase|push|fetch|pull|merge|cherry-pick|commit|stash|worktree)\b`)},
	{"gitread", regexp.MustCompile(`\bgit (log|show|diff|status|branch|grep|ls-remote|rev-parse)\b`)},
	{"live", regexp.MustCompile(`\b(ssh|scp|systemctl|systemd-run|journalctl|curl)\b|\bcos (thread|send|launch|shell|fleet|dispatch|e2e)\b`)},
	{"read", regexp.MustCompile(`(^|[;&|]\s*)(cat|sed|head|tail|grep|rg|ls|find|wc)\b`)},
}

// shellCategory names what a Bash command is for, so advisor work can be split by kind.
func shellCategory(cmd string) string {
	for _, c := range shellCategories {
		if c.re.MatchString(cmd) {
			return c.name
		}
	}
	return "other"
}
