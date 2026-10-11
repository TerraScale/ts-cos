package cos

import (
	"slices"
	"time"
)

// agentMaxWakes keeps the newest wakes in the replicated state.
const agentMaxWakes = 100

// WakeRecord is one COS wake, with its token use.
type WakeRecord struct {
	At          time.Time `json:"at"`
	Reason      string    `json:"reason"` // chat, digest, focus, failover, or the restart reason
	Items       int       `json:"items"`
	Run         string    `json:"run,omitempty"`
	Input       int       `json:"input"`
	CachedInput int       `json:"cachedInput"`
	CacheWrite  int       `json:"cacheWrite"`
	Output      int       `json:"output"`
}

// recordWake appends a wake and keeps the newest agentMaxWakes.
func (st *AgentState) recordWake(w WakeRecord) {
	st.Wakes = append(st.Wakes, w)
	if len(st.Wakes) > agentMaxWakes {
		st.Wakes = st.Wakes[len(st.Wakes)-agentMaxWakes:]
	}
}

// recentWakes returns the last n wakes, newest first.
func (st AgentState) recentWakes(n int) []WakeRecord {
	out := append([]WakeRecord{}, st.Wakes[max(0, len(st.Wakes)-n):]...)
	slices.Reverse(out)
	return out
}

// attachRun names the run of the newest wake that does not have one yet.
func (st *AgentState) attachRun(run string) {
	for i := range slices.Backward(st.Wakes) {
		if st.Wakes[i].Run == "" {
			st.Wakes[i].Run = run
			return
		}
	}
}

// meterWakes fills in the tokens of each wake whose run has finished.
func meterWakes(st *AgentState, p agentProjection) {
	for _, r := range p.Runs {
		if active(r.Status) {
			continue
		}
		for i := range st.Wakes {
			w := &st.Wakes[i]
			if w.Run != r.ID || w.Output > 0 {
				continue
			}
			w.Input, w.CachedInput, w.CacheWrite, w.Output = p.runUsage(r.RootNodeID)
		}
	}
}

// runUsage sums the token use of the provider turns under one run's root node.
func (p agentProjection) runUsage(root string) (input, cached, write, output int) {
	for _, turn := range p.ProviderTurns {
		if node, _ := turn["nodeId"].(string); root == "" || node != root {
			continue
		}
		usage, _ := turn["turnTokenUsage"].(map[string]any)
		input += tokenCount(usage, "inputTokens")
		cached += tokenCount(usage, "cachedInputTokens")
		write += tokenCount(usage, "cacheCreationTokens")
		output += tokenCount(usage, "outputTokens")
	}
	return input, cached, write, output
}

func tokenCount(usage map[string]any, key string) int {
	n, _ := usage[key].(float64)
	return int(n)
}
