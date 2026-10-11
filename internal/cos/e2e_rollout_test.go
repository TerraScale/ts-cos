package cos

import (
	"encoding/json"
	"testing"
)

// deployEv is a deploy event of rollout id as deployEvent writes it; Data carries the plan.
func deployEv(id string, kind EventKind, host string, hosts []string) Event {
	data, _ := json.Marshal(map[string]any{"rollout": id, "to": testSHA, "hosts": hosts})
	return Event{Source: SourceDeploy, Kind: kind, Host: host, Data: data}
}

// evOf is a test event: kind, plus the host for node.updated and gate.failed.
func evOf(kind EventKind, host string) Event { return Event{Kind: kind, Host: host} }

func TestDeployRuns(t *testing.T) {
	hosts := []string{"beta", "delta"}
	events := []Event{
		deployEv("r1", KindDeployStarted, "", hosts),
		{Kind: KindNodeUpdated, Data: json.RawMessage(`not json`)},
		deployEv("r1", KindNodeUpdated, "beta", hosts),
		deployEv("r1", KindDeployCompleted, "", hosts),
		{Kind: KindDeployStarted, Data: json.RawMessage(`{"to":"x"}`)},
		deployEv("r2", KindDeployStarted, "", hosts[:1]),
	}
	runs := deployRuns(events)
	if len(runs) != 2 {
		t.Fatalf("got %d runs, want 2", len(runs))
	}
	if runs[0].ID != "r1" || len(runs[0].Events) != 3 || runs[0].To != testSHA || len(runs[0].Hosts) != 2 {
		t.Errorf("run r1 = %+v", runs[0])
	}
	if runs[1].ID != "r2" || len(runs[1].Events) != 1 || runs[1].Hosts[0] != "beta" {
		t.Errorf("run r2 = %+v", runs[1])
	}
}

func TestCanaryProblems(t *testing.T) {
	hosts := []string{"beta", "delta", "mariogk-dev"}
	start, done := evOf(KindDeployStarted, ""), evOf(KindDeployCompleted, "")
	tests := []struct {
		name   string
		events []Event
		clean  bool
	}{
		{"clean 3-host run", []Event{start, evOf(KindNodeUpdated, "beta"), evOf(KindNodeUpdated, "delta"), evOf(KindNodeUpdated, "mariogk-dev"), done}, true},
		{"wrong order", []Event{start, evOf(KindNodeUpdated, "delta"), evOf(KindNodeUpdated, "beta"), evOf(KindNodeUpdated, "mariogk-dev"), done}, false},
		{"missing node.updated", []Event{start, evOf(KindNodeUpdated, "beta"), evOf(KindNodeUpdated, "mariogk-dev"), done}, false},
		{"gate failed", []Event{start, evOf(KindNodeUpdated, "beta"), evOf(KindGateFailed, "delta"), done}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := canaryProblems(deployRun{ID: "r1", To: testSHA, Hosts: hosts, Events: tc.events})
			if (len(got) == 0) != tc.clean {
				t.Errorf("problems = %v, clean want %t", got, tc.clean)
			}
		})
	}
}

func TestRollbackProblems(t *testing.T) {
	hosts := []string{"beta", "delta", "mariogk-dev"}
	start, rolled := evOf(KindDeployStarted, ""), evOf(KindDeployRolledBack, "")
	tests := []struct {
		name   string
		events []Event
		clean  bool
	}{
		{"failure on host 1, none updated", []Event{start, evOf(KindGateFailed, "beta"), rolled}, true},
		{"failure on host 2 after host 1", []Event{start, evOf(KindNodeUpdated, "beta"), evOf(KindGateFailed, "delta"), rolled}, true},
		{"node.updated after gate.failed", []Event{start, evOf(KindNodeUpdated, "beta"), evOf(KindGateFailed, "delta"), evOf(KindNodeUpdated, "mariogk-dev"), rolled}, false},
		{"gate.failed on the wrong host", []Event{start, evOf(KindGateFailed, "mariogk-dev"), rolled}, false},
		{"run ends in deploy.completed", []Event{start, evOf(KindNodeUpdated, "beta"), evOf(KindNodeUpdated, "delta"), evOf(KindNodeUpdated, "mariogk-dev"), evOf(KindDeployCompleted, "")}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rollbackProblems(deployRun{ID: "r1", To: testSHA, Hosts: hosts, Events: tc.events})
			if (len(got) == 0) != tc.clean {
				t.Errorf("problems = %v, clean want %t", got, tc.clean)
			}
		})
	}
}
