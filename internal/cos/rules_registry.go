package cos

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// RuleEnv is what a registered rule's custom action may use.
type RuleEnv struct {
	Bus     *Bus
	Publish func(context.Context, AutopilotEntry)
}

// Rule is a classifier another feature registers from its own file's init(). Event runs
// for every event on cos.events.> after the built-in rules; Beat runs on every leader
// heartbeat (budgets, thresholds). Either may be nil. Decisions are executed and logged
// like built-in ones (wake, merge, model, escalate) or through Decision.Do.
type Rule struct {
	Name  string
	Event func(e Event, live Live) []Decision
	Beat  func(ctx context.Context, live Live) []Decision
}

var registeredRules []Rule

// RegisterRule panics on an empty or duplicate name (programming error at init).
func RegisterRule(r Rule) {
	if r.Name == "" {
		panic("cos: rule without a name")
	}
	if slices.ContainsFunc(registeredRules, func(o Rule) bool { return o.Name == r.Name }) {
		panic("cos: duplicate rule " + r.Name)
	}
	registeredRules = append(registeredRules, r)
}

// Beat runs every registered Beat rule on the leader's heartbeat and executes its decisions.
// An unreadable paused set skips them all: a paused ticket must never be acted on.
func (r *RulesEngine) Beat(ctx context.Context, live Live) error {
	kv, err := r.settingsKV(ctx)
	if err != nil {
		return err
	}
	paused, err := pausedSet(ctx, kv)
	if err != nil {
		return err
	}
	for _, rule := range registeredRules {
		if rule.Beat == nil {
			continue
		}
		for _, d := range pauseGate(r.runRule(ctx, "beat", rule.Name, func() []Decision { return rule.Beat(ctx, live) }), live, paused) {
			if err := r.execute(ctx, "beat", d, nil, "", WakeMemory{}); err != nil {
				r.Publish(ctx, AutopilotEntry{Kind: "error", Event: "beat", Action: d.Rule, Reason: err.Error()})
			}
		}
	}
	return nil
}

// pauseGate turns registered decisions for a paused ticket into RECORDs: a pause takes no
// actions, built-in or registered. A decision without a ticket inherits its target thread's.
func pauseGate(ds []Decision, live Live, paused map[string]bool) []Decision {
	for i, d := range ds {
		ticket := strings.ToUpper(d.Ticket)
		if ticket == "" && d.Thread != "" {
			if t := findThread(live, d.Host, d.Thread); t != nil {
				ticket = strings.ToUpper(ticketPattern.FindString(t.Title + " " + t.Branch))
			}
		}
		if ticket != "" && paused[ticket] {
			ds[i] = Decision{Class: ClassRecord, Rule: d.Rule, Reason: d.Reason + "; " + ticket + " is paused", Host: d.Host, Thread: d.Thread, Ticket: ticket}
		}
	}
	return ds
}

// safeDo runs a registered rule's custom action; a panic becomes an error, so one broken rule cannot crash the leader.
func safeDo(ctx context.Context, do func(context.Context, RuleEnv) error, env RuleEnv) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("rule panicked: %v", p)
		}
	}()
	return do(ctx, env)
}

// runRule calls one registered rule and names its decisions "<rule>/<action>". A panic
// becomes an error entry, so one broken rule cannot stop the other rules or the heartbeat.
func (r *RulesEngine) runRule(ctx context.Context, eventID, name string, call func() []Decision) (out []Decision) {
	defer func() {
		if p := recover(); p != nil {
			out = nil
			r.Publish(ctx, AutopilotEntry{Kind: "error", Event: eventID, Action: name, Reason: fmt.Sprintf("rule panicked: %v", p)})
		}
	}()
	out = call()
	for i := range out {
		if !strings.HasPrefix(out[i].Rule, name+"/") {
			out[i].Rule = name + "/" + out[i].Rule
		}
	}
	return out
}
