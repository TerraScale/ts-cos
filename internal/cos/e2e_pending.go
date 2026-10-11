package cos

// e2ePending lists checks whose features are not on main yet. Each reports "skip" with its
// owning stream until the feature lands and the check gets a real implementation.
var e2ePending = func() []E2ECheck {
	rows := [][3]string{
		{"chat.non-leader-message", "TS-811", "COS chat"},
		{"chat.reply-all-nodes", "TS-811", "COS chat streaming"},
		{"chat.spawn-thread", "TS-811", "COS agent runtime (probe threads run on Opus 5.5)"},
		{"chat.focus-restart", "TS-811", "COS focus change"},
		{"chat.context-compaction", "TS-811", "COS context ownership"},
		{"chat.wake-budget-digest", "TS-811", "COS wake budget"},
		{"events.forgejo.valid", "TS-814", "Forgejo webhook ingest"},
		{"events.forgejo.bad-signature", "TS-814", "Forgejo webhook ingest"},
		{"events.forgejo.replay", "TS-814", "event dedup"},
		{"events.forgejo.out-of-order", "TS-814", "event ordering"},
		{"events.forgejo.foreign-author", "TS-814", "mariogk-only filter"},
		{"events.t3.host-offline", "TS-814", "T3 live events"},
		{"rules.red-ci-wake", "TS-813", "rules engine"},
		{"rules.ready-merge-once", "TS-813", "rules engine merge"},
		{"rules.heartbeat-presets", "TS-813", "heartbeat presets"},
		{"rules.reconciler-dropped-event", "TS-813", "reconciler"},
		{"inbox.needs-user-forward", "TS-815", "NEEDS-USER to Inbox"},
		{"failure.t3-down", "TS-812", "T3 client host failure handling"},
		{"failure.forgejo-5xx", "TS-812", "Forgejo client failure handling"},
		{"failure.t3-schema-change", "TS-812", "T3 contract check"},
		{"failure.nats-restart", "TS-810", "deployed cluster"},
		{"failure.sqlite-disk-full", "TS-810", "deployed cluster"},
		{"deploy.canary", "TS-818", "rolling canary deploy"},
		{"deploy.rollback", "TS-818", "automatic rollback"},
	}
	checks := make([]E2ECheck, len(rows))
	for i, r := range rows {
		checks[i] = E2ECheck{ID: r[0], Stream: r[1], Run: pending(r[1] + ": " + r[2] + " is not on main yet")}
	}
	return checks
}()
