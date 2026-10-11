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
		{"events.forgejo.out-of-order", "TS-814", "event ordering"},
		{"events.t3.host-offline", "TS-814", "T3 live events"},
		{"failure.t3-down", "TS-812", "T3 client host failure handling"},
		{"failure.forgejo-5xx", "TS-812", "Forgejo client failure handling"},
		{"failure.sqlite-disk-full", "TS-810", "deployed cluster"},
	}
	checks := make([]E2ECheck, len(rows))
	for i, r := range rows {
		checks[i] = E2ECheck{ID: r[0], Stream: r[1], Run: pending(r[1] + ": " + r[2] + " is not on main yet")}
	}
	return checks
}()
