package cos

import "strings"

// e2ePending lists checks whose features are not on main yet. Each reports "skip" with its
// owning stream until the feature lands and the check gets a real implementation.
var e2ePending = func() []E2ECheck {
	rows := [][3]string{
		{"access.login.tailnet", "TS-810", "login page"},
		{"access.login.public", "TS-810", "cos.terrascale.tech and login page"},
		{"access.wrong-password", "TS-810", "login page"},
		{"access.lockout", "TS-810", "login rate limiting"},
		{"access.session-cross-node", "TS-810", "replicated sessions"},
		{"access.logout", "TS-810", "logout"},
		{"access.password-change", "TS-810", "password change with a temporary account"},
		{"chat.non-leader-message", "TS-811", "COS chat"},
		{"chat.reply-all-nodes", "TS-811", "COS chat streaming"},
		{"chat.spawn-haiku-thread", "TS-811", "COS agent runtime"},
		{"chat.focus-restart", "TS-811", "COS focus change"},
		{"chat.context-compaction", "TS-811", "COS context ownership"},
		{"chat.wake-budget-digest", "TS-811", "COS wake budget"},
		{"events.forgejo.valid", "TS-814", "Forgejo webhook ingest"},
		{"events.forgejo.bad-signature", "TS-814", "Forgejo webhook ingest"},
		{"events.forgejo.replay", "TS-814", "event dedup"},
		{"events.forgejo.out-of-order", "TS-814", "event ordering"},
		{"events.forgejo.foreign-author", "TS-814", "mariogk-only filter"},
		{"events.ku.assignment", "TS-814", "Ku webhook ingest"},
		{"events.t3.host-offline", "TS-814", "T3 live events"},
		{"rules.red-ci-wake", "TS-813", "rules engine"},
		{"rules.ready-merge-once", "TS-813", "rules engine merge"},
		{"rules.heartbeat-presets", "TS-813", "heartbeat presets"},
		{"rules.reconciler-dropped-event", "TS-813", "reconciler"},
		{"inbox.add-visible", "TS-815", "Inbox"},
		{"inbox.answer-closes", "TS-815", "Inbox answers"},
		{"inbox.concurrent-answer", "TS-815", "Inbox answers"},
		{"inbox.dedup", "TS-815", "Inbox dedup"},
		{"inbox.needs-user-forward", "TS-815", "NEEDS-USER to Inbox"},
		{"failure.t3-down", "TS-812", "T3 client host failure handling"},
		{"failure.forgejo-5xx", "TS-812", "Forgejo client failure handling"},
		{"failure.ku-down", "TS-812", "Ku client failure handling"},
		{"failure.t3-schema-change", "TS-812", "T3 contract check"},
		{"failure.nats-restart", "TS-810", "deployed cluster"},
		{"failure.sqlite-disk-full", "TS-810", "deployed cluster"},
		{"deploy.canary", "TS-818", "rolling canary deploy"},
		{"deploy.rollback", "TS-818", "automatic rollback"},
		{"deploy.watchdog", "TS-818", "last-known-good watchdog"},
		{"deploy.reconnect", "TS-818", "zero-downtime reconnect and browser reload"},
		{"gate.self-change", "TS-817", "self-change merge gate on full e2e"},
	}
	checks := make([]E2ECheck, len(rows))
	for i, r := range rows {
		checks[i] = E2ECheck{ID: r[0], Stream: r[1], Smoke: strings.HasPrefix(r[0], "access.login."), Run: pending(r[1] + ": " + r[2] + " is not on main yet")}
	}
	return checks
}()
