package cos

// e2ePending lists checks whose features are not on main yet. Each reports "skip" with its
// owning stream until the feature lands and the check gets a real implementation.
var e2ePending = func() []E2ECheck {
	rows := [][3]string{
		{"events.forgejo.out-of-order", "TS-814", "event ordering"},
		{"events.t3.host-offline", "TS-814", "T3 live events"},
	}
	checks := make([]E2ECheck, len(rows))
	for i, r := range rows {
		checks[i] = E2ECheck{ID: r[0], Stream: r[1], Run: pending(r[1] + ": " + r[2] + " is not on main yet")}
	}
	return checks
}()
