// Package checks holds the acceptance checks of COS self-changes, one JSON file per
// ticket. COS commits a check that fails on main before a worker starts; CI runs
// every check against the PR build and COS reruns it on all nodes after rollout.
package checks

import "embed"

//go:embed *.json
var Files embed.FS
