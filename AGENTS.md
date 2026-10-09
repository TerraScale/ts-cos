# ts-cos

COS is one Go binary: a JSON CLI and a templ / Datastar / Pico.css dashboard.
Local SQLite (`~/.local/share/cos/cos.db`, modernc.org/sqlite, no cgo) is the
recorded exception to TerraScale's TerraBase storage rule (2026-10-09).

## Checks before every push

```sh
nice -n 19 go tool templ generate
nice -n 19 go build ./...
nice -n 19 go vet ./...
nice -n 19 go test ./...
```

Commit generated templ Go alongside its source. No JS/CSS build step.
Keep credentials in the existing COS / Forgejo config files; never log tokens.
Re-bootstrap from live Ku issues assigned to mariogk@terrascale.tech, T3 hosts,
and PRs authored by mariogk. Never import the old backlog, ledger, or story.

TS-769 ships in four reviewed PRs: CLI + state + read-only dashboard + embedded
NATS/micro transport; dashboard commands + plugins; rollout; deterministic
autopilot + JetStream jobs/events + inbox/rules/flows/visibility. Do not implement
later slices early. COS uses its own NATS cluster, never production NATS.
Use T3 `delegate_task` for independent tests/docs or review before the first push.
Commits start `TS-769: `. Never merge or push main.
