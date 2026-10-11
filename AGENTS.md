# ts-cos

COS is one Go binary: a JSON CLI and a templ / Datastar dashboard styled by one vanilla CSS file (`internal/cos/static/cos.css`, no framework).
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
OCI Vault creates and delivers credentials automatically; never log tokens.
Re-bootstrap from live Ku issues assigned to mariogk@terrascale.tech, T3 hosts,
and PRs authored by mariogk. Never import the old backlog, ledger, or story.

TS-769 ships in four reviewed PRs: CLI + state + read-only dashboard + embedded
NATS/micro transport; dashboard commands + plugins; rollout; deterministic
autopilot + JetStream jobs/events + inbox/rules/flows/visibility. Do not implement
later slices early. COS uses its own NATS cluster, never production NATS.
Use T3 `delegate_task` for independent tests/docs or review before the first push.
Commits start `TS-769: `. Never merge or push main.

## Zero configuration (user decision 2026-10-09, do not revert)

- Every feature is always on. No feature flags, on/off variables, opt-in settings, boolean enabled configuration keys, deploy toggles or kill switches in code, config or CI. Delete switches and their guarded paths.
- Every region always serves. NetBird network membership admits a node automatically; it discovers every region, infers its own region from where it runs and starts serving without a per-region enable step.
- OCI Vault automatically creates, delivers and configures every first-party production secret. Vendor-issued keys (such as Stripe and Cloudflare) are stored once in Vault and picked up automatically; nobody copies secrets by hand.
- Values have one correct setting in code. Expose no tuning knobs. Fakes, demo stores and skip-auth belong only in test code and test binaries.
- CI rejects new switch-style configuration; ts-reviewer requests changes. Only unavoidable third-party names may have a narrow allowance with a one-line reason.

Discover every host and regional endpoint automatically; OCI Vault creates and delivers transport, dashboard and integration credentials. Review and deployment checks have no bypass switches.
