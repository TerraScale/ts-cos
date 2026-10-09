# ts-cos

Build one `cos` binary for TerraScale's Chief of Staff:

```sh
nice -n 19 go tool templ generate
nice -n 19 go build -o cos .
nice -n 19 go vet ./...
nice -n 19 go test ./...
```

Go 1.27 (latest stable) with pure-Go SQLite. HTML is generated with the pinned
templ tool; Datastar and Pico.css load from pinned CDNs. No JS/CSS build step,
Node runtime, or cgo. Commit both `.templ` and generated `_templ.go` files.

## Run a host

Each host runs `cos serve --port 8790`. It embeds a dedicated COS NATS server
with JetStream and exposes local T3 operations as NATS micro services. The page
binds `0.0.0.0:8790`. This slice is read-only in the browser.

1. Copy [examples/nats.conf](examples/nats.conf) to `~/.config/cos/nats.conf`.
2. Set the node name, its Tailscale route listener, and shared client/route
   credentials for the COS cluster. Run `chmod 600 ~/.config/cos/nats.conf`.
3. Start `cos serve` on delta, beta, and mariogk-dev.
4. Open `http://delta:8790` over Tailscale. Sign in as `cos` with the password
   in that host's `~/.config/cos/dashboard-token`.

The dashboard token is generated on first startup and stored with mode 0600.
Every HTTP route, including `/status` and `/events`, requires it. Connections
are limited to loopback and Tailscale addresses; forwarded headers do not grant
access. The HTTP listener remains `0.0.0.0:8790` for the required host rollout.

The CLI connects to **its local** NATS node. Calls to `cos.<host>.<command>` route
through that cluster; remote T3 HTTP connections and SSH tunnels are not used.
Client cancellation is sent to the remote operation; it cannot undo a mutation
already dispatched. Unconfirmed remote cancellation is reported explicitly.
`nats micro ls` discovers each host service when the NATS CLI has matching
credentials. `fleet` scatters `shell` requests and sorts healthy hosts by load;
an unavailable node is reported without blocking healthy nodes.

`nats.conf` uses standard NATS server syntax. JetStream is enabled and its
storage defaults to `~/.local/share/cos/nats`. The cluster name must be `COS`
or `COS-*`, with authenticated routes between COS nodes over Tailscale. Client
authentication supports one plaintext user/password stored in this private file,
or authorized user nkeys with a private `~/.config/cos/nats.seed` (also 0600).
COS rejects anonymous access, production cluster names, and gateway/leaf bridges.
Its NATS cluster is independent of production ts-orchestrator NATS.

Credentials stay in existing files:

| Source | Path |
|---|---|
| Local T3 | `~/.config/cos/tokens/<local hostname>` |
| Ku MCP | `~/.config/cos/ku-token` |
| Forgejo | `~/.local/share/forgejo-cli/keys.json`, host `git.terrascale.tech` |
| COS transport | `~/.config/cos/nats.conf` (and `nats.seed` for nkeys) |
| Dashboard | `~/.config/cos/dashboard-token` (generated, 0600) |

## Commands

Output is JSON by default. Errors are JSON on stderr and return a nonzero exit
code. `cos help` prints the full command list. Host arguments are delta, beta,
or mariogk-dev; thread IDs are the full T3 IDs.

| Command | Purpose |
|---|---|
| `cos fleet` / `cos shell <host>` | Open threads and active load |
| `cos prs` | Every open PR authored by mariogk, with CI/review/merge gates |
| `cos status [--brief]` | Refresh all sources; `--brief` is at most 40 lines |
| `cos projects <host>` | T3 project IDs, titles, and roots |
| `cos launch <host> '<json>'` | Same launch fields as the old CLI; `projectTitle` can replace `projectId` |
| `cos thread <host> <id>` / `cos ctx <host> <id>` | Thread summary or context usage |
| `cos send <host> <id> '<text>' [steer]` | Queue behind an active run, or steer it |
| `cos handoff <host> <id> '<modelSelection>' ['<brief>']` | Wait for idle, compact if needed, check half the target context window, switch, send |
| `cos asks` / `cos asks-host <host>` | Unanswered NEEDS-USER markers and pending runtime requests |
| `cos respond <host> <id> <requestId> '<answers>'` | Answer a T3 runtime request |
| `cos fork <host> <id> ['<title>']` | Fork from the latest stable point |
| `cos watch <host> <id> <prUrl>` | Link and watch a PR |
| `cos model <host> <id> '<modelSelection>'` | Change the next-run model |
| `cos rename <host> <id> '<title>'` | Update the title |
| `cos settle <host> <id>` / `cos archive <host> <id>` | Settle finished work or archive dropped work |
| `cos close <host> <id>` / `cos delete <host> <id>` | Settle a subtree; delete also removes it. Active threads are skipped |
| `cos tidy <host>` | Settle inactive orphaned subagents/forks |
| `cos dispatch <host> '<command json>'` | Send another T3 orchestration command |
| `cos ku <tool> ['<arguments json>']` | Call a Ku MCP tool |
| `cos merge <repo> <number>` | Squash a ready COS PR at the checked head SHA |

Merge requires a mariogk-authored open PR, a `cos/` or `cos-hold/` branch,
mergeability, successful checks, reviewer approval, and no requested changes.
`COS_ADOPTED=1` permits an explicitly adopted non-COS branch. `COS_FULL_SEND=1`
bypasses client-side ts-reviewer status/approval only: at least one other successful check,
mergeability, and no other reviewer's requested changes still apply. Waiving a
security, data-loss, correctness, or contract finding remains a COS judgment
before invoking full send. The existing `.cos/skip-reviewer` marker preserves
the temporary user bypass; removing it restores the gate on the next request.
Every merge uses `force_merge=false`, including Full Send and reviewer bypasses.
Forgejo's server-side branch protections still apply; a server rejection is
reported without a forced retry.

## Story and live data

State uses `~/.local/share/cos/cos.db` with WAL and a 5-second busy timeout.
SQLite is the user-approved exception to TerraScale's TerraBase storage rule.
There is no import of the old backlog, ledger, or `state.json`.

On startup and every 60 seconds, the daemon derives work from **only** Ku issues
assigned to mariogk@terrascale.tech (shared assignments count), T3 open threads
on every COS host, and mariogk-authored Forgejo PRs. Completed/cancelled/archived
issues and settled/archived/deleted threads are excluded. FULL SEND work sorts
first. Source failures are visible and preserve the last successful snapshot.
`task list` reads that snapshot; `status` refreshes it first.

Only annotations and story are edited by hand:

```sh
cos task set '{"ticket":"TS-769","why":"Remove status drift","modelWhy":"Go implementation","next":"Review slice 1"}'
cos task list
cos task rm TS-769
cos note direction 'Ship the COS CLI and live dashboard.'
cos note need 'TS-769: choose a deployment window.'
cos note decide 'SQLite is the recorded storage exception.'
cos note missing 'Live deployment verification is pending.'
cos note suggest 'Run autopilot after the rollout.'
cos note need --rm 2
cos note missing --clear
```

`task set` merges `why`, `modelWhy`, and `next` annotations. It rejects manual
status, host, model, priority, outcome, repo, and PR fields; these belong to live
sources. `task rm` removes annotations, not a Ku issue. When a previously open
issue disappears after a successful Ku refresh, its annotations are removed.
Direction replaces the current direction; other notes append and return an ID.

The dashboard shows direction, needs you (including live worker questions),
decisions, priorities with reasons/model/PR/next, missing work, suggestions,
open PRs, and all open threads grouped by host. Datastar SSE patches HTML;
SQLite/WAL and post-commit notifications push story edits from another process without
waiting for the 60-second source refresh. No completed-work history is shown.

## TS-769 slices

1. **This PR:** CLI parity, derived SQLite state, read-only live dashboard,
   embedded authenticated NATS/JetStream, and cross-host micro transport.
2. Dashboard actions with merge/close confirmations; goja plugins, hot reload,
   isolated COS API, and throwing-plugin/reload tests.
3. Host installation and systemd replacement; update the umbrella setup/COS
   skill and delete the old CLI/dashboard/backlog/ledger usage in place.
4. Deterministic autopilot, replicated JetStream work queue/events, idempotency,
   retry/dead-letter, inbox/answers, rules/routing, new/full-send/deployed flows,
   audit/cost/doctor, optional ntfy. The queue and events use **3 replicas**;
   their three-node tests cover retry/dead-letter and event fan-out. Jobs for an
   offline host wait in its queue. No production NATS dependency.

The current cluster test covers three embedded nodes, authenticated micro RPC
across routes, discovery, replicated JetStream, and event fan-out. Autopilot,
durable action delivery, and rollout are not active in slice 1. Each slice gets
its own PR; only the last slice closes TS-769. COS merges; workers never merge.
