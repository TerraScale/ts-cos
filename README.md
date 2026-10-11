# ts-cos

Build one `cos` binary for TerraScale's Chief of Staff:

```sh
nice -n 19 go tool templ generate
nice -n 19 go build -o cos .
nice -n 19 go vet ./...
nice -n 19 go test ./...
```

Go 1.27 (latest stable) with pure-Go SQLite. HTML is generated with the pinned
templ tool; Datastar loads from a pinned CDN. The one hand-written stylesheet,
`internal/cos/static/cos.css`, is embedded and served at a content-hashed URL
with long cache headers. No JS/CSS build step, Node runtime, or cgo. Commit both `.templ` and generated `_templ.go` files.

## Run a host

Every COS host (delta, beta, mariogk-dev) runs the same user service with no
configuration file:

```sh
deploy/install.sh   # bootstraps this clean checkout as the last-known-good build and (re)starts cos.service
```

`deploy/cos.service` runs `cos serve` (dashboard on `0.0.0.0:8790`), restarts on
any exit and starts at boot (user lingering is on for mariogk on all hosts).
`install.sh` is only for bootstrapping a host; rollouts replace builds afterwards.

On start a node:

1. Reads the `cos-master` secret from the dedicated COS OCI Vault with the host's
   `~/.oci/config` API key. The first node to boot creates it with random
   content. NATS route and client passwords are derived from it (HKDF), so every
   node agrees without copying secrets.
2. Embeds NATS with JetStream: clients on `127.0.0.1:7790`, routes on the
   host's Tailscale IPv4 port `7791`, cluster name `COS`, storage in
   `~/.local/share/cos/nats`.
3. Discovers peers every 15 seconds from the Tailscale LocalAPI: online Linux
   devices of the same tailnet owner. Membership is admission. A peer gets the
   route password only after its listener answers with a COS route `INFO`.
   NATS gossip completes the mesh and reconnects restarted peers, which catch
   up from JetStream.
4. Writes the local client credential to `/run/user/<uid>/cos/nats.json`
   (tmpfs, 0600). CLI commands read it; they need the local service running.

The dedicated vault and key (compartment `TerraScale`, `sa-saopaulo-1`) are fixed
in `internal/cos/vault.go`. Production cell vaults are never used. COS uses its
own NATS cluster, never production NATS.

### Leader

Exactly one node runs singleton work (heartbeat, agent driver) through
`Bus.Lead`. Leadership is a three-replica JetStream KV lease (`cos_leader`) with
a 15-second TTL refreshed every 5 seconds. A stopping leader releases it at
once; a crashed or partitioned leader steps down when a refresh fails, and
another node takes over within about 20 seconds. Two of three nodes keep the
cluster working. `cos cluster` prints the local host, the leader and the build
revision; every node also advertises its revision in its micro metadata.

### Nodes

Every node samples its host (CPU, memory, load, disks, network, temperatures), its COS
process and its local T3 every 10 seconds and publishes the sample to JetStream. Three
replicated streams keep the samples: `COS_NODES_RAW` (6 hours), `COS_NODES_1M` (7 days)
and `COS_NODES_15M` (90 days). Each node averages its own samples into 1-minute and
15-minute buckets in memory; a bucket still open when that node restarts is lost.
The leader turns a disk over 85 %, memory over 90 % and a host with no sample for 60 s into `node.alert` events. The rules engine files each as an Inbox FYI, deduplicated: disk per mount per day, memory per day, missing once per outage.
Synthetic e2e hosts raise info-level alerts that never reach web push.

`/nodes` shows a live card per node with current values and last-hour sparklines.

### Sign-in and public URL

`https://cos.terrascale.tech` and `http://<host>:8790` on the tailnet serve the
same dashboard from any node. Cloudflare Tunnel `cos`
(`88533e25-445e-4353-9739-2e4a5509fab6`, CNAME record
`ccb97b18ca5ab1fc0a9f409c5983be96`) routes the hostname to
`http://localhost:8790`; every node runs its own `cloudflared` connector with the
token from Vault secret `cos-tunnel`, so Cloudflare fails over between hosts and
a stopped node stops receiving traffic. The connector token was stored once in
Vault; nobody copies it.

Only `GET /login` is public. Users, sessions and lockouts are three-replica
JetStream KV (`cos_auth`, `cos_sessions`), so a session or password change from
one node works on every node at once. On first start each node seeds
`~/.config/cos/admin.json` (`{"user","bcrypt"}`, 0600) unless the user already
exists; the file is never rewritten and the hash is never logged.

- Sessions: random 32-byte token, `HttpOnly`, `SameSite=Lax`, `Secure` over
  HTTPS, 7 days; logout ends the session on every node.
- `/password` (current + new, 12 to 72 characters) replaces the bcrypt hash
  everywhere and signs out the user's other sessions.
- Five consecutive failures lock a user for 15 minutes on every node, even with
  the right password. Each node allows 10 attempts per client address per
  minute (`Cf-Connecting-Ip` from the local connector).
- Cross-site form posts are rejected; pages are `no-store` and cannot be framed.
  Requests from outside loopback and Tailscale are refused.

### Webhook ingress

Forgejo POSTs to `https://cos.terrascale.tech/hooks/forgejo` (also served on
every tailnet URL), outside the login:

- Forgejo signs with `X-Forgejo-Signature` (hex HMAC-SHA256 of the body). The
  secret is derived from the Vault master on every node (`Bus.HookSecret(source)`);
  TS-814 registers it with the sender. It is never printed.
- A verified body is stored unchanged on `cos.ingest.forgejo`
  (stream `COS_INGEST`, created on start) with the request headers (except
  `Cookie` and `Authorization`) and `Nats-Msg-Id` = Forgejo's delivery UUID or
  the body's SHA-256, so redeliveries are stored once. The sender gets `202`
  only after a JetStream quorum stored it, else `503`.
- Unsigned or wrongly signed: `401`. Bodies over 1 MiB: `413`. More than 300
  requests per minute from one address on one node: `429`.

Every other HTTP route, including `/status` and `/events`, requires a session.

### Builds and the watchdog (TS-818)

Each host keeps its builds in `~/.local/share/cos/builds/<revision>/cos` (the
newest three plus whatever is in use). `current` is the build systemd starts,
`good` the last-known-good one, and `~/.local/bin/cos` points at `current`.
`cos version` (and `cos.<host>.release.version` over NATS) reports the revision,
commit and build time, the last-known-good revision and whether the running
build is still on trial.

A rollout installs a build on one host at a time through
`cos.<host>.release.install`: the host fetches it from the replicated `cos_builds`
object store (digest checked), runs `cos version` on it, points `current` at it,
writes `trial.next` and restarts. `cos.service` turns `trial.next` into `trial`
for that start. The unit is `Type=notify` with a 60-second start timeout: a
build on trial sends `READY=1` only once it is a NATS cluster member, JetStream
is in sync, `/login` answers 200 and, signed in with a short-lived `cos-probe`
session over loopback, its dashboard renders and its SSE stream opens. Any stop
while `trial` exists (crash, no `READY` within 60 seconds, kill, rollback) makes
`ExecStopPost`, which is plain `sh`, point `current` back at `good` before
systemd restarts it. A trial build gets exactly one start: if `ExecStopPost`
never ran (the whole unit was SIGKILLed, the host lost power), `ExecStartPre`
reverts before starting. The watchdog
therefore needs neither the new binary nor the cluster. The rollout confirms a
healthy build with `release.commit` (it becomes `good`); an unconfirmed build
stops itself after 10 minutes, which reverts it. `release.restore` puts a host
back on a given build. A host never activates a build its watchdog reverted; only a different build clears that.

### Rolling deploys (TS-818)

The leader checks main every minute. When the newest main commit that main's CI
has not found red (merged commits already passed the merge gate; it does not wait
for main's push CI, which queues for minutes during a merge train; never past the
build the cluster runs) and some host does not run it, and every host answers healthy with none
on trial, it builds that commit once (git checkout in `~/.local/share/cos/src`,
`go build -trimpath`, build time stamped) into the three-replica `cos_builds`
object store (newest three kept) and rolls it out one host at a time:
non-leaders by name, then the leader. Before acting on itself the leader calls
`Bus.Resign`; an updated node takes over and finishes. Each host must pass the
gate within two minutes: it runs the new revision, is a cluster member with
JetStream in sync, its dashboard and SSE work, its COS operations service
is registered from the new build, and TS-816's smoke checks for that host
(`E2ESmoke`: unit, version, error-free log) pass. Then the host commits the build as
last-known-good and the next one starts. A failed gate (or the watchdog
reverting the host) restores every updated host to its previous build, last
updated first, adds an Inbox FYI ("Deploy rolled back: <change>", source
`deploy`) and records `failed.<revision>`. A gate that timed out (often a busy
cluster, not a bad build) is retried after 5 and then 15 minutes, and each
`deploy.rolled_back` event says when; a build the watchdog reverted, one that
does not build or start, or one that failed three times is only retried by
`cos deploy` or replaced by a newer main build. The Inbox FYI is sent when no
retry is left.

The rollout record is `rollout` in the three-replica `cos_deploy` KV, written
with compare-and-set: a resumed leader continues it, and a second rollout is
refused while one is active.

```sh
cos deploy            # roll out main now (same checks; refused while a rollout runs)
cos deploy <branch>   # roll out a branch or commit, e.g. to prove a rollback
cos deploy status     # the current or last rollout
cos deploy why        # what the leader's last main check found and why it is (not) rolling
```

Main rolls out once it has been unchanged for 3 minutes, and when main moves during a rollout the rollout stops after the current host and the newest build rolls out instead.

Every step is an event on `cos.events.deploy.>`: `deploy.started`,
`node.updated`, `gate.failed`, `deploy.rolled_back`, `deploy.completed`, with
`{"rollout","to","change","hosts","from","reason"}` data.

### Zero-downtime updates in the browser (TS-818)

A node registers its Cloudflare connector only after it passes the same signed-in
dashboard probe as the gate, so `cos.terrascale.tech` never sends users to a node
that is still starting. Long-lived SSE streams open with `sseGet(path)`
(`{retry: 'always', retryMaxCount: 1000}`): Datastar reconnects after a clean
stream end or an error page too, so an open tab follows a restarting node to
another one. Every page body includes `@releaseWatch()`: it subscribes to
`/release`, which streams the cluster release (`release` in `cos_deploy`, set only
when a rollout completes), and reloads the tab once when it changes, then shows
"Updated: <change>". The change text is the deployed commit's `Update-Note:`
trailer, else its subject without the ticket key and PR number. `GET /version`
returns this node's build and the cluster release.

The CLI connects to **its local** NATS node. Calls to `cos.<host>.<command>` route
through that cluster; remote T3 HTTP connections and SSH tunnels are not used.
Client cancellation is sent to the remote operation; it cannot undo a mutation
already dispatched. Unconfirmed remote cancellation is reported explicitly.
`fleet` scatters `shell` requests and sorts healthy hosts by load; an
unavailable node is reported without blocking healthy nodes.

Legacy credential locations below are implementation debt; OCI Vault must deliver credentials automatically:

| Source | Path |
|---|---|
| Local T3 | `~/.config/cos/tokens/<local hostname>` |
| Forgejo | `~/.local/share/forgejo-cli/keys.json`, host `git.terrascale.tech` |

## Event stream

Every source publishes normalized events (Go type `Event` in `internal/cos/events.go`)
to the replicated JetStream stream `COS_EVENTS` on `cos.events.<source>.<kind>`. The
stream has 3 replicas, keeps 7 days, and drops duplicates by event ID for 24 hours.
The rules engine and the Inbox consume it.

| Source | Kinds |
|---|---|
| `t3` | turn.completed, turn.failed, turn.interrupted, request.pending, context.high, worker.needs_user |
| `forgejo` | pr.opened, pr.synchronized, pr.mergeable, pr.conflict, pr.merged, pr.closed, ci.status, review.submitted, push.main |
| `user` | chat.message, user.answer |
| `inbox` | inbox.item_added, inbox.item_closed |

Each node ingests only its local T3 shell stream. The `forgejo` source covers only PRs
authored by mariogk, CI, and pushes on main.

Forgejo webhooks reach the stream in two steps. The cluster leader registers the
TerraScale org hook and the `Tools/ku` repository hook to
`https://cos.terrascale.tech/hooks/forgejo`, signed with a secret every node derives
from the Vault master. Deliveries land on `COS_INGEST` (`cos.ingest.forgejo`, work
queue), and one durable consumer normalizes each once on whichever node pulls it.
Mergeability is read from the API 15 seconds after a PR update or a push to main.

`cos events [<subject filter>] [<count>]` prints recent events as JSON from any node.

### Install and notifications

Install COS, then turn notifications on in Settings (`/settings`).

- **Android Chrome:** menu > Install app.
- **iOS Safari:** Share > Add to Home Screen. Then open COS from the Home Screen and turn notifications on. iOS sends pushes only to Home Screen apps.
- **Desktop Chrome or Firefox:** install icon in the address bar.

Four categories push, each switchable in Settings and on by default:

- **Inbox items:** approvals, questions, decisions and blockers.
- **COS replies:** each final reply, when it lands (replies older than 10 minutes do not push).
- **Finished and deployed work:** `deploy.completed` and rollbacks.
- **Node alerts:** warn or urgent node events.

"Send test" pushes one notice to your own devices; test notices ignore the category switches.

The VAPID key is generated once and kept in the replicated `cos_push` bucket, so every node signs with the same key. Subscriptions are accepted only for real push services (FCM, Apple, Mozilla, Windows); any other endpoint gets HTTP 400.

## Commands

Output is JSON by default. Errors are JSON on stderr and return a nonzero exit
code. `cos help` prints the full command list. Host arguments are delta, beta,
or mariogk-dev; thread IDs are the full T3 IDs.

| Command | Purpose |
|---|---|
| `cos fleet` / `cos shell <host>` | Open threads and active load |
| `cos cluster` | Local host, current leader and build revision |
| `cos nodes [<host> [1h\|24h\|7d]]` | Latest sample of every node, or one host's samples for the window (default 1h) |
| `cos prs` | Every open PR authored by mariogk in any unarchived repo (TerraScale, ...), with CI/review/merge gates; `repo` is `name` in TerraScale, `owner/name` elsewhere |
| `cos status [--brief]` | Live ticket threads on every host with their PRs (ci/bot/merge/READY), last reply, open NEEDS-USER, and `orphanPrs` (cos.mjs shape); `--brief` refreshes the dashboard state and prints at most 40 lines |
| `cos context list\|get\|set\|rm` | Shared context files for the COS agent |
| `cos agent [restart '<reason>']` | COS thread state, or a fresh brief at the next wake |
| `cos projects <host>` | T3 project IDs, titles, and roots |
| `cos launch <host> '<json>'` | Same launch fields as the old CLI; `projectTitle` can replace `projectId` |
| `cos thread <host> <id> [n]` / `cos ctx <host> <id>` | Thread summary (plus the last `n` replies in full) or context usage |
| `cos send <host> <id> '<text>' [queue]` | Steer the running run (default), or queue behind it |
| `cos compact <host> <id> ['<focus>']` | Wait for idle, `/compact` with focus, return context before/after |
| `cos stream <host> [<id>]` | Live thread (or host thread-list) events as JSON lines, from any node |
| `cos handoff <host> <id> '<modelSelection>' ['<brief>']` | Wait for idle, compact if needed, check half the target context window, switch, send |
| `cos asks` / `cos asks-host <host>` | Unanswered NEEDS-USER markers and pending runtime requests |
| `cos inbox add '<json>'` / `list` / `history` | File an Inbox item; list open items or recent closed ones |
| `cos inbox answer <id> '<text>'` / `dismiss <id>` | Answer an Inbox item (option or text), or dismiss it |
| `cos respond <host> <id> <requestId> '<answers>'` | Answer a T3 runtime request |
| `cos fork <host> <id> ['<title>']` | Fork from the latest stable point |
| `cos watch <host> <id> <prUrl>` | Link and watch a PR |
| `cos model <host> <id> '<modelSelection>'` | Change the next-run model |
| `cos rename <host> <id> '<title>'` | Update the title |
| `cos settle <host> <id>` / `cos archive <host> <id>` | Settle finished work or archive dropped work |
| `cos close <host> <id>` / `cos delete <host> <id>` | Settle a subtree; delete also removes it. Active threads are skipped |
| `cos tidy <host>` | Settle inactive orphaned subagents/forks |
| `cos inventory <host>` | Every non-deleted thread, compact, for cleanup planning |
| `cos purge <host> ['<keep csv>']` | Delete settled top-level threads and their subtrees, except ids containing a kept substring; trees with an active run are skipped |
| `cos purge-children <host>` | Delete finished subagent/fork threads, deepest first; children of active threads stay |
| `cos stop <host> ['<keep csv>']` | Interrupt every running run and cancel queued ones, except kept threads |
| `cos dispatch <host> '<command json>'` | Send another T3 orchestration command |
| `cos merge <repo> <number> [--full-send\|--force] [--adopt] --reason '<user order>'` | Squash a ready COS PR at the checked head SHA; `<repo>` is `name` (TerraScale) or `owner/name`. Flags are recorded orders past the gate (see Merge) |
| `cos version` | Git revision the running binary was built from |
| `cos e2e [--smoke] [prefix...]` | Live checks against the deployed cluster (see below) |

### T3 client (internal/cos/t3*.go)

`T3` talks to the local T3 server (`127.0.0.1:3773`, WS RPC protocol 2). Other nodes reach it
through NATS: request/reply commands on `cos.<host>.<command>` and live streams on
`cos.<host>.stream`.

- `SubscribeThread(ctx, id, fn)` / `SubscribeShell(ctx, fn)`: live `ThreadEvent` / `ShellEvent`
  on this host. The first event is a snapshot; resubscribe after an error. Thread events carry
  T3's event `ID` and `Sequence` plus decoded `Run` (turn state), `Message`, `Request` (pending
  questions/approvals), `Item` (question text) and `Context` (used/max tokens).
- `Bus.StreamThread(ctx, host, id, fn)` / `Bus.StreamShell(ctx, host, fn)`: the same events from
  any host. An offline host is a clear `COS node <host> unavailable` error.
- `Launch`, `Send(…, SendSteer|SendQueue)`, `Interrupt`, `Compact`, `WaitIdle`, `WaitMessage`,
  `CloseTree`, `Projection(…).PendingRequests()`, `ContextOf`.
- T3's RPC is internal. Decoding checks the fields COS relies on and fails with
  `T3SchemaError` ("T3 schema changed on <host>: …") instead of returning empty data.
  `testdata/t3` holds recorded real responses; re-record them after a T3 upgrade.

Merge requires a mariogk-authored open PR, a `cos/` or `cos-hold/` branch,
mergeability, successful checks, reviewer approval, and no requested changes.
User orders go past the gate on one command line: `--full-send` waives only
ts-reviewer, `--adopt` allows a non-COS branch, `--force` needs only mergeability
and overrides branch protection. `--full-send` and `--force` cannot be combined;
`--adopt` combines with either. Every non-normal merge needs `--reason '<user order>'`
(one line, at most 200 characters). Each one is recorded as a `merge.order` event
and as a system chat entry. A normal merge records nothing.

## Live e2e

`cos e2e` checks the deployed cluster from any COS host and prints one JSON report:
every check has `status` (`pass`, `fail`, or `skip` while its feature is not on main),
`evidence` (redacted, clipped) and its owning `stream`. Any failure exits nonzero.
It finishes within 5 minutes, cleans up after itself and never touches real work.

- `deploy.*`: `cos.service` active, running `cos version` equals origin/main
  (15-minute deploy window), no unattributed automatic restarts in 25 minutes, no
  error or panic lines since the process started (at most 17 minutes, which leaves
  out the previous run's own chaos).
- Chaos is attributable: each perturbation first writes a record to the `cos_e2e`
  KV bucket (node, check, action, time) and a `cos-e2e` journal line on the target.
  `deploy.restarts` counts only automatic restarts with no matching `sigkill` record
  (allowance 0); deploy installs are reported, not counted.
- `--smoke` never runs chaos. COS runs `--smoke` every 10 minutes and the full
  suite hourly.
- `cluster.*`: all three hosts answer micro PING; every JetStream stream is R3 with
  a leader and current replicas; every node's `cos cluster` names the same leader.
  A non-leader is restarted and must catch up within 60 s. The leader (or another
  non-runner host) is partitioned: iptables on every peer address except ssh, with
  a root timer that heals it after 120 s even if the runner dies. The majority
  must elect a new leader within 30 s, and after healing all nodes agree on one
  leader. Finally, the leader is SIGKILLed and the survivors must agree on a new
  one within 30 s.
- `rules.heartbeat-leader`: the autopilot heartbeat runs on the cluster leader.
- `rules.heartbeat-presets` (TS-813): the Settings page renders every heartbeat preset and
  the stored interval is one of them. It reads the page and never sets a preset.
- `failure.t3-schema-change` (TS-812): every host answers the T3 shell in a schema COS
  understands. A changed schema fails the check, naming the host.
- `deploy.watchdog.<host>` (TS-818): `cos.service` is the build-store unit (notify, 60 s start
  timeout, trial revert in ExecStartPre and ExecStopPost), `current` and `good` point at
  `builds/<40-hex>`, idle means no trial and no rollout, `cos version` reports protocol 1
  and rollout 2 or later, and at most three builds in total. A rollout in progress skips.
- `deploy.reconnect` (TS-818): on each node `/version` reports the host, protocol and one
  shared revision, `/release` streams its `_release` signal (or `: release unknown`), and
  the dashboard renders `releaseWatch`. It skips until `/version` is served.
- `nodes.collector-stop` and `nodes.collector-missing`: a synthetic host publishes two
  samples and stops; the leader must raise a missing `node.alert` for it within 100 s.
  Its samples are purged afterwards.
- `--smoke` runs the fast health-gate subset (under 25 s). The rollout gate calls
  `E2ESmoke(ctx, cfg, host, revision)`: smoke checks for that host plus cluster-wide
  ones, with `deploy.version` expecting the rolled-out revision.
- `access.*`, `deploy.*` and `gate.*` are protected: a self-change may never break
  or remove them (`protected: true` in the report).
- Per-request acceptance checks register with `RegisterE2E` from an
  `e2e_accept_*.go` init function, IDs `accept.<slug>.<name>`.
- Full runs wait up to 5 min for the cluster to settle (no rollout, one build, every
  `/login` 200). If it never settles they report only `cluster.settled`; checks that
  fail during a mid-run rollout are retried once after it settles.

## Behavior files

`behavior/` holds what COS says and how often it wakes: `brief.md` and `rules.md`
(the agent brief), `wake.json` (wake budget) and `templates/*.tmpl` (chat lines
such as `Updated: ...`). They ship in the binary as a fallback. The JetStream
meta leader copies `behavior/` from main into the replicated `COS_BEHAVIOR` KV
bucket within 10 seconds of a merge; every node hot-reloads it without a deploy.
A bundle that fails validation (unknown file, bad JSON, out-of-range budget,
template that does not render) is never published and is rejected on load; the
node keeps its previous behavior. `cos behavior` shows the published version,
any rejection and the version each node runs.

## Self-change loop

When the user asks the COS chat to fix or change COS itself, the agent runs
`cos self change '<json>'` with the user's words, a one-line summary and an
acceptance check: a dashboard path or a behavior template, plus text that must
appear or disappear. The cluster leader then drives it with no further input:

1. Key `SELF-<n>` from a counter in `COS_SELF`, branch `cos/self-<n>` whose first
   commit is `checks/SELF-<n>.json`; the check must fail on all 3 live nodes.
2. Opus 5.5 worker thread (effort high; Haiku 5.5 only as its delegate_task
   subagents) on the least busy host with a brief rendered from `behavior/templates/selfbrief.tmpl`.
3. Gate: every CI context green (CI runs every `checks/*.json` against the PR
   build); ts-reviewer must finish but only security/data-loss findings block.
   PRs that touch login, deploy/rollback, CI, e2e, the self-change code or
   another check are refused ("needs you").
4. Squash merge with an `Update-Note:` trailer, one rollout at a time; behavior-only
   changes are live once every node loads the new bundle, code waits for the
   TS-818 rollout event.
5. The check runs on every node; on success the chat shows
   `Updated: <summary> (<sha>)` and the worker thread closes.

`cos self undo [ticket]` reverts the newest (or named) live change through the
same gate and posts `Reverted: ...` once the check fails again everywhere.
`cos self log [all]` lists today's (or every) change with ticket, PR, commit and
verified time. Records live in the replicated `COS_SELF` KV bucket.

## Story and live data

State uses `~/.local/share/cos/cos.db` with WAL and a 5-second busy timeout.
SQLite is the user-approved exception to TerraScale's TerraBase storage rule.
There is no import of the old backlog, ledger, or `state.json`.

On startup and every 60 seconds, the daemon derives work from **only** T3 open
threads on every COS host and mariogk-authored Forgejo PRs. Tasks are the
`TS-<n>` keys found in those threads' titles and branches and in those PRs.
Settled/archived/deleted threads are excluded. FULL SEND work sorts first.
Source failures are visible and preserve the last successful snapshot.
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
sources. `task rm` removes annotations only. When a key disappears from every open thread
and PR after a successful refresh, its annotations are removed.
Direction replaces the current direction; other notes append and return an ID.

The dashboard home is one chat with the COS agent: message list, composer,
markdown replies with collapsed tool activity, and a header with running threads,
open PRs and a settings link. Chat entries live in the replicated JetStream KV
bucket `COS_CHAT`; each node follows it and patches every open page over Datastar
SSE, so any node shows the same chat within a second and a restarted node catches
up from JetStream. Header counts follow SQLite changes, including CLI writes.

Shared context files (durable user decisions, preferences, learnings) live in the
replicated bucket `COS_CONTEXT`:

```sh
cos context list
cos context get user-decisions
cos context set user-decisions '<markdown, at most 16 KiB>'
cos context rm user-decisions
```

Only the JetStream meta leader creates buckets; other nodes bind once it exists.

### COS agent

The cluster leader (`Bus.Lead`) drives the COS agent: one T3 thread on its own
host (Opus 5.5, project `cos-agent` at `~/.local/share/cos/agent`). ts-cos writes
the operating rules there as `CLAUDE.md` (`behavior/brief.md` + `rules.md`, through
the hot-reloaded behavior loader) plus a `./cos` wrapper, so COS's only tool is this
CLI. The first message of every thread renders `behavior/templates/brief.tmpl`. The thread's state lives in
the replicated bucket `COS_AGENT`; the chat stays in `COS_CHAT`, never in T3.

- Every user chat message is sent to the thread (queued behind a running turn).
  The leader mirrors each turn into one chat entry: text and collapsed tool calls.
- A fresh thread starts with a brief (the `focus` context file first, then every
  shared context file up to 48 KiB, the last chat exchanges, `cos status --brief`)
  when: the `focus` file or the FULL SEND set changes, a PR leaves the open set (merged or closed),
  COS was idle for 4 hours, a new behavior version arrived, context passed 120k
  tokens while idle, after a
  failover, or COS ran `cos agent restart '<reason>'`. Past 120k tokens during a
  running turn, it queues `/compact` instead. Restarts wait for the next wake,
  except focus changes and an unanswered message after a failover.
- On failover the new leader claims the thread state first, starts its own
  thread, and deletes the old one on the previous host once it answers.

```sh
cos agent                        # thread, host, last restart reason
cos agent restart 'new plan'     # fresh brief at the next wake
cos context set focus '<text>'   # change focus: COS restarts with a fresh brief now
```

## Autopilot: heartbeat and rules engine

Every node serves the same Autopilot panel. The heartbeat interval is one of
10, 15, 30 or 60 minutes (default 30); one tap on any node stores it in the
replicated `COS_AUTOPILOT` KV bucket. Only the cluster leader (the TS-810 lease)
beats and runs the rules engine; mariogk-desktop resigns the lease. **Run now**
sends a NATS request that only the leader answers, so it works from any node.
Each beat records its time in KV first, so a new leader keeps the schedule.

The rules engine is the durable `cos-rules` consumer on `cos.events.>`. Each
event is classified with zero tokens:

| Class | When | What happens |
|---|---|---|
| ACT | READY `cos/` or `cos-hold/` PR of a running ticket | squash-merge |
| ACT | CI failure, requested changes, CONFLICT, > 1,500 changed lines, failed or interrupted run, slice merged (`Part of`) | templated wake message queued to the owning thread, once per head commit |
| ACT | the same failure after a new push | model escalation (Opus 5.5 high → xhigh) plus ESCALATE |
| ACT | a top-level thread on any Haiku model | switch to Opus 5.5 high |
| RECORD | paused tickets, tickets with no open thread or PR (`not-running`), progress, main changes | feed only |
| ESCALATE | worker question or pending request, orphan PR, same failure twice | `cos.autopilot.log.escalate` for the COS inbox |

Wake texts are behavior files (`behavior/templates/wake_<rule>.tmpl`): a change
merged to main reaches every node over NATS without a rebuild, and a bundle missing
one or rendering more than one line is rejected. A ticket runs while an open
thread or open PR carries its key and it is not paused: `cos pause TS-<n>` /
`cos resume TS-<n>` edit the replicated paused set; rules record events of paused
tickets and never wake them. Wake
memory lives in KV, so a new leader does not repeat a wake. Every decision is
published with its reason to the 3-replica `COS_AUTOPILOT_LOG` stream
(`cos.autopilot.log.<class>`); each node mirrors the stream and the KV state into
its SQLite, so a restarted or joining node catches up and every node shows the same feed.

**Reconciler.** Every beat diffs live Forgejo PR state against the fingerprints
the previous beat saw, stored under the `reconcile` key in `COS_AUTOPILOT`. A
changed PR (CI, mergeability, review or head commit) publishes a synthetic event
(`Synthetic: true`) into `cos.events.>`, so a dropped webhook still reaches the
rules engine within one heartbeat. The first beat only records fingerprints, so
the existing backlog does not replay. Event IDs are deterministic (the source,
the item and a hash of its state), so a repeat publish is deduplicated by the
stream. Thread reconciliation is deferred: a synthetic `turn.failed` would
duplicate the real one under a different event ID and count as a second failure
for same-failure escalation. The step uses no tokens, and its feed entry reads
`Reconciler: N synthetic events` only when it published something.

**Worker auto-compaction (TS-813).** Each beat reads the context use of every idle
top-level worker thread; subagents and running threads are never touched. At 70%
or more the thread is compacted with a handoff brief (`compact_focus.tmpl`), at
most once per 30 minutes. Five minutes later, a thread still at 50% or more is
replaced by a fresh Opus 5.5 thread (effort high) on the same worktree, seeded with
its last reply (`fresh_brief.tmpl`); the old thread is settled, not deleted. When a
ticket leaves the paused set, its idle threads are compacted and sent `RESUME`.
Each action or failure is one feed entry. Per-thread state lives in `COS_AUTOPILOT`
under `compact.<hash>`, so a new leader keeps it.

**Registered rules:** a feature adds a rule from its own file with `RegisterRule(Rule{...})`
in `init()`. `Event` runs for every event after the built-in rules; `Beat` runs on each
leader heartbeat. Decisions are logged like built-in ones, and a decision with `Do` runs
its custom action with a `RuleEnv` (the `Bus` and `Publish`). Node alerts (TS-825) and
budgets (TS-826) register their rules this way. A panicking rule logs an `error` entry
and the other rules still run.

**Action-log writes** retry three times with backoff (1 s, then 3 s), 5 s per attempt.
A WARN marks the first retry; an ERROR is logged only when every attempt fails.

**Model policy (TS-820).** Every thread is an Opus 5.5 advisor at high effort, and
xhigh only after the same failure twice. Haiku 5.5 runs only as `delegate_task`
workers under an Opus advisor: a Haiku worker under a parent that is not on Opus
escalates the parent (once per two hours). Each beat reads TS-826's usage rows for
yesterday and today from SQLite; the policy has no usage reader of its own. A thread
past its first hour gets a nudge at most every two hours when it has at least 20,000
output tokens and under 50% of them on Haiku 5.5, naming the build, git, read, gitread,
live and CI calls its advisor ran itself. A thread with advisor output and no Haiku
worker at all gets the no-workers nudge. A Haiku 4.5 subagent escalates and gets a
nudge. Every nudge is also a wake text ending with the advisor split line. Nudge
times live in the `COS_AUTOPILOT` KV bucket.

## Inbox

The Inbox (`/inbox`, linked with its open count from the chat header) holds
everything COS needs from you. Item kinds (`kind`: `question`,
`approval`, `decision`, `blocker`, `fyi`):

- **Questions:** 2-4 options, recommended first, plus free text.
- **Approvals:** Approve or Reject, with an optional note.
- **Decisions:** choices COS made for you. Keep them or override them.
- **FYI blockers:** informational, with an optional reply.
- **FYI:** informational only (for example a rolled-back deploy). Dismiss only;
  never urgent, and its event is `info`, so it does not wake COS.

`cos inbox add` takes JSON with these fields: `kind`, `title`, `context`,
`blocks`, `options`, `ticket`, `links`, `host`, `thread`, `source` (`cos`,
`rules`, `worker`, or `deploy`), `key`, `urgent`, and `supersedes`. Unknown fields are
rejected.

Items dedupe by `key`. The default key is the thread (or ticket) plus the title,
ignoring case and extra spacing. An open item updates in place. An answered or
dismissed key is not asked again. A superseded item closes with a note.

Worker NEEDS-USER blocks become items automatically. An answer is forwarded to
that worker as a `USER ANSWER` message.

State lives in the replicated `COS_INBOX` JetStream KV bucket (3 replicas). Every
node mirrors it into SQLite, so any node shows and answers the same Inbox.
Answering emits a `user.answer` event on `cos.events.>`.

## Token saving (TS-828)

- `cos memory`: typed entries. Recall returns pinned entries, then BM25/FTS5
  matches, then recent ones, under a 1,500-token cap. Shared context files are
  read as entries.
- `BuildDigest`: a capped 4 KiB wake digest with dedup, coalescing, and priority
  ordering. Wired into the COS agent in slice 2.
- `cos savings [days]`: per-day avoided tokens by kind and wall-clock seconds
  saved. Days are UTC. Worker and rule-action savings sit on the tokens page.
- `cos plan '<PlanInput>'`: splits a task into up to 4 parallel Haiku 5.5 worker
  briefs per wave under the Opus 5.5 advisor. Files a worker owns never overlap
  within a wave.
- `cos plan done '<PlanResult>'`: records wall-clock seconds saved by parallel
  workers in the savings ledger. Worker and advisor tokens are printed, not
  recorded.

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
