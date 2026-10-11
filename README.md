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

Every COS host (delta, beta, mariogk-dev) runs the same user service with no
configuration file:

```sh
deploy/install.sh   # builds this checkout into ~/.local/bin/cos and (re)starts cos.service
```

`deploy/cos.service` runs `cos serve` (dashboard on `0.0.0.0:8790`), restarts on
any exit and starts at boot (user lingering is on for mariogk on all hosts).

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

Every other HTTP route, including `/status` and `/events`, requires a session.

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
| Ku MCP | `~/.config/cos/ku-token` |
| Forgejo | `~/.local/share/forgejo-cli/keys.json`, host `git.terrascale.tech` |

## Commands

Output is JSON by default. Errors are JSON on stderr and return a nonzero exit
code. `cos help` prints the full command list. Host arguments are delta, beta,
or mariogk-dev; thread IDs are the full T3 IDs.

| Command | Purpose |
|---|---|
| `cos fleet` / `cos shell <host>` | Open threads and active load |
| `cos cluster` | Local host, current leader and build revision |
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
Reviewer and deployment checks always apply. Bypass environment variables
and marker-file instructions are retired; their existing product-code paths are
inventoried for a separate removal change. Forgejo's server-side branch
protections still apply.

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

## Autopilot heartbeat

Every node serves the same Autopilot panel. The heartbeat interval is one of
10, 15, 30 or 60 minutes (default 30); one tap on any node stores it in the
replicated `COS_AUTOPILOT` KV bucket. Only the holder of the `COS_AUTOPILOT_LEADER`
KV lock (45-second TTL, renewed every 15 seconds over a quorum) beats;
mariogk-desktop never takes the lock. **Run now** sends a NATS request that only
the leader answers, so it works from any node. Each beat records its time in KV
first, so a failover leader keeps the schedule instead of beating again.

The beat is the reconciler: it refreshes T3, Forgejo and Ku and logs what it saw.
Every step is published with its reason to the 3-replica `COS_AUTOPILOT_LOG`
stream; each node mirrors the stream and the KV state into its SQLite, so a
restarted or joining node catches up and the feed shows the same entries everywhere.

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
