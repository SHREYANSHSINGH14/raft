# Raft Consensus — Learning Implementation

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go.
The goal is not just to make it work, but to deeply understand *why* each design decision exists — from goroutine lifecycle management to the subtle timing constraints that make distributed consensus correct.

---

## What is Raft?

Raft is a consensus algorithm designed to be understandable. It allows a cluster of nodes to agree on a shared log of commands even in the presence of failures (as long as a majority of nodes are alive).

The three core problems Raft solves:

- **Leader election** — one node becomes the authoritative leader per term
- **Log replication** — the leader replicates entries to followers
- **Safety** — committed entries are never lost, even across leader changes

---

## Project Structure

```
.
├── cmd/                  # CLI entrypoint (cobra)
│   └── server.go
├── config/               # Config loading from env vars + peers.yaml
│   └── config.go
├── raft/                 # Core Raft logic
│   ├── peer.go           # Peer struct, role transitions, startup
│   ├── election.go       # Candidate election loop
│   ├── heartbeat.go      # Leader heartbeat / log replication
│   ├── request_vote.go   # RequestVote RPC handler
│   └── append_entries.go # AppendEntries RPC handler
├── server/               # gRPC server + HTTP debug server
│   ├── server.go
│   └── debug_server.go
├── db/                   # PebbleDB-backed persistent storage
│   └── db.go
├── proto/                # Protobuf definitions
│   ├── rpc.proto
│   └── log.proto
├── types/                # Generated protobuf types
├── peers.yaml            # Peer discovery config
├── docker-compose.yaml   # 5-node cluster setup
└── config.dev.env        # Local dev environment variables
```

---

## Architecture

Each node runs as an independent process with three concurrent concerns:

```
┌─────────────────────────────────────────────────────┐
│                        Node                         │
│                                                     │
│  ┌──────────────┐   ┌──────────────────────────┐   │
│  │  gRPC Server │   │       Raft Peer           │   │
│  │  :50051      │──▶│  Role: Follower /         │   │
│  │              │   │        Candidate / Leader  │   │
│  └──────────────┘   └──────────┬───────────────┘   │
│                                │                    │
│  ┌──────────────┐   ┌──────────▼───────────────┐   │
│  │  Debug HTTP  │   │       PebbleDB            │   │
│  │  :8080       │   │  currentTerm, votedFor,   │   │
│  └──────────────┘   │  log entries              │   │
│                     └──────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### Role State Machine

```
          election timeout
Follower ─────────────────▶ Candidate ──── majority votes ──▶ Leader
   ▲                            │                                 │
   └────────────────────────────┘                                 │
          higher term seen / vote granted        higher term seen  │
   ▲─────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions (and the intuitions behind them)

### Timing Constraints
Raft correctness depends on a strict timing relationship:

```
RPC_TIMEOUT_MS < HEARTBEAT_MS < ELECTION_MIN_MS < ELECTION_MAX_MS
      50       <     100      <      1000        <     5000
```

If heartbeats are slower than election timeouts, followers would constantly trigger spurious elections. If RPCs are slower than heartbeats, the leader would pile up goroutines. The config layer validates these relationships at startup and panics early rather than silently misbehaving.

### Context Propagation
Every goroutine receives a `context.Context`. The server creates a root context; cancelling it stops the gRPC server *and* all Raft goroutines (election timer, heartbeat senders) in a single operation. This prevents the class of bugs where a stopped server's goroutines continue making RPCs or triggering elections.

### `wg.Wait()` as a Synchronization Boundary — and Why It Breaks

The same structural mistake appeared in two different places, and understanding the connection is the core concurrency lesson of this project.

**In the election loop:** `wg.Wait()` collected `RequestVote` responses from all peers before processing results. When `startElection` cancelled the context and spawned a new election goroutine (on ticker fire or `electionTimeoutCh`), the old goroutine was still blocked at `wg.Wait()`. When it eventually unblocked, it tried to send to `electionResChan` — but the buffer was already full from the newer election. Permanent block. Goroutine leak. On every election timeout cycle, leaking goroutines incremented the term via `SetCurrentTerm`, eventually causing term inflation into the hundreds of thousands and OOM.

**In the heartbeat loop:** `sendLogs` used `wg.Wait()` to wait for *all* peers before returning. Total execution time (db reads + RPC timeout + response processing + scheduling overhead) consistently exceeded the heartbeat interval on a loaded machine. Every tick spawned a new goroutine while the previous one was still inside `wg.Wait()`. Goroutines accumulated, all hammering the same peers concurrently, latency spiked, RPCs started hitting deadlines more aggressively — a feedback loop that made things progressively worse until the leader lost authority entirely.

**The shared pattern:** `wg.Wait()` creates a synchronization point that doesn't respect external signals — whether that's a context cancellation or a ticker firing. The goroutine is stuck waiting for the *slowest* thing to finish while the rest of the system has moved on.

**The fix in both cases:** eliminate `wg.Wait()` as the coordination mechanism. For election, a buffered channel sized exactly to the number of peers lets every goroutine send its result without blocking, regardless of whether the caller is still listening. For heartbeats, one independent goroutine loop per peer (`sendLogsPerPeer`) means there's no "wait for all 4" synchronization point at all — each peer loop runs on its own ticker with its own `inFlight` guard. A slow peer only affects its own pipeline, not the others.

This is also why per-peer goroutines are the correct design, not just a performance optimization. `wg.Wait()` couples all peers together — one slow follower holds up heartbeats to everyone else. Independent loops isolate the failure. This is how etcd, TiKV, and CockroachDB implement replication.

### Follower as the Safe Default — Fixing Term Inflation

A subtler election bug: when a candidate failed to win (no majority reached, or all peers rejected the vote), the original code returned `ServerRole_Candidate` from `election()` and immediately called `becomeCandidate()` again — bypassing `startElectionOut` and its randomized timeout entirely.

The failure mode was visible when two nodes with stale logs rejoined the cluster simultaneously. Both kept getting rejected by the log-up-to-date check on every attempt. Each election attempt takes ~50ms (one RPC timeout). With two nodes each trying ~20 times per second, terms inflated by ~40/second — reaching 783 terms in under 12 seconds with no stable leader.

The Raft paper is explicit: a candidate that fails to win should wait for a new randomized timeout before trying again. `startElectionOut` implements exactly that wait — but `becomeCandidate()` was bypassing it by jumping straight to `startElection`.

**The fix:** on a `Candidate` result from `election()`, call `becomeFollower()` instead of `becomeCandidate()`. This puts the node back into the randomized timeout wait in `startElectionOut`. When a leader's heartbeat arrives, the timer resets and the node stays follower. If no heartbeat arrives, the timeout fires and it tries again — with proper spacing.

The deeper principle: **follower is the safe default**. Any role that fails to fulfill its responsibility should retreat to follower, not retry immediately:

- Candidate fails to win → follower. Let the randomized timer decide when to try again.
- Leader can't initialize state → follower. Let someone healthier lead.
- Leader sees a higher term → follower. Acknowledge the legitimate authority.

A zombie leader sends conflicting `AppendEntries`. A runaway candidate inflates terms and disrupts stable leaders. A follower just waits — the only role that can't cause harm to the cluster.

### Child Goroutines Never Drive Role Transitions

After fixing the `wg.Wait()` issue in heartbeats, a second bug surfaced: `sendLogs` was calling `p.becomeFollower()` directly when it saw a higher term in an `AppendEntries` response. This created a three-part failure chain:

1. `sendLogs` called `becomeFollower()`, which started a new election timer — but `heartbeatCtx` was never cancelled, so all `sendLogsPerPeer` goroutines kept running as zombies. The node was simultaneously follower *and* leader.
2. When the node won the next election and called `becomeLeader()`, it spawned a *second* set of heartbeat goroutines on top of the existing ones. Every leadership cycle added another layer of goroutines that never stopped.
3. These zombie goroutines kept sending `AppendEntries` RPCs with stale leader state, corrupting follower logs and causing spurious step-downs in an unbounded loop.

**The fix — same pattern as election.go:** child goroutines never drive role transitions directly. `sendLogs` signals a `stepDownCh` channel and returns. `startSendLogs` (the orchestrator) reads that signal, calls `cancel()` to stop all `sendLogsPerPeer` goroutines via context, *then* calls `becomeFollower()` exactly once. One owner, clean shutdown.

The `stepDownCh` buffer is sized to the number of peers. Context cancellation doesn't preempt running code — it only fires at the next `select` or ctx check. So after `startSendLogs` reads the first signal and cancels, other `sendLogs` goroutines that are already past the RPC call can still reach the `stepDownCh <- struct{}{}` line before noticing cancellation. Worst case: all peers respond with a higher term simultaneously — `len(peers)` senders. Buffering all of them means no sender ever blocks. The unread signals are GC'd when `startSendLogs` returns and the channel goes out of scope.

### Startup Quorum Wait
Before starting the election timer, a node waits for a majority of peers to be reachable. This avoids a window where containers start at slightly different times and trigger spurious elections before the cluster is healthy.

---

## Running Locally

### Prerequisites
- Go 1.25+
- Docker + Docker Compose
- `protoc` (for regenerating protobuf types)

### Run a 5-node cluster

```bash
docker compose up --build
```

Each peer exposes two ports:

| Peer  | gRPC  | Debug HTTP |
|-------|-------|------------|
| peer1 | 50051 | 8081       |
| peer2 | 50052 | 8082       |
| peer3 | 50053 | 8083       |
| peer4 | 50054 | 8084       |
| peer5 | 50055 | 8085       |

### Run a single node locally

```bash
# Export env vars into the current shell, then build and run
set -a
source config.dev.env
set +a

make build
make run
```

`set -a` causes every variable sourced from the file to be automatically exported to the environment. `make run` just executes the binary (`./raftd server start`) — it does not build, so `make build` must come first.

---

## Debug HTTP API

Each node exposes a debug HTTP server for manual inspection and testing.

### Check all nodes at once
```bash
for i in 1 2 3 4 5; do curl -s http://localhost:808${i}/status; echo; done
```
```json
{"id":"peer1","role":"LEADER","term":1,"commit_index":2,"leader_id":""}
{"id":"peer2","role":"FOLLOWER","term":1,"commit_index":2,"leader_id":"peer1"}
{"id":"peer3","role":"FOLLOWER","term":1,"commit_index":2,"leader_id":"peer1"}
{"id":"peer4","role":"FOLLOWER","term":1,"commit_index":2,"leader_id":"peer1"}
{"id":"peer5","role":"FOLLOWER","term":1,"commit_index":2,"leader_id":"peer1"}
```

### Write a log entry
```bash
curl -s -X POST http://localhost:8081/logs/append \
  -H "Content-Type: application/json" \
  -d '{"data": "set x=5"}'
```
```json
{"success":true,"error_msg":"","leader_id":""}
```

If you hit a follower, it won't accept the write — redirect to the returned `leader_id`:
```bash
curl -s -X POST http://localhost:8082/logs/append \
  -H "Content-Type: application/json" \
  -d '{"data": "set x=5"}'
```
```json
{"success":false,"error_msg":"not the leader","leader_id":"peer1"}
```

### Read log entries
```bash
curl -s "http://localhost:8081/logs/get?start=1&end=5"
```
```json
{"entries":[{"index":1,"term":1,"data":"set x=5"},{"index":2,"term":1,"data":"set y=10"}],"error_msg":"","leader_id":""}
```

Omit `end` to fetch everything from `start` to the latest:
```bash
curl -s "http://localhost:8081/logs/get?start=1"
```

---

## Configuration

All configuration is loaded from environment variables. Key parameters:

| Variable              | Default | Description                                     |
|-----------------------|---------|-------------------------------------------------|
| `ID`                  | —       | Node identifier                                 |
| `PORT`                | —       | gRPC listen port                                |
| `DEBUG_PORT`          | —       | HTTP debug server port                          |
| `PEER_INFO`           | —       | Path to `peers.yaml`                            |
| `RPC_TIMEOUT_MS`      | 50      | Timeout per outbound RPC                        |
| `HEARTBEAT_MS`        | 100     | Leader heartbeat interval                       |
| `ELECTION_MIN_MS`     | 1000    | Minimum election timeout                        |
| `ELECTION_MAX_MS`     | 5000    | Maximum election timeout (randomized within range) |
| `COMMIT_UPDATER_SLEEP_S` | 1   | How often the leader recalculates commit index  |
| `LOG_LEVEL`           | debug   | Zerolog level: debug/info/warn/error            |

Peer discovery uses a shared `peers.yaml`:

```yaml
peers:
  - id: peer1
    rpc_url: peer1:50051
  - id: peer2
    rpc_url: peer2:50052
  # ...
```

---

## Tech Stack

| Concern           | Library                  |
|-------------------|--------------------------|
| RPC               | gRPC + Protobuf          |
| Storage           | CockroachDB Pebble (LSM) |
| Logging           | zerolog                  |
| CLI               | cobra                    |
| Testing           | testify + mock           |

---

## What's Implemented

- [x] Leader election with randomized timeouts
- [x] Vote safety (one vote per term, log up-to-date check)
- [x] Log replication via `AppendEntries`
- [x] Heartbeat / leader keepalive
- [x] Commit index advancement (majority match)
- [x] Persistent state (`currentTerm`, `votedFor`, log entries)
- [x] Client `WriteLog` / `ReadLog` RPCs
- [x] Graceful shutdown via context cancellation
- [x] Debug HTTP server

## What's Not Yet Implemented

- [ ] Apply loop (state machine execution after commit)
- [ ] Wait-for-commit on `WriteLog` (currently returns after local append)
- [ ] Log compaction / snapshots
- [ ] Cluster membership changes (joint consensus)
- [ ] Linearizable reads

---

## Further Reading

- [Raft paper (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf)
- [The Secret Lives of Data — visual Raft demo](http://thesecretlivesofdata.com/raft/)
- [Raft FAQ](https://pdos.csail.mit.edu/6.824/papers/raft-faq.txt)