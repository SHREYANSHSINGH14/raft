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

**The fix in both cases:** eliminate `wg.Wait()` as the coordination mechanism. For election, a buffered channel sized exactly to the number of peers lets every goroutine send its result without blocking, regardless of whether the caller is still listening. For heartbeats, one independent goroutine loop per peer means there's no "wait for all 4" synchronization point at all — each peer loop runs on its own ticker with its own `inFlight` guard. A slow peer only affects its own pipeline, not the others.

This is also why per-peer goroutines are the correct design, not just a performance optimization. `wg.Wait()` couples all peers together — one slow follower holds up heartbeats to everyone else. Independent loops isolate the failure. This is how etcd, TiKV, and CockroachDB implement replication.

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

### Check node status
```bash
curl http://localhost:8081/status
```

### Write a log entry (must hit the leader)
```bash
curl -X POST http://localhost:8081/logs/append \
  -H "Content-Type: application/json" \
  -d '{"data": "set x=5"}'
```

If the node is not the leader, the response includes a `leader_id` field for redirect.

### Read log entries
```bash
# Entries from index 1 to 10
curl "http://localhost:8081/logs/get?start=1&end=10"
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