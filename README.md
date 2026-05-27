# Raft Consensus — Learning Implementation

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go,
refactored into an importable library. The `raft/` package exposes three interfaces — `Transport`,
`Storage`, and `StateMachine` — that the caller implements. The library owns consensus; the caller
owns networking, persistence, and application state.

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
├── raft/                 # Core Raft library (import this)
│   ├── node.go           # Node struct, NewNode, Start/Stop
│   ├── interfaces.go     # Transport, Storage, StateMachine interfaces
│   ├── types.go          # Plain Go structs (LogEntry, RequestVoteArgs, …)
│   ├── election.go       # Candidate election loop
│   ├── heartbeat.go      # Leader heartbeat / log replication
│   ├── request_vote.go   # RequestVote RPC handler
│   └── append_entries.go # AppendEntries RPC handler
├── server/               # gRPC server + HTTP debug server
│   ├── server.go         # grpcTransport adapter wires gRPC → raft.Transport
│   └── debug_server.go
├── db/                   # PebbleDB-backed raft.Storage implementation
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
│  ┌──────────────┐   ┌──────────────────────────┐    │
│  │  gRPC Server │   │       Raft Peer          │    │
│  │  :50051      │──▶│  Role: Follower /        │    │
│  │              │   │        Candidate / Leader│    │
│  └──────────────┘   └──────────┬───────────────┘    │
│                                │                    │
│  ┌──────────────┐   ┌──────────▼───────────────┐    │
│  │  Debug HTTP  │   │       PebbleDB           │    │
│  │  :8080       │   │  currentTerm, votedFor,  │    │
│  └──────────────┘   │  log entries             │    │
│                     └──────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

### Role State Machine

```
          election timeout
Follower ─────────────────▶ Candidate ──── majority votes ──▶ Leader
   ▲                            │                                 │
   └────────────────────────────┘                                 │
          higher term seen / vote granted        higher term seen │
   ▲─────────────────────────────────────────────────────────────┘
```

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