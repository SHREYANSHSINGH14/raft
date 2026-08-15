# Raft Consensus — Learning Implementation

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go,
refactored into an importable library. The root package exposes three interfaces — `Transport`,
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

The **repo root is the library** — `import "github.com/SHREYANSHSINGH14/raft"` and you get the
consensus engine and nothing else. `example/` is one concrete embedding of it (gRPC + PebbleDB +
a cobra CLI), kept in-tree so the interfaces have a worked reference. The dependency points one
way only: `example/` imports the root, never the reverse.

```
.
├── node.go               # Node struct, NewNode, Start/Stop
├── interfaces.go         # Transport, Storage, StateMachine interfaces
├── types.go              # Plain Go structs (LogEntry, RequestVoteArgs, …)
├── election.go           # Pre-vote round + candidate election loop
├── heartbeat.go          # Leader heartbeat / log replication
├── apply_loop.go         # Applies committed entries to the StateMachine
├── write_logs.go         # Propose — append, return a Future
├── future.go             # Future.Wait — the commit-wait
├── request_vote.go       # RequestVote RPC handler
├── pre_vote.go           # PreVote RPC handler (Ongaro §9.6)
├── timeout_now.go        # TimeoutNow RPC handler (leadership transfer)
├── append_entries.go     # AppendEntries RPC handler
├── install_snapshot.go   # InstallSnapshot RPC handler
├── snapshot.go           # Snapshot capture + log compaction
├── configuration.go      # Config-entry handling
├── raft_config.go        # Config, Peer, the configurations struct
├── add_member.go         # AddMember — stage, catch up, promote
├── remove_member.go      # RemoveMember — incl. self-removal handoff
│
├── example/              # One concrete embedding — not part of the library
│   ├── main.go           # Binary entrypoint
│   ├── cmd/              # CLI entrypoint (cobra)
│   ├── config/           # Config loading from env vars + peers.yaml
│   ├── server/           # gRPC server + HTTP debug server
│   │   ├── server.go     # grpcTransport adapter wires gRPC → raft.Transport
│   │   ├── rpc.go        # inbound Raft RPCs, incl. streaming InstallSnapshot
│   │   ├── debug_server.go
│   │   └── debug_kv.go   # /kv/* endpoints — propose, wait, report
│   ├── statemachine/     # PebbleDB-backed raft.StateMachine implementation
│   ├── db/               # PebbleDB-backed raft.Storage implementation
│   ├── proto/            # Protobuf definitions
│   ├── types/            # Generated protobuf types
│   ├── scripts/          # generate_protos.sh
│   ├── peers.yaml        # Peer discovery config
│   ├── Dockerfile
│   ├── docker-compose.yaml # 5-node cluster setup
│   └── config.dev.env    # Local dev environment variables
│
└── docs/                 # Every doc except this one
    ├── INVARIANTS.md     # Architecture + the invariants that are easy to break
    ├── JOURNEY.md        # Bugs hit, why they happened, what each taught
    ├── STATE.md          # What is in flight right now
    ├── architecture.md   # Per-concern breakdown (+ architecture.mmd)
    ├── command-results.md # How a per-command result reaches the caller
    └── membership-change.md
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
cd example && docker compose up --build
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
source example/config.dev.env
set +a

go build -o raftd example/main.go
./raftd server start
```

`set -a` causes every variable sourced from the file to be automatically exported to the environment.
Run both from the repo root — `PEER_INFO` in `config.dev.env` is `example/peers.yaml`, resolved
relative to the working directory.

### Run a 3-node cluster without Docker

Faster to iterate on than `docker compose`, and the logs land in files you can `tail`. Each node needs
its own data directory, snapshot directory, and pair of ports.

```bash
go build -o /tmp/raftd example/main.go

cat > /tmp/peers3.yaml <<'EOF'
peers:
  - id: peer1
    rpc_url: localhost:50051
  - id: peer2
    rpc_url: localhost:50052
  - id: peer3
    rpc_url: localhost:50053
EOF

start() {  # start <id> <grpc-port> <debug-port>
  ID=$1 BASE_URL=127.0.0.1 PORT=$2 DEBUG_PORT=$3 \
  DB_DIR=/tmp/raft/$1/data SNAPSHOT_DIR=/tmp/raft/$1/snapshots \
  PEER_INFO=/tmp/peers3.yaml LOG_LEVEL=debug \
  RPC_TIMEOUT_MS=50 HEARTBEAT_MS=100 ELECTION_MIN_MS=1000 ELECTION_MAX_MS=5000 \
  SNAPSHOT_INTERVAL_S=300 SNAPSHOT_THRESHOLD=1000 \
  /tmp/raftd server start > /tmp/raft/$1.log 2>&1 &
}

mkdir -p /tmp/raft
start peer1 50051 8081
start peer2 50052 8082
start peer3 50053 8083
```

A leader is elected within a couple of seconds. Stop them with
`pkill -f '/tmp/raftd server start'`, and wipe state between runs with `rm -rf /tmp/raft`.

**A node will not campaign until it can reach a majority** — it logs `waiting for quorum...` until
enough peers answer. Starting fewer than two of three means no leader, by design.

---

## Debug HTTP API

Each node exposes a debug HTTP server for manual inspection and testing.

### Check all nodes at once
```bash
for i in 1 2 3; do curl -s http://localhost:808${i}/status | jq -c '{id,role,term,commit_index,leader_id}'; done
```
```json
{"id":"peer1","role":"FOLLOWER","term":1,"commit_index":3,"leader_id":"peer3"}
{"id":"peer2","role":"FOLLOWER","term":1,"commit_index":3,"leader_id":"peer3"}
{"id":"peer3","role":"LEADER","term":1,"commit_index":3,"leader_id":""}
```

The full response carries replication state as well — `last_log_index` next to `commit_index` answers
"how far behind is this node" in one call, and `peers` is the live configuration
(`configurations.latest`), not the bootstrap seed:

```bash
curl -s http://localhost:8083/status
```
```json
{
  "id": "peer3", "role": "LEADER", "is_leader": true, "term": 1, "leader_id": "",
  "commit_index": 3, "last_log_index": 3,
  "snapshot_index": 0, "snapshot_term": 0,
  "peers": {
    "peer1": {"peer_state": "VOTER", "next_index": 4, "match_index": 3},
    "peer2": {"peer_state": "VOTER", "next_index": 4, "match_index": 3},
    "peer3": {"peer_state": "VOTER", "next_index": 0, "match_index": 0}
  }
}
```

A leader's entry for *itself* shows zeroes: it does not replicate to itself, so it keeps no
next/match index — `getMajorityMatchIndex` substitutes its own last log index when counting.

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
curl -s "http://localhost:8083/logs/get?start=1"
```
```json
{
  "node_id": "peer3", "role": "LEADER", "term": 1, "commit_index": 3,
  "start_index": 1, "count": 3,
  "entries": [
    {"index": 1, "term": 1, "type": "NO_OP", "type_code": 1, "committed": true, "data_size": 0},
    {
      "index": 2, "term": 1, "type": "COMMAND", "type_code": 0,
      "committed": true, "data_size": 124,
      "data": {"id": "18c5c09a…", "op": "SET", "key": "user:1", "value": "eyJuYW1lIjoiYWxpY2UifQ=="},
      "command": {"id": "18c5c09a…", "op": "SET", "key": "user:1", "value": {"name": "alice"}}
    }
  ]
}
```

`data` is the entry as stored — `value` is base64 there because Go marshals `[]byte` that way.
`command` is the same entry decoded, which is what you actually want to read. It appears only for
`type: COMMAND` entries whose payload parses.

`type` is reported by name because the numeric `EntryType` is an `iota` whose meaning is invisible in a
dump, and the proto enum is offset by one — comparing the raw numbers across the two will mislead you.

The envelope names the node that answered and where it stood. The same query against two nodes
legitimately returns different things, and `committed` is per-node: an entry can be present in the log
but not yet committed.

Omit `start` to default to 1; there is no `end` — it returns everything from `start` to the latest.

### Key/value operations

These go through the state machine, so they are the ones that actually exercise replication. Values are
raw JSON — whatever you send comes back unchanged. Writes must go to the leader.

```bash
# set — send writes to the leader
curl -s -X POST http://localhost:8083/kv/set \
  -d '{"key":"user:1","value":{"name":"alice"}}'
```
```json
{"success":true,"key":"user:1","node_id":"peer3","role":"LEADER","commit_index":2,
 "command_id":"18c5c09ab8e6b747c59efa6461158c11","index":2}
```

`command_id` and `index` identify the entry the write produced — look it up with
`/logs/get?start=<index>`.

```bash
# read it back from every node — this is what proves replication
for i in 1 2 3; do curl -s "http://localhost:808${i}/kv/get?key=user:1"; echo; done
```
```json
{"success":true,"key":"user:1","value":{"name":"alice"},"node_id":"peer1","role":"FOLLOWER","commit_index":3,"leader_id":"peer3"}
{"success":true,"key":"user:1","value":{"name":"alice"},"node_id":"peer2","role":"FOLLOWER","commit_index":3,"leader_id":"peer3"}
{"success":true,"key":"user:1","value":{"name":"alice"},"node_id":"peer3","role":"LEADER","commit_index":3}
```

`/kv/get` is a **stale local read** — it never touches the log, so it reports whatever that node has
applied so far. That is why every response names the node and its `commit_index`: a follower that has
not applied the entry yet answers `not found` for a moment, and the position is what tells you whether
that is lag or a real miss.

```bash
# compare-and-swap
curl -s -X POST http://localhost:8083/kv/set -d '{"key":"counter","value":1}'
curl -s -X POST http://localhost:8083/kv/cas -d '{"key":"counter","expected":1,"value":2}'
```

A CAS whose expected value does not match is a *command* failure, not a node failure — the entry still
committed, the write just did not happen, and the apply loop carries on. It still gets an index:
```bash
curl -s -X POST http://localhost:8083/kv/cas -d '{"key":"user:1","expected":{"name":"bob"},"value":1}'
```
```json
{"success":false,"key":"user:1","node_id":"peer3","role":"LEADER","commit_index":3,
 "command_id":"2a0224628de68bb2381268aa0852bd5b","index":3,
 "error_msg":"command failed: cas: value mismatch for key \"user:1\""}
```
`HTTP 409` for a refused command, `404` for a missing key on `/kv/get`, `500` for anything else —
including a write sent to a follower, which returns the `leader_id` to redirect to.

```bash
# delete
curl -s -X POST http://localhost:8083/kv/delete -d '{"key":"user:1"}'
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
- [x] **Pre-vote** — a probe round before the term bump, so a partitioned node can't inflate terms
- [x] **Leadership transfer** (`TimeoutNow`), receiving half
- [x] Log replication via `AppendEntries`, one independent goroutine per peer
- [x] Heartbeat / leader keepalive
- [x] Commit index advancement (majority match, Voters only)
- [x] Persistent state (`currentTerm`, `votedFor`, log entries)
- [x] Apply loop — committed entries reach the `StateMachine`
- [x] `Propose` returns a `Future`; `Future.Wait` is the commit-wait, failing fast with
      `ErrLeadershipLost` on step-down
- [x] `Node.Fatal()` — an apply loop that stops for good is reported rather than silently frozen
- [x] Snapshots — creation, on-disk format, `InstallSnapshot` send + receive, log compaction
- [x] **Cluster membership changes** — `AddMember` and `RemoveMember`, single-server changes
      (Ongaro §4.1), including a leader removing itself
- [x] **The whole Raft surface over gRPC** — `RequestVote`, `AppendEntries`, `PreVote`, `TimeoutNow`,
      and a client-streaming `InstallSnapshot`
- [x] **A real `StateMachine`** — `example/statemachine`, backed by Pebble: `Apply`, `Snapshot`,
      `Restore`, with per-command results routed back to the caller
- [x] Client `WriteLog` / `ReadLog` RPCs, and key/value endpoints on the debug server
- [x] Graceful shutdown via context cancellation
- [x] Debug HTTP server

## What's Not Yet Implemented

- [ ] **Linearizable reads** — `/kv/get` reads the local Pebble directly, so it returns whatever that
      node has applied. A correct read needs a log entry per read or a ReadIndex round trip.
- [ ] **Exactly-once commands** — command ids are server-generated, so a client retry proposes a
      second entry. Needs client-supplied ids plus a dedup table in `Apply`.
- [ ] **Tests for `example/statemachine`** — the only package in the repo without any. A
      `Persist` → `Restore` round trip is the obvious first one.
- [ ] Futures carrying `(index, term)` — keyed by index alone, a truncated proposal is
      indistinguishable from a committed one, so it conservatively reports `ErrLeadershipLost`

---

## Further Reading

- [Raft paper (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf)
- [The Secret Lives of Data — visual Raft demo](http://thesecretlivesofdata.com/raft/)
- [Raft FAQ](https://pdos.csail.mit.edu/6.824/papers/raft-faq.txt)