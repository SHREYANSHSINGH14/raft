#!/usr/bin/env bash
#
# cluster-flow-test.sh — end-to-end driver for a running raft cluster.
#
# Walks the paths that only exist once real nodes are talking to each other, in
# the order that each one's preconditions are met:
#
#   1. cluster status          — is there exactly one leader, does everyone agree
#   2. propose                 — a write through the leader, and a follower refusing one
#   3. get                     — read it back from every node
#   4. applied latency         — set/delete/cas in rapid succession, timed to
#                                commit and then to convergence across all nodes
#   5. lag + snapshot + rejoin — stop a follower, write past the snapshot
#                                threshold, restart it and watch it catch up
#                                (this is what exercises InstallSnapshot)
#   6. addMember               — join a node that was never in the configuration
#   7. removeMember            — a follower, then the leader removing itself
#
# Every HTTP call's request and response is written to a Markdown report
# ($REPORT, default scripts/cluster-flow-report.md), TRUNCATED FRESH each run.
# Timings and verdicts go in with them, so the report is the artefact — the
# terminal output is just progress.
#
# Runs against a cluster you already have up (it does NOT start one), but it does
# stop and start containers for scenarios 5 and 7, so it needs `docker compose`
# to be pointed at the same project. Set DRIVER=none to skip those two scenarios
# and run purely over HTTP.
#
# PRECONDITIONS:
#   - `docker compose up` from example/, all peers healthy
#   - SNAPSHOT_THRESHOLD low enough that scenario 5 can actually trigger a
#     snapshot within WRITE_BURST entries (compose ships 10)
#   - peer6/peer7 present but NOT in peers.yaml — they exist to be joined at
#     runtime by scenario 6
#
# Requirements: bash, curl, jq, docker compose.
#
# Usage:
#   ./scripts/cluster-flow-test.sh
#   PEERS=3 ./scripts/cluster-flow-test.sh              # 3-node cluster
#   SCENARIOS=1,2,3,4 ./scripts/cluster-flow-test.sh    # read/write only, leaves topology alone
#   DRIVER=none ./scripts/cluster-flow-test.sh          # HTTP only, no container restarts
#   KEEP_GOING=1 ./scripts/cluster-flow-test.sh         # record failures, don't abort
#
# Scenarios 1-4 only write keys. Scenario 5 stops and starts a container; 6 and 7
# change cluster membership and 7 forces a leadership change. Use SCENARIOS to
# keep a cluster you care about intact.
#
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ============================================================================
# CONFIG
# ============================================================================
HOST="${HOST:-http://localhost}"
DEBUG_PORT_BASE="${DEBUG_PORT_BASE:-8080}"   # peer N is at DEBUG_PORT_BASE+N
PEERS="${PEERS:-5}"                          # members in the initial configuration
JOIN_PEER="${JOIN_PEER:-peer6}"              # scenario 6 joins this one
JOIN_RPC="${JOIN_RPC:-peer6:50056}"          # ...at this address, as the transport sees it
JOIN_PORT="${JOIN_PORT:-8086}"               # ...and this is its debug port
REPORT="${REPORT:-$ROOT/scripts/cluster-flow-report.md}"
DRIVER="${DRIVER:-docker}"                   # docker | none  (none => skip stop/start scenarios)
COMPOSE="${COMPOSE:-docker compose}"
KEEP_GOING="${KEEP_GOING:-0}"                # 1 => record a failure and continue
SCENARIOS="${SCENARIOS:-1,2,3,4,5,6,7}"      # which scenarios to run, comma separated

WRITE_BURST="${WRITE_BURST:-20}"             # scenario 4/5 burst size
CONVERGE_TIMEOUT_MS="${CONVERGE_TIMEOUT_MS:-10000}"
POLL_MS="${POLL_MS:-50}"
BOOT_TIMEOUT_S="${BOOT_TIMEOUT_S:-30}"       # max wait for a leader / a restarted node

# ============================================================================
# Plumbing
# ============================================================================
RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; CYN=$'\033[36m'; DIM=$'\033[2m'; RST=$'\033[0m'
RESP=""; CODE=""
FAILURES=0

command -v jq   >/dev/null || { echo "${RED}jq is required${RST}";   exit 1; }
command -v curl >/dev/null || { echo "${RED}curl is required${RST}"; exit 1; }

step() { echo >&2; echo "${CYN}=== $* ===${RST}" >&2; printf '\n## %s\n' "$*" >>"$REPORT"; }
sub()  { echo "${CYN}--- $* ---${RST}" >&2; printf '\n### %s\n' "$*" >>"$REPORT"; }
info() { echo "${DIM}  $*${RST}" >&2; }
ok()   { echo "${GRN}  ✓ $*${RST}" >&2; printf -- '- ✅ %s\n' "$*" >>"$REPORT"; }
warn() { echo "${YEL}  ! $*${RST}" >&2; printf -- '- ⚠️ %s\n' "$*" >>"$REPORT"; }

# bad — record a failed expectation. Aborts unless KEEP_GOING=1, because a broken
# precondition usually makes every later scenario meaningless noise.
bad() {
  echo "${RED}  ✗ $*${RST}" >&2
  printf -- '- ❌ **%s**\n' "$*" >>"$REPORT"
  FAILURES=$((FAILURES + 1))
  [[ "$KEEP_GOING" == "1" ]] || { summary; exit 1; }
}

now_ms() { date +%s%3N; }
port_of() { echo "$((DEBUG_PORT_BASE + $1))"; }

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
md_init() {
  : >"$REPORT"
  {
    printf '# Raft cluster end-to-end report\n\n'
    printf -- '- Generated: `%s`\n' "$(date -u '+%Y-%m-%d %H:%M:%SZ')"
    printf -- '- Host: `%s`, peers: `%s`, driver: `%s`\n' "$HOST" "$PEERS" "$DRIVER"
    printf -- '- Write burst: `%s`\n' "$WRITE_BURST"
    printf '\nEvery HTTP call is recorded with its request, response and status. Timings are\n'
    printf 'wall clock from the client, so they include curl and process startup — treat them\n'
    printf 'as an upper bound on what the cluster did, not a benchmark.\n'
  } >>"$REPORT"
}

md_call() {
  local label="$1" method="$2" url="$3" req="$4" resp="$5" code="$6"
  {
    printf '\n**%s** — `%s %s` → `%s`\n' "$label" "$method" "$url" "$code"
    [[ -n "$req" ]] && printf '\nRequest\n```json\n%s\n```\n' "$(echo "$req" | jq . 2>/dev/null || echo "$req")"
    printf '\nResponse\n```json\n%s\n```\n' "$(echo "$resp" | jq . 2>/dev/null || echo "$resp")"
  } >>"$REPORT"
}

md_note() { printf '\n%s\n' "$*" >>"$REPORT"; }
md_table() { printf '\n%s\n' "$*" >>"$REPORT"; }

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
# call LABEL METHOD URL [BODY] — populates $RESP/$CODE, records to the report.
# Does NOT abort on a non-2xx: several scenarios expect one (a write to a
# follower, a CAS mismatch), so the caller decides what counts as failure.
call() {
  local label="$1" method="$2" url="$3" body="${4:-}"
  local args=(-sS -X "$method" "$url" -H 'Content-Type: application/json' --max-time 30)
  [[ -n "$body" ]] && args+=(-d "$body")
  local out
  out="$(curl "${args[@]}" -w $'\n%{http_code}' 2>&1)"
  CODE="${out##*$'\n'}"; RESP="${out%$'\n'*}"
  md_call "$label" "$method" "$url" "$body" "$RESP" "$CODE"
}

status_of() { curl -sS --max-time 5 "$HOST:$1/status" 2>/dev/null; }

# leader_port — the debug port of whichever node currently says it is the leader.
# Empty if there is none (mid-election, or no quorum).
leader_port() {
  local i p
  for ((i = 1; i <= PEERS; i++)); do
    p="$(port_of "$i")"
    if status_of "$p" | jq -e '.is_leader == true' >/dev/null 2>&1; then echo "$p"; return 0; fi
  done
  return 1
}

# wait_for_leader — poll until exactly one node claims leadership.
wait_for_leader() {
  local deadline=$(( $(date +%s) + BOOT_TIMEOUT_S )) lp
  while :; do
    lp="$(leader_port || true)"
    [[ -n "$lp" ]] && { echo "$lp"; return 0; }
    (( $(date +%s) < deadline )) || return 1
    sleep 1
  done
}

live_ports() {
  local i p
  for ((i = 1; i <= PEERS; i++)); do
    p="$(port_of "$i")"
    status_of "$p" >/dev/null 2>&1 && echo "$p"
  done
}

# ---------------------------------------------------------------------------
# Convergence
# ---------------------------------------------------------------------------
# wait_converged KEY EXPECTED_JSON — poll /kv/get on every live node until they
# all report EXPECTED_JSON. Echoes the elapsed ms, or -1 on timeout.
#
# This is the only honest measure of "applied": /kv/set returns once the LEADER
# has applied it, which says nothing about the followers. /kv/get is a stale
# local read, so polling it per node is exactly the question being asked.
wait_converged() {
  local key="$1" want="$2"
  local start; start="$(now_ms)"
  local deadline=$(( start + CONVERGE_TIMEOUT_MS ))
  local p got all
  while :; do
    all=1
    for p in $(live_ports); do
      got="$(curl -sS --max-time 5 "$HOST:$p/kv/get?key=$key" 2>/dev/null | jq -c '.value // empty' 2>/dev/null)"
      [[ "$got" == "$want" ]] || { all=0; break; }
    done
    (( all == 1 )) && { echo $(( $(now_ms) - start )); return 0; }
    (( $(now_ms) < deadline )) || { echo "-1"; return 1; }
    sleep "$(awk "BEGIN{print $POLL_MS/1000}")"
  done
}

# wait_absent KEY — the delete equivalent: every live node reports not-found.
wait_absent() {
  local key="$1"
  local start; start="$(now_ms)"
  local deadline=$(( start + CONVERGE_TIMEOUT_MS ))
  local p code all
  while :; do
    all=1
    for p in $(live_ports); do
      code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$HOST:$p/kv/get?key=$key" 2>/dev/null)"
      [[ "$code" == "404" ]] || { all=0; break; }
    done
    (( all == 1 )) && { echo $(( $(now_ms) - start )); return 0; }
    (( $(now_ms) < deadline )) || { echo "-1"; return 1; }
    sleep "$(awk "BEGIN{print $POLL_MS/1000}")"
  done
}

# ---------------------------------------------------------------------------
# Cluster driver (docker only; DRIVER=none makes these no-ops that skip)
# ---------------------------------------------------------------------------
driver_available() { [[ "$DRIVER" == "docker" ]]; }
peer_stop()  { ( cd "$ROOT" && $COMPOSE stop "$1" ) >/dev/null 2>&1; }
peer_start() { ( cd "$ROOT" && $COMPOSE start "$1" ) >/dev/null 2>&1; }

wait_node_up() {
  local p="$1" deadline=$(( $(date +%s) + BOOT_TIMEOUT_S ))
  while :; do
    status_of "$p" >/dev/null 2>&1 && return 0
    (( $(date +%s) < deadline )) || return 1
    sleep 1
  done
}

# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------
status_table() {
  local p rows="| node | role | term | commit | last log | snapshot | leader |"
  rows+=$'\n'"|---|---|---|---|---|---|---|"
  for ((i = 1; i <= PEERS; i++)); do
    p="$(port_of "$i")"
    local s; s="$(status_of "$p")"
    if [[ -z "$s" ]]; then
      rows+=$'\n'"| peer$i | _(down)_ | | | | | |"
    else
      rows+=$'\n'"$(echo "$s" | jq -r '"| \(.id) | \(.role) | \(.term) | \(.commit_index) | \(.last_log_index) | \(.snapshot_index) | \(.leader_id // "-") |"')"
    fi
  done
  md_table "$rows"
  echo "$rows" | sed 's/|/ /g' >&2
}

summary() {
  {
    printf '\n---\n\n## Summary\n\n'
    if (( FAILURES == 0 )); then printf 'All scenarios passed.\n'
    else printf '**%d failed expectation(s).** See the ❌ entries above.\n' "$FAILURES"; fi
  } >>"$REPORT"
  echo >&2
  if (( FAILURES == 0 )); then echo "${GRN}All scenarios passed.${RST}" >&2
  else echo "${RED}${FAILURES} failed expectation(s).${RST}" >&2; fi
  echo "${CYN}Report: ${REPORT}${RST}" >&2
}

# ============================================================================
# 1. Cluster status
# ============================================================================
scenario_status() {
  step "1. Cluster status"

  LEADER_PORT="$(wait_for_leader)" || bad "no leader within ${BOOT_TIMEOUT_S}s"
  LEADER_ID="$(status_of "$LEADER_PORT" | jq -r .id)"
  ok "leader is ${LEADER_ID} (debug port ${LEADER_PORT})"

  status_table

  # Exactly one leader. Two means a split brain, which is the single worst thing
  # this suite could find, so it is checked before anything is written.
  local leaders; leaders="$(for ((i = 1; i <= PEERS; i++)); do
    status_of "$(port_of "$i")" | jq -r 'select(.is_leader == true) | .id'
  done | wc -l)"
  [[ "$leaders" == "1" ]] && ok "exactly one leader" || bad "expected 1 leader, found ${leaders}"

  # Everyone agrees on who it is, and on the term.
  local agree=1 t lt
  lt="$(status_of "$LEADER_PORT" | jq -r .term)"
  for ((i = 1; i <= PEERS; i++)); do
    local s; s="$(status_of "$(port_of "$i")")"
    [[ -z "$s" ]] && continue
    t="$(echo "$s" | jq -r .term)"
    [[ "$t" == "$lt" ]] || agree=0
    echo "$s" | jq -e '.is_leader == true' >/dev/null && continue
    [[ "$(echo "$s" | jq -r .leader_id)" == "$LEADER_ID" ]] || agree=0
  done
  (( agree == 1 )) && ok "all nodes agree on leader ${LEADER_ID} at term ${lt}" \
                   || bad "nodes disagree on the leader or the term"
}

# ============================================================================
# 2. Propose
# ============================================================================
scenario_propose() {
  step "2. Propose"

  call "set through the leader" POST "$HOST:$LEADER_PORT/kv/set" \
    '{"key":"flow:a","value":{"n":1}}'
  [[ "$CODE" == "200" ]] && ok "leader accepted the write (index $(echo "$RESP" | jq -r '.index // "?"'))" \
                         || bad "leader rejected the write: HTTP ${CODE}"

  # A follower must refuse and name the leader, or clients have no way to redirect.
  local fp
  for ((i = 1; i <= PEERS; i++)); do
    fp="$(port_of "$i")"
    [[ "$fp" == "$LEADER_PORT" ]] && continue
    status_of "$fp" >/dev/null 2>&1 && break
  done
  call "set through a follower (expected to fail)" POST "$HOST:$fp/kv/set" \
    '{"key":"flow:reject","value":1}'
  if [[ "$CODE" != "200" ]] && [[ "$(echo "$RESP" | jq -r '.leader_id // empty')" == "$LEADER_ID" ]]; then
    ok "follower refused the write and named ${LEADER_ID}"
  else
    bad "follower did not refuse-and-redirect (HTTP ${CODE})"
  fi
}

# ============================================================================
# 3. Get
# ============================================================================
scenario_get() {
  step "3. Get from every node"

  local ms; ms="$(wait_converged "flow:a" '{"n":1}')"
  if [[ "$ms" == "-1" ]]; then bad "flow:a never converged across all nodes"
  else ok "flow:a converged on all ${PEERS} nodes in ${ms}ms"; fi

  local p
  for ((i = 1; i <= PEERS; i++)); do
    p="$(port_of "$i")"
    call "get flow:a from peer$i" GET "$HOST:$p/kv/get?key=flow:a"
  done

  call "get a missing key (expected 404)" GET "$HOST:$LEADER_PORT/kv/get?key=flow:nope"
  [[ "$CODE" == "404" ]] && ok "missing key returns 404" || bad "missing key returned ${CODE}, expected 404"
}

# ============================================================================
# 4. Applied latency — set / delete / cas in rapid succession
# ============================================================================
scenario_applied() {
  step "4. Applied latency"

  md_note "\`/kv/set\` returns once the **leader** has applied the entry, so its own latency is
commit + apply on one node. Convergence is measured separately by polling
\`/kv/get\` on every node until they agree — that is the number that says
replication finished."

  sub "4a. ${WRITE_BURST} sets in rapid succession"
  local start total i ms
  local -a lat=()
  start="$(now_ms)"
  for ((i = 1; i <= WRITE_BURST; i++)); do
    local t0 t1
    t0="$(now_ms)"
    curl -sS --max-time 30 -o /dev/null -X POST "$HOST:$LEADER_PORT/kv/set" \
      -d "{\"key\":\"burst:$i\",\"value\":$i}" 2>/dev/null
    t1="$(now_ms)"
    lat+=( $(( t1 - t0 )) )
  done
  total=$(( $(now_ms) - start ))

  local sum=0 max=0 min=999999
  for ms in "${lat[@]}"; do
    sum=$(( sum + ms )); (( ms > max )) && max=$ms; (( ms < min )) && min=$ms
  done
  local avg=$(( sum / WRITE_BURST ))
  ok "${WRITE_BURST} sets in ${total}ms — per-write commit+apply min ${min}ms / avg ${avg}ms / max ${max}ms"
  md_table "| metric | ms |
|---|---|
| total for ${WRITE_BURST} writes | ${total} |
| per-write min | ${min} |
| per-write avg | ${avg} |
| per-write max | ${max} |"

  ms="$(wait_converged "burst:${WRITE_BURST}" "$WRITE_BURST")"
  if [[ "$ms" == "-1" ]]; then bad "the burst never converged across all nodes"
  else ok "last burst entry converged on all nodes ${ms}ms after the burst ended"; fi

  sub "4b. delete"
  local t0; t0="$(now_ms)"
  call "delete burst:1" POST "$HOST:$LEADER_PORT/kv/delete" '{"key":"burst:1"}'
  [[ "$CODE" == "200" ]] && ok "delete applied on the leader in $(( $(now_ms) - t0 ))ms" \
                         || bad "delete failed: HTTP ${CODE}"
  ms="$(wait_absent "burst:1")"
  if [[ "$ms" == "-1" ]]; then bad "the delete never converged"
  else ok "delete converged on all nodes in ${ms}ms"; fi

  sub "4c. compare-and-swap"
  call "seed cas:key = 1" POST "$HOST:$LEADER_PORT/kv/set" '{"key":"cas:key","value":1}'

  t0="$(now_ms)"
  call "cas 1 -> 2 (expected to match)" POST "$HOST:$LEADER_PORT/kv/cas" \
    '{"key":"cas:key","expected":1,"value":2}'
  [[ "$CODE" == "200" ]] && ok "matching cas applied in $(( $(now_ms) - t0 ))ms" \
                         || bad "matching cas failed: HTTP ${CODE}"
  ms="$(wait_converged "cas:key" "2")"
  [[ "$ms" == "-1" ]] && bad "cas result never converged" || ok "cas result converged in ${ms}ms"

  call "cas 1 -> 9 (expected to be refused)" POST "$HOST:$LEADER_PORT/kv/cas" \
    '{"key":"cas:key","expected":1,"value":9}'
  [[ "$CODE" == "409" ]] && ok "mismatched cas refused with 409, apply loop unaffected" \
                         || bad "mismatched cas returned ${CODE}, expected 409"

  call "value after the refused cas" GET "$HOST:$LEADER_PORT/kv/get?key=cas:key"
  [[ "$(echo "$RESP" | jq -c .value)" == "2" ]] && ok "refused cas left the value untouched" \
                                                || bad "refused cas changed the value"

  # A refused command must not stop the node — that is the whole ErrCommandFailed split.
  status_of "$LEADER_PORT" | jq -e '.is_leader == true' >/dev/null \
    && ok "leader still healthy after a refused command" \
    || bad "leader unhealthy after a refused command"
}

# ============================================================================
# 5. Lag, snapshot, rejoin
# ============================================================================
scenario_snapshot_rejoin() {
  step "5. Stop a follower, snapshot, rejoin"

  if ! driver_available; then
    warn "DRIVER=none — skipping (needs to stop and start a container)"
    return
  fi

  # Pick a follower to sideline.
  local victim="" vport=""
  for ((i = 1; i <= PEERS; i++)); do
    local p; p="$(port_of "$i")"
    [[ "$p" == "$LEADER_PORT" ]] && continue
    victim="peer$i"; vport="$p"; break
  done
  info "sidelining ${victim}"

  peer_stop "$victim"
  sleep 2
  status_of "$vport" >/dev/null 2>&1 && bad "${victim} still answering after stop" || ok "${victim} is down"

  # Write past the snapshot threshold so the leader compacts while the follower is
  # away. If the log it needs is gone, catch-up has to go through InstallSnapshot —
  # which is the only way that path gets exercised by this suite.
  sub "5a. write past the snapshot threshold"
  local i
  for ((i = 1; i <= WRITE_BURST; i++)); do
    curl -sS --max-time 30 -o /dev/null -X POST "$HOST:$LEADER_PORT/kv/set" \
      -d "{\"key\":\"gap:$i\",\"value\":$i}" 2>/dev/null
  done
  ok "wrote ${WRITE_BURST} entries while ${victim} was down"

  info "waiting for the snapshot loop…"
  local deadline=$(( $(date +%s) + BOOT_TIMEOUT_S )) snap=0
  while (( $(date +%s) < deadline )); do
    snap="$(status_of "$LEADER_PORT" | jq -r '.snapshot_index // 0')"
    (( snap > 0 )) && break
    sleep 2
  done
  if (( snap > 0 )); then ok "leader snapshotted at index ${snap}"
  else warn "no snapshot within ${BOOT_TIMEOUT_S}s — rejoin will catch up from the log, not a snapshot"; fi

  call "leader status before rejoin" GET "$HOST:$LEADER_PORT/status"

  sub "5b. rejoin"
  peer_start "$victim"
  wait_node_up "$vport" || bad "${victim} did not come back within ${BOOT_TIMEOUT_S}s"

  local start; start="$(now_ms)"
  local ms; ms="$(wait_converged "gap:${WRITE_BURST}" "$WRITE_BURST")"
  if [[ "$ms" == "-1" ]]; then
    bad "${victim} never caught up after rejoining"
    call "${victim} status after failed catch-up" GET "$HOST:$vport/status"
  else
    ok "${victim} caught up $(( $(now_ms) - start ))ms after restart"
  fi

  call "${victim} status after rejoin" GET "$HOST:$vport/status"
  status_table
}

# ============================================================================
# 6. addMember
# ============================================================================
scenario_add_member() {
  step "6. addMember"

  if ! status_of "$JOIN_PORT" >/dev/null 2>&1; then
    warn "${JOIN_PEER} is not reachable on ${JOIN_PORT} — skipping (start it, but keep it out of peers.yaml)"
    return
  fi

  call "add ${JOIN_PEER} as a VOTER" POST "$HOST:$LEADER_PORT/cluster/add" \
    "{\"id\":\"${JOIN_PEER}\",\"rpc_url\":\"${JOIN_RPC}\",\"peer_state\":\"VOTER\"}"
  if [[ "$CODE" != "200" ]]; then
    bad "addMember failed: HTTP ${CODE} — $(echo "$RESP" | jq -r '.error_msg // empty')"
    return
  fi
  ok "${JOIN_PEER} added"

  # The addition is only real once the new member holds the data it never saw
  # being written.
  PEERS=$((PEERS + 1))
  local ms; ms="$(wait_converged "flow:a" '{"n":1}')"
  if [[ "$ms" == "-1" ]]; then bad "${JOIN_PEER} joined but never caught up"
  else ok "${JOIN_PEER} caught up and serves earlier writes (${ms}ms)"; fi

  call "leader view of the new configuration" GET "$HOST:$LEADER_PORT/status"
  echo "$RESP" | jq -e --arg id "$JOIN_PEER" '.peers[$id].peer_state == "VOTER"' >/dev/null \
    && ok "${JOIN_PEER} is a VOTER in the leader's configuration" \
    || bad "${JOIN_PEER} is not a VOTER in the leader's configuration"

  status_table
}

# ============================================================================
# 7. removeMember — a follower, then the leader
# ============================================================================
scenario_remove_member() {
  step "7. removeMember"

  sub "7a. remove a follower"
  local victim=""
  for ((i = 1; i <= PEERS; i++)); do
    local p; p="$(port_of "$i")"
    [[ "$p" == "$LEADER_PORT" ]] && continue
    status_of "$p" >/dev/null 2>&1 && { victim="peer$i"; break; }
  done
  # Prefer the node we joined in scenario 6, so the suite leaves the cluster as it
  # found it.
  status_of "$JOIN_PORT" >/dev/null 2>&1 && \
    curl -sS "$HOST:$LEADER_PORT/status" | jq -e --arg id "$JOIN_PEER" '.peers[$id]' >/dev/null 2>&1 && \
    victim="$JOIN_PEER"

  call "remove ${victim}" POST "$HOST:$LEADER_PORT/cluster/remove" "{\"id\":\"${victim}\"}"
  if [[ "$CODE" == "200" ]]; then
    ok "${victim} removed"
    echo "$RESP" | jq -e --arg id "$victim" '.peers[$id] == null' >/dev/null \
      && ok "${victim} is gone from the configuration" \
      || bad "${victim} still present in the returned configuration"
    [[ "$victim" == "$JOIN_PEER" ]] && PEERS=$((PEERS - 1))
  else
    bad "removing ${victim} failed: HTTP ${CODE} — $(echo "$RESP" | jq -r '.error_msg // empty')"
  fi

  # The cluster must still make progress on a smaller configuration.
  call "write after the removal" POST "$HOST:$LEADER_PORT/kv/set" '{"key":"after:remove","value":1}'
  [[ "$CODE" == "200" ]] && ok "cluster still accepts writes after the removal" \
                         || bad "cluster stopped accepting writes after the removal"

  sub "7b. the leader removes itself"
  local old_leader="$LEADER_ID"
  call "remove the leader (${old_leader})" POST "$HOST:$LEADER_PORT/cluster/remove" \
    "{\"id\":\"${old_leader}\"}"
  if [[ "$CODE" != "200" ]]; then
    bad "leader self-removal failed: HTTP ${CODE} — $(echo "$RESP" | jq -r '.error_msg // empty')"
    return
  fi
  ok "${old_leader} removed itself"

  # Ongaro §4.2.2: the removed leader replicates the new configuration without
  # counting itself, then steps down. Somebody else has to take over.
  local newp; newp="$(wait_for_leader)" || { bad "no leader after the old one removed itself"; return; }
  local newid; newid="$(status_of "$newp" | jq -r .id)"
  if [[ "$newid" == "$old_leader" ]]; then
    bad "${old_leader} removed itself but is still leader"
  else
    ok "leadership moved to ${newid}"
    LEADER_PORT="$newp"; LEADER_ID="$newid"
  fi

  call "write through the new leader" POST "$HOST:$LEADER_PORT/kv/set" '{"key":"after:selfremove","value":1}'
  [[ "$CODE" == "200" ]] && ok "the surviving cluster accepts writes" \
                         || bad "the surviving cluster cannot accept writes"

  status_table
}

# ============================================================================
# Run
# ============================================================================
md_init
echo "${CYN}Report: ${REPORT}${RST}" >&2

# Scenario 1 always runs: everything after it needs the leader it discovers.
wants() { [[ ",$SCENARIOS," == *",$1,"* ]]; }

scenario_status
wants 2 && scenario_propose
wants 3 && scenario_get
wants 4 && scenario_applied
wants 5 && scenario_snapshot_rejoin
wants 6 && scenario_add_member
wants 7 && scenario_remove_member

md_note "_Scenarios run: ${SCENARIOS}_"

summary
exit $(( FAILURES > 0 ? 1 : 0 ))
