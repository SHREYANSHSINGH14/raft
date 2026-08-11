#!/usr/bin/env bash
# gen_proto.sh — generate Go code from .proto files
# Usage: ./gen_proto.sh [proto_file...]   (defaults to all *.proto in script dir)
#
# Output:
#   ../types  ← Go structs  (protoc-gen-go)
#   ../proto  ← gRPC stubs  (protoc-gen-go-grpc)

set -euo pipefail

# ── Resolve directories ────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_SRC="${SCRIPT_DIR}/../proto"          # where your .proto files live
OUT_TYPES="${SCRIPT_DIR}/../types" # Go structs
OUT_PROTO="${SCRIPT_DIR}/../types" # gRPC stubs

# ── Colours ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[gen]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC} $*"; }
err()     { echo -e "${RED}[err]${NC}  $*" >&2; }

# ── Dependency check ───────────────────────────────────────────────────────────
check_dep() {
  if ! command -v "$1" &>/dev/null; then
    err "Missing: $1 — $2"
    exit 1
  fi
}

check_dep protoc            "install via: brew install protobuf  OR  apt install protobuf-compiler"
check_dep protoc-gen-go     "install via: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
check_dep protoc-gen-go-grpc "install via: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"

# ── Ensure output dirs exist ───────────────────────────────────────────────────
mkdir -p "${OUT_TYPES}" "${OUT_PROTO}"

# ── Collect proto files ────────────────────────────────────────────────────────
if [[ $# -gt 0 ]]; then
  PROTO_FILES=("$@")
else
  mapfile -t PROTO_FILES < <(find "${PROTO_SRC}" -maxdepth 1 -name "*.proto")
fi

if [[ ${#PROTO_FILES[@]} -eq 0 ]]; then
  warn "No .proto files found in ${PROTO_SRC}"
  exit 0
fi

# ── Generate ───────────────────────────────────────────────────────────────────
for proto in "${PROTO_FILES[@]}"; do
  info "Generating: $(basename "${proto}")"

  protoc \
    --proto_path="${PROTO_SRC}" \
    --proto_path="${GOPATH:-$HOME/go}/pkg/mod/github.com/googleapis/googleapis@latest" \
    \
    --go_out="${OUT_TYPES}" \
    --go_opt=paths=source_relative \
    \
    --go-grpc_out="${OUT_PROTO}" \
    --go-grpc_opt=paths=source_relative \
    \
    "${proto}"
done

info "Done."
echo -e "  structs → ${OUT_TYPES}"
echo -e "  stubs   → ${OUT_PROTO}"