#!/usr/bin/env bash
#
# runbenchmark.sh — run the slabbis vs Redis performance comparison.
#
# Each binary (slabbis, redis-server) is looked up in order:
#   1. PATH
#   2. current working directory
#
# If a binary is not found its target is skipped in the output.
# Any servers started by this script are stopped on exit.
#
# Flags passed to this script are forwarded to the bench program:
#   ./runbenchmark.sh -duration 10s -concurrency 20 -value-size 512
#
# To benchmark only Redis (no slabbis server):
#   ./runbenchmark.sh -slabbis-addr ""   (leave blank to not probe)
#
set -euo pipefail

SLABBIS_PORT=6399
REDIS_PORT=6379

SLABBIS_PID=""
REDIS_PID=""
SLABBIS_LOG=$(mktemp /tmp/slabbis-bench-XXXXXX.log)
REDIS_LOG=$(mktemp /tmp/redis-bench-XXXXXX.log)

# ── cleanup ───────────────────────────────────────────────────────────────────

cleanup() {
    if [[ -n "$SLABBIS_PID" ]] && kill -0 "$SLABBIS_PID" 2>/dev/null; then
        kill "$SLABBIS_PID" 2>/dev/null || true
        wait "$SLABBIS_PID" 2>/dev/null || true
    fi
    if [[ -n "$REDIS_PID" ]] && kill -0 "$REDIS_PID" 2>/dev/null; then
        kill "$REDIS_PID" 2>/dev/null || true
        wait "$REDIS_PID" 2>/dev/null || true
    fi
    rm -f "$SLABBIS_LOG" "$REDIS_LOG"
}
trap cleanup EXIT

# ── helpers ───────────────────────────────────────────────────────────────────

find_bin() {
    local name="$1"
    if command -v "$name" &>/dev/null; then
        echo "$name"
        return
    fi
    if [[ -x "./$name" ]]; then
        echo "./$name"
        return
    fi
    echo ""
}

wait_for_port() {
    local port="$1" label="$2"
    local attempts=40  # 8 seconds total
    for ((i=0; i<attempts; i++)); do
        if (echo "" | nc -w1 127.0.0.1 "$port") &>/dev/null 2>&1; then
            return 0
        fi
        # fallback if nc is unavailable
        if (</dev/tcp/127.0.0.1/"$port") &>/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    echo "  ! $label did not come up on port $port after 8s" >&2
    return 1
}

# ── locate binaries ───────────────────────────────────────────────────────────

SLABBIS_BIN=$(find_bin "slabbis")
REDIS_BIN=$(find_bin "redis-server")

# ── start slabbis ─────────────────────────────────────────────────────────────

if [[ -n "$SLABBIS_BIN" ]]; then
    echo "  starting slabbis ($SLABBIS_BIN) on :$SLABBIS_PORT"
    "$SLABBIS_BIN" -addr "127.0.0.1:$SLABBIS_PORT" >"$SLABBIS_LOG" 2>&1 &
    SLABBIS_PID=$!
    if ! wait_for_port "$SLABBIS_PORT" "slabbis"; then
        echo "  slabbis log:" >&2
        cat "$SLABBIS_LOG" >&2
        SLABBIS_PID=""
    fi
else
    echo "  slabbis binary not found in PATH or cwd — skipping RESP target"
fi

# ── start Redis ───────────────────────────────────────────────────────────────

if [[ -n "$REDIS_BIN" ]]; then
    echo "  starting Redis  ($REDIS_BIN)  on :$REDIS_PORT"
    "$REDIS_BIN" \
        --port "$REDIS_PORT" \
        --save "" \
        --appendonly no \
        --loglevel warning \
        --daemonize no \
        >"$REDIS_LOG" 2>&1 &
    REDIS_PID=$!
    if ! wait_for_port "$REDIS_PORT" "redis-server"; then
        echo "  redis-server log:" >&2
        cat "$REDIS_LOG" >&2
        REDIS_PID=""
    fi
else
    echo "  redis-server not found in PATH or cwd — skipping Redis target"
fi

echo ""

# ── build and run bench ───────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$SCRIPT_DIR/bench"

if [[ ! -f "$BENCH_DIR/go.mod" ]]; then
    echo "  error: bench/go.mod not found — is this script in the slabbis root?" >&2
    exit 1
fi

(
    cd "$BENCH_DIR"
    go run . \
        -slabbis-addr "127.0.0.1:$SLABBIS_PORT" \
        -redis-addr   "127.0.0.1:$REDIS_PORT" \
        "$@"
)
