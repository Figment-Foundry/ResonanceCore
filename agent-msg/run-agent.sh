#!/usr/bin/env bash
# Usage: run-agent.sh <agent-id> <bus-db> <harness> [harness args...]
# Example: run-agent.sh codex-auth-refactor ~/proj/.agent-msg/bus.db codex
set -euo pipefail
ID="$1"; DB="$2"; HARNESS="$3"; shift 3
export AGENT_MSG_ID="$ID"
export AGENT_MSG_DB="$DB"
exec agent-msg run --as "$ID" --harness "$HARNESS" -- "$HARNESS" "$@"
