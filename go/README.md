# Resonance native service in Go

This service provides the native messaging foundation beside the existing Python
and TypeScript adapters. It supports detached start/status/stop, SQLite session
attachment/lookup/supervision, pinned send/list/drain/replies and acknowledgements.
Windows uses current-user SID secured named pipes with peer-token verification.
Linux uses a private socket directory and peer-UID verification. Session ownership
includes process creation identity; PID alone is insufficient.

With Go 1.27 or newer, run from this directory:

```text
go build -trimpath -o resonance-service ./cmd/resonance-service
go test ./...
```

On Windows use `-o resonance-service.exe`. The Python service adapter selects Go
automatically on Windows. For an installed Python package, set
`RESONANCE_GO_BIN` to the absolute path of the built executable in the
environment of the launcher and hooks. A source checkout also discovers the
binary built in this directory.

On Linux, stop the existing Python service before
selecting `RESONANCE_SERVICE_BACKEND=go`; use the same `RESONANCE_RUNTIME_DIR` for
the service and its callers, and keep one service backend per SQLite bus.

Use `resonance-service --bus ABSOLUTE_DB start`, `status`, `stop` or `request`
(JSON on stdin). Native message commands include `send --from SENDER --to RECIPIENT
BODY`, `list --as RECIPIENT` and `drain --as RECIPIENT --session SESSION`.
CLI drain prints successfully before acknowledgement. Raw request drain returns
a snapshot and requires a separate ack. Reads are capped at 1,000 messages and
responses at 1 MB. Native and Codex consumers keep exclusive ownership.
Windows stop waits for the service process to exit before returning, so SQLite
file handles are released before callers clean up their fixture or data directory.

The listener never replaces an existing endpoint, file or symlink. Inspect a
stale owned Unix socket before deliberately removing it. Peer messages grant no
owner authorization; exact-thread evidence never resumes or guesses a thread.
Existing bridge, broadcast and harness orchestration remain Python/TypeScript.

Windows amd64 and Linux amd64 under Ubuntu WSL were verified. Other OSes remain
unverified. `../agent-msg/tests/test_go_integration.py` exercises the actual host
binary against Python-created SQLite buses, process retirement and session pins.
Build the host executable before running this isolated fixture.
