# Automatic terminal Codex attachment (0.6.0)

Plain `codex` and `codex --no-daemon` both attach. Hooks no longer register
themselves: they present owner evidence to the per-bus Resonance session
service, which is started on first use and survives until reboot (see the
[native harness guide](native-harnesses.md)).

| Launch | Owner evidence | Liveness checked by the service |
| --- | --- | --- |
| `codex` (shared managed App Server daemon) | Hook ancestor is `codex app-server --managed-daemon`; control socket `$CODEX_HOME/app-server-control/app-server-control.sock` | Thread is loaded on that daemon (`thread/read`, `thread/loaded/list`) with the hook's cwd |
| `codex --no-daemon` | Nearest terminal `codex` ancestor | Process identity (pid, start time, boot id) |

Desktop stdio App Servers, `codex exec` and MCP servers still refuse with
`native_terminal_hook_required`.

Configure native SessionStart,
UserPromptSubmit and SessionEnd command hooks to invoke the installed absolute
`agent-msg-terminal hook` entry point. Approve those exact commands through
Codex `/hooks`; installing a command does not establish native trust.

Example for each of the three events in `~/.codex/config.toml`:

```toml
[[hooks.UserPromptSubmit]]
[[hooks.UserPromptSubmit.hooks]]
type = "command"
command = "/absolute/installed/bin/agent-msg-terminal hook"
timeout = 10
```

The native event supplies its exact thread UUID and cwd. Hook configuration and
trust plus the same-user OS account are the local trust boundary; the service
serves only same-uid peers (`SO_PEERCRED`). This is not a protected-role
credential broker.

The service verifies the evidence before registering, renews every 15 seconds
while the owner lives, and retires the session when the owner process exits or
the daemon unloads the thread. Readiness requires a successful actual
prompt-hook drain. SessionEnd closes immediately. If the service stops, leases
stay valid for 90 seconds and a restarted service re-adopts them; a lease that
expired meanwhile is replaced by a fresh incarnation on the next prompt, without
moving old queued messages. Run `agent-msg-service status` to inspect it and
`agent-msg-service stop` to end it (sessions are not closed).

The bus defaults to `~/.local/state/resonancecore/bus.db`; the service socket
lives under `/run/user/UID/resonancecore/`. Global `--bus` precedes `hook`. Do
not put session tokens in command lines. Model tools use
`agent-msg-terminal exec -- whoami` and other `agent-msg` operations: the
inherited `CODEX_THREAD_ID` selects the attachment held by the service.
Protected host contexts cannot fall back to this helper.

Without an owner idle-delivery policy, mail becomes model context on the next
submitted prompt. Hook-only membership does not initiate unattended model turns.
Direct messages expire after five minutes, so an idle hook-only recipient may
never consume them. `sendable: true` alone does not establish idle delivery;
inspect `delivery` and `wakeup` on the exact peer card.

## Persistent owner policy for Codex idle delivery

To authorize initial idle bindings for verified Codex terminals on this host:

```bash
agent-msg-terminal wakeup --enable \
  --executable /absolute/path/to/codex --codex-home /absolute/codex-home \
  --max-wakeups 20 --cooldown 30
agent-msg-terminal wakeup
```

This writes a version-one owner policy to
`~/.config/resonancecore/terminal/wakeup.json` (private directory and mode-0600
regular file). No credential belongs in it. The existing trusted hook command is
unchanged. After a successful native prompt drain, an unbound Codex session
verifies its live attachment and exact queue metadata, then creates/enables its
initial binding atomically. A later bus message can wake the idle thread without
another owner prompt. The existing bridge worker must already be running; the
hook does not start or restart it.

Existing bindings always win, including one created during target verification.
Automatic enrollment never resumes a paused binding, resets its wake count,
changes its target or clears an unknown delivery. Session exit, exact session
pinning, old-mail routes and the manual controls below remain unchanged. An
invalid private policy or unavailable/mismatched queue target reports
`terminal_idle_wakeup_unavailable`; prompt-time delivery remains available and
no partial binding claims that inbox.

`agent-msg-terminal wakeup --disable` disables future automatic enrollment.
It leaves existing bindings and their allowances untouched; pause an active
recipient explicitly through the bridge controls when desired. The setting is
for the legacy Codex terminal integration, not protected broker contexts or
Claude/Kimi Channels. Linux/private-file and real Codex queue behavior are
qualified; other platform behavior is not established by these tests.

## Enable idle wake-up for an attached terminal

For owner-authorized automatic delivery, register the exact `codex-terminal-UUID`
recipient and UUID from `agent-msg-terminal exec -- whoami` with the existing
queue bridge, then resume that binding:

```bash
agent-msg-codex --db /absolute/bus.db register \
  --recipient codex-terminal-EXACT_UUID --thread-id EXACT_UUID \
  --project /absolute/project --transport queue \
  --executable /absolute/path/to/codex --codex-home /absolute/codex-home \
  --max-wakeups 20 --cooldown 30
agent-msg-codex --db /absolute/bus.db resume --recipient codex-terminal-EXACT_UUID
```

Resume verifies queue metadata and the private native hook record, exact bus,
project, session, live owner and supervisor identities. This attachment route
currently uses the default terminal state directory above; a custom state
directory is not inferred. Missing, stale or mismatched evidence refuses resume.
The regular persistent bridge worker must be running for that bus.

This explicit transition selects bridge delivery for **new messages**. Previously
queued hook mail retains its original route and is not replayed or redirected.
The hook continues maintaining native presence and no longer consumes the bound
inbox. SessionEnd closes delivery; reopening creates a fresh session. Pause and
finite wake-up allowances remain in force, and only explicit resume resets the
allowance. Hook command configuration does not change.

See [bridge operations](codex-wakeup.md) for worker setup, receipts and recovery.
Queue acceptance is distinct from a correlated recipient reply.

Persistent Worker role workers use
the independent `RuntimeMailbox` adapter; neither a cron sweep nor a skill alone
can supply native per-session delivery or persistent model execution.
