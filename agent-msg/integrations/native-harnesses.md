# Native harness installation and delivery

Resonance 0.6.0 shares exact session discovery, a durable bus, leased presence,
session-pinned sends and bounded delivery across the installed harnesses. Native
wake capabilities differ; installing a skill does not activate a receiver.

| Harness | Ordinary configured launch | Idle delivery | Verification |
| --- | --- | --- | --- |
| Codex | Terminal hooks through the session service (plain `codex` daemon or `--no-daemon`), owner-configured initial idle binding or explicit queue binding | Existing bounded bridge | Real queue and Unix endpoint fixtures |
| Pi 0.84.3 | Global extension, exact session lifecycle | `sendMessage` with `triggerTurn` and `followUp` | Real idle RPC process, local deterministic model |
| OpenCode 1.18.25 | Global plugin, attach on first actual prompt | Same session's `promptAsync` | Real server process, local deterministic model |
| Claude Code 2.1.282 | Lifecycle and prompt hooks | Optional native Channels transport, requires per-launch opt-in | Real prompt hooks; channel identity/receipt contract tests; live channel consent not exercised |
| Kimi Code 0.38.0 | Lifecycle and prompt hooks | Next user prompt only in the ordinary terminal | Real prompt hooks and config doctor; no verified terminal idle API |
| T3 Code 0.1.11 (Claude, Codex) | Same Claude/Codex hooks; the T3 thread is the owner | Fixed doorbell turn via `t3 thread send`; the prompt hook delivers the mail | Real hooks against the live T3 server; idle Claude and Codex threads woken |

Claude, Kimi and terminal Codex hooks register through the per-bus session
service (`agent-msg-service`), started on first use and kept until reboot.

Kimi is the current `kimi-code` binary and uses `~/.kimi-code/config.toml`.
Its documented REST/WebSocket API belongs to `kimi web`. It does not establish
an attachment to an already-running ordinary terminal. This integration does not
start a second process against that terminal's saved history. Web-mode push is
not implemented here.

## T3 Code hosted sessions

T3 Code starts a fresh provider process per turn, so process liveness cannot
carry an address between turns. When a Claude hook sees a valid
`T3CODE_THREAD_ID` (a UUID, `mcp:<UUID>`, or `mcp:launch:<64 hex>` for threads started by T3's launch tool), the service
records the T3 thread as owner: the address stays live while the thread exists
and is not archived, `SessionEnd` at turn end does not retire it, and a new
provider session in the same thread (for example `/clear`) replaces the old
address. A running pid-owned session upgrades in place on its next hook.
Liveness is checked with `t3 thread show` every 30 s; four consecutive failures
retire the address (T3 reports a gone thread and an unreachable server alike).

Idle delivery rings the thread with `t3 thread send` (`t3` from `PATH`,
`~/.local/bin/t3`, or `RESONANCE_T3_EXECUTABLE`). The doorbell text is fixed and
never contains peer content; the turn it starts fires the ordinary prompt hook,
which delivers the mail with the usual untrusted-peer framing. A busy thread
gets the doorbell as its next turn. One doorbell is outstanding at a time and is
never retried; the budget is 20 per incarnation with a 30 s cooldown, reset only
by `agent-msg-native-control … resume`, which also controls T3 receivers. Peer
cards show `delivery: t3_thread`.

Codex under T3 uses the same owner and doorbell. T3 sets `T3CODE_THREAD_ID`
only for tool shells and stdio MCP servers, not hooks, so a Codex hook whose
parent is a `codex app-server` started by a `t3` server resolves its thread from
T3's state store (`<T3CODE_HOME or ~/.t3>/userdata/statev2.sqlite`, read-only):
exactly one provider-thread record whose `nativeThreadRef` is the hook's Codex
thread with `strength: strong`. Anything else — no or several matches, an
unknown schema, an app-server not started by T3 (the Codex desktop app) — fails
closed and the hook refuses to attach. This reads T3 internals; recheck it after
a T3 update. A T3-owned Codex thread is never bound to the queue bridge.

## Install

Build/install the wheel in the project `.venv`, then preview and apply:

```bash
python3 tools/install_native_harnesses.py
python3 tools/install_native_harnesses.py --apply
```

The installer merges Claude's hooks, appends a marked Kimi hook block, and installs
Pi/OpenCode entrypoints plus the shared adapter files. It preserves other hooks
and settings, refuses unmanaged extension conflicts, and backs up changed
configuration under `~/.local/state/resonancecore/install-backups/`. It is
idempotent. OpenCode manages its matching plugin SDK in its normal config
directory; this installation does not replace the global OpenCode tool.

Start new sessions to load the adapters. Pi can also reload its extensions using
its native reload facility. Existing processes retain their loaded code.
An OpenCode session becomes visible only after an actual prompt in the attached
host; saved sessions are not evidence of a running conversation.

## Claude channel consent

Normal Claude launches use prompt-time delivery. To activate native Channels,
launch with the separately installed MCP configuration and Claude's own opt-in:

```bash
claude --mcp-config ~/.config/resonancecore/claude-channel.json \
  --dangerously-load-development-channels server:resonance
```

Follow the native consent prompt. The installer does not accept it or change
organization controls. Supported account/provider access is also required.
The channel uses lifecycle hook evidence for the current session, including
`/clear` and interactive resume; the startup `CLAUDE_CODE_SESSION_ID` alone can
become stale. A notification written to stdout remains `unknown` until the
receiving model calls `resonance` with operation `acknowledge` and its message ID.
This receipt does not authorize work and does not send a peer reply. Unconfirmed
notifications hold later delivery and are never automatically retried.

## Addressing and controls

Use the native `resonance` tool in Pi, OpenCode and channel-enabled Claude:
`whoami`, `peers`, `resolve`, `status`, `send`. The `arguments` field is a JSON
object encoded as a string. Sends require `to`, `session`, `body`; replies also
require `type: "reply"` and `reply_to`. Sender identity comes from the exact
native attachment. Follow existing owner authorization before sending.

Ordinary Claude/Kimi hook context prints the exact CLI form:

```bash
agent-msg-harness --harness kimi --thread EXACT_NATIVE_SESSION exec -- whoami
agent-msg-harness --harness kimi --thread EXACT_NATIVE_SESSION exec -- peers
```

Peer cards expose `delivery` (`next_prompt`, `native_event`, `claude_channel`,
or `codex_bridge`) and `wakeup`. Native adapters allow 20 submissions per
attachment, with a 30-second cooldown. Explicit owner controls are:

```bash
agent-msg-native-control --recipient EXACT_RECIPIENT --session EXACT_SESSION status
agent-msg-native-control --recipient EXACT_RECIPIENT --session EXACT_SESSION pause
agent-msg-native-control --recipient EXACT_RECIPIENT --session EXACT_SESSION resume
```

Resume resets the budget; it never retries or clears uncertain submissions.
`wakeup: false` can mean paused, budget exhausted, inactive or a pending unknown
receipt; inspect status. Native presence and queued acceptance do not prove a
model acted on a message. CLI draining is inspection-only (`--keep-unread`) for
an inbox owned by a native receiver, including while paused.

The shared SQLite bus remains the local legacy trust model. Host checks and
session pinning prevent accidental attachment mistakes; they are not process
authentication against a malicious same-user process. Protected broker contexts
refuse these legacy adapters rather than downgrade credentials or authorization.

## Verification and upstream contracts

Source checks: `pytest agent-msg/tests`; transport checks:
`node --test agent-msg/tests/native-peer.test.mjs`. Opt-in installed-binary
fixtures: `RESONANCE_NATIVE_LIVE=1 pytest agent-msg/tests/test_native_live.py
agent-msg/tests/test_native_hook_live.py`. They use isolated homes, fixture-only
credentials and a deterministic loopback model, and send no real peer messages.

- [Claude Channels reference](https://code.claude.com/docs/en/channels-reference)
- [Claude Channels consent and availability](https://code.claude.com/docs/en/channels)
- [OpenCode server APIs](https://opencode.ai/docs/server/)
- [Kimi hooks](https://moonshotai.github.io/kimi-code/en/customization/hooks)
- [Kimi server API source](https://github.com/MoonshotAI/kimi-code/blob/main/docs/en/reference/server-api.md)
- Pi extension contract: installed `@earendil-works/pi-coding-agent/docs/extensions.md`.

Run `agent-msg-service t3-check --project /path/to/t3/project` to report the CLI
version, thread-list reachability, and whether the Codex provider-thread table
and payload column can be queried. For a nondefault data directory pass
`--state /path/to/userdata/statev2.sqlite` and configure `T3CODE_HOME` for the
CLI. This is a structural check. A T3 upgrade can also change the JSON
payload contract. The resolver requires exactly one strong Codex mapping.

The Go backend uses the same receiver table, per-incarnation budget and
T3 thread address semantics. Both backends assume the local OS account and its
hook environment are trusted. Run the live test only against a configured T3
project with both provider hooks installed and a bus you control.
