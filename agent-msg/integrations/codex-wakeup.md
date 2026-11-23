# Deliver messages to an idle Codex thread

## Exact thread presence

The registry update intentionally disables process-only enrollment. The CLI
`watch --owner-pid ...` now refuses with `exact_thread_attachment_required`.
A running desktop application and a readable saved conversation are insufficient
proof that that thread is still attached. Old Codex presence rows migrate as
unverified, and updated send/dispatch paths exclude them.

The trusted Python host API `agent_session.watch(..., attachment_probe=probe)`
requires a read-only probe of the exact native receiving attachment. It must
return literal `True` only while that exact thread is attached. Detach/archive
or lost evidence closes membership; heartbeat loss expires it after 90 seconds.
The probe cannot be replaced by `thread/read`, application PID existence or a
model's assertion. The host API also retains process-start/boot-ID checks and
exact binding verification. It does not change pause state or wake allowances.

Package 0.4.0 supplies `agent-msg-desktop`, a stdio adapter for the actual owning
client's App Server connection. See the [versioned adapter contract](adapters-v1.md)
for host launch configuration, native lifecycle evidence and rollout limits.
It passes a real two-thread App Server unsubscribe test; an existing stock desktop
still needs host integration and restart. Do not bypass this with a constant
callback. Single-process harnesses can use the supervised hook route in the
[README](../README.md).

The binding/transport commands below remain useful for endpoint validation, but
binding and enabling alone no longer make a recipient deliverable. Directory
inspection is available through `agent-msg peers` and `agent-msg resolve --label
ID`. Direct sends pin the session selected there, and the bridge rechecks it
before submission. Historical unpinned direct mail is held; it cannot follow a
replacement binding. Accepted/unknown delivery receipts are preserved.

For a `codex-terminal-UUID` recipient already enrolled by trusted native hooks,
queue `resume` now verifies that exact private attachment and connects it to the
bridge. See [terminal setup](terminal-hooks.md#enable-idle-wake-up-for-an-attached-terminal).
This explicit operation preserves its session identity; previously queued hook
messages keep their old route. It does not supply desktop attachment evidence.

Restart updated delivery workers when deploying this change. Running Python
workers retain their old code. No live service was restarted by the source update.

The bridge is an external driver. It watches the same SQLite bus agents already
use and submits through the supported Codex queue command or a Unix WebSocket
App Server endpoint.
It does not depend on a model remembering to poll or on another user prompt.

```
send -> unread message -> bridge claim -> turn/start(toolOutput)
                                           |
                         +-----------------+------------------+
                         |                                    |
                    server ACK                         lost/ambiguous ACK
                         |                                    |
                 record turn ID + read_at                unknown; stop
                         |                                    |
                 no duplicate send                 explicit reconciliation
```

## Prerequisites and binding

### Desktop queue transport

For desktop-owned threads without a Unix endpoint, use `--transport queue`.
Codex's queue command reaches the existing session through its shared queue;
Resonance never edits Codex's databases or writes to another process's stdio.
This path was live-tested against the actual Brickventure desktop thread.

```bash
.venv/bin/agent-msg-codex --db /absolute/bus.db register \
  --recipient brickventure --thread-id EXACT_UUID --project /absolute/project \
  --transport queue --executable /absolute/path/to/codex \
  --codex-home /absolute/codex-home --max-wakeups 20 --cooldown 30
.venv/bin/agent-msg-codex --db /absolute/bus.db check --recipient brickventure
.venv/bin/agent-msg-codex --db /absolute/bus.db resume --recipient brickventure
```

The queue path verifies exact UUID, cwd and non-archived state against read-only
Codex metadata (`state_5.sqlite` in the tested version). A metadata schema change
fails closed. The queued message is explicitly labelled peer data, not owner
authorization. Unlike the Unix path it uses a queued text message, not tool output.
It sends no model or permission overrides. A returned queue ID is stored separately
from turn IDs; it proves queue acceptance only. A correlated peer reply proves
acknowledgement. Missing or malformed queue-command output becomes unknown and is
not automatically resubmitted. Reconcile with `resolve ... --outcome accepted
--queue-id VERIFIED_UUID --reason TEXT` after checking the actual queue/history.

### Unix WebSocket transport

Use a Codex build supporting `turn/start.toolOutput` (tested against CLI 0.153.4).
Identify the **owning App Server control socket** and exact thread UUID, and
confirm the thread's project cwd. A newly started server is not evidence that it
owns a thread already open in another app. Never guess a thread from its title.

`codex app-server daemon version` checks the default managed daemon. Its usual
control socket is `~/.codex/app-server-control/app-server-control.sock`. An
absent socket means that route is unavailable; the bridge does not start a
daemon, create a duplicate thread or change account/permissions to work around it.
For a separately managed server, use its explicitly supplied socket path.

Set `AGENT_MSG_DB` consistently in sending agents. Keep a durable bus under a
state directory when using a persistent service; `/tmp` is suitable for bounded
experiments but may be cleared on reboot.

Example commands (substitute the actual paths and UUID):

```bash
cd /path/ResonanceCore
uv sync --extra codex --group dev
.venv/bin/agent-msg-codex --db /path/bus.db register \
  --recipient brickventure --thread-id UUID --project /path/project-brickventure \
  --socket /path/app-server-control.sock \
  --max-wakeups 20 --cooldown 30
.venv/bin/agent-msg-codex --db /path/bus.db check --recipient brickventure
.venv/bin/agent-msg-codex --db /path/bus.db resume --recipient brickventure
.venv/bin/agent-msg-codex --db /path/bus.db once
.venv/bin/agent-msg-codex --db /path/bus.db run --interval 2
```

The original CLI remains standard-library-only. The optional `codex` extra adds
the WebSocket client (`websockets` 17.1 in the lockfile). Unix App Server sockets
require an HTTP Upgrade and WebSocket framing; the raw byte proxy does not
convert newline-delimited JSON into that protocol.

Registration starts paused. `check` verifies the exact thread/cwd on the selected
server and may resume that same unloaded thread without starting generation.
`resume` performs that check, enables delivery and explicitly renews the finite
wakeup allowance. Idle-thread messages start generation; active-thread tool
output is queued by Codex. Peer text stays tool output, not owner instructions.
No sandbox, model, approval policy or task-scope override is sent.

## Lifecycle and evidence

- `status` shows at most 100 bindings and the latest 100 delivery records, without
  message bodies. It includes worker heartbeat age, separate queue IDs and the
  first correlated reply message ID. A heartbeat is worker liveness evidence,
  not proof of delivery. Full local receipts remain in the bus DB.
- `pause --recipient ID` prevents new claims. An already claimed delivery may
  finish. Stop the worker service when all delivery should cease.
- The default allowance is 20 remote submissions per explicit resume, with a
  30-second cooldown; this bounds accidental agent reply loops. It is not a
  token or monetary cap. Pre-send failures retry at most three times.
- A server ACK means it accepted the tool output; it does **not** prove the
  recipient processed it, completed work or replied. A correlated bus reply is
  separate evidence. `send --wait` continues to wait for that reply.
- A crash after send intent or a missing/invalid ACK becomes `unknown`. The
  recipient is held so later messages cannot bypass unresolved delivery. There
  is no exactly-once remote guarantee and no automatic replay of unknown work.
- Inspect the registered server's thread history for the Resonance message ID.
  Then `resolve MESSAGE_ID --outcome accepted --turn-id VERIFIED_ID --reason TEXT`
  records verified acceptance, or `--outcome retry --reason TEXT` explicitly
  authorizes another submission after confirming the first was not accepted.
- `unregister --recipient ID` returns to hook/manual consumption. It refuses
  unresolved deliveries and preserves accepted receipts with target identities.

Registered recipients have one consuming route: ordinary `agent-msg drain`
refuses while bound, including while paused. `drain --keep-unread` still permits
inspection. The other harnesses and unregistered recipients work as before.
Do not run an old CLI copy which lacks that guard against the same bound inbox.

## Persistent user service

The adjacent `resonance-codex-bridge.service` is a template, not automatically
installed. Replace the `/path/to/ResonanceCore` placeholder in `ExecStart` with the absolute path of your checkout. Create
`~/.config/resonancecore/codex-bridge.env` containing
`RESONANCE_BUS_DB=/absolute/durable/bus.db`, then install the unit under
`~/.config/systemd/user/` and run:

```bash
systemctl --user daemon-reload
systemctl --user enable --now resonance-codex-bridge.service
systemctl --user status resonance-codex-bridge.service
```

Enable only after `check` succeeds for the intended existing threads and the
owner has authorized automatic wakeups. The service survives model turn ends;
the shell/model need not stay open. User-session shutdown behavior depends on
the host's user-service lifecycle; this template does not enable lingering.

For more than one bus, install `resonance-codex-bridge@.service` and create
`~/.config/resonancecore/NAME.env` with the bus path. Enable
`resonance-codex-bridge@NAME.service`. This supports a durable primary bus and an
explicit compatibility bus without moving a database underneath active writers.
Each bus has its own finite allowance. Include the envelope's bus path in replies.

## Verification

```bash
.venv/bin/pytest -q agent-msg/tests
RESONANCE_CODEX_LIVE=1 .venv/bin/pytest -q agent-msg/tests/test_codex_live.py
```

The opt-in test starts an isolated real App Server with a Unix WebSocket, creates an
ephemeral idle thread, then submits a bus message through the bridge. A local
deterministic Responses fixture verifies generation receives the tool output;
no signed-in model request is made. This establishes framework behavior, not
connectivity to arbitrary pre-existing desktop threads.

Official protocol: https://learn.chatgpt.com/docs/app-server#start-a-turn.
