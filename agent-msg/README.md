# agent-msg — cross-harness agent message bus

Lets coding agents running in parallel under different harnesses
(Claude Code, Codex, Kimi, opencode, pi) send each other addressed
messages over a shared SQLite database.

## Security boundary

The current bus assumes cooperating peers with the same trust and authority.
It does not authenticate sender labels or enforce project/thread access control.
A database writer can read other inboxes and modify bridge bindings, including
the queue executable. Read the [security boundary and threat model](../SECURITY.md)
before connecting agents with different permissions.

Inbox records are displayed as JSON with escaped control characters and an
untrusted-peer notice. `send --wait` checks both reply addresses against the
original message, but those addresses remain unauthenticated labels.

## Isolated security work

The [protected broker and registry](secure-broker.md) provide per-session identity, scoped discovery and leases. Production adapters remain opt-in; see that guide for activation limits.

## Quick start

Launch each inbox-capable harness under its own supervisor. Its first successful
consuming drain establishes readiness; preserve the supplied `AGENT_MSG_SESSION`.

```bash
export AGENT_MSG_DB=/absolute/shared/bus.db
agent-msg run --as my-agent --harness claude --project /absolute/project -- claude
# Within that supervised session:
agent-msg whoami
agent-msg peers --project /absolute/project
agent-msg resolve --label other-agent
agent-msg send --to other-agent --session EXACT_SESSION_FROM_DIRECTORY 'check src/api.py please'
agent-msg drain --as "$AGENT_MSG_ID"
```

The recipient must also be enrolled and ready. New direct messages expire after
five minutes and are pinned to the selected session. Unknown, stale, retired,
paused or unverified recipients are refused. Reusing a label never transfers old
mail. Replies validate both original session IDs. Unregistered senders can send a
notification on the cooperative legacy bus, but cannot receive a correlated reply
until a new request is sent from an enrolled session.

Historical direct mail without a pinned target is inspection-only (`list` or
`drain --keep-unread`); rebinding does not release it. Accepted and unknown bridge
receipts remain available for reconciliation. Upgrade both senders and delivery
workers together: an already running old worker does not gain these checks.

## Resolve a project label to a current session

On the legacy bus, `resolve` first checks the exact recipient label. If that
recipient is unavailable, it searches eligible sessions whose project basename
matches the label. For example, `worker` can resolve to a current
`codex-terminal-...` recipient even when an old `worker` bridge alias is stale.

```bash
agent-msg resolve --label worker --project "$PWD"
# Inspect the returned project, recipient, session, state and sendable fields.
agent-msg send --to EXACT_RETURNED_RECIPIENT --session EXACT_RETURNED_SESSION 'message'
```

This follows the useful address-resolution idea behind ARP: separate a logical
destination from its current reachable address. It is a local directory lookup,
not Ethernet ARP. The selected bus supplies the discovery scope; the project name
is the logical label; recipient plus session identifies the concrete endpoint.
Names and directory records on this bus are not authenticated.

Resolution consults current leases on every lookup. An exact live recipient takes
precedence. A logical label must have exactly one eligible match; multiple live
sessions are `recipient_ambiguous`, never “pick newest.” Use `--project` to narrow
colliding project names, then `peers --project ...` to inspect multiple sessions
within that project. The project filter is strict and cannot make an unrelated
label select an arbitrary peer. Follow `next_offset` when paging through peers.

`recipient_unavailable` means no eligible address for this lookup, not proof that
a conversation is dead. The diagnostic distinguishes a known exclusion from a
missing live project match. Check the bus, project and current peers. Saved
`codex-terminal-...` addresses never fall back to a different project endpoint.

Discovery does not bind or wake a thread. Send with the returned exact recipient
and session; a stale selection is refused. A fresh logical lookup may identify a
replacement, but queued mail, replies and saved exact-session sends never move to
that replacement automatically. Protected broker discovery retains its authorized
context roster and explicit labels; it does not use this project-name fallback.
Protected `peers`/`resolve` reject `--project` rather than silently ignoring it.

For Codex terminals, the owner can enable a persistent initial idle-delivery
policy with `agent-msg-terminal wakeup`; see [terminal setup](integrations/terminal-hooks.md#persistent-owner-policy-for-codex-idle-delivery).
Without that policy or an explicit bridge binding, an enrolled terminal remains
prompt-only. Existing pause, allowance, unknown-delivery and session controls are
preserved by automatic enrollment.

## Commands

    agent-msg send --to ID [--from ID] [--type notify|request|reply]
                   [--session EXACT_RECIPIENT_SESSION] [--reply-to N] [--wait] [--timeout S] BODY
    agent-msg drain --as ID [--keep-unread]
    agent-msg list [--unread] [--from ID] [--to ID] [--limit N]
    agent-msg broadcast [--from ID] [--project /absolute/project]
                        --dry-run BODY
    agent-msg broadcast [--from ID] [--project /absolute/project]
                        --key UNIQUE_KEY BODY
    agent-msg broadcast-status BROADCAST_ID [--limit N] [--offset N]
    agent-msg run --as ID --harness NAME [--project /absolute/project]
                  -- COMMAND [ARGS...]

    agent-msg whoami
    agent-msg peers [--project /absolute/project] [--limit N] [--offset N]
    agent-msg resolve --label LABEL [--project /absolute/project]

The legacy directory is bus-scoped and explicitly unauthenticated. In protected
mode (`RESONANCE_PEER_SOCKET` plus a privately provisioned credential file), use
`peers --context CONTEXT_ID` and `resolve --context CONTEXT_ID --label LABEL`.
`whoami` derives identity from the credential. Protected direct sends use
`send --context CONTEXT_ID --key UNIQUE_KEY --to EXACT_SESSION_ID BODY`;
`--route-revision` can pin the directory selection further. Use the MCP/native
adapter for protected inbox, ACK and announcement operations. Unsupported commands
or broker outages fail rather than falling back to the legacy bus.

## Broadcast to ongoing threads

`broadcast` reaches participating sessions on the selected `AGENT_MSG_DB`, across
projects and harnesses. Busy and idle sessions both count. Installation alone is
not presence: a session needs a working inbox integration and a live supervisor.
An optional `--project` filter narrows the audience to one absolute project path.

```bash
agent-msg broadcast --dry-run 'The shared API contract has changed; see docs/api.md.'
agent-msg broadcast --key api-contract-v2 'The shared API contract has changed; see docs/api.md.'
agent-msg broadcast-status BROADCAST_UUID
```

Preview reports recipients and exclusions without creating or migrating the bus.
Sending snapshots up to 100 recipients, excludes the sender, and atomically
enqueues an ordinary notification for each. Repeat the same sender/key/body/scope
to recover the original result after lost output; changing the body or scope with
that key is an error. Empty snapshots retain the key and return `state: empty`.
New joiners do not receive earlier broadcasts. A preview does not reserve its
audience: the send result contains the actual snapshot.

For ordinary launches, use the [native harness setup](integrations/native-harnesses.md)
for exact identity, lifecycle hooks, Pi/OpenCode idle delivery, and optional
Claude Channels. Kimi terminal delivery remains prompt-time. Peer cards expose
these capabilities explicitly.

Native lifecycle hooks (Claude, Kimi, terminal Codex including the shared Codex
daemon) register through the per-bus session service `agent-msg-service`. The
first live session starts it detached (a systemd transient unit when available),
later sessions connect to it, and it survives until reboot. It verifies each
owner's liveness, renews leases, and retires sessions whose owner exits or whose
daemon thread unloads. `agent-msg-service status|stop` inspects or ends it; see
the [native harness guide](integrations/native-harnesses.md).

For the older supervised workflow, use `run-agent.sh` with the existing inbox hook for Claude, Codex CLI, Kimi,
OpenCode or Pi. The wrapper now supervises the harness and sets a fresh
`AGENT_MSG_SESSION`. Its first successful consuming `drain`, even with an empty
inbox, establishes readiness. The supervisor renews presence every 15 seconds
without model turns; normal exit closes it and a crashed supervisor expires after
90 seconds. A duplicate live recipient ID is refused. Direct sends and consuming
drains now require current session participation too. Heartbeat failure does not
terminate a legacy harness, but expiry stops delivery until a fresh supervised
session is enrolled; a stale token cannot reclaim another session's alias.

Codex desktop/bridge recipients require a trusted per-thread attachment probe.
The former process-only `watch` command now refuses with
`exact_thread_attachment_required`: a live application plus readable history does
not prove an active thread. Existing process-only presence is unverified, so it
cannot receive new direct mail or broadcasts. Package 0.4.0 supplies the native
owning-connection launcher `agent-msg-desktop` and a separate runtime-owned private
invocation mailbox API. See the [versioned adapter contract](integrations/adapters-v1.md)
for capabilities, host setup and consumer migration. Supervised hook sessions
continue to work.

Broadcasts preserve existing transport timing: hook recipients consume on a later
turn, while the bridge can submit to idle or active Codex threads. Status separates
`queued`, `consumed`, `accepted`, `pending`, `sending`, `held`, `blocked`, `unknown`
and `skipped`. `accepted` means transport acceptance; `consumed` means inbox output
was written. Only a correctly addressed correlated reply sets `acknowledged`.
Replies go directly to the sender using the child message ID, never reply-all.
Peer announcements do not change the owner's objective or permissions.

Paused, closed, expired or unready sessions are excluded. Existing bridge cooldown,
wakeup allowance and ambiguity holds still apply to selected recipients. An
announcement pinned to a retired session is never delivered to a replacement
session with the same alias. The bridge records such unsent children as `skipped`
without marking them read or holding the replacement; an already ambiguous
submission still requires reconciliation. Inspect retained hook mail with
`drain --keep-unread`; it cannot be consumed by a replacement session.

## Interaction models

- **Codex idle wakeup:** the optional [Codex bridge](integrations/codex-wakeup.md)
  watches the bus independently and supplies peer messages as App Server tool
  output. It needs an explicit thread/socket binding; it can start an idle turn
  without waiting for a user prompt. Delivery receipts distinguish accepted,
  blocked and unknown outcomes.

- Async (default): `send` returns immediately; the recipient sees the
  message injected into its context at its NEXT turn (per-harness hook).
- Sync: `send --wait --timeout 120` blocks until the recipient sends a
  `--type reply --reply-to <id>` message, or exits 3 on timeout.

## Launching agents

Always launch through the wrapper so identity and bus location are set:

    ./run-agent.sh <unique-id> <bus-db> <harness> [args...]

## Installation

Symlink the CLI onto PATH and merge the relevant fragment from
`integrations/` into each harness's config. See file headers for
per-harness instructions.

For the optional Codex wakeup worker, run `uv sync --extra codex --group dev`
from the repository root and use `.venv/bin/agent-msg-codex`. Follow the
[binding and service guide](integrations/codex-wakeup.md) before starting delivery.

Note: Codex hooks require trust review before they run. Non-managed
hooks must be approved via `/hooks` in a Codex session, or they are
silently skipped.

## Design notes

- The original hook/manual route is pull-based: harness agents are turn-driven and
  cannot receive pushed messages between turns. Hooks close the gap to
  one-turn latency.
- Crash safety: the CLI marks mail read only after it successfully
  writes drained output to stdout; crashes or SIGPIPE before that point
  cause redelivery, never loss at the CLI level. Anything that loses the
  output between the CLI write and harness context injection (e.g. a
  hook result being dropped) can still lose that mail.

## Manual smoke test (per harness)

The per-harness loop needs an interactive TUI session, so it is run by
hand. For EACH installed harness (`claude`, `codex`, `kimi`,
`opencode`, `pi`):

1. Terminal 1 — seed mail for the agent before launching:

       export AGENT_MSG_DB=/tmp/opencode/agent-integration/bus.db
       agent-msg send --to claude-test --from human "Reply with the word PONG"

2. Terminal 2 — launch through the wrapper:

       agent-msg/run-agent.sh claude-test /tmp/opencode/agent-integration/bus.db claude

3. Send any prompt in the harness TUI; expect a PONG-related inbox block
   visible in context.

Repeat with `codex`, `kimi`, `opencode`, `pi` replacing `claude`.
