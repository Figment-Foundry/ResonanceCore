# Adapter contract: resonance-agent-msg 0.4.0

`agent-msg capabilities` reports `desktop_owner_stdio: 1`, `runtime_mailbox: 1`,
`peer_directory: 1` and `legacy_unregistered_send: false`. Consumers must check
the required interface version and run a disposable-store behavioral preflight.
The package version changed because unregistered legacy `send` calls no longer
provide a private runtime transport. Do not admit that behavior using the old
0.3.0 version check or fabricate registered hook consumers.

## Native owning-thread attachment

`agent-msg-desktop` is a stdio launcher for the **actual owning client's** Codex
App Server. Configure a host that supports an App Server launch command to run:

```bash
CODEX_HOME=/absolute/codex-home agent-msg-desktop --db /absolute/bus.db -- \
  /absolute/path/to/codex app-server
```

The host connects its normal protocol stdin/stdout through this launcher. Merely
running the command in a terminal does not attach an existing desktop thread.
An already running stock desktop needs a supported host launch integration and
restart; this package does not patch its binaries or retrofit its connection.
No alternate server, observer subscription or saved-history scan can establish
that an existing UI connection owns a subscription.

The launcher forwards native frames unchanged, including server requests. It
never synthesizes start/resume, model calls, approval responses or queue writes.
Successful owner `thread/start`, `thread/resume` and `thread/fork` responses prove
an exact thread UUID and project. Matching, explicitly configured queue bindings
on the same CODEX_HOME acquire fresh registry sessions. Bindings can be created
after subscription. The adapter does not create aliases or change pause state,
cooldowns or wake budgets. Follow [binding setup](codex-wakeup.md) and run the
existing bridge separately for delivery. A paused binding remains unsendable.

```text
native owning client <-> stdio adapter <-> its Codex App Server
                             |
                    exact subscription + binding
                             |
                    register / renew registry lease
                             |
                 directory -> session-pinned send -> bridge queue

unsubscribe / archive / delete -> retire exact session immediately
owner EOF / terminal event     -> close affected membership
adapter crash / lease loss     -> expiry; stale send refused
```

The lease renews without model turns, by default every 15 seconds for 90 seconds.
One thread's unsubscribe retires only that thread, even while native loaded
status stays idle. Failed unsubscribe remains conservatively retired until a new
successful owner subscription. Closing the connection retires all its sessions;
a crash expires them. Delayed responses cannot resurrect a retired subscription.
History reads, notifications of other subscribers and loaded lists never enroll.
Binding mutation closes the old session; historical mail cannot follow it.
Duplicate active ownership is refused rather than replacing the existing owner.

An embedding host can use `desktop_attachment.DesktopAttachment(db)` directly:
feed each owner message to `client()` before forwarding and each native reply to
`server()` before delivery; call `heartbeat()` every 15 seconds and `close()` on
disconnect. Its `probe(binding)` supplies the exact attachment probe required by
the existing watcher. Use one enrollment owner: the class manages leases itself,
so do not run an additional watcher for the same alias. For embedding routes
other than the launcher's queue route, the trusted host must verify that the
binding targets its own native connection. Never substitute a constant probe.

Protocol frames are capped at 16 MiB, pending subscription requests at 256 and
tracked attachments/bindings at 100. Nonblocking bounded forwarding preserves
partial writes and allows heartbeat/shutdown during backpressure. Malformed
protocol, lease failure or registry failure closes the connection conservatively.
Plan host integration accordingly; this is not a passive diagnostic sidecar.
The registry identity is visible through `peers`/`resolve`; the launcher does not
inject `AGENT_MSG_SESSION` into an already running model's tool environment.

## Private runtime invocation endpoints

`runtime_mailbox.RuntimeMailbox` is owned exclusively by the trusted controller
runtime, outside restricted role tool surfaces. It uses a dedicated private
SQLite file for one run, with a private 0700 parent and 0600 file. An exclusive
owner lock rejects concurrent controllers. Never point it at the shared bus.
The runtime's `authority` callback must raise whenever its existing run/admission
fence is lost. Omitting it is only suitable for independently owned standalone
runtimes that have no external fence.

```python
from runtime_mailbox import RuntimeMailbox

# assert_current_run is the controller's existing admission/fence check.
with RuntimeMailbox(private_run_dir / "invocations.db", run_id,
                    authority=assert_current_run) as mail:
    controller = mail.register(controller_session, controller_epoch, control_invocation)
    role = mail.register(role_session, role_epoch, role_invocation)
    request = mail.send(controller, role, {"artifact_digest": input_digest},
                        key=request_key, kind="request")
    envelope = mail.read(role)[0]  # Trusted runtime reads; the role has no inbox tool.
    # Validate referenced artifact, admit/execute the role through existing policy,
    # and persist the result artifact before publishing its pointer.
    mail.ack(role, [envelope["id"]])
    reply = mail.reply(role, request, {"artifact_digest": result_digest}, key=result_key)
    result = mail.read(controller)[0]
    # Validate result/correlation and record it durably before acknowledging.
    mail.ack(controller, [result["id"]])
    assert mail.receipt(reply)["state"] == "consumed"
    mail.retire(role)
```

This example shows transport calls, not authorization to execute the referenced
work. Payloads are opaque JSON objects of at most 4096 encoded bytes. Keep them
to artifact references; the caller owns schema, digest, path and scope checks.
The library neither dereferences artifacts nor grants tools, launches workers,
injects model prompts or alters role packets.

| Operation | Version 1 contract |
| --- | --- |
| `register(session, epoch, invocation, ttl=300)` | Returns a unique opaque endpoint. Exact tuple may be registered only once per store; runtime registration establishes readiness. |
| `renew(endpoint, ttl=300)` | Extends a still-live endpoint, never resurrects it. TTL must be positive and at most 3600 seconds. |
| `send(a, b, payload, key=..., kind=...)` | Both exact endpoints must be live in this runtime incarnation. Same sender/key and intent returns the original ID; changed intent conflicts. |
| `reply(role, request_id, payload, key=...)` | Requires an unexpired request addressed to this exact role endpoint; returns only to its original live sender. |
| `read(endpoint, limit=5)` | Returns up to 100 unread live envelopes, including run ID and both session/epoch/invocation identities; does not acknowledge. |
| `ack(endpoint, ids)` | Explicit, atomic receipt recording for up to 100 IDs addressed to the exact endpoint. |
| `receipt(id)` | Diagnostic queued, consumed, expired or endpoint_inactive state. Consumed means runtime acknowledgement, not successful model work. |
| `retire(endpoint)` / `close()` | Retire one invocation / all owned endpoints; subsequent operations refuse stale identities. |

Registration defaults to five-minute leases; the runtime must renew live
endpoints during longer invocations. Message deadlines are bounded by both
endpoints and their own TTL (default five minutes). Renewing an endpoint does not
extend already queued messages. Set the original request TTL explicitly for
longer work, at most one hour. Expired requests cannot receive late replies.
Retire roles after their replies have been consumed; retirement intentionally
makes still-queued replies unavailable. Receipts remain diagnostic history.

A reopened store gets a new owner incarnation, so it cannot consume or reuse
previous endpoints. A different run ID is rejected. New invocations require new
identities; there is no automatic recovery/replay of old work. Idempotency keys
recover an ambiguous local enqueue only while its exact endpoints remain live;
use receipts for historical inspection. Controller policy alone decides retries.
Each run is bounded to 1000 endpoints, 10000 messages and 100 live unread messages
per recipient. Capacity exhaustion fails explicitly; archive/delete completed
run storage through controller retention policy rather than reusing identities.
