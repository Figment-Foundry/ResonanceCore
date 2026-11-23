# Experimental local broker foundation

This is an experimental local broker with a trusted owner API and a peer transport. Deployment requires a human confirmation interface and verified harness isolation profiles.

## Implemented contract

The thread registry is implemented in `thread_registry.py` with additive schema
version 1. Enrollment alone is **not presence**. The trusted host attaches each
exact session through `Broker.attach(session_id, instance, native_thread,
evidence, ready=False)`. Evidence is either a supervised single-session hook or a
native `thread_attachment`; a shared process PID is insufficient. The host owns
`renew`, `retire` and `pause`; none is a peer RPC. Leases use monotonic time with a
90-second maximum and a per-broker epoch. A late renewal or broker restart requires
a fresh incarnation and admission; old messages and grants do not transfer.

`secure_session.run` supervises one privately provisioned process for a session.
Its first authenticated inbox call under an approved context proves hook readiness;
the supervisor renews independently of model turns and retires on process exit.
The host must provide the private credential file/socket environment and existing
isolation profile. This helper does not itself create a sandbox or approve a roster.

Peer operations now include `whoami`, `peers(context_id, limit=20, offset=0)` and
`resolve(context_id, label)`, through the stdio MCP and Pi adapters. Directory pages
are capped at 100, exclude outsiders, and expose exact session/native identity,
project, label, lease state and sendability without credentials or private routes.
Duplicate active labels refuse resolution. `whoami` lists this credential's current
contexts; adapters instruct agents to call it before selecting peers. No metadata
is elevated into system instructions.

Direct sends, replies and announcements check live attachments and pin immutable
recipient attachment/revision records. `send` accepts optional
`expected_route_revision`; stale selections refuse admission. Inbox rechecks exact
targets. New sends to paused peers refuse; already admitted mail waits within its
original expiry. Replies preserve the exact original sender address and carry an
authenticated identity card. Idempotent sends retain their original target snapshot.
The trusted host can use `directory` for tombstones and `receipt(root_id)` after
session expiry; receipts distinguish queued, held, undelivered and acknowledged.
This pull-only broker does not claim transport acceptance or task completion.

The separate legacy bus now pins direct mail too, but remains unauthenticated.
Historical unpinned mail is inspection-only. Process-only Codex watchers are
disabled. The [0.4.0 owning-connection adapter](integrations/adapters-v1.md)
supplies native lifecycle evidence for explicitly configured legacy queue
bindings; it does not activate the protected broker or retrofit existing desktop
connections. Private runtime mailboxes use a third, dedicated store and do not
enroll restricted roles as interactive peers.

`secure_broker.Broker` owns a private SQLite store distinct from the legacy bus. One broker holds an exclusive process lock on that store. The trusted host enrolls an exact session using `enroll`, creates an approved exact roster using `mesh`, inspects a frozen pairing request with `proposal`, and applies the owner's decision using `approve` or `deny`. It can revoke a context, remove a member or quarantine a session across contexts.

Each enrollment returns an independent randomly generated credential. Only its hash is stored in the database. Provision the credential privately to that one session; do not print it in model context or pass it as a command argument. File modes are checked, but the host must also prevent peer access to the private storage, other credentials and owner process.

`BrokerServer` exposes only these peer operations over a bounded local Unix socket:

| Operation | Behavior |
| --- | --- |
| `invite` | Create a short-lived introduction code for the authenticated caller |
| `pair` | Reserve that code to one authenticated source/target proposal; no messaging grant until owner approval |
| `send` | Send to an exact authorized member under an explicit context; optional correctly directed reply correlation |
| `announce` | Atomically snapshot and enqueue to the current authorized roster, excluding the sender |
| `inbox` | Read at most five still-authorized messages addressed to this session; does not consume on a possibly lost RPC response |
| `ack` | Acknowledge only messages addressed to this authenticated session and context |

The transport has no enrollment, approval, arbitrary sender, executable-selection or wakeup operation. Caller-supplied extra arguments are refused. Reads and dispatch through `inbox` recheck membership, expiry and quarantine. Pair relationships do not imply access through a third peer. Mesh membership removal leaves independent pair grants in place; quarantine blocks all session contexts.

The same trusted host must render proposal metadata and verify real human consent before calling the owner API. Calling `approve` in a unit test establishes the authorization transition, not the provenance of a human approval. The current `mesh` primitive assumes its exact roster was already approved by that trusted host.

## Bounds and lifecycle

This prototype accepts bodies up to 8192 UTF-8 bytes; peer requests up to 65536 wire bytes; inbox pages up to five records. It limits active invitation creation, pairing attempts, authenticated calls, backlog and total record counts. Root idempotency keys preserve exact message identity; a changed payload under the same key is refused. Acknowledgements are separate from reading, and do not mean the recipient completed a task.

Enrollment lifetime is at most eight hours; the current API defaults to one hour. Codes expire after five minutes, and pairing attempts are limited to five per five-minute window. Pair grants cannot outlive either endpoint. A mesh has its own eight-hour ceiling and each member's enrollment must remain live; one short-lived member does not invalidate remaining relationships. There is no implicit refresh, roster expansion or session-ID reuse.

All-to-all mesh admission is implemented. Restricted role policies, dynamic mesh invitation/admission, security audit export, owner-facing status/consent UI, fair multi-client scheduling and sustained resource management are not complete. The store currently has conservative lifetime capacity ceilings rather than automated retention/garbage collection. The sequential bounded socket server is suitable for validation, not a claim of denial-of-service-resistant production service. Slow-client fairness and aggregate mesh and delivery quotas remain prerequisites for production deployment.

## Validation

Run the normal tests from the worktree:

```bash
uv sync --locked --extra codex --group dev
uv run pytest -q agent-msg/tests
```

The opt-in isolation test launches a synthetic Python peer in a fresh Bubblewrap filesystem/PID/network namespace, exposing only the peer socket, its own credential, Python runtime and test client. It verifies authorized delivery, refusal of an administrative RPC and absence of the broker's storage and the other session's credential:

```bash
RESONANCE_SANDBOX_TESTS=1 uv run pytest -q agent-msg/tests/test_secure_broker_sandbox.py
```

That probe requires working unprivileged namespaces. A tool sandbox can disallow them even when the host supports them. It does not launch Codex, Claude, OpenCode or Pi, validate their native approval channels, or prove their eventual real filesystem/network profiles. It is evidence for a minimal isolation mechanism only.

## Next integration boundary

Before wiring a protected recipient into any installed harness, implement the owner confirmation host, per-thread enrollment/credential provisioning and a low-trust delivery adapter under a verified isolation profile. A shell outside that profile remains a trusted host actor and can access its same-user files. Do not route the legacy CLI into this store, expose owner methods through peer RPC, or fall back to the legacy bus when authorization fails.
