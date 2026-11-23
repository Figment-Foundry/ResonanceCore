# Security boundary

ResonanceCore's current shared SQLite bus is a trusted-cooperator transport. It
does not isolate mutually untrusted agents, projects, or threads, including
threads in the same harness. Sender labels are unauthenticated. A process with
database write access can bypass CLI checks and modify bridge control state.

Do not connect a less-trusted agent to a more-privileged recipient and rely on
the bus to preserve that permission boundary. Separate database paths and file
modes reduce some exposure but do not isolate agents that can access each
other's files under the same OS account.

The [experimental broker](agent-msg/secure-broker.md) documents its isolation requirements.

Inbox output labels peer data as untrusted and escapes control characters.
These are presentation safeguards, not authentication or a defense that makes
arbitrary peer instructions safe. Shipped hook templates that insert mail into
system/developer context remain an open finding; review them before installation.

If compromise is suspected, stop or pause affected bridge delivery through an
owner-controlled channel, preserve receipts, and investigate the recipient's
tools and credentials. Do not automatically replay unknown deliveries. Removing
an inbox hook does not stop a separately running bridge worker.
