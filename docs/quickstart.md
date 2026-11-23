# Send your first message

Two Bash terminals can send and receive a message on an isolated ResonanceCore
bus. Use the same working directory in both terminals. Install the CLI from the
repository first with `uv tool install .`.

## 1. Start the reviewer

In terminal A, create a temporary bus and print its path. Copy the printed path
for terminal B.

```bash
demo_dir=$(mktemp -d)
export AGENT_MSG_DB="$demo_dir/bus.db"
printf '%s\n' "$AGENT_MSG_DB"
agent-msg run --as reviewer --harness shell --project "$PWD" -- bash
```

Inside the new shell, do a first drain so the reviewer becomes discoverable.

```bash
agent-msg drain --as "$AGENT_MSG_ID"
```

Leave this shell open. An empty inbox is expected.

## 2. Resolve the reviewer and send

In terminal B, replace the example path with the exact path from terminal A.

```bash
export AGENT_MSG_DB='/paste/the/printed/bus.db'
agent-msg run --as builder --harness shell --project "$PWD" -- bash
```

Inside the builder shell, resolve the reviewer and use the session ID returned
by the directory. A successful send prints a numeric message ID.

```bash
agent-msg drain --as "$AGENT_MSG_ID"
address=$(agent-msg resolve --label reviewer --project "$PWD")
session=$(printf '%s\n' "$address" | python3 -c 'import json, sys; print(json.load(sys.stdin)["session"])')
agent-msg send --to reviewer --session "$session" 'Please review the parser.'
```

## 3. Read the message

Back in terminal A, drain the inbox again.

```bash
agent-msg drain --as "$AGENT_MSG_ID"
```

The inbox includes a JSON record with the sender `builder`, the numeric
`message_id`, and the body `Please review the parser.`. Draining again leaves
the inbox empty. Exit both supervised shells when you finish.

[Watch the recorded CLI example](media/message-demo.mp4). The video shows
selected output from an isolated test run and omits the session ID.

## If the reviewer does not appear

- Confirm both shells have the same `AGENT_MSG_DB` and working directory.
- Confirm terminal A is still running the supervised reviewer shell and has
  drained its inbox once.
- If the reviewer was restarted, resolve the label again before sending.
  The old session ID cannot address the new incarnation.
- For native hooks and idle delivery, follow the
  [harness setup and support matrix](../agent-msg/integrations/native-harnesses.md).
