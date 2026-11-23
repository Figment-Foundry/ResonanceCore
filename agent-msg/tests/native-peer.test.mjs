import { test } from "node:test"
import assert from "node:assert/strict"
import { EventEmitter } from "node:events"
import { PassThrough, Writable } from "node:stream"
import { NativePeer } from "../integrations/native-peer.mjs"

function fixture(deliver) {
  const child = new EventEmitter()
  const sent = []
  child.exitCode = child.signalCode = null
  child.stdout = new PassThrough()
  child.stderr = new PassThrough()
  child.stdin = new Writable({ write(chunk, _encoding, callback) {
    const request = JSON.parse(chunk)
    sent.push(request)
    queueMicrotask(() => child.stdout.write(JSON.stringify({ id: request.id, result: {} }) + "\n"))
    callback()
  }, final(callback) { child.exitCode = 0; child.emit("exit", 0); callback() } })
  child.kill = () => { child.exitCode = 1; child.emit("exit", 1) }
  const peer = new NativePeer("pi", "thread", "/tmp", deliver, { spawn: () => child })
  child.stdout.write('{"event":"ready","identity":{"recipient":"pi-native-thread"}}\n')
  return { peer, child, sent }
}

test("ACK follows native acceptance and includes exact message ID", async () => {
  let release
  const accepted = new Promise(resolve => { release = resolve })
  const { peer, child, sent } = fixture(() => accepted)
  await peer.ready
  child.stdout.write('{"event":"message","message_id":23,"content":"peer data"}\n')
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(sent.length, 0)
  release()
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(sent[0].arguments, { message_id: 23, state: "accepted" })
  await peer.close()
})

test("native delivery failure records unknown instead of acceptance", async () => {
  const { peer, child, sent } = fixture(() => { throw new Error("rejected") })
  await peer.ready
  child.stdout.write('{"event":"message","message_id":24,"content":"peer data"}\n')
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(sent[0].arguments.state, "unknown")
  await peer.close()
})

test("detached session cannot receive a delayed event", async () => {
  let delivered = 0
  const { peer, child } = fixture(() => { delivered++ })
  await peer.ready
  child.stdout.write('{"event":"message","message_id":25,"content":"late"}\n')
  await peer.close()
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(delivered, 0)
})
