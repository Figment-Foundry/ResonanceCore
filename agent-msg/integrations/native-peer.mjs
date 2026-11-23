import { spawn } from "node:child_process"

// One pipe and identity per exact native session, including multi-session hosts.
export class NativePeer {
  constructor(harness, thread, project, deliver, options = {}) {
    this.pending = new Map()
    this.sequence = 0
    this.closed = false
    this.buffer = ""
    this.ready = new Promise((resolve, reject) => { this.resolveReady = resolve; this.rejectReady = reject })
    this.child = (options.spawn ?? spawn)(options.executable ?? "agent-msg-native",
      ["--harness", harness, "--thread", thread, "--project", project],
      { stdio: ["pipe", "pipe", "pipe"] })
    this.child.stdout.setEncoding("utf8")
    this.child.stderr.resume()
    this.child.on("error", () => this.fail())
    this.child.on("exit", () => this.fail())
    this.child.stdin.on("error", () => this.fail())
    this.child.stdout.on("data", chunk => {
      this.buffer += chunk
      if (this.buffer.length > 131072) return this.fail()
      while (this.buffer.includes("\n")) {
        const end = this.buffer.indexOf("\n")
        const line = this.buffer.slice(0, end)
        this.buffer = this.buffer.slice(end + 1)
        try {
          const value = JSON.parse(line)
          if (value.event === "ready") {
            clearTimeout(this.startup)
            this.identity = value.identity
            this.resolveReady(value.identity)
          } else if (value.event === "message") {
            Promise.resolve().then(() => {
              if (this.closed) throw new Error("detached")
              return deliver(value.content, value.message_id)
            }).then(
              () => this.request("delivered", { message_id: value.message_id, state: "accepted" }),
              () => this.request("delivered", { message_id: value.message_id, state: "unknown" }),
            ).catch(() => this.fail())
          } else {
            const pending = this.pending.get(value.id)
            if (pending) {
              clearTimeout(pending.timeout)
              this.pending.delete(value.id)
              value.error ? pending.reject(new Error(value.error)) : pending.resolve(value.result)
            }
          }
        } catch { this.fail() }
      }
    })
    this.startup = setTimeout(() => this.fail(), 10000)
  }

  request(operation, args = {}) {
    if (this.closed || this.pending.size >= 32) return Promise.reject(new Error("Resonance unavailable"))
    const id = ++this.sequence
    const text = JSON.stringify({ id, operation, arguments: args }) + "\n"
    if (Buffer.byteLength(text) > 65536) return Promise.reject(new Error("Resonance request too large"))
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error("Resonance request timeout"))
        this.fail()
      }, 10000)
      this.pending.set(id, { resolve, reject, timeout })
      this.child.stdin.write(text)
    })
  }

  fail() {
    if (this.closed) return
    this.closed = true
    clearTimeout(this.startup)
    this.rejectReady(new Error("Resonance native attachment unavailable"))
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timeout)
      pending.reject(new Error("Resonance native attachment closed"))
    }
    this.pending.clear()
    this.child.stdin.end()
    const timer = setTimeout(() => this.child.kill("SIGTERM"), 2000)
    timer.unref()
  }

  async close() {
    this.fail()
    if (this.child.exitCode !== null || this.child.signalCode !== null) return
    await new Promise(resolve => {
      const timeout = setTimeout(resolve, 2500)
      this.child.once("exit", () => { clearTimeout(timeout); resolve() })
    })
  }
}
