"""Version 1 native owning-client lifecycle adapter for Codex App Server stdio.

Run as the owning client's App Server launch command. Only successful subscription
requests observed on that connection establish attachment. Read/list notifications
cannot enroll a thread. This adapter never synthesizes start/resume/model requests.
"""
import argparse
import json
import os
from pathlib import Path
import selectors
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

import agent_broadcast as membership
import codex_bridge

INTERFACE_VERSION = 1
MAX_FRAME = 16 * 1024 * 1024


class DesktopAttachment:
    def __init__(self, db, *, clock=time.monotonic, ttl=90, codex_home=None):
        if not 1 <= ttl <= 90:
            raise ValueError('invalid_lease')
        self.db, self.clock, self.ttl = db, clock, ttl
        self.codex_home = str(Path(codex_home).resolve()) if codex_home is not None else None
        self.lock = threading.RLock()
        self.pending, self.attached, self.retired, self.sessions = {}, {}, {}, {}
        self.sequence, self.closed = 0, False

    def client(self, message):
        """Observe an unmodified message from the native owning client."""
        with self.lock:
            if self.closed or not isinstance(message, dict):
                return
            self.sequence += 1
            method, params = message.get('method'), message.get('params') or {}
            if not isinstance(params, dict):
                return
            tid = params.get('threadId')
            if method in ('thread/unsubscribe', 'thread/archive', 'thread/delete') and isinstance(tid, str):
                # Retirement precedes forwarding the close request. Failed close needs
                # a new successful owner subscription, never an automatic resurrection.
                self._retire(tid)
            if method in ('thread/start', 'thread/resume', 'thread/fork') and type(message.get('id')) in (str, int):
                if len(self.pending) >= 256:
                    raise ValueError('pending_request_limit')
                key = json.dumps(message['id'])
                if key in self.pending:
                    raise ValueError('duplicate_request_id')
                self.pending[key] = (method, tid, self.sequence, self.clock() + 60)

    def server(self, message):
        """Observe native responses and terminal lifecycle events; no passive enrollment."""
        with self.lock:
            if self.closed or not isinstance(message, dict):
                return
            self.sequence += 1
            method, params = message.get('method'), message.get('params') or {}
            if isinstance(params, dict):
                tid = params.get('threadId')
                terminal = method in ('thread/closed', 'thread/archived', 'thread/deleted')
                terminal = terminal or (method == 'thread/status/changed' and
                    isinstance(params.get('status'), dict) and params['status'].get('type') == 'notLoaded')
                if terminal and isinstance(tid, str):
                    self._retire(tid)
            # Server-initiated requests may reuse numeric IDs in the opposite direction.
            if 'method' in message or 'id' not in message:
                return
            pending = self.pending.pop(json.dumps(message['id']), None)
            if pending is None or 'error' in message:
                return
            method, expected, sequence, deadline = pending
            result = message.get('result')
            thread = result.get('thread') if isinstance(result, dict) else None
            if not isinstance(thread, dict) or self.clock() >= deadline:
                return
            tid, project = thread.get('id'), thread.get('cwd')
            try:
                uuid.UUID(tid)
                if not isinstance(project, str) or not Path(project).is_absolute():
                    return
            except (ValueError, TypeError, AttributeError):
                return
            if method == 'thread/resume' and expected != tid:
                return
            if thread.get('archived') or self.retired.get(tid, 0) >= sequence:
                return
            if len(self.attached) >= 100 and tid not in self.attached:
                raise ValueError('attachment_limit')
            canonical = str(Path(project).resolve())
            if tid in self.attached and self.attached[tid] != canonical:
                self._retire(tid)
                return
            self.attached[tid] = canonical
            self.heartbeat()

    def probe(self, binding):
        """Concrete attachment_probe for hosts embedding this observer."""
        with self.lock:
            route_matches = self.codex_home is None or (binding['transport'] == 'queue' and
                str(Path(binding['codex_home']).resolve()) == self.codex_home)
            return not self.closed and route_matches and self.attached.get(binding['thread_id']) == binding['project']

    def _retire(self, tid):
        self.retired[tid] = self.sequence
        self.attached.pop(tid, None)
        # Keep only tombstones needed to fence outstanding subscription responses.
        if len(self.retired) > 512:
            oldest = min((p[2] for p in self.pending.values()), default=self.sequence)
            self.retired = {key: seq for key, seq in self.retired.items() if seq >= oldest}
        for alias, (session, target, thread) in list(self.sessions.items()):
            if thread == tid:
                membership.close(self.db, alias, session)
                del self.sessions[alias]

    def heartbeat(self):
        with self.lock:
            if self.closed:
                return
            self.pending = {key: value for key, value in self.pending.items() if value[3] > self.clock()}
            rows = self.db.execute('SELECT * FROM codex_bindings ORDER BY recipient LIMIT 101').fetchall()
            if len(rows) > 100:
                raise ValueError('binding_limit')
            bound = {row['recipient']: dict(row) for row in rows}
            for alias, (session, target, tid) in list(self.sessions.items()):
                current = bound.get(alias)
                if not current or membership.binding_target(current) != target or not self.probe(current):
                    membership.close(self.db, alias, session)
                    del self.sessions[alias]
                    continue
                # If the lease expired, fail the adapter rather than silently mint a replacement.
                membership.renew(self.db, alias, session, ready=True, ttl=self.ttl)
            for alias, binding in bound.items():
                if alias in self.sessions or not self.probe(binding):
                    continue
                session = membership.register(self.db, alias, binding['project'], 'codex', 'codex',
                    ttl=self.ttl, evidence='thread_attachment')
                self.sessions[alias] = (session, membership.binding_target(binding), binding['thread_id'])
                membership.renew(self.db, alias, session, ready=True, ttl=self.ttl)

    def close(self):
        with self.lock:
            if self.closed:
                return
            self.closed = True
            failure = None
            try:
                for alias, (session, _, _) in self.sessions.items():
                    try:
                        membership.close(self.db, alias, session)
                    except (ValueError, sqlite3.Error) as exc:
                        failure = exc
            finally:
                self.sessions.clear()
                self.attached.clear()
                self.pending.clear()
            if failure is not None:
                raise failure


def relay(db_path, command, *, interval=15):
    """Transparent owner stdio relay; stdout contains native protocol frames only."""
    if not command or not 0 < interval <= 15:
        raise ValueError('invalid_host_command')
    db = codex_bridge.database(db_path)
    host = DesktopAttachment(db, codex_home=os.environ.get('CODEX_HOME', str(Path.home() / '.codex')))
    child = None
    selector = selectors.DefaultSelector()
    previous = {}
    output_modes = {}
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    try:
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            previous[sig] = signal.signal(sig, stop)
        child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None, bufsize=0)
        inputs = {'client': sys.stdin.buffer, 'server': child.stdout}
        outputs = {'client': child.stdin, 'server': sys.stdout.buffer}
        for direction, stream in inputs.items():
            selector.register(stream, selectors.EVENT_READ, ('read', direction))
        for stream in outputs.values():
            output_modes[stream.fileno()] = os.get_blocking(stream.fileno())
            os.set_blocking(stream.fileno(), False)
        buffers = {'client': bytearray(), 'server': bytearray()}
        outgoing = {'client': bytearray(), 'server': bytearray()}
        eof = False
        server_eof = False
        next_heartbeat = time.monotonic() + interval
        while not stopping:
            for key, _ in selector.select(timeout=min(0.25, interval)):
                operation, direction = key.data
                if operation == 'write':
                    try:
                        written = os.write(key.fd, outgoing[direction][:65536])
                    except BlockingIOError:
                        continue
                    del outgoing[direction][:written]
                    if not outgoing[direction]:
                        selector.unregister(key.fd)
                        if not eof:
                            selector.register(inputs[direction], selectors.EVENT_READ, ('read', direction))
                    continue
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    eof = True
                    server_eof = direction == 'server'
                    for stream in inputs.values():
                        try:
                            selector.unregister(stream)
                        except KeyError:
                            pass
                    break
                buffer = buffers[direction]
                buffer.extend(chunk)
                while b'\n' in buffer:
                    line, _, rest = buffer.partition(b'\n')
                    if len(line) > MAX_FRAME:
                        raise ValueError('native_frame_limit')
                    buffer[:] = rest
                    message = json.loads(line)
                    getattr(host, direction)(message)
                    outgoing[direction].extend(line + b'\n')
                if len(buffer) > MAX_FRAME:
                    raise ValueError('native_frame_limit')
                if outgoing[direction]:
                    # Stop reading this direction until its bounded queue drains.
                    # Nonblocking partial writes keep leases and shutdown responsive.
                    selector.unregister(key.fd)
                    selector.register(outputs[direction], selectors.EVENT_WRITE, ('write', direction))
            if eof and not any(outgoing.values()):
                stopping = True
            if time.monotonic() >= next_heartbeat:
                host.heartbeat()
                next_heartbeat = time.monotonic() + interval
        if server_eof:
            try:
                return child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                return 2
        return child.poll() or 0
    finally:
        try:
            host.close()
        finally:
            selector.close()
            for fd, mode in output_modes.items():
                os.set_blocking(fd, mode)
            db.close()
            if child is not None and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True)
    parser.add_argument('--interval', type=float, default=15)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    try:
        return relay(args.db, command, interval=args.interval)
    except (ValueError, OSError, sqlite3.Error):
        print('resonance desktop attachment unavailable; owner connection stopped', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
