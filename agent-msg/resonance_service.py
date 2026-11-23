"""Per-bus Resonance session service: connect-or-start registration and liveness.

Hooks no longer supervise themselves. They present owner evidence to one
service per bus, which verifies it, owns hook-session membership, and retires a
session when its owner process exits or its Codex daemon thread unloads.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import socketserver
import stat
import struct
import subprocess
import sys
import threading
import time

import t3_delivery

if os.name != 'nt':
    import fcntl
    import agent_msg
    import agent_broadcast as membership
    from agent_session import process_identity

VERSION = '0.6.0'
MAX_REQUEST = 65_536
MAX_RESPONSE = 1_048_576
PID_INTERVAL, CODEX_INTERVAL, RENEW_INTERVAL, T3_INTERVAL = 2, 5, 15, 30
START_TIMEOUT = 5
HARNESSES = ('codex', 'claude', 'kimi')
EVENTS = ('SessionStart', 'UserPromptSubmit', 'SessionEnd')

SCHEMA = '''
CREATE TABLE IF NOT EXISTS service_attachments (
 recipient TEXT PRIMARY KEY, harness TEXT NOT NULL, thread TEXT NOT NULL,
 session TEXT NOT NULL, project TEXT NOT NULL, owner TEXT NOT NULL, created REAL NOT NULL
);
'''


def code(error):
    """Only our own refusal codes leave the process; other messages may quote input."""
    value = error.args[0] if error.args else None
    return value if isinstance(value, str) and re.fullmatch(r'[a-z_]{1,64}', value) else 'invalid_request'


# --- paths -----------------------------------------------------------------

def runtime_dir():
    override = os.environ.get('RESONANCE_RUNTIME_DIR')
    if override:
        base = Path(override)
    else:
        run = Path(f'/run/user/{os.getuid()}')
        base = (run if run.is_dir() and os.access(run, os.W_OK)
                else Path.home() / '.local/state/resonancecore/run') / 'resonancecore'
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = base.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('private_state_required')
    return base


def exact_bus(bus):
    path = Path(bus)
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError('exact_bus_path_required')
    return str(path)


def go_executable():
    configured = os.environ.get('RESONANCE_GO_BIN')
    name = 'resonance-service.exe' if os.name == 'nt' else 'resonance-service'
    executable = Path(configured) if configured else Path(__file__).resolve().parents[1] / 'go' / name
    if not executable.is_absolute() or not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError('native_service_unavailable')
    return str(executable)


def digest(bus):
    return hashlib.sha256(exact_bus(bus).encode()).hexdigest()[:16]


def socket_path(bus):
    return runtime_dir() / f'service-{digest(bus)}.sock'


# --- client ----------------------------------------------------------------

class ServiceDown(Exception):
    pass


def _call(path, message, timeout):
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise ServiceDown() from None
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError('service_socket_untrusted')
    payload = json.dumps(message, separators=(',', ':')).encode() + b'\n'
    if len(payload) > MAX_REQUEST:
        raise ValueError('request_limit')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        try:
            client.connect(str(path))
        except (ConnectionRefusedError, FileNotFoundError):
            raise ServiceDown() from None
        _, uid, _ = struct.unpack('3i', client.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize('3i')))
        if uid != os.getuid():
            raise ValueError('service_socket_untrusted')
        try:
            client.sendall(payload)
            data = b''
            while not data.endswith(b'\n'):
                chunk = client.recv(65_536)
                if not chunk:
                    break
                data += chunk
                if len(data) > MAX_RESPONSE:
                    raise ValueError('response_limit')
        except OSError:
            raise ValueError('service_unavailable') from None
    try:
        reply = json.loads(data)
    except ValueError:
        raise ValueError('service_unavailable') from None
    if not isinstance(reply, dict):
        raise ValueError('service_unavailable')
    if not reply.get('ok'):
        raise ValueError(code(ValueError(reply.get('error'))))
    return reply.get('result')


def _alive(path):
    try:
        _call(path, {'op': 'ping'}, 1)
        return True
    except (ServiceDown, ValueError, OSError):
        return False


def _spawn(bus):
    module_dir = str(Path(__file__).resolve().parent)
    argv = [sys.executable, '-m', 'resonance_service', 'run', '--bus', bus]
    pythonpath = os.pathsep.join(filter(None, [module_dir, os.environ.get('PYTHONPATH')]))
    if os.environ.get('RESONANCE_SERVICE_SPAWN') != 'process' and shutil.which('systemd-run'):
        # A transient unit has its own cgroup: closing the starting terminal or
        # harness scope does not take the service with it.
        setenv = [f'--setenv=PYTHONPATH={pythonpath}']
        if os.environ.get('RESONANCE_RUNTIME_DIR'):
            setenv.append('--setenv=RESONANCE_RUNTIME_DIR=' + os.environ['RESONANCE_RUNTIME_DIR'])
        try:
            result = subprocess.run(['systemd-run', '--user', '--quiet', '--collect',
                                     '--unit', f'resonance-service-{digest(bus)}', *setenv, *argv],
                                    stdin=subprocess.DEVNULL, capture_output=True, timeout=START_TIMEOUT)
            if result.returncode == 0 or b'already' in result.stderr:
                return
        except (OSError, subprocess.TimeoutExpired):
            pass
    subprocess.Popen(argv, env={**os.environ, 'PYTHONPATH': pythonpath},
                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                     start_new_session=True, close_fds=True)


def _start(bus, path):
    lock = os.open(path.with_name(path.stem + '.lock'), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if _alive(path):
            return
        _spawn(bus)
        deadline = time.monotonic() + START_TIMEOUT
        while time.monotonic() < deadline:
            if _alive(path):
                return
            time.sleep(0.05)
        raise ValueError('service_unavailable')
    finally:
        os.close(lock)


def request(bus, message, *, start=True, timeout=10):
    """Call the bus's service; when allowed, start it first if none is running."""
    bus = exact_bus(bus)
    if os.name == 'nt' or os.environ.get('RESONANCE_SERVICE_BACKEND') == 'go':
        executable = go_executable()
        if not start:
            probe = subprocess.run([executable, '--bus', bus, 'status'], stdin=subprocess.DEVNULL,
                                   capture_output=True, timeout=timeout)
            if probe.returncode:
                raise ValueError('service_unavailable')
        result = subprocess.run([executable, '--bus', bus, 'request'],
                                input=json.dumps(message), text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            value = result.stderr.strip().removeprefix('error: ')
            raise ValueError(code(ValueError(value)))
        return json.loads(result.stdout)
    path = socket_path(bus)
    try:
        return _call(path, message, timeout)
    except ServiceDown:
        if not start:
            raise ValueError('service_unavailable') from None
    _start(bus, path)
    try:
        return _call(path, message, timeout)
    except ServiceDown:
        raise ValueError('service_unavailable') from None


# --- Codex daemon evidence -------------------------------------------------

def codex_read(sock, thread):
    """Return (status, cwd) of a thread on its App Server, or None if unknown."""
    from codex_rpc import CodexRPC, Rejected, TransportError
    try:
        with CodexRPC(sock, timeout=5) as rpc:
            try:
                result = rpc.call('thread/read', {'threadId': thread})
            except Rejected:
                return None
    except TransportError:
        raise ValueError('codex_daemon_unavailable') from None
    if not isinstance(result, dict):
        raise ValueError('codex_daemon_unavailable')
    found = result.get('thread')
    if not isinstance(found, dict) or found.get('id') != thread:
        return None
    status = found.get('status') if isinstance(found.get('status'), dict) else {}
    return status.get('type'), found.get('cwd')


def codex_loaded(sock):
    from codex_rpc import CodexRPC, TransportError
    loaded, cursor = set(), None
    try:
        with CodexRPC(sock, timeout=5) as rpc:
            for _ in range(20):
                page = rpc.call('thread/loaded/list', {'cursor': cursor} if cursor else {})
                if not isinstance(page, dict):
                    raise ValueError('codex_daemon_unavailable')
                loaded.update(t for t in page.get('data') or () if isinstance(t, str))
                cursor = page.get('nextCursor')
                if not cursor:
                    break
    except TransportError:
        raise ValueError('codex_daemon_unavailable') from None
    return loaded


# --- service ---------------------------------------------------------------

def owner_evidence(value):
    if not isinstance(value, dict):
        raise ValueError('owner_evidence_required')
    if value.get('kind') == 'pid':
        pid, identity = value.get('pid'), value.get('identity')
        if (type(pid) is not int or pid <= 1 or not isinstance(identity, list) or len(identity) != 2
                or not all(isinstance(part, str) and len(part) <= 64 for part in identity)):
            raise ValueError('owner_evidence_required')
        return {'kind': 'pid', 'pid': pid, 'identity': identity}
    if value.get('kind') == 'codex_thread':
        sock = value.get('socket')
        if not isinstance(sock, str) or not sock.startswith('/') or len(sock.encode()) > 200:
            raise ValueError('owner_evidence_required')
        return {'kind': 'codex_thread', 'socket': sock}
    if value.get('kind') == 't3':
        # Only the thread is the owner; host_pid/host_identity are attach-time context.
        thread = value.get('thread')
        if t3_delivery.thread_from_environment({'T3CODE_THREAD_ID': thread}) is None:
            raise ValueError('owner_evidence_required')
        return {'kind': 't3', 'thread': thread}
    raise ValueError('owner_evidence_required')


def thread_key(harness, value):
    if harness not in HARNESSES:
        raise ValueError('unsupported_native_harness')
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', value):
        raise ValueError('invalid_thread_identity')
    return value


class Service:
    T3_FAILURES = 4  # T3 reports a gone thread and an unreachable server alike

    def __init__(self, bus, *, codex_read=codex_read, codex_loaded=codex_loaded, clock=time.monotonic,
                 t3_read=None, t3_send=None):
        self.bus = exact_bus(bus)
        self.codex_read, self.codex_loaded, self.clock = codex_read, codex_loaded, clock
        self.t3_read = t3_read or t3_delivery.read
        self.t3_send = t3_send or t3_delivery.send
        self.db = agent_msg.open_db(self.bus)
        self.db.executescript(membership.SCHEMA + SCHEMA + t3_delivery.SCHEMA)
        self.lock = threading.RLock()
        self.stopping = threading.Event()
        self.uid = os.getuid()
        self.started = time.time()
        self.server = None
        self.failures = {}
        self.verified = {}  # recipient -> monotonic time of the last successful live verification
        self.last_codex = self.last_renew = self.last_t3 = float('-inf')
        with self.lock:
            for row in self._rows():
                if not self._live_session(row):
                    self._forget(row)

    # storage helpers (callers hold self.lock)
    def _rows(self):
        return [dict(r) for r in self.db.execute('SELECT * FROM service_attachments ORDER BY recipient')]

    def _row(self, recipient):
        row = self.db.execute('SELECT * FROM service_attachments WHERE recipient=?', (recipient,)).fetchone()
        return dict(row) if row else None

    def _participant(self, recipient):
        return self.db.execute('SELECT * FROM participants WHERE recipient=?', (recipient,)).fetchone()

    def _live_session(self, row, session=None):
        part = self._participant(row['recipient'])
        return (part is not None and not part['closed'] and part['expires'] > time.time()
                and part['session'] == (session or row['session']))

    def _forget(self, row):
        with self.db:
            self.db.execute('DELETE FROM service_attachments WHERE recipient=? AND session=?',
                            (row['recipient'], row['session']))

    def _retire(self, row):
        try:
            membership.close(self.db, row['recipient'], row['session'])
        except ValueError:
            pass  # already replaced by another incarnation
        self._forget(row)

    # evidence
    def verify(self, owner, project, thread):
        if owner['kind'] == 't3':
            try:
                info = self.t3_read(owner['thread'])
            except ValueError:
                raise ValueError('owner_not_live') from None
            if not t3_delivery.usable(info):
                raise ValueError('owner_not_live')
            return
        if owner['kind'] == 'pid':
            if process_identity(owner['pid']) != tuple(owner['identity']):
                raise ValueError('owner_not_live')
            return
        found = self.codex_read(owner['socket'], thread)
        if found is None or found[0] in (None, 'notLoaded'):
            raise ValueError('codex_thread_not_loaded')
        if not isinstance(found[1], str) or str(Path(found[1]).resolve()) != project:
            raise ValueError('codex_project_mismatch')

    def owner_live(self, owner, project, thread):
        try:
            self.verify(owner, project, thread)
            return True
        except ValueError:
            return False

    def _legacy_owner(self, harness, thread, session):
        import terminal_session as terminal
        roots = [Path.home() / '.local/state/resonancecore/hooks' / harness]
        if harness == 'codex':
            roots.append(Path.home() / '.local/state/resonancecore/terminal')
        for root in roots:
            try:
                record = terminal.read_record(root, thread)
            except (ValueError, KeyError, OSError):
                continue
            if record and not record.get('closed') and record.get('session') == session:
                return {'kind': 'pid', 'pid': record['owner_pid'], 'identity': record['owner_identity']}
        return None

    def _t3_upgrade(self, recipient, raw, encoded, project):
        """Adopt a pid-owned attachment when its own host process now presents T3 evidence."""
        if not isinstance(raw, dict) or raw.get('kind') != 't3':
            return
        with self.lock:
            row = self._row(recipient)
            if not row or row['project'] != project or row['owner'] == encoded or not self._live_session(row):
                return
            old = json.loads(row['owner'])
            if (old['kind'] == 'pid' and old['pid'] == raw.get('host_pid')
                    and old['identity'] == raw.get('host_identity')):
                with self.db:
                    self.db.execute('UPDATE service_attachments SET owner=? WHERE recipient=? AND session=?',
                                    (encoded, recipient, row['session']))

    # operations
    def attach(self, message):
        harness = message.get('harness')
        thread = thread_key(harness, message.get('thread'))
        event = message.get('event')
        if event not in EVENTS:
            raise ValueError('unsupported_hook')
        owner = owner_evidence(message.get('owner'))
        project = membership.project_path(message.get('project', ''))
        recipient = f'{harness}-terminal-{thread}'
        encoded = json.dumps(owner, sort_keys=True)
        if owner['kind'] == 't3' and event == 'SessionEnd':
            # T3 ends the provider process after every turn; the thread owns the address.
            return {'recipient': recipient, 'closed': False}
        if event != 'SessionEnd':
            self.verify(owner, project, thread)  # RPC outside the lock
            self.verified[recipient] = self.clock()
            self._t3_upgrade(recipient, message.get('owner'), encoded, project)
        for _ in range(3):
            # A contested previous owner may be a daemon thread: check it without the lock,
            # then decide only if the attachment did not change meanwhile.
            with self.lock:
                seen = self._row(recipient)
                if seen is not None and json.loads(seen['owner'])['kind'] == 't3':
                    if event == 'SessionEnd':
                        return {'recipient': recipient, 'closed': False}
                    if seen['owner'] != encoded and self._live_session(seen):
                        raise ValueError('thread_already_attached')
                contested = seen is not None and seen['owner'] != encoded and self._live_session(seen)
            previous_live = contested and self.owner_live(json.loads(seen['owner']), seen['project'], thread)
            with self.lock:
                if self._row(recipient) == seen:
                    result = self._decide(seen, previous_live, harness, thread, event, owner, encoded,
                                          project, recipient)
                    if owner['kind'] == 't3':
                        self._t3_bind(result, owner['thread'], event)
                    return result
        raise ValueError('attach_contended')

    def _decide(self, seen, previous_live, harness, thread, event, owner, encoded, project, recipient):
        # Caller holds self.lock and has confirmed `seen` is the current attachment.
        row, retired = seen, False
        if row and self._live_session(row):
            if row['owner'] != encoded:
                if previous_live:
                    raise ValueError('thread_already_attached')
                self._retire(row)
                row, retired = None, True
            elif row['project'] != project:
                raise ValueError('session_scope_changed')
        elif row:
            # Expired while unrenewed (service down) or replaced: fresh incarnation.
            self._retire(row)
            row, retired = None, True
        if event == 'SessionEnd':
            if row or retired:
                if row:
                    self._retire(row)
                return {'recipient': recipient, 'closed': True}
            part = self._participant(recipient)
            if (part is not None and not part['closed'] and part['harness'] == harness
                    and self._legacy_owner(harness, thread, part['session']) == owner):
                membership.close(self.db, recipient, part['session'])  # pre-service session
                return {'recipient': recipient, 'closed': True}
            return {'recipient': recipient, 'closed': False}
        if row is None:
            session = self._adopt_or_register(recipient, harness, thread, project, owner)
            with self.db:
                self.db.execute('INSERT OR REPLACE INTO service_attachments VALUES (?,?,?,?,?,?,?)',
                                (recipient, harness, thread, session, project, encoded, time.time()))
        else:
            session = row['session']
            membership.renew(self.db, recipient, session)
        part = self._participant(recipient)
        if event == 'UserPromptSubmit' and part['route'] == 'codex':
            bound = membership.binding(self.db, recipient)
            if (not bound or bound['thread_id'] != thread or bound['project'] != project
                    or membership.binding_target(bound) != part['target']):
                raise ValueError('native_binding_mismatch')
            # The bridge owns consumption, including while paused. Hooks maintain presence.
            membership.renew(self.db, recipient, session, ready=True)
        return {'recipient': recipient, 'session': session, 'route': part['route'], 'project': project}

    def _adopt_or_register(self, recipient, harness, thread, project, owner):
        part = self._participant(recipient)
        if part is not None and not part['closed'] and part['expires'] > time.time():
            if part['harness'] != harness or part['project'] != project:
                raise ValueError('recipient_already_active')
            legacy = self._legacy_owner(harness, thread, part['session'])
            if legacy is None or legacy == owner:
                return part['session']  # adopt the live legacy incarnation
            if self.owner_live(legacy, project, thread):
                raise ValueError('thread_already_attached')
            membership.close(self.db, recipient, part['session'])
        bound = membership.binding(self.db, recipient)
        if bound and (bound['thread_id'] != thread or bound['project'] != project or bound['transport'] != 'queue'):
            raise ValueError('native_binding_mismatch')
        return membership.register(self.db, recipient, project, harness,
                                   route='codex' if bound else 'hook',
                                   evidence='thread_attachment' if bound else None)

    def _t3_bind(self, result, t3_thread, event):
        # Caller holds self.lock. One address per T3 thread: a new provider session
        # (/clear, fresh context) retires the previous incarnation.
        for row in self._rows():
            if row['recipient'] != result['recipient'] and json.loads(row['owner']) == {'kind': 't3', 'thread': t3_thread}:
                self._retire(row)
        if result['route'] != 'hook':
            return
        t3_delivery.bind(self.db, result['recipient'], result['session'], t3_thread)
        if event == 'UserPromptSubmit':
            t3_delivery.prompt_seen(self.db, result['recipient'], result['session'])
        result['delivery'] = 't3_thread'

    def lookup(self, message):
        harness = message.get('harness')
        recipient = f'{harness}-terminal-{thread_key(harness, message.get("thread"))}'
        with self.lock:
            row = self._row(recipient)
            if not row or not self._live_session(row):
                raise ValueError('native_session_not_attached')
        if not self.owner_live(json.loads(row['owner']), row['project'], row['thread']):
            raise ValueError('native_session_not_attached')
        return {'recipient': recipient, 'session': row['session'], 'project': row['project'],
                'bus': self.bus, 'owner': json.loads(row['owner'])['kind']}

    def status(self):
        with self.lock:
            rows = self._rows()
        return {'pid': os.getpid(), 'version': VERSION, 'bus': self.bus, 'started': self.started,
                'attachments': [{'recipient': r['recipient'], 'owner': json.loads(r['owner'])['kind'],
                                 'project': r['project']} for r in rows[:100]]}

    def dispatch(self, message):
        if not isinstance(message, dict):
            raise ValueError('invalid_request')
        op = message.get('op')
        if op == 'ping':
            return {'version': VERSION}
        if op == 'attach':
            return self.attach(message)
        if op == 'lookup':
            return self.lookup(message)
        if op == 'status':
            return self.status()
        if op == 'stop':
            self.stopping.set()
            if self.server is not None:
                threading.Thread(target=self.server.shutdown, daemon=True).start()
            return {'stopping': True}
        raise ValueError('unsupported_operation')

    # supervision
    def tick(self, *, force=False):
        now = self.clock()
        # Renew first and independently: slow or failing evidence must not let live leases lapse.
        if force or now - self.last_renew >= RENEW_INTERVAL:
            self.last_renew = now
            with self.lock:
                for row in self._rows():
                    try:
                        membership.renew(self.db, row['recipient'], row['session'])
                    except ValueError:
                        self._forget(row)
                    except Exception:
                        pass  # transient storage error: retry next tick
        with self.lock:
            rows = self._rows()
        loaded, unreachable = {}, set()
        if force or now - self.last_codex >= CODEX_INTERVAL:
            self.last_codex = now
            for sock in {json.loads(r['owner']).get('socket') for r in rows} - {None}:
                try:
                    loaded[sock] = self.codex_loaded(sock)
                    self.failures[sock] = 0
                except Exception:
                    # One failed poll is tolerated; a stopped daemon has unloaded its threads.
                    self.failures[sock] = self.failures.get(sock, 0) + 1
                    if self.failures[sock] >= 2:
                        unreachable.add(sock)
        t3_checked = self.clock()
        t3_gone = set()
        if force or now - self.last_t3 >= T3_INTERVAL:
            self.last_t3 = now
            for thread in {json.loads(r['owner']).get('thread') for r in rows
                           if json.loads(r['owner'])['kind'] == 't3'}:
                try:
                    gone = not t3_delivery.usable(self.t3_read(thread))
                    self.failures['t3:' + thread] = 0
                except Exception:
                    self.failures['t3:' + thread] = self.failures.get('t3:' + thread, 0) + 1
                    gone = self.failures['t3:' + thread] >= self.T3_FAILURES
                if gone:
                    t3_gone.add(thread)
        for row in rows:
            try:
                self._supervise_row(row, loaded, unreachable, t3_gone, t3_checked)
            except Exception:
                continue  # one bad row must not stop supervision of the others
        try:
            t3_delivery.pump(self.db, sender=self.t3_send, lock=self.lock)
        except Exception:
            pass  # storage contention: the next tick retries the claim

    def _supervise_row(self, row, loaded, unreachable, t3_gone, t3_checked):
        owner = json.loads(row['owner'])
        checked = self.clock()
        if owner['kind'] == 't3':
            checked = t3_checked
            alive = owner['thread'] not in t3_gone
        elif owner['kind'] == 'pid':
            alive = process_identity(owner['pid']) == tuple(owner['identity'])
        elif owner['socket'] in unreachable:
            alive = False  # two failed polls: do not call a hung daemon once per thread
        else:
            current = loaded.get(owner['socket'])
            # A list snapshot can be stale; retire only on fresh per-thread evidence.
            alive = current is None or row['thread'] in current or self.owner_live(owner, row['project'], row['thread'])
        if not alive:
            with self.lock:
                # An attach that verified the owner after this evidence was gathered wins.
                if self._row(row['recipient']) == row and self.verified.get(row['recipient'], float('-inf')) < checked:
                    self._retire(row)

    def supervise(self):
        while not self.stopping.wait(PID_INTERVAL):
            try:
                self.tick()
            except Exception:  # keep supervising; one bad row must not stop the loop
                pass


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        service = self.server.service
        self.request.settimeout(5)
        try:
            _, uid, _ = struct.unpack('3i', self.request.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED,
                                                                     struct.calcsize('3i')))
            if uid != service.uid:
                raise ValueError('peer_refused')
            line = self.rfile.readline(MAX_REQUEST + 1)
            if len(line) > MAX_REQUEST or not line.endswith(b'\n'):
                raise ValueError('request_limit')
            try:
                message = json.loads(line)
            except ValueError:
                raise ValueError('invalid_request') from None
            reply = {'ok': True, 'result': service.dispatch(message)}
        except ValueError as error:
            reply = {'ok': False, 'error': code(error)}
        except Exception:
            reply = {'ok': False, 'error': 'internal_error'}
        try:
            self.wfile.write(json.dumps(reply, separators=(',', ':')).encode() + b'\n')
        except OSError:
            pass


if os.name != 'nt':
    class _Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
        daemon_threads = True


def serve(service, path):
    """Bind the private socket and return the server; the caller runs serve_forever."""
    old = os.umask(0o077)
    try:
        server = _Server(str(path), _Handler)
    finally:
        os.umask(old)
    os.chmod(path, 0o600)
    server.service = service
    service.server = server
    return server


def run(bus):
    bus = exact_bus(bus)
    path = socket_path(bus)
    pidfile = os.open(path.with_name(path.stem + '.pid'), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(pidfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return 0  # another instance owns this bus
    os.ftruncate(pidfile, 0)
    os.write(pidfile, f'{os.getpid()}\n'.encode())
    path.unlink(missing_ok=True)  # stale: the pid lock proves no live owner
    service = Service(bus)
    server = serve(service, path)
    inode = path.stat().st_ino
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, lambda *_: threading.Thread(target=server.shutdown, daemon=True).start())
    supervisor = threading.Thread(target=service.supervise, daemon=True)
    supervisor.start()
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        service.stopping.set()
        server.server_close()
        try:
            if path.stat().st_ino == inode:
                path.unlink()
        except FileNotFoundError:
            pass
        os.close(pidfile)
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == 't3-check':
        parser = argparse.ArgumentParser(description='Check T3 CLI and provider-thread schema.')
        parser.add_argument('--state', type=Path)
        parser.add_argument('--project', type=Path, default=Path.cwd())
        args = parser.parse_args(argv[1:])
        print(json.dumps(t3_delivery.compatibility(args.state, project=args.project), sort_keys=True))
        return 0
    if os.name == 'nt' or os.environ.get('RESONANCE_SERVICE_BACKEND') == 'go':
        try:
            executable = go_executable()
        except ValueError as error:
            print(json.dumps({'ok': False, 'error': code(error),
                              'remediation': 'Build the Go service and set RESONANCE_GO_BIN to its absolute path.'}))
            return 2
        return subprocess.run([executable, *argv],
                              stdin=subprocess.DEVNULL).returncode
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bus', default=str(Path.home() / '.local/state/resonancecore/bus.db'))
    parser.add_argument('command', choices=['run', 'status', 'stop', 'start'])
    args = parser.parse_args(argv)
    try:
        if args.command == 'run':
            return run(args.bus)
        result = request(args.bus, {'op': 'ping' if args.command == 'start' else args.command},
                         start=args.command == 'start')
        print(json.dumps(result, sort_keys=True))
        return 0
    except ValueError as error:
        print(f'error: {code(error)}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
