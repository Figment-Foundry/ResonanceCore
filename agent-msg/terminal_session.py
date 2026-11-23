"""Attach native terminal sessions through trusted lifecycle hooks.

The hook establishes the thread identity and owner evidence; the per-bus
Resonance service (resonance_service) verifies that evidence, owns membership,
and retires the session when its owner exits or its daemon thread unloads.
It never enrolls desktop stdio servers or infers threads from history.
"""
import argparse
from contextlib import contextmanager
import json
import math
import os
import re
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
import time
import uuid
import subprocess

if os.name != 'nt':
    import fcntl
import agent_msg
import agent_broadcast as membership
from agent_session import process_identity
import resonance_service as service
import t3_delivery


def _windows_argv(command):
    """Parse a process command line with the same rules Windows uses."""
    import ctypes
    from ctypes import wintypes

    argc = ctypes.c_int()
    shell32 = ctypes.WinDLL('shell32', use_last_error=True)
    kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
    shell32.CommandLineToArgvW.argtypes = (wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_int))
    shell32.CommandLineToArgvW.restype = ctypes.POINTER(wintypes.LPWSTR)
    kernel32.LocalFree.argtypes = (wintypes.HLOCAL,)
    kernel32.LocalFree.restype = wintypes.HLOCAL
    argv = shell32.CommandLineToArgvW(command, ctypes.byref(argc))
    if not argv:
        raise OSError(ctypes.get_last_error(), 'CommandLineToArgvW failed')
    try:
        return [argv[index] for index in range(argc.value)]
    finally:
        kernel32.LocalFree(ctypes.cast(argv, wintypes.HLOCAL))


def _windows_process(pid):
    before = process_identity(pid)
    if before is None:
        raise ValueError('native_terminal_hook_required')
    script = ("$n=[int]$env:RESONANCE_PROBE_PROCESS_ID; "
              "$p=Get-CimInstance Win32_Process -Filter ('ProcessId=' + $n); "
              "if($p){$p|Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine|ConvertTo-Json -Compress}")
    environment = dict(os.environ, RESONANCE_PROBE_PROCESS_ID=str(pid))
    result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', script],
                            env=environment, stdin=subprocess.DEVNULL,
                            capture_output=True, text=True, timeout=2)
    if result.returncode or not result.stdout.strip():
        raise ValueError('native_terminal_hook_required')
    data = json.loads(result.stdout)
    if (not isinstance(data, dict) or type(data.get('ProcessId')) is not int
            or data['ProcessId'] != pid or type(data.get('ParentProcessId')) is not int
            or not isinstance(data.get('ExecutablePath'), str)
            or not isinstance(data.get('CommandLine'), str)):
        raise ValueError('native_terminal_hook_required')
    if process_identity(pid) != before:
        raise ValueError('native_terminal_hook_required')
    return data['ParentProcessId'], Path(data['ExecutablePath']).name.lower(), data['CommandLine']


def terminal_owner(*, windows_probe=_windows_process):
    """Find the actual native terminal ancestor of this hook, not an arbitrary PID."""
    pid = os.getppid()
    if os.name == 'nt':
        shells = {'cmd.exe', 'powershell.exe', 'pwsh.exe', 'windowsterminal.exe'}
        harnesses = {'codex.exe', 'claude.exe', 'kimi.exe', 'codex', 'claude', 'kimi'}
        for _ in range(24):
            try:
                parent, executable, command = windows_probe(pid)
            except (OSError, ValueError, KeyError, subprocess.SubprocessError, json.JSONDecodeError):
                break
            if executable in harnesses:
                arguments = [token.casefold() for token in _windows_argv(command)[1:]]
                if any(token in arguments for token in ('app-server', 'exec', 'mcp-server')):
                    raise ValueError('native_terminal_hook_required')
                identity = process_identity(pid)
                if identity is not None:
                    return pid, list(identity)
                break
            if executable not in shells or parent <= 1 or parent == pid:
                break
            pid = parent
        raise ValueError('native_terminal_hook_required')
    for _ in range(24):
        root = Path('/proc') / str(pid)
        try:
            args = root.joinpath('cmdline').read_bytes().split(b'\0')
            executable = Path(os.readlink(root / 'exe')).name
            status = root.joinpath('status').read_text()
            parent = int(next(line.split()[1] for line in status.splitlines() if line.startswith('PPid:')))
        except (OSError, StopIteration, ValueError):
            break
        if executable == 'codex':
            if b'app-server' in args[1:] and b'--managed-daemon' in args[1:]:
                # The shared daemon runs terminal threads; it answers for their liveness.
                return managed_daemon_evidence()
            if args[1:2] == [b'app-server']:
                # T3 Code hosts Codex threads in its own app-server; the T3 thread owns liveness.
                # Its hooks carry no thread variable, so hook() resolves the thread later.
                return (t3_delivery.owner_from_host(os.environ, pid)
                        or {'kind': 't3_codex_host', 'pid': pid})
            if any(a in (b'app-server', b'exec', b'mcp-server') for a in args[1:]):
                raise ValueError('native_terminal_hook_required')
            identity = process_identity(pid)
            if identity is None:
                break
            return pid, list(identity)
        # Native command hooks may have a shell between themselves and Codex.
        # Do not enroll from a model tool, MCP server, or arbitrary descendant.
        if executable not in ('bash', 'dash', 'sh'):
            break
        if parent <= 1 or parent == pid:
            break
        pid = parent
    raise ValueError('native_terminal_hook_required')


def managed_daemon_evidence():
    home = Path(os.environ.get('CODEX_HOME') or Path.home() / '.codex')
    sock = home / 'app-server-control' / 'app-server-control.sock'
    try:
        info = sock.stat()
    except OSError:
        raise ValueError('codex_daemon_unavailable') from None
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError('codex_daemon_unavailable')
    return {'kind': 'codex_thread', 'socket': str(sock)}


def owner_evidence(value):
    """Accept the legacy (pid, identity) probe shape as pid evidence."""
    if isinstance(value, (tuple, list)):
        pid, identity = value
        return {'kind': 'pid', 'pid': pid, 'identity': list(identity) if identity else None}
    return value


def thread_id(value):
    if not isinstance(value, str) or str(uuid.UUID(value)) != value:
        raise ValueError('invalid_thread_identity')
    return value


def session_key(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', value):
        raise ValueError('invalid_thread_identity')
    return value


def private_root(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError('exact_private_state_path_required')
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('private_state_required')
    return path


@contextmanager
def locked(root, thread):
    fd = os.open(root / (thread + '.lock'), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('private_state_required')
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _read_private_json(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_nlink != 1 or info.st_size > 4096:
            raise ValueError('invalid_session_record')
        record = json.load(stream)
    return record


def read_record(root, thread):
    record = _read_private_json(root / (thread + '.json'))
    if record is not None and record['thread'] != thread:
        raise ValueError('session_record_mismatch')
    return record


def _write_private_json(root, name, record):
    raw = json.dumps(record, sort_keys=True).encode()
    if len(raw) > 4096:
        raise ValueError('session_record_limit')
    fd, temporary = tempfile.mkstemp(prefix='.session-', dir=root)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, root / name)
    finally:
        Path(temporary).unlink(missing_ok=True)


def write_record(root, record):
    _write_private_json(root, record['thread'] + '.json', record)


def wakeup_policy_root():
    return Path.home() / '.config/resonancecore/terminal'


def validate_wakeup_policy(value):
    fields = {'version', 'enabled', 'executable', 'codex_home', 'max_wakeups', 'cooldown'}
    if (not isinstance(value, dict) or set(value) - fields
            or type(value.get('version')) is not int or value['version'] != 1
            or type(value.get('enabled')) is not bool):
        raise ValueError('invalid_wakeup_policy')
    if not value['enabled']:
        return dict(version=1, enabled=False)
    if set(value) != fields:
        raise ValueError('invalid_wakeup_policy')
    for name in ('executable', 'codex_home'):
        path = value[name]
        if (not isinstance(path, str) or len(path.encode()) > 4096
                or not Path(path).is_absolute() or str(Path(path)) != path
                or '..' in Path(path).parts):
            raise ValueError('invalid_wakeup_paths')
    if (type(value['max_wakeups']) is not int or not 1 <= value['max_wakeups'] <= 1000
            or type(value['cooldown']) not in (int, float)
            or not math.isfinite(value['cooldown']) or not 0 <= value['cooldown'] <= 3600):
        raise ValueError('invalid_wakeup_limits')
    return value


def read_wakeup_policy():
    root = wakeup_policy_root()
    if not root.exists():
        return None
    record = _read_private_json(private_root(root) / 'wakeup.json')
    return None if record is None else validate_wakeup_policy(record)


def configure_wakeup(args):
    if not args.enable and any(value is not None for value in
                               (args.executable, args.codex_home, args.max_wakeups, args.cooldown)):
        raise ValueError('wakeup_enable_required')
    if args.enable or args.disable:
        policy = dict(version=1, enabled=False)
        if args.enable:
            policy = dict(version=1, enabled=True, executable=args.executable,
                          codex_home=args.codex_home,
                          max_wakeups=20 if args.max_wakeups is None else args.max_wakeups,
                          cooldown=30 if args.cooldown is None else args.cooldown)
        policy = validate_wakeup_policy(policy)
        _write_private_json(private_root(wakeup_policy_root()), 'wakeup.json', policy)
    policy = read_wakeup_policy()
    print(json.dumps({'policy_file': str(wakeup_policy_root() / 'wakeup.json'),
                      'policy': policy or dict(version=1, enabled=False)}))
    return 0


def ensure_initial_wakeup(bus, result, thread, project):
    """Provision only a fresh binding; existing controls are never reset here."""
    policy = read_wakeup_policy()
    if not policy or not policy['enabled']:
        return
    import codex_bridge as bridge
    from codex_queue import QueueClient
    from codex_rpc import Rejected
    db = bridge.database(str(bus))
    try:
        recipient = result['recipient']
        if membership.binding(db, recipient) is not None:
            return
        binding = dict(recipient=recipient, thread_id=thread, project=project, socket='',
                       transport='queue', executable=policy['executable'],
                       codex_home=policy['codex_home'])
        # Verify before registration: a bad target must not claim the hook inbox.
        try:
            QueueClient(binding).verify()
        except Rejected:
            raise ValueError('wakeup_target_unavailable') from None
        record = wakeup_attachment(db, binding)
        enable_wakeup(db, binding, record, initial_policy=policy)
    finally:
        db.close()


def alive(pid, identity):
    return identity is not None and process_identity(pid) == tuple(identity)


def _environment(record):
    return {'AGENT_MSG_DB': record['bus'], 'AGENT_MSG_ID': record['recipient'], 'AGENT_MSG_SESSION': record['session']}


@contextmanager
def session_environment(record):
    values = _environment(record)
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def wakeup_attachment(db, binding):
    """Verify the exact native attachment before entering a bus write transaction."""
    if os.environ.get('RESONANCE_PEER_SOCKET') or os.environ.get('RESONANCE_CREDENTIAL_FILE'):
        raise ValueError('protected_host_adapter_required')
    thread = thread_id(binding['recipient'].removeprefix('codex-terminal-'))
    bus = str(Path(db.execute('PRAGMA database_list').fetchone()[2]).resolve())
    try:
        record = service.request(bus, {'op': 'lookup', 'harness': 'codex', 'thread': thread}, start=False)
    except ValueError:
        raise ValueError('exact_native_attachment_required') from None
    if (record['recipient'] != binding['recipient'] or record['project'] != binding['project']
            or binding['thread_id'] != thread or binding['transport'] != 'queue'):
        raise ValueError('exact_native_attachment_required')
    return record


def enable_wakeup(db, binding, record, *, initial_policy=None):
    """Atomically create an initial binding or apply an explicit manual resume."""
    target = membership.binding_target(binding)
    with db:
        db.execute('BEGIN IMMEDIATE')
        fresh = membership.binding(db, record['recipient'])
        if initial_policy is not None and fresh is not None:
            # A concurrent owner/control operation wins; never reset its allowance.
            return
        row = db.execute('SELECT * FROM participants WHERE recipient=?', (record['recipient'],)).fetchone()
        if ((initial_policy is None and (not fresh or membership.binding_target(fresh) != target))
                or row is None or row['session'] != record['session'] or row['closed']
                or row['expires'] <= time.time() or not row['ready']
                or row['project'] != record['project']
                or (initial_policy is not None and row['route'] != 'hook')
                or (row['route'] == 'codex' and row['target'] != target)):
            raise ValueError('native_session_changed')
        if initial_policy is not None:
            # Native identity, owner policy and queue target are already verified.
            # Insert and route transition share this transaction, leaving no paused
            # intermediate row that could block prompt delivery or race a control.
            db.execute('''INSERT INTO codex_bindings
                (recipient,thread_id,project,socket,enabled,max_wakeups,cooldown,transport,executable,codex_home)
                VALUES (?,?,?,?,1,?,?,?,?,?)''',
                (record['recipient'], binding['thread_id'], binding['project'], '',
                 initial_policy['max_wakeups'], initial_policy['cooldown'], 'queue',
                 binding['executable'], binding['codex_home']))
        # Previously queued hook mail retains its original route and is never retargeted.
        db.execute("UPDATE participants SET route='codex',target=?,evidence='thread_attachment' WHERE recipient=?",
                   (target, record['recipient']))
        if initial_policy is None:
            db.execute('UPDATE codex_bindings SET enabled=1,wakeups=0 WHERE recipient=?', (record['recipient'],))


def resume_wakeup(db, binding):
    """Connect an explicitly enabled queue binding to its trusted native attachment."""
    enable_wakeup(db, binding, wakeup_attachment(db, binding))


def hook(event, bus, *, owner_probe=terminal_owner, harness='codex'):
    if harness not in ('codex', 'claude', 'kimi'):
        raise ValueError('unsupported_native_harness')
    if os.environ.get('RESONANCE_PEER_SOCKET') or os.environ.get('RESONANCE_CREDENTIAL_FILE'):
        raise ValueError('protected_host_adapter_required')
    if not isinstance(event, dict):
        raise ValueError('invalid_hook_payload')
    kind = event.get('hook_event_name')
    if kind not in ('SessionStart', 'UserPromptSubmit', 'SessionEnd'):
        raise ValueError('unsupported_hook')
    thread = (thread_id if harness == 'codex' else session_key)(event.get('session_id'))
    project = membership.project_path(event.get('cwd', ''))
    path = Path(bus)
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError('exact_bus_path_required')
    owner = owner_evidence(owner_probe())
    if owner.get('kind') == 't3_codex_host':
        owner = harness == 'codex' and t3_delivery.owner_for_codex_host(thread, owner['pid'])
        if not owner:
            raise ValueError('native_terminal_hook_required')
    message = dict(op='attach', harness=harness, thread=thread, project=project, owner=owner, event=kind)
    try:
        # Only a live session may start the service; an ending one just reports out.
        result = service.request(str(path), message, start=kind != 'SessionEnd')
    except ValueError as error:
        if kind == 'SessionEnd' and service.code(error) == 'service_unavailable':
            return 0
        raise
    if kind == 'UserPromptSubmit' and result['route'] == 'hook':
        record = dict(bus=str(path), recipient=result['recipient'], session=result['session'])
        with session_environment(record):
            drained = agent_msg.main(['drain', '--as', record['recipient']])
        if drained == 0 and harness == 'codex' and owner.get('kind') != 't3':
            try:
                ensure_initial_wakeup(path, result, thread, project)
            except (ValueError, OSError, sqlite3.Error) as error:
                reason = service.code(error) if isinstance(error, ValueError) else type(error).__name__
                print('warning: terminal_idle_wakeup_unavailable (' + reason + ')', file=sys.stderr)
        return drained
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', default=str(Path.home()/'.local/state/resonancecore/terminal'))
    parser.add_argument('--bus', default=str(Path.home()/'.local/state/resonancecore/bus.db'))
    commands = parser.add_subparsers(dest='action', required=True)
    commands.add_parser('hook')
    wakeup = commands.add_parser('wakeup', help='Inspect or configure owner-authorized initial idle delivery')
    mode = wakeup.add_mutually_exclusive_group()
    mode.add_argument('--enable', action='store_true')
    mode.add_argument('--disable', action='store_true')
    wakeup.add_argument('--executable')
    wakeup.add_argument('--codex-home')
    wakeup.add_argument('--max-wakeups', type=int)
    wakeup.add_argument('--cooldown', type=float)
    execute = commands.add_parser('exec', help='Use this native thread’s enrolled identity for agent-msg commands')
    execute.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        if args.action == 'hook':
            raw = sys.stdin.buffer.read(1_048_577)
            if len(raw) > 1_048_576:
                raise ValueError('hook_input_limit')
            return hook(json.loads(raw), args.bus)
        if os.environ.get('RESONANCE_PEER_SOCKET') or os.environ.get('RESONANCE_CREDENTIAL_FILE'):
            raise ValueError('protected_host_adapter_required')
        if args.action == 'wakeup':
            return configure_wakeup(args)
        thread = thread_id(os.environ.get('CODEX_THREAD_ID'))
        record = service.request(args.bus, {'op': 'lookup', 'harness': 'codex', 'thread': thread}, start=False)
        arguments = args.arguments[1:] if args.arguments[:1] == ['--'] else args.arguments
        with session_environment(record):
            return agent_msg.main(arguments)
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
        # Hook payloads can contain prompt text. Never echo it on malformed input.
        print('error: terminal_session_unavailable (' + (service.code(exc) if isinstance(exc, ValueError)
              else type(exc).__name__) + ')', file=sys.stderr)
        return 0 if args.action == 'hook' else 2


if __name__ == '__main__':
    raise SystemExit(main())
