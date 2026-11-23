"""Presence follows a harness process, independently of model turn timing."""
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import threading
import time

if os.name == 'nt':
    import ctypes
    from ctypes import wintypes

import agent_broadcast as membership
import agent_msg

RENEW_INTERVAL = 15


@contextmanager
def stopping_signals(handler, signals=None):
    signals = signals or tuple(sig for name in ('SIGTERM', 'SIGHUP', 'SIGINT')
                               if (sig := getattr(signal, name, None)) is not None)
    previous = {sig: signal.signal(sig, handler) for sig in signals}
    try:
        yield
    finally:
        for sig, original in previous.items():
            signal.signal(sig, original)


def run(path, recipient, project, harness, command, *, interval=RENEW_INTERVAL, ttl=membership.LEASE_SECONDS):
    """Supervise any inbox-capable harness, including Worker workers."""
    if not command:
        raise ValueError('harness_command_required')
    path = str(Path(path).resolve())
    db = agent_msg.open_db(path)
    child, session = None, None
    stopping = threading.Event()

    def forward(sig, _frame):
        if sig != signal.SIGINT:
            stopping.set()
        # The terminal already broadcasts SIGINT to this foreground group.
        terminal_interrupt = sig == signal.SIGINT and sys.stdin.isatty()
        if child is not None and child.poll() is None and not terminal_interrupt:
            try:
                child.send_signal(sig)
            except ProcessLookupError:
                pass

    try:
        session = membership.register(db, recipient, project, harness, ttl=ttl)
        env = {**os.environ, 'AGENT_MSG_DB': path, 'AGENT_MSG_ID': recipient,
               'AGENT_MSG_SESSION': session}
        with stopping_signals(forward):
            child = subprocess.Popen(command, env=env)
            next_renewal = time.monotonic() + interval
            presence_warning = False
            while child.poll() is None and not stopping.is_set():
                try:
                    child.wait(timeout=min(interval, 0.25))
                except subprocess.TimeoutExpired:
                    if time.monotonic() >= next_renewal and not stopping.is_set():
                        try:
                            membership.renew(db, recipient, session, ttl=ttl)
                            presence_warning = False
                        except (ValueError, sqlite3.Error):
                            if not presence_warning:
                                print('warning: presence renewal failed; harness continues. '
                                      'Expired sessions need relaunch to rejoin broadcasts.', file=sys.stderr)
                            presence_warning = True
                        next_renewal = time.monotonic() + interval
            if stopping.is_set() and child.poll() is None:
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    child.kill()
            code = child.wait()
            return code if code >= 0 else 128 - code
    except OSError:
        raise ValueError('harness_launch_failed') from None
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        try:
            if session:
                membership.close(db, recipient, session)
        except (ValueError, sqlite3.Error):
            # A replaced token cannot close another session; an unreachable lease expires.
            print('warning: presence cleanup unavailable; session leases remain bounded.', file=sys.stderr)
        finally:
            db.close()


def process_identity(pid):
    """Process creation identity prevents PID reuse from extending membership."""
    if pid <= 0:
        raise ValueError('invalid_owner_pid')
    if os.name == 'nt':
        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return None
        creation = wintypes.FILETIME()
        exit_time, kernel, user = wintypes.FILETIME(), wintypes.FILETIME(), wintypes.FILETIME()
        try:
            if not ctypes.windll.kernel32.GetProcessTimes(
                    handle, ctypes.byref(creation), ctypes.byref(exit_time),
                    ctypes.byref(kernel), ctypes.byref(user)):
                return None
            created = (creation.dwHighDateTime << 32) | creation.dwLowDateTime
            return ('windows', str(created))
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        stat = Path(f'/proc/{pid}/stat').read_text()
        fields = stat[stat.rindex(')') + 2:].split()
        if fields[0] in ('Z', 'X'):
            return None
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        return (boot, fields[19])  # /proc field 22, after pid and comm.
    except (OSError, ValueError, IndexError):
        return None


def verify_presence(bound, connector):
    """Check an enrolled thread without resuming it or starting generation."""
    client = connector(bound)
    try:
        if bound['transport'] == 'queue':
            client.verify()
        else:
            result = client.call('thread/read', {'threadId': bound['thread_id'], 'includeTurns': False})
            thread = result.get('thread', {})
            if thread.get('id') != bound['thread_id'] or thread.get('cwd') != bound['project'] or thread.get('archived', False):
                raise ValueError('thread_identity_mismatch')
    finally:
        client.close()


def watch(db, recipient, owner_pid, harness='codex', *, connector=None,
          interval=RENEW_INTERVAL, ttl=membership.LEASE_SECONDS, attachment_probe=None):
    """An explicit enrollment tied to an owner process and verified thread binding."""
    from codex_bridge import connect_target
    if attachment_probe is None:
        raise ValueError('exact_thread_attachment_required')
    connector = connector or connect_target
    identity = process_identity(owner_pid)
    if identity is None:
        raise ValueError('owner_process_unavailable')
    bound = membership.binding(db, recipient)
    if bound is None:
        raise ValueError('matching_codex_binding_required')
    expected = membership.binding_target(bound)
    verify_presence(bound, connector)
    if attachment_probe(bound) is not True:
        raise ValueError('exact_thread_attachment_required')
    session = membership.register(db, recipient, bound['project'], harness, 'codex', ttl=ttl, evidence='thread_attachment')
    stopped = threading.Event()
    try:
        with stopping_signals(lambda *_: stopped.set()):
            while not stopped.is_set() and process_identity(owner_pid) == identity:
                current = membership.binding(db, recipient)
                if current is None or membership.binding_target(current) != expected:
                    raise ValueError('binding_changed')
                verify_presence(current, connector)
                if attachment_probe(current) is not True:
                    raise ValueError('thread_detached')
                membership.renew(db, recipient, session, ready=True, ttl=ttl)
                stopped.wait(interval)
    finally:
        membership.close(db, recipient, session)
    return 0
