"""Acceptance S1-S8 for the per-bus session service (spec 2026-10-04)."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import uuid

import pytest

import agent_msg
import agent_broadcast as membership
import resonance_service as service
import terminal_session as terminal
from .service_helpers import running_service
from .test_agent_session import wait_until

DAEMON = {'kind': 'codex_thread', 'socket': '/fixture/daemon.sock'}


def pid_owner(pid=None):
    pid = pid or os.getpid()
    return {'kind': 'pid', 'pid': pid, 'identity': list(terminal.process_identity(pid))}


class Daemon:
    """Controlled substitute for App Server thread/read and thread/loaded/list."""
    def __init__(self):
        self.threads, self.down = {}, False

    def read(self, sock, thread):
        if self.down:
            raise ValueError('codex_daemon_unavailable')
        return self.threads.get(thread)

    def loaded(self, sock):
        if self.down:
            raise ValueError('codex_daemon_unavailable')
        return {t for t, (status, _) in self.threads.items() if status != 'notLoaded'}


def attach(bus, owner, project, thread, event='SessionStart', harness='codex'):
    return service.request(str(bus), dict(op='attach', harness=harness, thread=thread, project=str(project),
                                          owner=owner, event=event), start=False)


def participant(bus, recipient):
    with agent_msg.open_db(str(bus)) as db:
        return dict(db.execute('SELECT * FROM participants WHERE recipient=?', (recipient,)).fetchone())


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    project = tmp_path / 'project'
    project.mkdir()
    return tmp_path / 'bus.db', project.resolve()


@pytest.fixture
def sleeper():
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    yield child
    if child.poll() is None:
        child.terminate()
    child.wait(timeout=5)


def test_first_attach_starts_one_detached_service_that_outlives_its_starters(env):
    bus, project = env
    code = 'import resonance_service as s, sys; print(s.request(sys.argv[1], {"op": "status"})["pid"])'
    starters = [subprocess.Popen([sys.executable, '-c', code, str(bus)], stdout=subprocess.PIPE, text=True,
                                 env={**os.environ, 'PYTHONPATH': str(Path(service.__file__).parent)})
                for _ in range(4)]
    pids = {int(p.communicate(timeout=30)[0]) for p in starters}
    assert len(pids) == 1 and all(p.returncode == 0 for p in starters)  # S1: exactly one service
    pid = pids.pop()
    try:
        assert service.request(str(bus), {'op': 'status'}, start=False)['pid'] == pid  # S2: survives starters
        result = attach(bus, pid_owner(), project, 'later_session', harness='claude')  # S3: later attach
        assert participant(bus, result['recipient'])['session'] == result['session']
    finally:
        service.request(str(bus), {'op': 'stop'}, start=False)
    wait_until(lambda: not service.socket_path(str(bus)).exists())
    # Stopping the service leaves leases to expire naturally; it does not close sessions.
    assert participant(bus, 'claude-terminal-later_session')['closed'] == 0


def test_managed_daemon_thread_registers_and_retires_when_unloaded(env):
    bus, project = env
    daemon, thread = Daemon(), str(uuid.uuid4())
    with running_service(bus, codex_read=daemon.read, codex_loaded=daemon.loaded) as svc:
        with pytest.raises(ValueError, match='codex_thread_not_loaded'):
            attach(bus, DAEMON, project, thread)
        daemon.threads[thread] = ('notLoaded', str(project))
        with pytest.raises(ValueError, match='codex_thread_not_loaded'):
            attach(bus, DAEMON, project, thread)
        daemon.threads[thread] = ('idle', '/elsewhere')
        with pytest.raises(ValueError, match='codex_project_mismatch'):
            attach(bus, DAEMON, project, thread)
        daemon.threads[thread] = ('idle', str(project))
        first = attach(bus, DAEMON, project, thread)
        assert first['recipient'] == 'codex-terminal-' + thread and first['route'] == 'hook'
        assert attach(bus, DAEMON, project, thread, 'UserPromptSubmit')['session'] == first['session']
        svc.tick(force=True)
        assert participant(bus, first['recipient'])['closed'] == 0
        daemon.threads[thread] = ('notLoaded', str(project))  # S5: thread unloaded
        svc.tick(force=True)
        assert participant(bus, first['recipient'])['closed'] == 1
        assert svc.status()['attachments'] == []


def test_one_failed_daemon_poll_is_tolerated_and_two_retire(env):
    bus, project = env
    daemon, thread = Daemon(), str(uuid.uuid4())
    daemon.threads[thread] = ('active', str(project))
    with running_service(bus, codex_read=daemon.read, codex_loaded=daemon.loaded) as svc:
        recipient = attach(bus, DAEMON, project, thread)['recipient']
        daemon.down = True
        svc.tick(force=True)
        assert participant(bus, recipient)['closed'] == 0
        svc.tick(force=True)
        assert participant(bus, recipient)['closed'] == 1


def test_pid_owner_exit_closes_session_and_renewal_extends_lease(env, sleeper):
    bus, project = env
    with running_service(bus) as svc:
        recipient = attach(bus, pid_owner(sleeper.pid), project, 'pid_session', harness='claude')['recipient']
        before = participant(bus, recipient)['expires']
        svc.tick(force=True)
        assert participant(bus, recipient)['expires'] >= before
        sleeper.terminate()
        sleeper.wait(timeout=5)
        svc.tick(force=True)
        assert participant(bus, recipient)['closed'] == 1


def test_restart_readopts_live_attachment_without_new_incarnation(env):
    bus, project = env
    with running_service(bus):
        first = attach(bus, pid_owner(), project, 'restart_session', harness='kimi')
    with running_service(bus) as svc:
        assert [a['recipient'] for a in svc.status()['attachments']] == [first['recipient']]
        again = attach(bus, pid_owner(), project, 'restart_session', 'UserPromptSubmit', harness='kimi')
        assert again['session'] == first['session']


def test_live_legacy_session_is_adopted_not_duplicated(env, sleeper):
    bus, project = env
    home = Path(os.environ['HOME'])
    with agent_msg.open_db(str(bus)) as db:
        session = membership.register(db, 'claude-terminal-legacy_a', str(project), 'claude')
        other = membership.register(db, 'claude-terminal-legacy_b', str(project), 'claude')
    root = terminal.private_root(home / '.local/state/resonancecore/hooks/claude')
    for thread, sid, pid in (('legacy_a', session, os.getpid()), ('legacy_b', other, sleeper.pid)):
        terminal.write_record(root, dict(thread=thread, session=sid, closed=False, owner_pid=pid,
                                         owner_identity=list(terminal.process_identity(pid))))
    with running_service(bus):
        assert attach(bus, pid_owner(), project, 'legacy_a', harness='claude')['session'] == session
        # A different live legacy owner keeps its thread, and cannot be ended by another owner.
        with pytest.raises(ValueError, match='thread_already_attached'):
            attach(bus, pid_owner(), project, 'legacy_b', harness='claude')
        assert attach(bus, pid_owner(), project, 'legacy_b', 'SessionEnd', harness='claude')['closed'] is False
        # Its own owner's SessionEnd closes the pre-service session.
        assert attach(bus, pid_owner(sleeper.pid), project, 'legacy_b', 'SessionEnd', harness='claude')['closed']
        assert participant(bus, 'claude-terminal-legacy_b')['closed'] == 1


def test_owner_conflicts_scope_changes_and_end(env, sleeper):
    bus, project = env
    elsewhere = project.parent / 'elsewhere'
    elsewhere.mkdir()
    with running_service(bus):
        first = attach(bus, pid_owner(sleeper.pid), project, 'shared', harness='claude')
        with pytest.raises(ValueError, match='thread_already_attached'):
            attach(bus, pid_owner(), project, 'shared', harness='claude')
        with pytest.raises(ValueError, match='session_scope_changed'):
            attach(bus, pid_owner(sleeper.pid), elsewhere, 'shared', harness='claude')
        sleeper.terminate()
        sleeper.wait(timeout=5)
        # The old owner is gone: a new owner gets a fresh incarnation.
        second = attach(bus, pid_owner(), project, 'shared', harness='claude')
        assert second['session'] != first['session']
        assert attach(bus, pid_owner(), project, 'shared', 'SessionEnd', harness='claude')['closed'] is True
        assert participant(bus, second['recipient'])['closed'] == 1


def test_expired_lease_starts_a_fresh_incarnation_instead_of_failing_forever(env):
    bus, project = env
    with running_service(bus):
        first = attach(bus, pid_owner(), project, 'expiring', harness='claude')
        with agent_msg.open_db(str(bus)) as db, db:
            db.execute('UPDATE participants SET expires=0')
        second = attach(bus, pid_owner(), project, 'expiring', 'UserPromptSubmit', harness='claude')
        assert second['session'] != first['session']
        assert participant(bus, second['recipient'])['closed'] == 0


def _raw(bus, payload):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(5)
        client.connect(str(service.socket_path(str(bus))))
        client.sendall(payload)
        return json.loads(client.makefile('rb').readline())


def test_requests_are_bounded_peer_checked_and_never_echo_input(env):
    bus, project = env
    with running_service(bus) as svc:
        assert service.socket_path(str(bus)).stat().st_mode & 0o777 == 0o600
        assert _raw(bus, b'x' * (service.MAX_REQUEST + 10) + b'\n') == {'ok': False, 'error': 'request_limit'}
        reply = _raw(bus, b'{"op": "SECRET_PROMPT_TEXT\n')
        assert reply == {'ok': False, 'error': 'invalid_request'}
        reply = _raw(bus, json.dumps({'op': 'attach', 'harness': 'claude', 'thread': 'x',
                                      'project': 'SECRET_PROMPT_TEXT', 'owner': pid_owner(),
                                      'event': 'SessionStart'}).encode() + b'\n')
        assert 'SECRET' not in json.dumps(reply) and reply['ok'] is False
        with pytest.raises(ValueError, match='owner_not_live'):
            attach(bus, {'kind': 'pid', 'pid': os.getpid(), 'identity': ['x', 'y']}, project, 'z', harness='claude')
        svc.uid = os.getuid() + 1
        # Credential checks happen on connect. Read the refusal without racing
        # a client write against the server closing an unauthenticated socket.
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(5)
            client.connect(str(service.socket_path(str(bus))))
            with client.makefile('rb') as response:
                assert json.loads(response.readline()) == {'ok': False, 'error': 'peer_refused'}
        # An ordinary caller can observe either the refusal or a transport reset.
        with pytest.raises(ValueError, match='^(peer_refused|service_unavailable)$'):
            service.request(str(bus), {'op': 'ping'}, start=False)


def test_managed_daemon_evidence_requires_the_private_control_socket(isolated_service_runtime, monkeypatch):
    home = isolated_service_runtime / 'codex'
    monkeypatch.setenv('CODEX_HOME', str(home))
    with pytest.raises(ValueError, match='codex_daemon_unavailable'):
        terminal.managed_daemon_evidence()
    (home / 'app-server-control').mkdir(parents=True)
    sock = home / 'app-server-control' / 'app-server-control.sock'
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(sock))
        assert terminal.managed_daemon_evidence() == {'kind': 'codex_thread', 'socket': str(sock)}


def test_stale_loaded_snapshot_never_retires_a_thread_fresh_evidence_shows_live(env):
    """Review finding 1: retire only on fresh per-thread evidence, not a list snapshot."""
    bus, project = env
    daemon, thread = Daemon(), str(uuid.uuid4())
    daemon.threads[thread] = ('idle', str(project))
    with running_service(bus, codex_read=daemon.read, codex_loaded=lambda sock: set()) as svc:
        recipient = attach(bus, DAEMON, project, thread)['recipient']
        svc.tick(force=True)  # snapshot omits the thread, thread/read says it is loaded
        assert participant(bus, recipient)['closed'] == 0
        daemon.threads[thread] = ('notLoaded', str(project))
        svc.tick(force=True)
        assert participant(bus, recipient)['closed'] == 1


def test_contended_attach_checks_the_old_codex_owner_without_holding_the_lock(env, sleeper):
    """Review finding 2: a slow daemon RPC must not stall unrelated requests."""
    import threading
    import time
    bus, project = env
    thread, release, entered = str(uuid.uuid4()), threading.Event(), threading.Event()
    slow = {'enabled': False}
    def read(sock, tid):
        if slow['enabled']:
            entered.set()
            release.wait(10)
        return ('idle', str(project))
    with running_service(bus, codex_read=read, codex_loaded=lambda sock: {thread}) as svc:
        attach(bus, DAEMON, project, thread)
        slow['enabled'] = True
        contender = threading.Thread(target=lambda: pytest.raises(
            ValueError, attach, bus, pid_owner(sleeper.pid), project, thread), daemon=True)
        contender.start()
        assert entered.wait(5)
        started = time.monotonic()
        svc.status()
        assert time.monotonic() - started < 1
        release.set()
        contender.join(10)


def test_session_end_reports_closing_a_dead_owners_attachment(env, sleeper):
    """Review finding 3: the reply must match what happened."""
    bus, project = env
    with running_service(bus):
        recipient = attach(bus, pid_owner(sleeper.pid), project, 'ending', harness='claude')['recipient']
        sleeper.terminate()
        sleeper.wait(timeout=5)
        assert attach(bus, pid_owner(), project, 'ending', 'SessionEnd', harness='claude')['closed'] is True
        assert participant(bus, recipient)['closed'] == 1


def test_unexpected_evidence_error_does_not_skip_supervision_of_other_sessions(env, sleeper):
    """Opus review 1: one bad daemon must not stop renewal and pid checks for the bus."""
    bus, project = env
    thread = str(uuid.uuid4())
    def broken(sock):
        raise AttributeError('daemon returned a non-object result')
    with running_service(bus, codex_read=lambda s, t: ('idle', str(project)), codex_loaded=broken) as svc:
        live = attach(bus, pid_owner(), project, 'alive_owner', harness='claude')['recipient']
        dead = attach(bus, pid_owner(sleeper.pid), project, 'dead_owner', harness='claude')['recipient']
        attach(bus, DAEMON, project, thread)
        with agent_msg.open_db(str(bus)) as db, db:
            db.execute('UPDATE participants SET expires=expires-60')
        before = participant(bus, live)['expires']
        sleeper.terminate()
        sleeper.wait(timeout=5)
        svc.tick(force=True)
        assert participant(bus, live)['expires'] > before
        assert participant(bus, dead)['closed'] == 1


def test_codex_rpc_non_object_results_are_daemon_unavailable(monkeypatch):
    import codex_rpc
    class Fake:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def call(self, method, params):
            return None
    monkeypatch.setattr(codex_rpc, 'CodexRPC', Fake)
    for probe in (lambda: service.codex_read('/fixture.sock', 'x'), lambda: service.codex_loaded('/fixture.sock')):
        with pytest.raises(ValueError, match='codex_daemon_unavailable'):
            probe()


def test_database_error_during_attach_never_leaks_the_service_lock(env, monkeypatch):
    """Opus review 2: a failing re-read under the lock must still release it."""
    import sqlite3
    bus, project = env
    with running_service(bus) as svc:
        original, calls = svc._row, {'n': 0}
        def flaky(recipient):
            calls['n'] += 1
            if calls['n'] == 2:
                raise sqlite3.OperationalError('database is locked')
            return original(recipient)
        monkeypatch.setattr(svc, '_row', flaky)
        with pytest.raises(ValueError, match='internal_error|invalid_request|service_unavailable'):
            attach(bus, pid_owner(), project, 'locked', harness='claude')
        assert svc.lock.acquire(timeout=2)
        svc.lock.release()


def test_session_end_reports_retiring_an_expired_attachment(env):
    """Opus review 3: the expired-lease branch also counts as closing."""
    bus, project = env
    with running_service(bus):
        recipient = attach(bus, pid_owner(), project, 'expired_end', harness='claude')['recipient']
        with agent_msg.open_db(str(bus)) as db, db:
            db.execute('UPDATE participants SET expires=0')
        assert attach(bus, pid_owner(), project, 'expired_end', 'SessionEnd', harness='claude')['closed'] is True
        assert participant(bus, recipient)['closed'] == 1


def test_unreachable_daemon_threads_retire_without_per_thread_rpcs(env):
    """Opus review 4: after two failed polls, do not call a hung daemon once per thread."""
    bus, project = env
    threads, reads = [str(uuid.uuid4()) for _ in range(3)], []
    def read(sock, thread):
        reads.append(thread)
        return ('idle', str(project))
    def down(sock):
        raise ValueError('codex_daemon_unavailable')
    with running_service(bus, codex_read=read, codex_loaded=down) as svc:
        recipients = [attach(bus, DAEMON, project, t)['recipient'] for t in threads]
        reads.clear()
        svc.tick(force=True)
        svc.tick(force=True)
        assert reads == []
        assert all(participant(bus, r)['closed'] == 1 for r in recipients)
