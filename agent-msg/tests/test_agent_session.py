from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time

import pytest

import agent_broadcast as membership
import agent_msg
import agent_session
from .test_broadcast import Client, codex_join


def wait_until(predicate, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.03)
    pytest.fail('session condition did not become true')


@contextmanager
def launched(tmp_path, harness='worker', ready=True, ignore_term=False):
    path = str(tmp_path / 'bus.db')
    db = agent_msg.open_db(path)
    child_code = (
        'import os,subprocess,sys,time\n'
        + ('import signal; signal.signal(signal.SIGTERM,signal.SIG_IGN)\n' if ignore_term else '')
        + (f'subprocess.run([sys.executable,{agent_msg.__file__!r},"drain","--as",os.environ["AGENT_MSG_ID"]],check=True)\n' if ready else '')
        + 'time.sleep(30)\n'
    )
    env = {**os.environ, 'AGENT_MSG_DB': path}
    env.pop('AGENT_MSG_SESSION', None)
    command = [sys.executable, agent_msg.__file__, 'run', '--as', 'worker',
               '--harness', harness, '--project', str(tmp_path), '--', sys.executable, '-c', child_code]
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        row = wait_until(lambda: db.execute('SELECT * FROM participants WHERE recipient="worker" AND ready=?', (int(ready),)).fetchone())
        yield db, process, dict(row)
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            for pid in Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split():
                os.kill(int(pid), signal.SIGKILL)
            process.kill()
            process.communicate(timeout=5)
        db.close()


@pytest.mark.parametrize('harness', ['claude', 'codex', 'kimi', 'opencode', 'pi', 'worker'])
def test_launcher_requires_working_hook_and_closes_on_exit(tmp_path, harness):
    with launched(tmp_path, harness) as (db, process, row):
        assert row['harness'] == harness
        result = membership.broadcast(db, 'sender', 'hello', 'once')
        assert result['recipient_count'] == 1
        process.terminate()
        process.communicate(timeout=5)
        assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1
        assert membership.broadcast(db, 'sender', 'hello', dry_run=True)['recipient_count'] == 0


def test_installed_but_no_working_hook_is_excluded(tmp_path):
    with launched(tmp_path, ready=False) as (db, _, _row):
        preview = membership.broadcast(db, 'sender', 'hello', dry_run=True)
        assert preview['recipient_count'] == 0
        assert preview['excluded_counts'] == {'integration_not_ready': 1}


def test_shutdown_is_bounded_when_harness_ignores_termination(tmp_path):
    with launched(tmp_path, ignore_term=True) as (db, process, _):
        process.terminate()
        process.wait(timeout=4)
        assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1


def test_worker_session_receives_broadcast_on_next_drain(tmp_path):
    path = str(tmp_path / 'bus.db')
    gate = tmp_path / 'next-turn'
    child = (f'import os,subprocess,sys,time; from pathlib import Path\n'
             f'command=[sys.executable,{agent_msg.__file__!r},"drain","--as",os.environ["AGENT_MSG_ID"]]\n'
             'subprocess.run(command,check=True)\n'
             f'while not Path({str(gate)!r}).exists(): time.sleep(0.02)\n'
             'subprocess.run(command,check=True)\n')
    env = {**os.environ, 'AGENT_MSG_DB': path}
    db = agent_msg.open_db(path)
    process = subprocess.Popen([sys.executable, agent_msg.__file__, 'run', '--as', 'worker',
        '--harness', 'worker', '--project', str(tmp_path), '--', sys.executable, '-c', child],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        wait_until(lambda: db.execute('SELECT 1 FROM participants WHERE ready=1').fetchone())
        result = membership.broadcast(db, 'sender', 'BROADCAST_WORKER_CANARY', 'once')
        gate.touch()
        output, errors = process.communicate(timeout=5)
        assert process.returncode == 0, errors
        assert output.count(b'BROADCAST_WORKER_CANARY') == 1
        assert result['broadcast_id'].encode() in output
        assert membership.status(db, result['broadcast_id'])['counts'] == {'consumed': 1}
    finally:
        if process.poll() is None:
            process.terminate()
        process.communicate(timeout=5)
        db.close()


def test_idle_process_renews_without_another_model_turn(tmp_path):
    # Run the real supervisor with a short lease; one initial drain is sufficient.
    path = str(tmp_path / 'bus.db')
    db = agent_msg.open_db(path)
    child = f'import subprocess,sys,time; subprocess.run([sys.executable,{agent_msg.__file__!r},"drain","--as","idle"],check=True); time.sleep(2.5)'
    code = ('import agent_session,sys; sys.exit(agent_session.run('
            f'{path!r},"idle",{str(tmp_path)!r},"worker",'
            f'[sys.executable,"-c",{child!r}],interval=0.15,ttl=1))')
    env = {**os.environ, 'PYTHONPATH': str(Path(agent_msg.__file__).parent)}
    process = subprocess.Popen([sys.executable, '-c', code], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        initial = wait_until(lambda: db.execute('SELECT * FROM participants WHERE ready=1 AND expires<?', (time.time() + 3,)).fetchone())
        wait_until(lambda: db.execute('SELECT expires FROM participants').fetchone()[0] > initial['expires'] + 1)
        assert membership.broadcast(db, 'sender', 'still idle', 'once')['recipient_count'] == 1
        assert process.wait(timeout=5) == 0
        assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1
    finally:
        if process.poll() is None:
            process.terminate()
        process.communicate(timeout=5)
        db.close()


def test_crash_leaves_finite_lease_and_process_identity_rejects_dead_owner(tmp_path):
    with launched(tmp_path) as (db, process, row):
        identity = agent_session.process_identity(process.pid)
        assert identity
        # Kill supervisor and child together, without touching any unrelated process.
        children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
        for pid in children:
            os.kill(int(pid), signal.SIGTERM)
        process.kill()
        process.communicate(timeout=5)
        assert agent_session.process_identity(process.pid) is None
        assert membership.broadcast(db, 'sender', 'expired', dry_run=True,
                                    now=row['expires'] + 1)['excluded_counts'] == {'expired': 1}


def test_watcher_verifies_exact_thread_and_never_resumes_it(tmp_path, monkeypatch):
    import codex_bridge
    path = str(tmp_path / 'bus.db')
    db = agent_msg.open_db(path)
    token = codex_join(db, tmp_path, 'worker')
    membership.close(db, 'worker', token)
    clients = []
    def connector(bound):
        client = Client(bound)
        clients.append(client)
        return client
    identities = iter(['owner', 'owner', 'owner', None])
    monkeypatch.setattr(agent_session, 'process_identity', lambda _: next(identities))
    assert agent_session.watch(db, 'worker', 123, 'worker', connector=connector, interval=0.01, attachment_probe=lambda _: True) == 0
    assert all(method == 'thread/read' for client in clients for method, _ in client.calls)
    assert db.execute('SELECT harness,closed FROM participants').fetchone()[:] == ('worker', 1)
    db.close()


def test_watcher_closes_on_binding_change_or_failed_verification(tmp_path, monkeypatch):
    path = str(tmp_path / 'bus.db')
    db = agent_msg.open_db(path)
    token = codex_join(db, tmp_path, 'peer')
    membership.close(db, 'peer', token)
    monkeypatch.setattr(agent_session, 'process_identity', lambda _: 'owner')
    calls = 0
    def fail_after_registration(bound):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError('thread_identity_mismatch')
        return Client(bound)
    with pytest.raises(ValueError, match='thread_identity_mismatch'):
        agent_session.watch(db, 'peer', 123, connector=fail_after_registration, interval=0.01, attachment_probe=lambda _: True)
    assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1
    db.close()


def test_missing_command_does_not_leave_live_membership(tmp_path):
    path = str(tmp_path / 'bus.db')
    with pytest.raises(ValueError, match='launch_failed'):
        agent_session.run(path, 'peer', str(tmp_path), 'worker', ['/no/such/harness'])
    db = agent_msg.open_db(path)
    assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1
    db.close()


@pytest.mark.parametrize('failure', [sqlite3.OperationalError('database locked'), ValueError('session_expired_or_replaced')])
def test_presence_failure_does_not_terminate_harness_work(tmp_path, monkeypatch, failure):
    marker = tmp_path / 'completed'
    command = [sys.executable, '-c',
               f'import time; from pathlib import Path; time.sleep(0.4); Path({str(marker)!r}).touch()']
    def failed_renewal(*args, **kwargs):
        raise failure
    monkeypatch.setattr(membership, 'renew', failed_renewal)
    assert agent_session.run(str(tmp_path / 'bus.db'), 'peer', str(tmp_path), 'worker',
                             command, interval=0.05) == 0
    assert marker.exists()
