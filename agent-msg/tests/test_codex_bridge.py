import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
import threading
from contextlib import contextmanager
from websockets.sync.server import unix_serve

import pytest
from .registry_helpers import hook

import agent_msg
import codex_bridge as bridge
from codex_rpc import CodexRPC, TransportError


def test_missing_socket_is_transport_error_and_cleanup_is_repeatable(tmp_path):
    client = CodexRPC(str(tmp_path / 'missing.sock'), timeout=0.1)
    with pytest.raises(TransportError, match='connection_unavailable'):
        client.__enter__()
    client.close()
    assert client.connection is None and client.manager is None


def test_failed_handshake_does_not_exit_unentered_manager(monkeypatch):
    class FailedManager:
        def __enter__(self):
            raise OSError('handshake failed')
        def __exit__(self, *args):
            pytest.fail('manager was never entered')
    monkeypatch.setattr('websockets.sync.client.unix_connect', lambda *a, **kw: FailedManager())
    client = CodexRPC('/unused.sock')
    with pytest.raises(TransportError, match='connection_unavailable'):
        client.__enter__()
    client.close()


@pytest.fixture
def registered(tmp_path):
    path = tmp_path / 'bus.db'
    db = bridge.database(str(path))
    thread = str(uuid.uuid4())
    bridge.register(db, 'worker', thread, str(tmp_path), str(tmp_path / 'server.sock'), cooldown=0)
    with db:
        db.execute('UPDATE codex_bindings SET enabled=1')
    hook(db, tmp_path, 'worker', route='codex')
    yield db, str(path), thread
    db.close()


class Server:
    def __init__(self, thread, project, outcome='ok', active=False):
        self.thread, self.project, self.outcome = thread, project, outcome
        self.calls, self.active = [], active

    def call(self, method, params):
        self.calls.append((method, params))
        if method == 'thread/read':
            return {'thread': {'id': self.thread, 'cwd': self.project}}
        if method == 'thread/loaded/list':
            return {'data': [self.thread]}
        if method == 'turn/start':
            if self.outcome == 'lost':
                raise TransportError('lost_ack')
            self.active = True
            return {'turn': {'id': 'turn-existing' if len(self.calls) > 3 else 'turn-new'}}
        return {}

    def close(self):
        pass


@pytest.mark.parametrize('active', [False, True])
def test_idle_or_active_recipient_accepts_tool_output_without_model_poll(registered, active):
    db, path, thread = registered
    mid = agent_msg.send(db, 'sender', 'worker', 'Please inspect the saved diagnostic.', 'request')
    server = Server(thread, str(Path(path).parent), active=active)
    result = bridge.tick(db, path, lambda _: server)
    assert result['state'] == 'accepted' and server.active
    params = server.calls[-1][1]
    assert params['input'] == [] and params['threadId'] == thread
    assert set(params) == {'threadId', 'input', 'toolOutput'}
    envelope = json.loads(params['toolOutput']['output'])
    assert envelope['message_id'] == mid and envelope['sender'] == 'sender'
    assert envelope['source'] == 'resonance-agent-msg'
    assert db.execute('SELECT read_at FROM messages WHERE id=?', (mid,)).fetchone()[0]
    assert bridge.tick(db, path, lambda _: pytest.fail('duplicate delivery'))['state'] == 'idle'


def test_lost_ack_blocks_recipient_until_explicit_resolution(registered):
    db, path, thread = registered
    mid = agent_msg.send(db, 'sender', 'worker', 'one')
    agent_msg.send(db, 'sender', 'worker', 'two')
    server = Server(thread, str(Path(path).parent), outcome='lost')
    assert bridge.tick(db, path, lambda _: server)['state'] == 'unknown'
    assert db.execute('SELECT read_at FROM messages WHERE id=?', (mid,)).fetchone()[0] is None
    assert bridge.tick(db, path, lambda _: pytest.fail('unsafe replay'))['state'] == 'idle'
    bridge.resolve(db, mid, 'accepted', 'Verified original tool output in server history', 'verified-turn')
    assert db.execute('SELECT read_at FROM messages WHERE id=?', (mid,)).fetchone()[0]


def test_crash_after_intent_is_unknown_not_replayed(registered):
    db, path, _ = registered
    mid = agent_msg.send(db, 'sender', 'worker', 'one')
    with db:
        db.execute("INSERT INTO codex_deliveries VALUES (?,?,'sending',1,0,'',NULL,'','{}')", (mid, 'worker'))
    assert bridge.tick(db, path, lambda _: pytest.fail('replayed'))['state'] == 'idle'
    assert db.execute('SELECT state FROM codex_deliveries').fetchone()[0] == 'unknown'


def test_connection_failure_retries_only_before_send_and_stops(registered):
    db, path, _ = registered
    agent_msg.send(db, 'sender', 'worker', 'one')
    def unavailable(_):
        raise OSError('private socket details')
    assert bridge.tick(db, path, unavailable, now=100)['state'] == 'pending'
    assert bridge.tick(db, path, unavailable, now=101)['state'] == 'idle'
    assert bridge.tick(db, path, unavailable, now=103)['state'] == 'pending'
    assert bridge.tick(db, path, unavailable, now=110)['state'] == 'blocked'
    assert 'private' not in json.dumps(dict(db.execute('SELECT * FROM codex_deliveries').fetchone()))


def test_pause_budget_wrong_target_and_payload_bounds(registered):
    db, path, thread = registered
    mid = agent_msg.send(db, 'sender', 'worker', 'one')
    with db:
        db.execute('UPDATE codex_bindings SET enabled=0')
    assert bridge.tick(db, path, lambda _: pytest.fail('paused'))['state'] == 'idle'
    with db:
        db.execute('UPDATE codex_bindings SET enabled=1,wakeups=max_wakeups')
    assert bridge.tick(db, path, lambda _: pytest.fail('over budget'))['state'] == 'idle'
    with db:
        db.execute('UPDATE codex_bindings SET wakeups=0')
    server = Server(str(uuid.uuid4()), str(Path(path).parent))
    assert bridge.tick(db, path, lambda _: server)['state'] == 'blocked'
    assert not any(method == 'turn/start' for method, _ in server.calls)
    bridge.resolve(db, mid, 'retry', 'Fixed endpoint after verifying no turn was started')
    with db:
        db.execute('UPDATE messages SET body=? WHERE id=?', ('x' * 20_000, mid))
    assert bridge.tick(db, path, lambda _: pytest.fail('oversize sent'), now=time.time()+10)['state'] == 'blocked'


def test_worker_lock_prevents_concurrent_delivery(registered):
    db, path, _ = registered
    with bridge.worker_lock(path):
        with pytest.raises(ValueError, match='worker_busy'):
            bridge.tick(db, path)


def test_legacy_drain_does_not_consume_bridge_mail(registered, capsys):
    from argparse import Namespace
    db, _, _ = registered
    agent_msg.send(db, 'sender', 'worker', 'one')
    assert agent_msg.cmd_drain(db, Namespace(as_='worker', keep_unread=False)) == 2
    assert agent_msg.cmd_drain(db, Namespace(as_='worker', keep_unread=True)) == 0
    assert len(agent_msg.fetch_unread(db, 'worker', inspect=True)) == 1


@contextmanager
def protocol_server(tmp_path, thread, outcome='ok'):
    log = tmp_path / 'received.json'
    def handler(connection):
        for line in connection:
            request=json.loads(line); method=request['method']; params=request.get('params',{})
            if 'id' not in request: continue
            result={}
            if method=='thread/read': result={'thread':{'id':thread,'cwd':str(tmp_path)}}
            if method=='thread/loaded/list': result={'data':[thread]}
            if method=='turn/start':
                log.write_text(json.dumps(params))
                if outcome=='lost': return
                result={'turn':{'id':'turn-fixture'}}
            connection.send(json.dumps({'id':request['id'],'result':result}))
    with unix_serve(handler, str(tmp_path/'server.sock')) as server:
        worker=threading.Thread(target=server.serve_forever,daemon=True)
        worker.start()
        try: yield log
        finally:
            server.shutdown()
            worker.join(5)


def test_running_worker_delivers_new_message_through_protocol_subprocess(registered):
    db, path, thread = registered
    directory = Path(path).parent
    with protocol_server(directory, thread) as log:
        worker = subprocess.Popen([sys.executable, str(Path(bridge.__file__)), '--db', path,
            'run', '--interval', '0.25'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            time.sleep(0.15)
            mid = agent_msg.send(db, 'peer', 'worker', 'wakeup without a user turn')
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                row = db.execute('SELECT state FROM codex_deliveries WHERE message_id=?', (mid,)).fetchone()
                if row and row[0] == 'accepted': break
                time.sleep(0.05)
            else: pytest.fail('worker did not deliver')
            params = json.loads(log.read_text())
            assert params['toolOutput']['namespace'] == 'resonance'
            assert json.loads(params['toolOutput']['output'])['message_id'] == mid
        finally:
            worker.terminate()
            worker.communicate(timeout=5)
    assert bridge.tick(db, path)['state'] == 'idle'


def test_protocol_disconnect_after_acceptance_stays_unknown(registered):
    db, path, thread = registered
    with protocol_server(Path(path).parent, thread, 'lost') as log:
        agent_msg.send(db, 'peer', 'worker', 'one')
        assert bridge.tick(db, path)['state'] == 'unknown'
        assert log.exists()
        assert bridge.tick(db, path)['state'] == 'idle'


@pytest.mark.parametrize('mode', ['timeout','flood'])
def test_transport_deadline_and_output_bounds(tmp_path,mode):
    from websockets.exceptions import ConnectionClosed
    def handler(connection):
        connection.recv()
        if mode=='timeout':
            time.sleep(0.3)
        else:
            connection.send('x'*1048577)
    with unix_serve(handler,str(tmp_path/'server.sock')) as server:
        worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
        try:
            with pytest.raises((TransportError,ConnectionClosed)):
                CodexRPC(str(tmp_path/'server.sock'),timeout=0.1).__enter__()
        finally:
            server.shutdown();worker.join(5)


def test_unregister_preserves_target_receipt_and_requires_new_session(registered, capsys):
    from argparse import Namespace
    db, path, thread = registered
    agent_msg.send(db, 'sender', 'worker', 'one')
    server = Server(thread, str(Path(path).parent))
    assert bridge.tick(db, path, lambda _: server)['state'] == 'accepted'
    bridge.unregister(db, 'worker')
    target = json.loads(db.execute('SELECT target FROM codex_deliveries').fetchone()[0])
    assert target['thread_id'] == thread
    with pytest.raises(ValueError, match='recipient_unavailable'):
        agent_msg.send(db, 'sender', 'worker', 'two')
    assert agent_msg.cmd_drain(db, Namespace(as_='worker', keep_unread=False)) == 0
    assert not agent_msg.fetch_unread(db, 'worker', inspect=True)


def test_unregister_refuses_ambiguous_delivery(registered):
    db, path, thread = registered
    agent_msg.send(db, 'sender', 'worker', 'one')
    assert bridge.tick(db, path, lambda _: Server(thread, str(Path(path).parent), 'lost'))['state'] == 'unknown'
    with pytest.raises(ValueError, match='unresolved_deliveries'):
        bridge.unregister(db, 'worker')
