"""Exact-session regression tests; all stores and peers are disposable."""
import time

import pytest

import agent_msg as bus
import agent_broadcast as membership
from secure_broker import Broker, Refused


def join(db, project, name):
    sid = membership.register(db, name, str(project), 'fixture')
    membership.renew(db, name, sid, ready=True)
    return sid


def test_direct_send_refuses_unknown_and_retired_recipient(tmp_path):
    with bus.open_db(str(tmp_path / 'bus.db')) as db:
        with pytest.raises(ValueError, match='recipient_unavailable'):
            bus.send(db, 'sender', 'missing', 'must not queue')
        sid = join(db, tmp_path, 'peer')
        membership.close(db, 'peer', sid)
        with pytest.raises(ValueError, match='recipient_unavailable'):
            bus.send(db, 'sender', 'peer', 'must not queue')
        assert db.execute('SELECT COUNT(*) FROM messages').fetchone()[0] == 0


def test_direct_mail_cannot_follow_reused_alias(tmp_path):
    with bus.open_db(str(tmp_path / 'bus.db')) as db:
        old = join(db, tmp_path, 'peer')
        bus.send(db, 'sender', 'peer', 'for old incarnation')
        membership.close(db, 'peer', old)
        new = join(db, tmp_path, 'peer')
        assert bus.fetch_unread(db, 'peer', session=new) == []
        assert bus.fetch_unread(db, 'peer') == []


def test_old_unpinned_mail_is_inspectable_but_not_deliverable(tmp_path):
    with bus.open_db(str(tmp_path / 'bus.db')) as db:
        db.execute("INSERT INTO messages(ts,sender,recipient,type,body) VALUES (?, 'old', 'peer', 'notify', 'historical')", (time.time(),))
        db.commit()
        sid = join(db, tmp_path, 'peer')
        assert bus.fetch_unread(db, 'peer', session=sid) == []
        assert len(bus.fetch_unread(db, 'peer', inspect=True)) == 1


def test_broker_requires_attached_recipient(tmp_path):
    broker = Broker(tmp_path / 'private' / 'db')
    try:
        a = broker.enroll('a', 'fixture', str(tmp_path))
        b = broker.enroll('b', 'fixture', str(tmp_path))
        ctx = broker.mesh([a['id'], b['id']])['context_id']
        with pytest.raises(Refused, match='not_ready'):
            broker.call(a['credential'], 'send', dict(context_id=ctx, to=b['id'], body='no receiver', key='x'))
    finally:
        broker.close()


@pytest.fixture
def registry(tmp_path):
    wall, mono = [1000.0], [10.0]
    broker = Broker(tmp_path / 'control' / 'db', clock=lambda: wall[0], monotonic=lambda: mono[0])
    peers = [broker.enroll(label, 'fixture', str(tmp_path)) for label in ('build', 'review', 'outsider')]
    for peer in peers:
        peer.update(broker.attach(peer['id'], 'fixture', peer['id'], 'thread_attachment', ready=True))
    ctx = broker.mesh([peer['id'] for peer in peers[:2]])['context_id']
    yield broker, peers, ctx, wall, mono
    broker.close()


def call(broker, peer, op, **args):
    return broker.call(peer['credential'], op, args)


def test_directory_is_scoped_and_identity_has_no_credentials(registry):
    import json
    broker, (a, b, c), ctx, _, _ = registry
    identity = call(broker, a, 'whoami')
    assert identity['id'] == a['id'] and identity['contexts'] == [ctx]
    peers = call(broker, a, 'peers', context_id=ctx)['peers']
    assert [p['id'] for p in peers] == [b['id']]
    assert peers[0]['sendable'] and not peers[0]['wakeup']
    assert b['credential'] not in json.dumps(peers)
    assert call(broker, a, 'resolve', context_id=ctx, label='review')['id'] == b['id']
    with pytest.raises(Refused, match='denied'):
        call(broker, c, 'peers', context_id=ctx)
    for op in ('attach', 'renew', 'retire', 'directory'):
        with pytest.raises(Refused, match='invalid_request'):
            call(broker, a, op)


def test_lease_is_monotonic_late_renewal_never_revives(registry):
    broker, (a, b, _), ctx, wall, mono = registry
    wall[0] -= 900
    assert call(broker, a, 'whoami')['state'] == 'active'
    mono[0] += 91
    assert call(broker, a, 'whoami')['state'] == 'stale'
    with pytest.raises(Refused, match='attachment_expired'):
        broker.renew(b['id'], b['attachment_id'])
    with pytest.raises(Refused, match='new_incarnation'):
        broker.attach(b['id'], 'fixture', b['id'], 'thread_attachment', ready=True)
    with pytest.raises(Refused, match='recipient_stale'):
        call(broker, a, 'send', context_id=ctx, to=b['id'], body='late', key='late')


def test_retired_peer_and_replacement_do_not_share_mail_or_grants(registry):
    broker, (a, b, _), ctx, _, _ = registry
    sent = call(broker, a, 'send', context_id=ctx, to=b['id'], body='old', key='old')
    broker.retire(b['id'], b['attachment_id'])
    replacement = broker.enroll('review', 'fixture', a.get('project', '/fixture'))
    broker.attach(replacement['id'], 'fixture', b['id'], 'thread_attachment', ready=True)
    with pytest.raises(Refused, match='recipient_retired'):
        call(broker, a, 'send', context_id=ctx, to=b['id'], body='new', key='new')
    assert call(broker, a, 'send', context_id=ctx, to=b['id'], body='old', key='old') == sent
    with pytest.raises(Refused, match='denied'):
        call(broker, replacement, 'inbox', context_id=ctx)
    new_ctx = broker.mesh([a['id'], replacement['id']])['context_id']
    assert call(broker, replacement, 'inbox', context_id=new_ctx)['messages'] == []


def test_pause_stale_selection_and_delivery_recheck(registry):
    broker, (a, b, _), ctx, _, _ = registry
    with pytest.raises(Refused, match='stale_selection'):
        call(broker, a, 'send', context_id=ctx, to=b['id'], body='x', key='bad', expected_route_revision=2)
    sent = call(broker, a, 'send', context_id=ctx, to=b['id'], body='x', key='good', expected_route_revision=1)
    broker.pause(b['id'])
    assert not call(broker, a, 'peers', context_id=ctx)['peers'][0]['sendable']
    with pytest.raises(Refused, match='recipient_paused'):
        call(broker, b, 'inbox', context_id=ctx)
    broker.pause(b['id'], False)
    inbox = call(broker, b, 'inbox', context_id=ctx)['messages']
    assert inbox[0]['id'] == sent['message_ids'][0]
    assert inbox[0]['reply_address'] == a['id']
    broker.retire(b['id'], b['attachment_id'])
    with pytest.raises(Refused, match='recipient_retired'):
        call(broker, b, 'inbox', context_id=ctx)


def test_duplicate_labels_and_namespaced_native_threads(registry):
    broker, (a, b, _), ctx, _, _ = registry
    d = broker.enroll('review', 'fixture', '/fixture')
    with pytest.raises(Refused, match='thread_already_attached'):
        broker.attach(d['id'], 'fixture', b['id'], 'thread_attachment', ready=True)
    broker.attach(d['id'], 'different-instance', b['id'], 'thread_attachment', ready=True)
    ctx = broker.mesh([a['id'], b['id'], d['id']])['context_id']
    with pytest.raises(Refused, match='ambiguous'):
        call(broker, a, 'resolve', context_id=ctx, label='review')
    first = call(broker, a, 'peers', context_id=ctx, limit=1)
    second = call(broker, a, 'peers', context_id=ctx, limit=1, offset=first['next_offset'])
    assert first['peers'][0]['id'] != second['peers'][0]['id']
    with pytest.raises(Refused, match='invalid_page'):
        call(broker, a, 'peers', context_id=ctx, limit=True)


def test_restart_fences_old_attachments_but_keeps_identity_and_receipt(registry):
    broker, (a, b, _), ctx, wall, mono = registry
    sent = call(broker, a, 'send', context_id=ctx, to=b['id'], body='old', key='old')
    broker.close()
    reopened = Broker(broker.path, clock=lambda: wall[0], monotonic=lambda: mono[0])
    try:
        assert call(reopened, b, 'whoami')['state'] == 'stale'
        with pytest.raises(Refused, match='recipient_stale'):
            call(reopened, b, 'inbox', context_id=ctx)
        assert call(reopened, a, 'send', context_id=ctx, to=b['id'], body='old', key='old') == sent
    finally:
        reopened.close()


def test_shared_process_alone_does_not_prove_thread_attachment(tmp_path):
    import os
    import uuid
    import agent_session
    import codex_bridge
    db = codex_bridge.database(str(tmp_path / 'bus.db'))
    try:
        codex_bridge.register(db, 'peer', str(uuid.uuid4()), str(tmp_path), '/unused.sock')
        with pytest.raises(ValueError, match='exact_thread_attachment_required'):
            agent_session.watch(db, 'peer', os.getpid())
        sid = membership.register(db, 'peer', str(tmp_path), 'codex', 'codex')
        membership.renew(db, 'peer', sid, ready=True)
        with pytest.raises(ValueError, match='recipient_unavailable'):
            bus.send(db, 'sender', 'peer', 'no attachment')
    finally:
        db.close()


def test_bridge_rechecks_after_connection_and_never_retargets(tmp_path):
    import uuid
    import codex_bridge
    from .registry_helpers import hook
    from .test_codex_bridge import Server
    path = str(tmp_path / 'bus.db')
    db = codex_bridge.database(path)
    try:
        tid = str(uuid.uuid4())
        codex_bridge.register(db, 'peer', tid, str(tmp_path), '/unused.sock')
        with db:
            db.execute('UPDATE codex_bindings SET enabled=1')
        hook(db, tmp_path, 'peer', route='codex')
        bus.send(db, 'sender', 'peer', 'race')
        server = Server(tid, str(tmp_path))
        def connector(_):
            with db:
                db.execute('UPDATE codex_bindings SET thread_id=?', (str(uuid.uuid4()),))
            return server
        result = codex_bridge.tick(db, path, connector)
        assert result['state'] == 'blocked' and result['reason'] == 'binding_changed'
        assert not any(method == 'turn/start' for method, _ in server.calls)
    finally:
        db.close()


def test_hook_mail_cannot_be_consumed_after_route_becomes_bridge(tmp_path):
    import uuid
    import codex_bridge
    with codex_bridge.database(str(tmp_path / 'bus.db')) as db:
        sid = join(db, tmp_path, 'peer')
        bus.send(db, 'sender', 'peer', 'hook mail')
        codex_bridge.register(db, 'peer', str(uuid.uuid4()), str(tmp_path), '/unused.sock')
        assert bus.fetch_unread(db, 'peer', session=sid) == []


def test_bridge_rechecks_wakeup_allowance_after_connection(tmp_path):
    import uuid
    import codex_bridge
    from .registry_helpers import hook
    from .test_codex_bridge import Server
    path = str(tmp_path / 'bus.db')
    with codex_bridge.database(path) as db:
        tid = str(uuid.uuid4())
        codex_bridge.register(db, 'peer', tid, str(tmp_path), '/unused.sock')
        with db:
            db.execute('UPDATE codex_bindings SET enabled=1')
        hook(db, tmp_path, 'peer', route='codex')
        bus.send(db, 'sender', 'peer', 'race')
        server = Server(tid, str(tmp_path))
        def connector(_):
            with db:
                db.execute('UPDATE codex_bindings SET wakeups=max_wakeups')
            return server
        result = codex_bridge.tick(db, path, connector)
        assert result['state'] == 'blocked' and result['reason'] == 'wakeup_budget_exhausted'
        assert not any(method == 'turn/start' for method, _ in server.calls)


def test_two_supervised_processes_discover_exchange_and_retire(tmp_path):
    import json
    import os
    from pathlib import Path
    import sys
    import threading
    from secure_broker import BrokerServer
    import secure_session

    broker = Broker(tmp_path / 'control' / 'db')
    peers = [broker.enroll(label, 'fixture', str(tmp_path)) for label in ('a', 'b')]
    ctx = broker.mesh([p['id'] for p in peers])['context_id']
    socket_path = tmp_path / 'peer.sock'
    threads, outcomes = [], []
    # Real subprocesses and socket; no model, shared PID or live conversation.
    code = '''import json,os,time
from pathlib import Path
from secure_broker import request
cred=Path(os.environ['RESONANCE_CREDENTIAL_FILE']).read_text()
def call(op, **args): return request(os.environ['RESONANCE_PEER_SOCKET'],cred,op,**args)
ctx=os.environ['TEST_CONTEXT']
mine=call('whoami')
call('inbox',context_id=ctx)
deadline=time.monotonic()+8
while time.monotonic()<deadline:
    peers=call('peers',context_id=ctx)['peers']
    if len(peers)==1 and peers[0]['sendable']: break
    time.sleep(.03)
else: raise AssertionError('other receiver not ready')
call('send',context_id=ctx,to=peers[0]['id'],body=mine['id'],key='one')
while time.monotonic()<deadline:
    mail=call('inbox',context_id=ctx)['messages']
    if mail:
        assert mail[0]['body']==peers[0]['id']
        Path(os.environ['TEST_RECEIVED']).write_text(mine['id'])
        break
    time.sleep(.03)
else: raise AssertionError('mail not delivered')
while not Path(os.environ['TEST_GATE']).exists(): time.sleep(.03)
'''
    with BrokerServer(socket_path, broker) as server:
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        try:
            for n, peer in enumerate(peers):
                credential = tmp_path / 'control' / f'credential-{n}'
                credential.write_text(peer['credential'])
                env = {**os.environ, 'PYTHONPATH': str(Path(bus.__file__).parent),
                       'RESONANCE_CREDENTIAL_FILE': str(credential), 'RESONANCE_PEER_SOCKET': str(socket_path),
                       'TEST_CONTEXT': ctx, 'TEST_RECEIVED': str(tmp_path / f'received-{n}'),
                       'TEST_GATE': str(tmp_path / 'gate')}
                thread = threading.Thread(target=lambda p=peer, e=env: outcomes.append(
                    secure_session.run(broker, p['id'], [sys.executable, '-c', code], env=e, interval=.15, ttl=1)))
                threads.append(thread)
                thread.start()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and not all((tmp_path / f'received-{n}').exists() for n in (0, 1)):
                time.sleep(.03)
            assert all((tmp_path / f'received-{n}').exists() for n in (0, 1))
            # Several lease periods while both processes are idle, with no peer calls.
            time.sleep(1.2)
            assert all(p['state'] == 'active' for p in broker.directory()['peers'])
        finally:
            (tmp_path / 'gate').touch()
            for thread in threads:
                thread.join(12)
            server.shutdown()
            server_thread.join(5)
            states = broker.directory()['peers']
            broker.close()
    assert outcomes == [0, 0]
    assert all(p['state'] == 'retired' for p in states)


def test_protected_cli_uses_credentials_and_never_falls_back(registry, tmp_path):
    import json
    import os
    from pathlib import Path
    import subprocess
    import sys
    import threading
    from secure_broker import BrokerServer

    broker, (a, b, _), ctx, _, _ = registry
    credential = tmp_path / 'control' / 'credential'
    credential.write_text(a['credential'])
    path = tmp_path / 'cli.sock'
    legacy = tmp_path / 'must-not-create.db'
    env = {**os.environ, 'RESONANCE_CREDENTIAL_FILE': str(credential),
           'RESONANCE_PEER_SOCKET': str(path), 'AGENT_MSG_DB': str(legacy)}
    def cli(*args):
        return subprocess.run([sys.executable, str(Path(bus.__file__)), *args],
                              env=env, capture_output=True, text=True, timeout=10)
    with BrokerServer(path, broker) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            identity = cli('whoami')
            assert identity.returncode == 0 and json.loads(identity.stdout)['id'] == a['id']
            peers = cli('peers', '--context', ctx)
            assert peers.returncode == 0 and json.loads(peers.stdout)['peers'][0]['id'] == b['id']
            sent = cli('send', '--context', ctx, '--key', 'cli', '--to', b['id'], 'CLI message')
            assert sent.returncode == 0 and json.loads(sent.stdout)['targets'][0]['recipient'] == b['id']
            assert a['credential'] not in identity.stdout + peers.stdout + sent.stdout
            assert cli('send', '--to', 'alias', 'no context').returncode == 2
        finally:
            server.shutdown()
            worker.join(5)
    assert cli('whoami').returncode == 2
    assert not legacy.exists()
