import os
from pathlib import Path
import uuid
import json
import queue
import threading

import pytest

import agent_msg
from claude_channel import Attachment
import harness_hooks as hooks
from .service_helpers import running_service
import terminal_session as terminal


@pytest.fixture
def native(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    for key in ('RESONANCE_PEER_SOCKET', 'RESONANCE_CREDENTIAL_FILE'):
        monkeypatch.delenv(key, raising=False)
    owner = lambda: (os.getpid(), list(terminal.process_identity(os.getpid())))
    monkeypatch.setattr(hooks, 'claude_channel_enabled', lambda *_: False)
    return tmp_path, owner


@pytest.mark.parametrize('harness,thread', [('claude', str(uuid.uuid4())), ('kimi', 'session_native_fixture')])
def test_native_hooks_join_drain_and_retire_without_wrapper(native, harness, thread, capsys):
    root, owner = native
    event = dict(session_id=thread, cwd=str(root), hook_event_name='UserPromptSubmit')
    path = str(root / 'bus.db')
    with running_service(path), agent_msg.open_db(path) as db:
        assert hooks.hook(event, harness, path, owner_probe=owner) == 0
        row = db.execute('SELECT * FROM participants').fetchone()
        assert row['harness'] == harness and row['ready'] == 1
        agent_msg.send(db, 'sender', row['recipient'], 'HOOK_PARITY')
        hooks.hook(event, harness, path, owner_probe=owner)
        assert 'HOOK_PARITY' in capsys.readouterr().out
        event['hook_event_name'] = 'SessionEnd'
        hooks.hook(event, harness, path, owner_probe=owner)
        assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1


def test_claude_channel_uses_current_hook_not_stale_mcp_environment(native, monkeypatch):
    root, owner = native
    monkeypatch.setattr(hooks, 'claude_channel_enabled', lambda *_: True)
    event = dict(session_id=str(uuid.uuid4()), cwd=str(root), hook_event_name='SessionStart')
    monkeypatch.setenv('CLAUDE_CODE_SESSION_ID', str(uuid.uuid4()))
    attachment = Attachment(os.getpid())
    try:
        attachment.sync()
        assert attachment.peer is None
        hooks.hook(event, 'claude', str(root / 'bus.db'), owner_probe=owner)
        attachment.sync()
        assert attachment.thread == event['session_id']
        old_session = attachment.peer.session
        mid = agent_msg.send(attachment.peer.db, 'sender', attachment.peer.recipient, 'CHANNEL_PARITY')
        assert attachment.peer.claim()['message_id'] == mid
        attachment.peer.finish(mid, 'unknown')
        assert attachment.peer.db.execute('SELECT read_at FROM messages').fetchone()[0] is None
        attachment.invoke('acknowledge', {'message_id': mid})
        assert attachment.peer.db.execute('SELECT read_at FROM messages').fetchone()[0] is not None
        event['session_id'] = str(uuid.uuid4())
        hooks.hook(event, 'claude', str(root / 'bus.db'), owner_probe=owner)
        attachment.sync()
        assert attachment.peer.session != old_session
        assert attachment.peer.db.execute('SELECT closed FROM participants WHERE session=?', (old_session,)).fetchone()[0] == 1
        event['hook_event_name'] = 'SessionEnd'
        hooks.hook(event, 'claude', str(root / 'bus.db'), owner_probe=owner)
        attachment.sync()
        assert attachment.peer is None
    finally:
        attachment.close()


def test_model_process_cannot_fabricate_native_hook_attachment(native):
    root, _ = native
    with pytest.raises(ValueError, match='native_host_required'):
        hooks.hook(dict(session_id=str(uuid.uuid4()), cwd=str(root), hook_event_name='SessionStart'), 'claude', str(root/'bus.db'))


def test_claude_channel_wire_receipt_requires_the_model_tool(native, monkeypatch):
    from claude_channel import serve
    root, owner = native
    monkeypatch.setattr(hooks, 'claude_channel_enabled', lambda *_: True)
    hooks.hook(dict(session_id=str(uuid.uuid4()), cwd=str(root), hook_event_name='SessionStart'),
               'claude', str(root/'bus.db'), owner_probe=owner)
    attached = Attachment(os.getpid())
    attached.sync()
    mid = agent_msg.send(attached.peer.db, 'sender', attached.peer.recipient, 'CHANNEL_WIRE')
    responses = queue.Queue()
    class Output:
        def write(self, value):
            responses.put(json.loads(value))
        def flush(self):
            pass
    incoming, writer = os.pipe()
    failures = []
    def run():
        try:
            serve(attached, incoming, Output())
        except BaseException as error:
            failures.append(error)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    def send(value):
        os.write(writer, (json.dumps({'jsonrpc': '2.0', **value})+'\n').encode())
    try:
        send({'id': 1, 'method': 'initialize'})
        assert responses.get(timeout=5)['result']['capabilities']['experimental'] == {'claude/channel': {}}
        send({'method': 'notifications/initialized'})
        notice = responses.get(timeout=5)
        assert notice['method'] == 'notifications/claude/channel'
        assert notice['params']['meta']['message_id'] == str(mid)
        send({'id': 2, 'method': 'tools/call', 'params': {'name': 'resonance',
             'arguments': {'operation': 'acknowledge', 'arguments': json.dumps({'message_id': mid})}}})
        reply = responses.get(timeout=5)
        assert json.loads(reply['result']['content'][0]['text'])['state'] == 'accepted'
        assert attached.peer.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0] == 1
    finally:
        os.close(writer)
        thread.join(timeout=5)
        os.close(incoming)
        assert not thread.is_alive()
        attached.close()
    assert not failures


def test_hook_failure_reports_refusal_code_without_payload(native, monkeypatch, capsys):
    import io
    root, _ = native
    def refuse(_harness):
        raise ValueError('native_host_required')
    monkeypatch.setattr(hooks, 'native_owner', refuse)
    event = dict(session_id=str(uuid.uuid4()), cwd=str(root), hook_event_name='SessionStart', prompt='SECRET_PROMPT')
    monkeypatch.setattr('sys.stdin', io.TextIOWrapper(io.BytesIO(json.dumps(event).encode())))
    assert hooks.main(['--harness', 'claude', '--bus', str(root/'bus.db'), 'hook']) == 1
    err = capsys.readouterr().err
    assert '(native_host_required)' in err and 'SECRET_PROMPT' not in err


def test_exec_resolves_identity_from_the_running_service(native, capsys):
    root, owner = native
    thread = str(uuid.uuid4())
    path = str(root / 'bus.db')
    with running_service(path):
        hooks.hook(dict(session_id=thread, cwd=str(root), hook_event_name='SessionStart'), 'claude', path,
                   owner_probe=owner)
        capsys.readouterr()
        assert hooks.main(['--harness', 'claude', '--bus', path, '--thread', thread, 'exec', '--', 'whoami']) == 0
        assert 'claude-terminal-' + thread in capsys.readouterr().out
    assert hooks.main(['--harness', 'claude', '--bus', path, '--thread', thread, 'exec', '--', 'whoami']) == 1
    assert '(service_unavailable)' in capsys.readouterr().err
