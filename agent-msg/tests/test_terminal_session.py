"""Controlled lifecycle fixtures; these do not enroll a real Codex terminal."""
import json
import os
from pathlib import Path
import sqlite3
import sys
import uuid

import pytest

import agent_msg
import agent_broadcast as membership
import resonance_service as service
import terminal_session as terminal
import codex_bridge as bridge
from .service_helpers import running_service


@pytest.fixture
def session(tmp_path, monkeypatch):
    for key in ('RESONANCE_PEER_SOCKET', 'RESONANCE_CREDENTIAL_FILE'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('HOME', str(tmp_path))
    bus = tmp_path / 'bus.db'
    event = dict(session_id=str(uuid.uuid4()), cwd=str(tmp_path), hook_event_name='SessionStart')
    owner = lambda: (os.getpid(), list(terminal.process_identity(os.getpid())))
    with running_service(bus) as svc:
        yield bus, event, owner, svc


def test_plain_session_lifecycle_drains_once_through_the_service(session, capsys):
    bus, event, owner, svc = session
    terminal.hook(event, bus, owner_probe=owner)
    db = agent_msg.open_db(str(bus))
    try:
        assert db.execute('SELECT ready FROM participants').fetchone()[0] == 0
        event['hook_event_name'] = 'UserPromptSubmit'
        terminal.hook(event, bus, owner_probe=owner)
        assert db.execute('SELECT ready FROM participants').fetchone()[0] == 1
        result = membership.broadcast(db, 'fixture-sender', 'FIXTURE_MESSAGE', 'once')
        terminal.hook(event, bus, owner_probe=owner)
        assert capsys.readouterr().out.count('FIXTURE_MESSAGE') == 1
        terminal.hook(event, bus, owner_probe=owner)
        assert 'FIXTURE_MESSAGE' not in capsys.readouterr().out
        assert len(svc.status()['attachments']) == 1
        assert membership.status(db, result['broadcast_id'])['counts'] == {'consumed': 1}
        event['hook_event_name'] = 'SessionEnd'
        terminal.hook(event, bus, owner_probe=owner)
        assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1
        assert svc.status()['attachments'] == []
    finally:
        db.close()


def test_malformed_hook_input_is_a_controlled_refusal(session):
    bus, *_ = session
    with pytest.raises(ValueError, match='invalid_hook_payload'):
        terminal.hook([], bus)


def test_protected_and_non_native_contexts_cannot_self_attach(session, monkeypatch):
    bus, event, owner, svc = session
    monkeypatch.setenv('RESONANCE_PEER_SOCKET', '/fixture/protected.sock')
    with pytest.raises(ValueError, match='protected_host_adapter_required'):
        terminal.hook(event, bus, owner_probe=owner)
    monkeypatch.delenv('RESONANCE_PEER_SOCKET')
    # pytest is not a terminal Codex ancestor, even with a plausible thread ID.
    with pytest.raises(ValueError, match='native_terminal_hook_required'):
        terminal.hook(event, bus)
    assert svc.status()['attachments'] == []


def test_thread_scope_is_enforced(session, tmp_path):
    bus, event, owner, _ = session
    terminal.hook(event, bus, owner_probe=owner)
    event['cwd'] = str(tmp_path.parent)
    with pytest.raises(ValueError, match='session_scope_changed'):
        terminal.hook(event, bus, owner_probe=owner)


def test_session_end_without_a_running_service_is_quiet(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    event = dict(session_id=str(uuid.uuid4()), cwd=str(tmp_path), hook_event_name='SessionEnd')
    owner = lambda: (os.getpid(), list(terminal.process_identity(os.getpid())))
    assert terminal.hook(event, tmp_path / 'bus.db', owner_probe=owner) == 0
    assert not service.socket_path(str(tmp_path / 'bus.db')).exists()


def test_model_command_uses_service_identity_without_inherited_launcher_environment(session, monkeypatch, capsys):
    bus, event, owner, _ = session
    event['hook_event_name'] = 'UserPromptSubmit'
    terminal.hook(event, bus, owner_probe=owner)
    monkeypatch.setenv('CODEX_THREAD_ID', event['session_id'])
    for key in ('AGENT_MSG_ID', 'AGENT_MSG_SESSION', 'AGENT_MSG_DB'):
        monkeypatch.delenv(key, raising=False)
    assert terminal.main(['--bus', str(bus), 'exec', '--', 'whoami']) == 0
    assert 'codex-terminal-' + event['session_id'] in capsys.readouterr().out
    assert os.environ.get('AGENT_MSG_SESSION') is None


@pytest.fixture
def wake_session(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    for key in ('RESONANCE_PEER_SOCKET', 'RESONANCE_CREDENTIAL_FILE'):
        monkeypatch.delenv(key, raising=False)
    bus = tmp_path / 'bus.db'
    event = dict(session_id=str(uuid.uuid4()), cwd=str(tmp_path), hook_event_name='UserPromptSubmit')
    owner = lambda: (os.getpid(), list(terminal.process_identity(os.getpid())))
    with running_service(bus) as svc:
        terminal.hook(event, bus, owner_probe=owner)
        record = service.request(str(bus), {'op': 'lookup', 'harness': 'codex', 'thread': event['session_id']},
                                 start=False)
        home = tmp_path / 'codex'
        home.mkdir()
        with sqlite3.connect(home / 'state_5.sqlite') as state:
            state.execute('CREATE TABLE threads(id,cwd,archived)')
            state.execute('INSERT INTO threads VALUES (?,?,0)', (event['session_id'], str(tmp_path)))
        db = bridge.database(str(bus))
        bridge.register(db, record['recipient'], event['session_id'], str(tmp_path), transport='queue',
                        executable=sys.executable, codex_home=str(home), cooldown=0)
        yield svc, bus, event, owner, record, db
        db.close()


def test_resume_native_terminal_delivers_without_another_prompt(wake_session, monkeypatch, capsys):
    svc, bus, event, owner, record, db = wake_session
    submissions = []
    qid = str(uuid.uuid4())
    def queue(argv, env):
        submissions.append(argv)
        return f'Queued message {qid} for thread {event["session_id"]}.'.encode()
    monkeypatch.setattr('codex_queue.bounded_command', queue)
    assert bridge.main(['--db', str(bus), 'resume', '--recipient', record['recipient']]) == 0
    mid = agent_msg.send(db, 'fixture-peer', record['recipient'], 'IDLE_WAKE_REGRESSION')
    assert bridge.tick(db, str(bus)) == {'state': 'accepted', 'message_id': mid, 'queue_id': qid}
    assert len(submissions) == 1
    assert 'IDLE_WAKE_REGRESSION' in submissions[0][-1]
    assert bridge.tick(db, str(bus))['state'] == 'idle'
    # A later native prompt must not compete with the bridge or report a drain error.
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    assert 'IDLE_WAKE_REGRESSION' not in capsys.readouterr().out


@pytest.mark.parametrize('invalid', ['closed', 'owner_dead', 'service_down', 'replaced', 'wrong_thread'])
def test_resume_requires_exact_live_native_attachment(wake_session, invalid):
    svc, bus, event, owner, record, db = wake_session
    if invalid == 'closed':
        terminal.hook(dict(event, hook_event_name='SessionEnd'), bus, owner_probe=owner)
    elif invalid == 'owner_dead':
        with svc.lock, svc.db:
            svc.db.execute("UPDATE service_attachments SET owner=?",
                           (json.dumps({'kind': 'pid', 'identity': ['wrong', 'identity'], 'pid': os.getpid()}),))
    elif invalid == 'service_down':
        svc.server.shutdown()
        svc.server.server_close()
        service.socket_path(str(bus)).unlink()
    elif invalid == 'replaced':
        with db:
            db.execute('UPDATE participants SET session=?', (str(uuid.uuid4()),))
    else:
        with db:
            db.execute('UPDATE codex_bindings SET thread_id=?', (str(uuid.uuid4()),))
    assert bridge.main(['--db', str(bus), 'resume', '--recipient', record['recipient']]) == 1
    assert db.execute('SELECT enabled FROM codex_bindings').fetchone()[0] == 0


def test_native_resume_does_not_retarget_existing_hook_mail(wake_session, monkeypatch):
    svc, bus, event, owner, record, db = wake_session
    # This was queued before the explicit transport change.
    with db:
        mid = db.execute("INSERT INTO messages(ts,sender,recipient,type,body) VALUES (0,'peer',?,'notify','OLD')",
                         (record['recipient'],)).lastrowid
        db.execute('INSERT INTO direct_targets VALUES (?,?,?,?,NULL,?,NULL)',
                   (mid, record['session'], 'hook', '{}', 9999999999))
    assert bridge.main(['--db', str(bus), 'resume', '--recipient', record['recipient']]) == 0
    monkeypatch.setattr('codex_queue.bounded_command', lambda *_: pytest.fail('old route was replayed'))
    assert bridge.tick(db, str(bus))['reason'] == 'binding_changed'
    assert db.execute('SELECT read_at FROM messages WHERE id=?', (mid,)).fetchone()[0] is None


def test_native_bridge_pause_budget_and_session_end_remain_effective(wake_session, monkeypatch):
    svc, bus, event, owner, record, db = wake_session
    assert bridge.main(['--db', str(bus), 'resume', '--recipient', record['recipient']]) == 0
    mid = agent_msg.send(db, 'fixture-peer', record['recipient'], 'LIMITED')
    monkeypatch.setattr('codex_queue.bounded_command', lambda *_: pytest.fail('held message submitted'))
    assert bridge.main(['--db', str(bus), 'pause', '--recipient', record['recipient']]) == 0
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    assert bridge.tick(db, str(bus))['state'] == 'idle'
    with db:
        db.execute('UPDATE codex_bindings SET enabled=1,wakeups=max_wakeups')
    assert bridge.tick(db, str(bus))['state'] == 'idle'
    event['hook_event_name'] = 'SessionEnd'
    terminal.hook(event, bus, owner_probe=owner)
    with db:
        db.execute('UPDATE codex_bindings SET wakeups=0')
    assert bridge.tick(db, str(bus))['reason'] == 'session_inactive'
    assert db.execute('SELECT read_at FROM messages WHERE id=?', (mid,)).fetchone()[0] is None


def test_reopened_bound_thread_has_new_session_and_keeps_pause(wake_session):
    svc, bus, event, owner, record, db = wake_session
    assert bridge.main(['--db', str(bus), 'resume', '--recipient', record['recipient']]) == 0
    assert bridge.main(['--db', str(bus), 'pause', '--recipient', record['recipient']]) == 0
    event['hook_event_name'] = 'SessionEnd'
    terminal.hook(event, bus, owner_probe=owner)
    event['hook_event_name'] = 'UserPromptSubmit'
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    row = db.execute('SELECT * FROM participants').fetchone()
    assert row['session'] != record['session'] and row['route'] == 'codex' and row['ready'] == 1
    assert db.execute('SELECT enabled FROM codex_bindings').fetchone()[0] == 0


def owner_wakeup_policy(tmp_path, event):
    home = tmp_path / 'codex-policy-home'
    home.mkdir(exist_ok=True)
    with sqlite3.connect(home / 'state_5.sqlite') as db:
        db.execute('CREATE TABLE IF NOT EXISTS threads(id,cwd,archived)')
        db.execute('INSERT INTO threads VALUES (?,?,0)', (event['session_id'], str(tmp_path)))
    folder = tmp_path / '.config/resonancecore/terminal'
    folder.mkdir(parents=True, mode=0o700, exist_ok=True)
    policy = dict(version=1, enabled=True, executable=sys.executable,
                  codex_home=str(home), max_wakeups=20, cooldown=0)
    path = folder / 'wakeup.json'
    path.write_text(json.dumps(policy))
    path.chmod(0o600)
    return path, policy


def test_owner_policy_fresh_prompt_is_idle_deliverable(session, tmp_path, monkeypatch):
    bus, event, owner, _ = session
    owner_wakeup_policy(tmp_path, event)
    event['hook_event_name'] = 'UserPromptSubmit'
    submissions = []
    qid = str(uuid.uuid4())
    def queue(argv, env):
        submissions.append(argv)
        return f'Queued message {qid} for thread {event["session_id"]}.'.encode()
    monkeypatch.setattr('codex_queue.bounded_command', queue)
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    db = bridge.database(str(bus))
    try:
        recipient = 'codex-terminal-' + event['session_id']
        row = db.execute('SELECT * FROM participants WHERE recipient=?', (recipient,)).fetchone()
        binding = membership.binding(db, recipient)
        assert binding is not None, 'owner-authorized fresh terminal remained prompt-only'
        assert binding['enabled'] == 1 and binding['max_wakeups'] == 20
        assert row['route'] == 'codex' and row['ready'] == 1
        mid = agent_msg.send(db, 'fixture-peer', recipient, 'AUTO_IDLE_WAKE_REGRESSION')
        assert bridge.tick(db, str(bus)) == dict(state='accepted', message_id=mid, queue_id=qid)
        assert len(submissions) == 1 and 'AUTO_IDLE_WAKE_REGRESSION' in submissions[0][-1]
        # No second prompt is needed for that delivery; later prompts preserve usage.
        assert terminal.hook(event, bus, owner_probe=owner) == 0
        assert membership.binding(db, recipient)['wakeups'] == 1
    finally:
        db.close()


def test_owner_policy_wrong_queue_identity_keeps_prompt_delivery(session, tmp_path, capsys):
    bus, event, owner, _ = session
    _, policy = owner_wakeup_policy(tmp_path, event)
    with sqlite3.connect(Path(policy['codex_home']) / 'state_5.sqlite') as metadata:
        metadata.execute('UPDATE threads SET cwd=?', (str(tmp_path.parent),))
    event['hook_event_name'] = 'UserPromptSubmit'
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    db = bridge.database(str(bus))
    try:
        recipient = 'codex-terminal-' + event['session_id']
        assert membership.binding(db, recipient) is None
        assert db.execute('SELECT route FROM participants WHERE recipient=?', (recipient,)).fetchone()[0] == 'hook'
        mid = agent_msg.send(db, 'fixture-peer', recipient, 'PROMPT_FALLBACK')
        assert terminal.hook(event, bus, owner_probe=owner) == 0
        assert 'PROMPT_FALLBACK' in capsys.readouterr().out
        assert db.execute('SELECT read_at FROM messages WHERE id=?', (mid,)).fetchone()[0] is not None
    finally:
        db.close()


@pytest.mark.parametrize('mode', ['paused', 'exhausted', 'unknown'])
def test_owner_policy_preserves_existing_delivery_controls(wake_session, tmp_path, monkeypatch, mode):
    _, bus, event, owner, record, db = wake_session
    owner_wakeup_policy(tmp_path, event)
    assert bridge.main(['--db', str(bus), 'resume', '--recipient', record['recipient']]) == 0
    if mode == 'paused':
        with db:
            db.execute('UPDATE codex_bindings SET enabled=0,wakeups=3')
    elif mode == 'exhausted':
        with db:
            db.execute('UPDATE codex_bindings SET wakeups=max_wakeups')
    else:
        from codex_rpc import TransportError
        def uncertain(*_):
            raise TransportError('queue_timeout')
        monkeypatch.setattr('codex_queue.bounded_command', uncertain)
        agent_msg.send(db, 'fixture-peer', record['recipient'], 'UNCERTAIN')
        assert bridge.tick(db, str(bus))['state'] == 'unknown'
    before = dict(membership.binding(db, record['recipient']))
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    assert dict(membership.binding(db, record['recipient'])) == before
    if mode == 'unknown':
        assert db.execute('SELECT state FROM codex_deliveries').fetchone()[0] == 'unknown'


@pytest.mark.parametrize('invalid', ['mode', 'symlink', 'shape', 'version', 'bool_budget', 'nan', 'relative', 'unknown_key'])
def test_invalid_owner_policy_reports_and_keeps_prompt_delivery(session, tmp_path, capsys, invalid):
    bus, event, owner, _ = session
    path, policy = owner_wakeup_policy(tmp_path, event)
    if invalid == 'mode':
        path.chmod(0o644)
    elif invalid == 'symlink':
        target = path.with_name('original.json')
        path.rename(target)
        path.symlink_to(target)
    else:
        if invalid == 'shape': policy = []
        elif invalid == 'version': policy['version'] = True
        elif invalid == 'bool_budget': policy['max_wakeups'] = True
        elif invalid == 'nan': policy['cooldown'] = float('nan')
        elif invalid == 'relative': policy['executable'] = 'relative-codex'
        elif invalid == 'unknown_key': policy['untrusted_extra'] = 'DO_NOT_ECHO'
        path.write_text(json.dumps(policy))
    event.update(hook_event_name='UserPromptSubmit', prompt='DO_NOT_ECHO')
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    output = capsys.readouterr()
    assert 'terminal_idle_wakeup_unavailable' in output.err
    assert 'DO_NOT_ECHO' not in output.err
    db = bridge.database(str(bus))
    try:
        assert membership.binding(db, 'codex-terminal-' + event['session_id']) is None
        assert db.execute('SELECT route,ready FROM participants').fetchone()[:] == ('hook', 1)
    finally:
        db.close()


def test_owner_policy_failed_native_transition_does_not_claim_inbox(session, tmp_path, monkeypatch):
    bus, event, owner, _ = session
    owner_wakeup_policy(tmp_path, event)
    request = service.request
    def changed(path, message, **kwargs):
        result = request(path, message, **kwargs)
        if message.get('op') == 'lookup':
            with sqlite3.connect(bus) as db:
                db.execute('UPDATE participants SET session=?', (str(uuid.uuid4()),))
        return result
    monkeypatch.setattr(service, 'request', changed)
    event['hook_event_name'] = 'UserPromptSubmit'
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    db = bridge.database(str(bus))
    try:
        assert membership.binding(db, 'codex-terminal-' + event['session_id']) is None
        assert db.execute('SELECT route FROM participants').fetchone()[0] == 'hook'
    finally:
        db.close()


def test_owner_wakeup_cli_persists_private_policy_and_disable_preserves_binding(session, tmp_path, capsys):
    bus, event, owner, _ = session
    _, policy = owner_wakeup_policy(tmp_path, event)
    assert terminal.main(['wakeup', '--enable', '--executable', policy['executable'],
                          '--codex-home', policy['codex_home'], '--cooldown', '0']) == 0
    path = tmp_path / '.config/resonancecore/terminal/wakeup.json'
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert json.loads(capsys.readouterr().out)['policy']['max_wakeups'] == 20
    event['hook_event_name'] = 'UserPromptSubmit'
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    db = bridge.database(str(bus))
    try:
        before = dict(membership.binding(db, 'codex-terminal-' + event['session_id']))
        assert terminal.main(['wakeup', '--disable']) == 0
        assert json.loads(capsys.readouterr().out)['policy']['enabled'] is False
        assert terminal.hook(event, bus, owner_probe=owner) == 0
        assert dict(membership.binding(db, before['recipient'])) == before
    finally:
        db.close()


def test_owner_wakeup_cli_cannot_configure_protected_context(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('RESONANCE_PEER_SOCKET', '/fixture/protected.sock')
    assert terminal.main(['wakeup', '--disable']) == 2
    assert not (tmp_path / '.config/resonancecore/terminal').exists()


def test_owner_policy_does_not_resume_a_binding_created_during_verification(session, tmp_path, monkeypatch):
    bus, event, owner, _ = session
    _, policy = owner_wakeup_policy(tmp_path, event)
    request = service.request
    replaced = []
    def concurrent_control(path, message, **kwargs):
        result = request(path, message, **kwargs)
        if message.get('op') == 'lookup' and not replaced:
            db = bridge.database(str(bus))
            try:
                recipient = 'codex-terminal-' + event['session_id']
                if membership.binding(db, recipient) is not None:
                    bridge.unregister(db, recipient)
                bridge.register(db, recipient, event['session_id'], str(tmp_path), transport='queue',
                                executable=policy['executable'], codex_home=policy['codex_home'],
                                max_wakeups=7, cooldown=0)
                with db:
                    db.execute('UPDATE codex_bindings SET wakeups=3 WHERE recipient=?', (recipient,))
                replaced.append(True)
            finally:
                db.close()
        return result
    monkeypatch.setattr(service, 'request', concurrent_control)
    event['hook_event_name'] = 'UserPromptSubmit'
    assert terminal.hook(event, bus, owner_probe=owner) == 0
    db = bridge.database(str(bus))
    try:
        binding = membership.binding(db, 'codex-terminal-' + event['session_id'])
        assert replaced and (binding['enabled'], binding['wakeups'], binding['max_wakeups']) == (0, 3, 7)
    finally:
        db.close()
