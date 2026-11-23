"""T3 Code hosts: thread-scoped liveness and a fixed-text doorbell turn."""
import json
import os
import uuid

import pytest

import agent_directory
import agent_msg
import harness_hooks as hooks
import native_delivery
import t3_delivery as t3
import terminal_session as terminal
from .service_helpers import running_service


class FakeT3:
    def __init__(self):
        self.threads, self.sent, self.fail_send = {}, [], False

    def read(self, thread):
        info = self.threads.get(thread)
        if info is None:
            raise ValueError('t3_thread_unavailable')
        return info

    def send(self, thread, text):
        if self.fail_send:
            raise ValueError('t3_send_failed')
        self.sent.append((thread, text))


@pytest.fixture
def host(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    for key in ('RESONANCE_PEER_SOCKET', 'RESONANCE_CREDENTIAL_FILE'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(hooks, 'claude_channel_enabled', lambda *_: False)
    fake = FakeT3()
    thread = str(uuid.uuid4())
    fake.threads[thread] = {'threadId': thread, 'archived': False}
    monkeypatch.setenv('T3CODE_THREAD_ID', thread)
    owner = lambda: (os.getpid(), list(terminal.process_identity(os.getpid())))
    return tmp_path, fake, thread, owner


def run_hook(root, owner, session_id, kind):
    event = dict(session_id=session_id, cwd=str(root), hook_event_name=kind)
    return hooks.hook(event, 'claude', str(root / 'bus.db'), owner_probe=owner)


def options(fake, **extra):
    return dict(t3_read=fake.read, t3_send=fake.send, **extra)


def test_thread_identity_comes_only_from_a_valid_t3_environment():
    thread = str(uuid.uuid4())
    assert t3.thread_from_environment({'T3CODE_THREAD_ID': thread}) == thread
    assert t3.thread_from_environment({'T3CODE_THREAD_ID': 'mcp:' + thread}) == 'mcp:' + thread
    launched = 'mcp:launch:' + '0123456789abcdef' * 4  # threads started by T3's thread-launch tool
    assert t3.thread_from_environment({'T3CODE_THREAD_ID': launched}) == launched
    for value in (None, '', 'mcp:launch:abc', 'mcp:launch:' + thread, 'mcp:mcp:' + thread, launched.upper(),
                  launched + '0', 'mcp:launch:' + 'g' * 64,
                  thread.upper(), '../' + thread, 'x:' + thread):
        env = {} if value is None else {'T3CODE_THREAD_ID': value}
        assert t3.thread_from_environment(env) is None


def test_t3_session_survives_turn_end_and_rings_without_peer_text(host, capsys):
    root, fake, thread, owner = host
    session_id = str(uuid.uuid4())
    path = str(root / 'bus.db')
    with running_service(path, **options(fake)) as svc, agent_msg.open_db(path) as db:
        assert run_hook(root, owner, session_id, 'UserPromptSubmit') == 0
        assert 'T3 thread wake-up' in capsys.readouterr().out
        recipient = 'claude-terminal-' + session_id
        # T3 ends the provider process after each turn; that must not retire the address.
        assert run_hook(root, owner, session_id, 'SessionEnd') == 0
        row = db.execute('SELECT * FROM participants WHERE recipient=?', (recipient,)).fetchone()
        assert row['closed'] == 0
        card = agent_directory.whoami(db, recipient, row['session'])
        assert card['delivery'] == 't3_thread' and card['wakeup'] is True

        agent_msg.send(db, 'sender', recipient, 'SECRET_PEER_BODY', expected_session=row['session'])
        svc.tick(force=True)
        assert len(fake.sent) == 1
        sent_thread, text = fake.sent[0]
        assert sent_thread == thread and 'SECRET_PEER_BODY' not in text
        assert 'not owner authorization' in text
        svc.tick(force=True)
        assert len(fake.sent) == 1  # the doorbell turn is still outstanding

        # The doorbell turn fires the ordinary prompt hook, which delivers the mail.
        assert run_hook(root, owner, session_id, 'UserPromptSubmit') == 0
        assert 'SECRET_PEER_BODY' in capsys.readouterr().out
        assert db.execute('SELECT session FROM participants WHERE recipient=?', (recipient,)).fetchone()[0] == row['session']
        svc.tick(force=True)
        assert len(fake.sent) == 1  # nothing new is unread


def test_doorbell_respects_budget_cooldown_and_failures(host):
    root, fake, thread, owner = host
    session_id = str(uuid.uuid4())
    path = str(root / 'bus.db')
    with running_service(path, **options(fake)) as svc, agent_msg.open_db(path) as db:
        run_hook(root, owner, session_id, 'UserPromptSubmit')
        recipient = 'claude-terminal-' + session_id
        session = db.execute('SELECT session FROM participants WHERE recipient=?', (recipient,)).fetchone()[0]
        with db:
            db.execute('UPDATE t3_receivers SET max_wakeups=2,cooldown=0')

        def ring():
            agent_msg.send(db, 'sender', recipient, 'm', expected_session=session)
            svc.tick(force=True)
            t3.prompt_seen(db, recipient, session)

        fake.fail_send = True
        ring()
        state = db.execute('SELECT * FROM t3_receivers').fetchone()
        assert fake.sent == [] and state['last_error'] == 't3_send_failed' and state['wakeups'] == 1
        svc.tick(force=True)
        assert db.execute('SELECT wakeups FROM t3_receivers').fetchone()[0] == 1  # never retried
        fake.fail_send = False
        ring()
        assert len(fake.sent) == 1
        ring()
        assert len(fake.sent) == 1  # budget exhausted
        assert agent_directory.whoami(db, recipient, session)['wakeup'] is False

        assert native_delivery.control_main(['--bus', path, '--recipient', recipient, '--session', session, 'resume']) == 0
        svc.tick(force=True)
        assert len(fake.sent) == 2


def test_gone_t3_thread_retires_after_repeated_failures(host):
    root, fake, thread, owner = host
    session_id = str(uuid.uuid4())
    path = str(root / 'bus.db')
    with running_service(path, **options(fake)) as svc, agent_msg.open_db(path) as db:
        run_hook(root, owner, session_id, 'SessionStart')
        recipient = 'claude-terminal-' + session_id
        del fake.threads[thread]
        for _ in range(svc.T3_FAILURES - 1):
            svc.tick(force=True)
            assert db.execute('SELECT closed FROM participants WHERE recipient=?', (recipient,)).fetchone()[0] == 0
        svc.tick(force=True)
        assert db.execute('SELECT closed FROM participants WHERE recipient=?', (recipient,)).fetchone()[0] == 1


def test_archived_t3_thread_is_refused(host):
    root, fake, thread, owner = host
    fake.threads[thread]['archived'] = True
    with running_service(str(root / 'bus.db'), **options(fake)):
        with pytest.raises(ValueError, match='owner_not_live'):
            run_hook(root, owner, str(uuid.uuid4()), 'SessionStart')


def test_new_provider_session_in_same_t3_thread_replaces_old_address(host):
    root, fake, thread, owner = host
    path = str(root / 'bus.db')
    first, second = str(uuid.uuid4()), str(uuid.uuid4())
    with running_service(path, **options(fake)), agent_msg.open_db(path) as db:
        run_hook(root, owner, first, 'SessionStart')
        run_hook(root, owner, second, 'SessionStart')  # e.g. /clear or a fresh-context send
        closed = dict(db.execute('SELECT recipient,closed FROM participants').fetchall())
        assert closed == {'claude-terminal-' + first: 1, 'claude-terminal-' + second: 0}
        assert [tuple(r) for r in db.execute('SELECT recipient FROM t3_receivers')] == [('claude-terminal-' + second,)]


def test_running_pid_session_upgrades_in_place_to_t3_owner(host, monkeypatch):
    root, fake, thread, owner = host
    path = str(root / 'bus.db')
    session_id = str(uuid.uuid4())
    with running_service(path, **options(fake)) as svc, agent_msg.open_db(path) as db:
        monkeypatch.delenv('T3CODE_THREAD_ID')
        run_hook(root, owner, session_id, 'SessionStart')  # attached before this release
        before = db.execute('SELECT session FROM participants').fetchone()[0]
        monkeypatch.setenv('T3CODE_THREAD_ID', thread)
        run_hook(root, owner, session_id, 'UserPromptSubmit')
        assert tuple(db.execute('SELECT session,closed FROM participants').fetchone()) == (before, 0)
        stored = json.loads(db.execute('SELECT owner FROM service_attachments').fetchone()[0])
        assert stored == {'kind': 't3', 'thread': thread}


def test_host_owner_evidence_requires_t3_environment_and_live_host():
    thread = str(uuid.uuid4())
    evidence = t3.owner_from_host({'T3CODE_THREAD_ID': thread}, os.getpid())
    assert evidence == {'kind': 't3', 'thread': thread, 'host_pid': os.getpid(),
                        'host_identity': list(terminal.process_identity(os.getpid()))}
    assert t3.owner_from_host({}, os.getpid()) is None
    assert t3.owner_from_host({'T3CODE_THREAD_ID': 'mcp:launch:x'}, os.getpid()) is None  # malformed


def test_codex_thread_hosted_by_t3_uses_thread_owner_and_skips_queue_bridge(host, monkeypatch):
    root, fake, thread, _ = host
    path = str(root / 'bus.db')
    codex_thread = str(uuid.uuid4())
    hosted = t3.owner_from_host({'T3CODE_THREAD_ID': thread}, os.getpid())
    monkeypatch.setattr(terminal, 'ensure_initial_wakeup',
                        lambda *a: (_ for _ in ()).throw(AssertionError('queue bridge must not bind T3 threads')))
    event = dict(session_id=codex_thread, cwd=str(root), hook_event_name='UserPromptSubmit')
    with running_service(path, **options(fake)), agent_msg.open_db(path) as db:
        assert terminal.hook(event, path, owner_probe=lambda: hosted, harness='codex') == 0
        event['hook_event_name'] = 'SessionEnd'
        terminal.hook(event, path, owner_probe=lambda: hosted, harness='codex')
        row = db.execute('SELECT * FROM participants').fetchone()
        assert row['recipient'] == 'codex-terminal-' + codex_thread and row['closed'] == 0
        assert agent_directory.whoami(db, row['recipient'], row['session'])['delivery'] == 't3_thread'


def t3_state(path, rows):
    import sqlite3
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE orchestration_v2_projection_provider_threads (provider_thread_id TEXT PRIMARY KEY,'
               ' thread_id TEXT, provider TEXT NOT NULL, status TEXT NOT NULL, payload_json TEXT NOT NULL)')
    for index, (app, native, driver, strength) in enumerate(rows):
        payload = {'driver': driver, 'appThreadId': app,
                   'nativeThreadRef': {'driver': driver, 'nativeId': native, 'strength': strength}}
        db.execute('INSERT INTO orchestration_v2_projection_provider_threads VALUES (?,?,?,?,?)',
                   (f'p{index}', app, driver, 'idle', json.dumps(payload)))
    db.commit()
    db.close()


def test_codex_native_thread_resolves_to_exactly_one_t3_thread(tmp_path):
    state = tmp_path / 'statev2.sqlite'
    codex_thread, app = str(uuid.uuid4()), 'mcp:' + str(uuid.uuid4())
    other = str(uuid.uuid4())
    t3_state(state, [(app, codex_thread, 'codex', 'strong'), (str(uuid.uuid4()), other, 'claudeAgent', 'strong')])
    assert t3.t3_thread_for_codex(codex_thread, state) == app
    assert t3.t3_thread_for_codex(str(uuid.uuid4()), state) is None
    assert t3.t3_thread_for_codex(other, state) is None  # not a Codex thread
    weak = tmp_path / 'weak.sqlite'
    t3_state(weak, [(app, codex_thread, 'codex', 'weak')])
    assert t3.t3_thread_for_codex(codex_thread, weak) is None
    ambiguous = tmp_path / 'ambiguous.sqlite'
    t3_state(ambiguous, [(app, codex_thread, 'codex', 'strong'), (str(uuid.uuid4()), codex_thread, 'codex', 'strong')])
    assert t3.t3_thread_for_codex(codex_thread, ambiguous) is None
    assert t3.t3_thread_for_codex(codex_thread, tmp_path / 'missing.sqlite') is None
    (tmp_path / 'other.sqlite').write_bytes(b'')
    assert t3.t3_thread_for_codex(codex_thread, tmp_path / 'other.sqlite') is None  # unknown schema fails closed


def test_codex_hook_under_t3_app_server_resolves_owner_from_t3_state(host, monkeypatch):
    root, fake, thread, _ = host
    monkeypatch.delenv('T3CODE_THREAD_ID')  # Codex hooks under T3 get no thread variable
    path = str(root / 'bus.db')
    codex_thread = str(uuid.uuid4())
    state = root / 'statev2.sqlite'
    t3_state(state, [(thread, codex_thread, 'codex', 'strong')])
    monkeypatch.setattr(t3, 'codex_host_state', lambda pid: state if pid == os.getpid() else None)
    monkeypatch.setattr(terminal, 'ensure_initial_wakeup',
                        lambda *a: (_ for _ in ()).throw(AssertionError('queue bridge must not bind T3 threads')))
    marker = {'kind': 't3_codex_host', 'pid': os.getpid()}
    event = dict(session_id=codex_thread, cwd=str(root), hook_event_name='UserPromptSubmit')
    with running_service(path, **options(fake)), agent_msg.open_db(path) as db:
        assert terminal.hook(event, path, owner_probe=lambda: marker, harness='codex') == 0
        row = db.execute('SELECT * FROM participants').fetchone()
        assert row['recipient'] == 'codex-terminal-' + codex_thread and row['closed'] == 0
        stored = json.loads(db.execute('SELECT owner FROM service_attachments').fetchone()[0])
        assert stored == {'kind': 't3', 'thread': thread}
        # An app-server not started by T3 (e.g. the Codex desktop app) never attaches.
        monkeypatch.setattr(t3, 'codex_host_state', lambda pid: None)
        other = dict(event, session_id=str(uuid.uuid4()))
        with pytest.raises(ValueError, match='native_terminal_hook_required'):
            terminal.hook(other, path, owner_probe=lambda: marker, harness='codex')


def test_compatibility_reports_provider_schema(tmp_path, monkeypatch):
    import sqlite3
    import t3_delivery

    state = tmp_path / 'state?#.sqlite'
    monkeypatch.setattr(t3_delivery, 'executable', lambda: '/missing/t3')
    assert t3_delivery.compatibility(state)['provider_thread_schema'] is False
    with sqlite3.connect(state) as db:
        db.execute('CREATE TABLE orchestration_v2_projection_provider_threads (payload_json TEXT)')
    assert t3_delivery.compatibility(state)['provider_thread_schema'] is True


def test_stale_pid_end_cannot_retire_t3_owner(host):
    root, fake, thread, owner = host
    session_id = str(uuid.uuid4())
    path = str(root / 'bus.db')
    with running_service(path, **options(fake)) as svc, agent_msg.open_db(path) as db:
        assert run_hook(root, owner, session_id, 'SessionStart') == 0
        pid, identity = owner()
        ended = svc.attach(dict(harness='claude', thread=session_id, event='SessionEnd',
                                project=str(root), owner=dict(kind='pid', pid=pid, identity=identity)))
        assert ended['closed'] is False
        assert db.execute('SELECT closed FROM participants').fetchone()[0] == 0


def test_fresh_attach_wins_over_inflight_t3_poll(host):
    root, fake, thread, owner = host
    session_id = str(uuid.uuid4())
    path = str(root / 'bus.db')
    with running_service(path, **options(fake)) as svc, agent_msg.open_db(path) as db:
        assert run_hook(root, owner, session_id, 'SessionStart') == 0

        def stale_read(thread_id):
            svc.t3_read = fake.read
            assert run_hook(root, owner, session_id, 'UserPromptSubmit') == 0
            return {'threadId': thread_id, 'archived': True}

        svc.t3_read = stale_read
        svc.tick(force=True)
        assert db.execute('SELECT closed FROM participants').fetchone()[0] == 0
