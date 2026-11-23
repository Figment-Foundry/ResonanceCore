"""Behavioral contracts for fan-out, presence and session isolation."""
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import time
import uuid

import pytest

import agent_msg as bus
import agent_broadcast as broadcast
import codex_bridge as bridge


@pytest.fixture
def db(tmp_path):
    conn = bus.open_db(str(tmp_path / 'bus.db'))
    yield conn
    conn.close()


def join(db, tmp_path, recipient, harness='claude', route='hook', now=None):
    token = broadcast.register(db, recipient, str(tmp_path), harness, route, now=now, evidence='thread_attachment' if route == 'codex' else None)
    broadcast.renew(db, recipient, token, ready=True, now=now)
    return token


def test_snapshot_reaches_all_harnesses_including_worker(db, tmp_path):
    tokens = {h: join(db, tmp_path, h, h) for h in ('claude', 'codex', 'kimi', 'opencode', 'pi', 'worker')}
    join(db, tmp_path, 'sender')
    join(db, tmp_path, 'stale', now=1)
    closed = join(db, tmp_path, 'closed')
    broadcast.close(db, 'closed', closed)
    broadcast.register(db, 'not-working', str(tmp_path), 'claude')
    preview = broadcast.broadcast(db, 'sender', 'announcement', dry_run=True)
    assert preview['recipient_count'] == 6
    assert preview['excluded_counts'] == {'closed': 1, 'integration_not_ready': 1, 'sender': 1, 'expired': 1}
    result = broadcast.broadcast(db, 'sender', 'announcement', 'once')
    assert result['recipient_count'] == 6
    join(db, tmp_path, 'later')
    for recipient, token in tokens.items():
        rows = bus.fetch_unread(db, recipient, token)
        assert len(rows) == 1 and rows[0]['broadcast_id'] == result['broadcast_id']
        assert 'not owner authorization' in bus.format_inbox(rows)
    assert bus.fetch_unread(db, 'later') == []
    assert broadcast.broadcast(db, 'sender', 'announcement', 'once')['recipient_count'] == 6


def test_project_filter_and_empty_snapshot_stay_immutable(db, tmp_path):
    other = tmp_path / 'other'
    other.mkdir()
    join(db, tmp_path, 'a')
    empty = broadcast.broadcast(db, 'sender', 'x', 'empty', str(other))
    assert empty['state'] == 'empty'
    join(db, other, 'b')
    assert broadcast.broadcast(db, 'sender', 'x', 'empty', str(other))['recipient_count'] == 0
    assert broadcast.broadcast(db, 'sender', 'x', 'new', str(other))['recipient_count'] == 1
    with pytest.raises(ValueError, match='key_conflict'):
        broadcast.broadcast(db, 'sender', 'changed', 'empty', str(other))
    with pytest.raises(ValueError, match='key_conflict'):
        broadcast.broadcast(db, 'sender', 'x', 'empty', str(tmp_path))


def test_failure_rolls_back_every_child_and_broadcast(db, tmp_path):
    join(db, tmp_path, 'a')
    join(db, tmp_path, 'b')
    db.executescript("""CREATE TRIGGER fail_second BEFORE INSERT ON messages
        WHEN NEW.recipient='b' BEGIN SELECT RAISE(ABORT,'simulated failure'); END;""")
    with pytest.raises(sqlite3.IntegrityError):
        broadcast.broadcast(db, 'sender', 'x', 'once')
    for table in ('broadcasts', 'broadcast_recipients', 'messages'):
        assert db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0] == 0


def test_concurrent_same_key_enqueues_once(db, tmp_path):
    join(db, tmp_path, 'peer')
    path = str(tmp_path / 'bus.db')
    def publish(_):
        conn = bus.open_db(path)
        try:
            return broadcast.broadcast(conn, 'sender', 'x', 'once')['broadcast_id']
        finally:
            conn.close()
    with ThreadPoolExecutor(max_workers=4) as workers:
        ids = list(workers.map(publish, range(8)))
    assert len(set(ids)) == 1
    assert db.execute('SELECT COUNT(*) FROM messages').fetchone()[0] == 1


def test_session_expiry_replacement_and_hook_consumption(db, tmp_path, capsys, monkeypatch):
    monkeypatch.delenv('AGENT_MSG_SESSION', raising=False)
    token = join(db, tmp_path, 'peer', now=100)
    with pytest.raises(ValueError, match='already_active'):
        broadcast.register(db, 'peer', str(tmp_path), 'pi', now=101)
    with pytest.raises(ValueError, match='expired'):
        broadcast.renew(db, 'peer', token, now=200)
    replacement = join(db, tmp_path, 'peer')
    with pytest.raises(ValueError, match='replaced'):
        broadcast.close(db, 'peer', token)
    bid = broadcast.broadcast(db, 'sender', 'x', 'one')['broadcast_id']
    assert bus.fetch_unread(db, 'peer') == []
    assert bus.cmd_drain(db, Namespace(as_='peer', keep_unread=False, session=replacement)) == 0
    assert bid in capsys.readouterr().out
    assert broadcast.status(db, bid)['counts'] == {'consumed': 1}
    second = broadcast.broadcast(db, 'sender', 'y', 'two')['broadcast_id']
    broadcast.close(db, 'peer', replacement)
    newest = join(db, tmp_path, 'peer')
    assert bus.fetch_unread(db, 'peer', newest) == []
    assert broadcast.status(db, second)['recipients'][0]['reason'] == 'session_replaced'
    assert len(bus.fetch_unread(db, 'peer', inspect=True)) == 1


def test_expired_session_cannot_send_or_consume_direct_mail(db, tmp_path, capsys):
    token = join(db, tmp_path, 'peer', now=1)
    broadcast.broadcast(db, 'sender', 'OLD_BROADCAST', 'once', now=2)
    with pytest.raises(ValueError, match='recipient_unavailable'):
        bus.send(db, 'sender', 'peer', 'DIRECT_MAIL')
    assert bus.cmd_drain(db, Namespace(as_='peer', keep_unread=False, session=token)) == 2
    assert not capsys.readouterr().out
    assert len(bus.fetch_unread(db, 'peer', inspect=True)) == 1


def test_status_requires_correct_reply_direction_and_has_pagination(db, tmp_path):
    join(db, tmp_path, 'sender')
    join(db, tmp_path, 'a')
    join(db, tmp_path, 'b')
    result = broadcast.broadcast(db, 'sender', 'x', 'once')
    mid = result['recipients'][0]['message_id']
    with pytest.raises(ValueError):
        bus.send(db, 'a', 'wrong', 'fake', 'reply', mid)
    with pytest.raises(ValueError):
        bus.send(db, 'wrong', 'sender', 'fake', 'reply', mid)
    assert broadcast.status(db, result['broadcast_id'])['acknowledged_count'] == 0
    reply = bus.send(db, 'a', 'sender', 'received', 'reply', mid)
    page = broadcast.status(db, result['broadcast_id'], limit=1)
    assert page['next_offset'] == 1 and page['acknowledged_count'] == 1
    assert page['recipients'][0]['reply_message_id'] == reply
    assert broadcast.status(db, result['broadcast_id'], limit=1, offset=1)['next_offset'] is None


def test_broadcast_reply_hint_quotes_sender_as_one_shell_argument(db, tmp_path):
    token = join(db, tmp_path, 'peer')
    sender = 'owner; echo unexpected'
    broadcast.broadcast(db, sender, 'x', 'once')
    block = bus.format_inbox(bus.fetch_unread(db, 'peer', token))
    hint = next(line for line in block.splitlines() if '(reply:' in line)
    command = hint.strip()[len('(reply: '):-1]
    assert shlex.split(command)[3] == sender


def test_limits_reject_without_silent_truncation(db, tmp_path):
    for i in range(101):
        join(db, tmp_path, str(i))
    with pytest.raises(ValueError, match='audience_limit'):
        broadcast.broadcast(db, 'sender', 'x', 'once')
    assert db.execute('SELECT COUNT(*) FROM messages').fetchone()[0] == 0
    with pytest.raises(ValueError, match='invalid_key'):
        broadcast.broadcast(db, 'sender', 'x')


def test_preview_does_not_create_or_migrate_bus(tmp_path):
    path = tmp_path / 'absent' / 'bus.db'
    command = [sys.executable, bus.__file__, 'broadcast', '--from', 'sender', '--dry-run', 'x']
    env = {**os.environ, 'AGENT_MSG_DB': str(path)}
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=5)
    assert result.returncode == 0 and json.loads(result.stdout)['recipient_count'] == 0
    assert not path.parent.exists()
    path.parent.mkdir()
    with sqlite3.connect(path) as old:
        old.executescript(bus.SCHEMA)
    before = path.read_bytes()
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=5)
    assert result.returncode == 0 and path.read_bytes() == before


def test_cli_preview_of_live_sessions_is_readonly(db, tmp_path):
    join(db, tmp_path, 'worker', 'worker')
    before = tuple(db.execute('SELECT * FROM participants').fetchone())
    result = subprocess.run([sys.executable, bus.__file__, 'broadcast', '--from', 'sender',
                             '--dry-run', 'x'], capture_output=True, text=True,
                            env={**os.environ, 'AGENT_MSG_DB': str(tmp_path / 'bus.db')}, timeout=5)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['recipient_count'] == 1
    assert tuple(db.execute('SELECT * FROM participants').fetchone()) == before
    assert db.execute('SELECT COUNT(*) FROM broadcasts').fetchone()[0] == 0


def test_additive_schema_preserves_existing_mail(tmp_path):
    path = str(tmp_path / 'old.db')
    with sqlite3.connect(path) as old:
        old.executescript(bus.SCHEMA)
        old.execute("INSERT INTO messages VALUES (1,1,'a','b','notify',NULL,'old',NULL)")
    migrated = bus.open_db(path)
    try:
        assert bus.fetch_unread(migrated, 'b') == []
        assert bus.fetch_unread(migrated, 'b', inspect=True)[0]['body'] == 'old'
        assert broadcast.has_table(migrated, 'participants')
    finally:
        migrated.close()


class Client:
    def __init__(self, target, lost=False):
        self.target, self.lost, self.calls = target, lost, []
    def call(self, method, params):
        from codex_rpc import TransportError
        self.calls.append((method, params))
        if method == 'thread/read':
            return {'thread': {'id': self.target['thread_id'], 'cwd': self.target['project']}}
        if method == 'thread/loaded/list':
            return {'data': [self.target['thread_id']]}
        if method == 'turn/start':
            if self.lost:
                raise TransportError('lost_ack')
            return {'turn': {'id': 'turn-test'}}
    def close(self):
        pass


def codex_join(db, tmp_path, name, thread=None):
    db.executescript(bridge.SCHEMA)
    # Use the normal additive bridge migration before registering.
    path = str(tmp_path / 'bus.db')
    migrated = bridge.database(path)
    migrated.close()
    bridge.register(db, name, thread or str(uuid.uuid4()), str(tmp_path), str(tmp_path / (name + '.sock')), cooldown=0)
    with db:
        db.execute('UPDATE codex_bindings SET enabled=1 WHERE recipient=?', (name,))
    return join(db, tmp_path, name, 'codex', 'codex')


def test_codex_partial_failure_and_sender_alias_exclusion(db, tmp_path):
    codex_join(db, tmp_path, 'a')
    codex_join(db, tmp_path, 'b')
    result = broadcast.broadcast(db, 'sender', 'x', 'once')
    connector = lambda target: Client(target, lost=target['recipient'] == 'a')
    assert bridge.tick(db, str(tmp_path / 'bus.db'), connector)['state'] == 'unknown'
    assert bridge.tick(db, str(tmp_path / 'bus.db'), connector)['state'] == 'accepted'
    assert bridge.tick(db, str(tmp_path / 'bus.db'), connector)['state'] == 'idle'
    assert broadcast.status(db, result['broadcast_id'])['counts'] == {'unknown': 1, 'accepted': 1}
    bound = broadcast.binding(db, 'b')
    bridge.register(db, 'sender', bound['thread_id'], str(tmp_path), str(tmp_path / 'alias.sock'))
    assert broadcast.broadcast(db, 'sender', 'x', dry_run=True)['recipient_count'] == 1
    with pytest.raises(ValueError, match='thread_already_active'):
        join(db, tmp_path, 'sender', 'codex', 'codex')


def test_codex_rebind_and_session_replacement_refuse_delivery(db, tmp_path):
    token = codex_join(db, tmp_path, 'peer')
    result = broadcast.broadcast(db, 'sender', 'x', 'once')
    with db:
        db.execute('UPDATE codex_bindings SET thread_id=?', (str(uuid.uuid4()),))
    assert bridge.tick(db, str(tmp_path / 'bus.db'), lambda _: pytest.fail('wrong target'))['state'] == 'skipped'
    assert broadcast.status(db, result['broadcast_id'])['counts'] == {'skipped': 1}
    broadcast.close(db, 'peer', token)
    join(db, tmp_path, 'peer', 'codex', 'codex')
    fresh = broadcast.broadcast(db, 'sender', 'new session', 'new')
    accepted = bridge.tick(db, str(tmp_path / 'bus.db'), Client)
    assert accepted['state'] == 'accepted' and accepted['message_id'] == fresh['recipients'][0]['message_id']


def test_codex_paused_budget_and_oversized_envelope(db, tmp_path):
    codex_join(db, tmp_path, 'peer')
    with db:
        db.execute('UPDATE codex_bindings SET enabled=0')
    assert broadcast.broadcast(db, 'sender', 'x', dry_run=True)['excluded_counts'] == {'paused': 1}
    with db:
        db.execute('UPDATE codex_bindings SET enabled=1,wakeups=max_wakeups')
    result = broadcast.broadcast(db, 'sender', 'x', 'once')
    assert broadcast.status(db, result['broadcast_id'])['recipients'][0]['reason'] == 'budget_exhausted'
    assert bridge.tick(db, str(tmp_path / 'bus.db'), lambda _: pytest.fail('budget bypass'))['state'] == 'idle'
    with pytest.raises(ValueError, match='message_limit'):
        broadcast.broadcast(db, 'sender', 'x' * 16300, 'oversized')
    assert db.execute('SELECT COUNT(*) FROM broadcasts').fetchone()[0] == 1
