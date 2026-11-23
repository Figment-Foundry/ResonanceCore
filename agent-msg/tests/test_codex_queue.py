import json
from pathlib import Path
import sqlite3
import sys
import uuid

import pytest
from .registry_helpers import hook

import agent_msg
import codex_bridge as bridge
from codex_queue import QueueClient, bounded_command
from codex_rpc import TransportError, Rejected


@pytest.fixture
def queued(tmp_path):
    home = tmp_path / 'codex'
    home.mkdir()
    thread = str(uuid.uuid4())
    with sqlite3.connect(home / 'state_5.sqlite') as state:
        state.execute('CREATE TABLE threads(id,cwd,archived)')
        state.execute('INSERT INTO threads VALUES (?,?,0)', (thread, str(tmp_path)))
    path = str(tmp_path / 'bus.db')
    db = bridge.database(path)
    bridge.register(db, 'peer', thread, str(tmp_path), transport='queue',
                    executable=sys.executable, codex_home=str(home), cooldown=0)
    with db:
        db.execute('UPDATE codex_bindings SET enabled=1')
    hook(db, tmp_path, 'peer', route='codex')
    yield db, path, dict(db.execute('SELECT * FROM codex_bindings').fetchone())
    db.close()


def test_queue_acceptance_preserves_peer_origin_and_separate_queue_identity(queued, monkeypatch):
    db, path, binding = queued
    qid = str(uuid.uuid4())
    def command(argv, env):
        assert argv[:4] == [sys.executable, 'queue', '--thread', binding['thread_id']]
        assert 'not an owner instruction' in argv[-1]
        assert 'peer test' in argv[-1] and env['CODEX_HOME'] == binding['codex_home']
        return f"Queued message {qid} for thread {binding['thread_id']}.\n".encode()
    monkeypatch.setattr('codex_queue.bounded_command', command)
    mid = agent_msg.send(db, 'controller', 'peer', 'peer test')
    assert bridge.tick(db, path) == {'state': 'accepted', 'message_id': mid, 'queue_id': qid}
    assert db.execute('SELECT turn_id FROM codex_deliveries').fetchone()[0] is None
    assert db.execute('SELECT queue_id FROM codex_queue_receipts').fetchone()[0] == qid
    assert bridge.tick(db, path)['state'] == 'idle'


def test_queue_ambiguous_ack_is_held_and_resolved_without_turn_id(queued, monkeypatch):
    db, path, _ = queued
    monkeypatch.setattr('codex_queue.bounded_command', lambda *args: b'possibly submitted')
    mid = agent_msg.send(db, 'controller', 'peer', 'probe')
    assert bridge.tick(db, path)['state'] == 'unknown'
    assert bridge.tick(db, path)['state'] == 'idle'
    qid = str(uuid.uuid4())
    bridge.resolve(db, mid, 'accepted', 'Verified queued message in Codex', queue_id=qid)
    assert db.execute('SELECT queue_id FROM codex_queue_receipts').fetchone()[0] == qid


def test_wrong_project_refuses_before_queue_invocation(queued, monkeypatch):
    db, path, _ = queued
    agent_msg.send(db, 'controller', 'peer', 'probe')
    with db:
        db.execute("UPDATE codex_bindings SET project='/wrong'")
    monkeypatch.setattr('codex_queue.bounded_command', lambda *args: pytest.fail('wrong thread submitted'))
    assert bridge.tick(db, path)['state'] == 'skipped'


def test_existing_unix_binding_migrates_without_changing_delivery(tmp_path):
    path = str(tmp_path / 'bus.db')
    db = agent_msg.open_db(path)
    db.executescript(bridge.SCHEMA)
    db.execute("INSERT INTO codex_bindings VALUES ('p','t','/p','/s',0,20,0,30,0)")
    db.commit(); db.close()
    db = bridge.database(path)
    row = dict(db.execute('SELECT * FROM codex_bindings').fetchone())
    assert row['transport'] == 'unix' and row['enabled'] == 0 and row['socket'] == '/s'
    db.close()


@pytest.mark.parametrize('code,reason', [
    ('import time; time.sleep(3)', 'queue_timeout'),
    ('print("x"*70000)', 'queue_output_limit'),
    ('raise SystemExit(2)', 'queue_command_failed')])
def test_queue_command_is_bounded_and_failure_text_is_not_exposed(code, reason):
    import os
    with pytest.raises(TransportError, match=reason):
        bounded_command([sys.executable, '-c', code], os.environ.copy(), timeout=0.2)
