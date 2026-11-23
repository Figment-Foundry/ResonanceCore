"""Explicit disposable receiving attachments for existing protocol fixtures."""
from pathlib import Path

import agent_broadcast


def hook(db, project, name, route='hook'):
    existing = db.execute('SELECT session FROM participants WHERE recipient=?', (name,)).fetchone()
    if existing:
        return existing['session']
    sid = agent_broadcast.register(db, name, str(project), 'fixture', route, evidence='thread_attachment' if route == 'codex' else None)
    agent_broadcast.renew(db, name, sid, ready=True)
    return sid


def session(db, name):
    return db.execute('SELECT session FROM participants WHERE recipient=?', (name,)).fetchone()[0]


def attach(broker, peer):
    peer.update(broker.attach(peer['id'], 'test-fixture', peer['id'], 'thread_attachment', ready=True))
    return peer
