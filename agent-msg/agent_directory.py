"""Legacy routing directory. Shared SQLite metadata is not authentication."""
import time
from pathlib import Path

import agent_broadcast as membership

SCHEMA = '''
CREATE TABLE IF NOT EXISTS direct_targets (
 message_id INTEGER PRIMARY KEY REFERENCES messages(id),
 session TEXT NOT NULL, route TEXT NOT NULL, target TEXT NOT NULL,
 sender_session TEXT, expires REAL NOT NULL, reason TEXT
);
CREATE TABLE IF NOT EXISTS broadcast_origins (
 broadcast_id TEXT PRIMARY KEY, sender_session TEXT
);
'''


def current(db, recipient, now=None):
    now = time.time() if now is None else now
    row = db.execute('SELECT * FROM participants WHERE recipient=?', (recipient,)).fetchone()
    if row is None or membership.exclusion(db, row, '', None, now):
        raise ValueError('recipient_unavailable')
    return row


def resolve(db, label, project=None, now=None):
    """Discover a fresh address; sending and consumption remain exact-session only."""
    membership.bounded_text(label, 'label')
    if project is not None:
        project = membership.project_path(project)
    now = time.time() if now is None else now
    exact = db.execute('SELECT * FROM participants WHERE recipient=?', (label,)).fetchone()
    reason = None
    if exact is not None:
        reason = membership.exclusion(db, exact, '', project, now)
        if reason is None:
            return card(db, exact, now)

    # Native thread addresses must never be reinterpreted as logical project names.
    if label.startswith(('codex-terminal-', 'claude-terminal-', 'kimi-terminal-',
                         'claude-native-', 'opencode-native-', 'pi-native-')):
        raise ValueError('recipient_unavailable: ' + (reason or 'not_found')
                         + '; exact terminal address cannot be replaced; use peers for fresh discovery')

    selected = None
    rows = db.execute('SELECT * FROM participants WHERE (? IS NULL OR project=?) ORDER BY recipient',
                      (project, project))
    for row in rows:
        if Path(row['project']).name != label or membership.exclusion(db, row, '', project, now):
            continue
        if selected is not None:
            raise ValueError('recipient_ambiguous: multiple live project-label matches; '
                             'use --project and peers to select an exact recipient and session')
        selected = row
    if selected is None:
        raise ValueError('recipient_unavailable: ' + (reason or 'no_live_project_match')
                         + '; use peers with the intended --project; this does not prove a thread is dead')
    return card(db, selected, now)


def guard(db, message_id, *, session=None, target=None, now=None):
    row = db.execute('''SELECT d.*,m.recipient FROM direct_targets d
        JOIN messages m ON m.id=d.message_id WHERE d.message_id=?''', (message_id,)).fetchone()
    if row is None:
        return 'legacy_unpinned'
    if row['reason']:
        return row['reason']
    now = time.time() if now is None else now
    if row['expires'] <= now:
        return 'message_expired'
    participant = db.execute('SELECT * FROM participants WHERE recipient=?', (row['recipient'],)).fetchone()
    if participant is None or participant['session'] != row['session']:
        return 'session_replaced'
    if participant['closed'] or participant['expires'] <= now or not participant['ready']:
        return 'session_inactive'
    if participant['route'] == 'codex' and participant['evidence'] != 'thread_attachment':
        return 'attachment_unverified'
    if participant['target'] != row['target'] or participant['route'] != row['route']:
        return 'binding_changed'
    if row['route'] == 'hook' and membership.binding(db, row['recipient']) is not None:
        return 'binding_changed'
    if target is not None:
        bound = membership.binding(db, row['recipient'])
        if row['route'] != 'codex' or row['target'] != target or not bound or membership.binding_target(bound) != target:
            return 'binding_changed'
        if not bound['enabled']:
            return 'paused'
    elif row['route'] != 'hook' or session != row['session']:
        return 'session_mismatch'
    return None


def card(db, row, now):
    reason = membership.exclusion(db, row, '', None, now)
    wakeup = False
    delivery = 'next_prompt'
    if row['route'] == 'codex':
        delivery = 'codex_bridge'
        bound = membership.binding(db, row['recipient'])
        wakeup = bool(bound and bound['enabled'] and bound['wakeups'] < bound['max_wakeups'])
    elif row['route'] == 'hook' and membership.has_table(db, 't3_receivers') and (
            t3 := db.execute('SELECT * FROM t3_receivers WHERE recipient=? AND session=?',
                             (row['recipient'], row['session'])).fetchone()):
        delivery = 't3_thread'
        wakeup = bool(t3['enabled'] and t3['wakeups'] < t3['max_wakeups'])
    elif membership.has_table(db, 'native_receivers'):
        native = db.execute('SELECT * FROM native_receivers WHERE recipient=? AND session=?', (row['recipient'], row['session'])).fetchone()
        if native:
            delivery = 'claude_channel' if row['harness'] == 'claude' else 'native_event'
            pending = db.execute("SELECT 1 FROM native_deliveries WHERE session=? AND state IN ('sending','unknown')", (row['session'],)).fetchone()
            wakeup = bool(native['enabled'] and native['wakeups'] < native['max_wakeups'] and not pending)
    return {**{k: row[k] for k in ('recipient', 'session', 'project', 'harness', 'route', 'expires')},
            'state': reason or 'active', 'sendable': reason is None,
            'delivery': delivery, 'wakeup': reason is None and wakeup,
            'identity_trust': 'unauthenticated_legacy_metadata'}


def peers(db, project=None, limit=20, offset=0):
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
        raise ValueError('invalid_page')
    if project is not None:
        project = membership.project_path(project)
    now = time.time()
    rows = db.execute('''SELECT * FROM participants WHERE (? IS NULL OR project=?)
        ORDER BY recipient LIMIT ? OFFSET ?''', (project, project, limit + 1, offset)).fetchall()
    return {'peers': [card(db, row, now) for row in rows[:limit]],
            'next_offset': offset + limit if len(rows) > limit else None, 'observed_at': now}


def whoami(db, recipient, session):
    row = db.execute('SELECT * FROM participants WHERE recipient=? AND session=?', (recipient, session)).fetchone()
    if row is None:
        raise ValueError('session_unknown')
    return card(db, row, time.time())
