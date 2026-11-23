"""Leased session membership and atomic fan-out over the existing inbox bus."""
import json
from pathlib import Path
import time
import uuid

MAX_RECIPIENTS = 100
LEASE_SECONDS = 90
TARGET_FIELDS = ('thread_id', 'project', 'socket', 'transport', 'executable', 'codex_home')

SCHEMA = '''
CREATE TABLE IF NOT EXISTS participants (
 recipient TEXT PRIMARY KEY, session TEXT NOT NULL UNIQUE, project TEXT NOT NULL,
 harness TEXT NOT NULL, route TEXT NOT NULL CHECK(route IN ('hook','codex')),
 target TEXT NOT NULL, expires REAL NOT NULL, ready INTEGER NOT NULL DEFAULT 0,
 closed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS broadcasts (
 id TEXT PRIMARY KEY, sender TEXT NOT NULL, key TEXT NOT NULL, ts REAL NOT NULL,
 body TEXT NOT NULL, project TEXT, UNIQUE(sender,key)
);
CREATE TABLE IF NOT EXISTS broadcast_recipients (
 broadcast_id TEXT NOT NULL REFERENCES broadcasts(id), recipient TEXT NOT NULL,
 message_id INTEGER NOT NULL UNIQUE REFERENCES messages(id), session TEXT NOT NULL,
 route TEXT NOT NULL, target TEXT NOT NULL, skipped_reason TEXT,
 PRIMARY KEY(broadcast_id,recipient)
);
'''


def has_table(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def bounded_text(value, name, limit=128):
    if not isinstance(value, str) or not value.strip() or len(value.encode()) > limit or '\x00' in value:
        raise ValueError('invalid_' + name)
    return value


def project_path(value):
    if not Path(value).is_absolute() or not Path(value).is_dir():
        raise ValueError('project_must_be_existing_absolute_directory')
    return str(Path(value).resolve())


def binding_target(binding):
    return json.dumps({key: binding[key] for key in TARGET_FIELDS}, sort_keys=True)


def binding(db, recipient):
    if not has_table(db, 'codex_bindings'):
        return None
    row = db.execute('SELECT * FROM codex_bindings WHERE recipient=?', (recipient,)).fetchone()
    return dict(row) if row else None


def register(db, recipient, project, harness, route='hook', *, now=None, ttl=LEASE_SECONDS, evidence=None):
    """Create a fresh incarnation; a live alias cannot be stolen or shared."""
    bounded_text(recipient, 'recipient')
    bounded_text(harness, 'harness', 64)
    project = project_path(project)
    if route not in ('hook', 'codex') or not 1 <= ttl <= 3600:
        raise ValueError('invalid_presence_options')
    now = time.time() if now is None else now
    with db:
        db.execute('BEGIN IMMEDIATE')
        old = db.execute('SELECT * FROM participants WHERE recipient=?', (recipient,)).fetchone()
        if old and not old['closed'] and old['expires'] > now:
            raise ValueError('recipient_already_active')
        target = '{}'
        bound = binding(db, recipient)
        if route == 'codex':
            if not bound or bound['project'] != project:
                raise ValueError('matching_codex_binding_required')
            target = binding_target(bound)
            # One thread must not receive a broadcast once per alias/transport.
            for row in db.execute("SELECT target FROM participants WHERE route='codex' AND closed=0 AND expires>?", (now,)):
                if json.loads(row['target'])['thread_id'] == bound['thread_id']:
                    raise ValueError('thread_already_active')
        elif bound:
            raise ValueError('recipient_managed_by_codex_bridge')
        session = str(uuid.uuid4())
        if evidence not in (None, 'thread_attachment'):
            raise ValueError('invalid_attachment_evidence')
        evidence = 'supervised_hook' if route == 'hook' else evidence or 'unverified'
        db.execute('''INSERT INTO participants(recipient,session,project,harness,route,target,expires,evidence)
            VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(recipient) DO UPDATE SET
            session=excluded.session,project=excluded.project,harness=excluded.harness,
            route=excluded.route,target=excluded.target,expires=excluded.expires,evidence=excluded.evidence,ready=0,closed=0''',
            (recipient, session, project, harness, route, target, now + ttl, evidence))
    return session


def renew(db, recipient, session, *, ready=False, now=None, ttl=LEASE_SECONDS):
    now = time.time() if now is None else now
    if not 1 <= ttl <= 3600:
        raise ValueError('invalid_lease')
    with db:
        changed = db.execute('''UPDATE participants SET expires=?,ready=MAX(ready,?)
            WHERE recipient=? AND session=? AND closed=0 AND expires>?''',
            (now + ttl, int(ready), recipient, session, now)).rowcount
        if not changed:
            raise ValueError('session_expired_or_replaced')


def close(db, recipient, session):
    with db:
        changed = db.execute('UPDATE participants SET closed=1,ready=0 WHERE recipient=? AND session=?',
                             (recipient, session)).rowcount
        if not changed:
            raise ValueError('session_replaced')


def exclusion(db, row, sender, project, now):
    if row['recipient'] == sender:
        return 'sender'
    if project and row['project'] != project:
        return 'project'
    if row['closed']:
        return 'closed'
    if row['expires'] <= now:
        return 'expired'
    if not row['ready']:
        return 'integration_not_ready'
    if row['route'] == 'codex' and ('evidence' not in row.keys() or row['evidence'] != 'thread_attachment'):
        return 'attachment_unverified'
    bound = binding(db, row['recipient'])
    if row['route'] == 'codex':
        if not bound or binding_target(bound) != row['target']:
            return 'binding_changed'
        sender_binding = binding(db, sender)
        if sender_binding and sender_binding['thread_id'] == bound['thread_id']:
            return 'sender'
        if not bound['enabled']:
            return 'paused'
    elif bound:
        return 'binding_changed'
    return None


def audience(db, sender, project=None, now=None):
    now = time.time() if now is None else now
    selected, excluded, counts = [], [], {}
    if has_table(db, 'participants'):
        for row in db.execute('SELECT * FROM participants ORDER BY recipient'):
            reason = exclusion(db, row, sender, project, now)
            if reason:
                counts[reason] = counts.get(reason, 0) + 1
                if len(excluded) < 50:
                    excluded.append({'recipient': row['recipient'], 'reason': reason})
            else:
                selected.append(dict(row))
                if len(selected) > MAX_RECIPIENTS:
                    raise ValueError('audience_limit_exceeded')
    return selected, excluded, counts


def broadcast(db, sender, body, key=None, project=None, *, dry_run=False, now=None):
    bounded_text(sender, 'sender')
    bounded_text(body, 'body', 16_384)
    if project is not None:
        project = project_path(project)
    if not dry_run:
        bounded_text(key, 'key')
    now = time.time() if now is None else now
    with db:
        db.execute('BEGIN' if dry_run else 'BEGIN IMMEDIATE')
        if not dry_run:
            prior = db.execute('SELECT * FROM broadcasts WHERE sender=? AND key=?', (sender, key)).fetchone()
            if prior:
                if prior['body'] != body or prior['project'] != project:
                    raise ValueError('broadcast_key_conflict')
                return summary(db, prior['id'], replayed=True)
        selected, excluded, counts = audience(db, sender, project, now)
        if dry_run:
            return {'dry_run': True, 'recipient_count': len(selected),
                    'recipients': [{k: r[k] for k in ('recipient', 'project', 'harness', 'route')} for r in selected],
                    'excluded': excluded, 'excluded_counts': counts}
        bid = str(uuid.uuid4())
        # Retain even empty snapshots so a retry cannot unexpectedly reach new joiners.
        db.execute('INSERT INTO broadcasts VALUES (?,?,?,?,?,?)', (bid, sender, key, now, body, project))
        origin = db.execute('SELECT session FROM participants WHERE recipient=? AND closed=0 AND expires>?', (sender, now)).fetchone()
        db.execute('INSERT INTO broadcast_origins VALUES (?,?)', (bid, origin['session'] if origin else None))
        for row in selected:
            mid = db.execute("INSERT INTO messages(ts,sender,recipient,type,body) VALUES (?,?,?,'notify',?)",
                             (now, sender, row['recipient'], body)).lastrowid
            db.execute('''INSERT INTO broadcast_recipients
                (broadcast_id,recipient,message_id,session,route,target) VALUES (?,?,?,?,?,?)''',
                       (bid, row['recipient'], mid, row['session'], row['route'], row['target']))
            if row['route'] == 'codex':
                # Validate the actual serialized child; failure rolls back the entire set.
                from codex_bridge import payload
                path = db.execute('PRAGMA database_list').fetchone()[2]
                payload({'id': mid, 'sender': sender, 'recipient': row['recipient'],
                         'type': 'notify', 'reply_to': None, 'body': body,
                         'thread_id': json.loads(row['target'])['thread_id'], 'broadcast_id': bid}, path)
        result = summary(db, bid)
        result.update(excluded=excluded, excluded_counts=counts)
        return result


def summary(db, bid, replayed=False):
    rows = db.execute('SELECT recipient,message_id FROM broadcast_recipients WHERE broadcast_id=? ORDER BY recipient', (bid,)).fetchall()
    return {'broadcast_id': bid, 'recipient_count': len(rows), 'replayed': replayed,
            'recipients': [dict(row) for row in rows], 'state': 'queued' if rows else 'empty'}


def child_guard(db, message_id, *, session=None, target=None, now=None):
    """A broadcast remains pinned to the session selected at publication."""
    row = db.execute('SELECT * FROM broadcast_recipients WHERE message_id=?', (message_id,)).fetchone()
    if not row:
        return None
    if row['skipped_reason']:
        return row['skipped_reason']
    participant = db.execute('SELECT * FROM participants WHERE recipient=?', (row['recipient'],)).fetchone()
    now = time.time() if now is None else now
    if not participant or participant['session'] != row['session']:
        return 'session_replaced'
    if participant['closed'] or participant['expires'] <= now or not participant['ready']:
        return 'session_inactive'
    if participant['route'] == 'codex' and participant['evidence'] != 'thread_attachment':
        return 'attachment_unverified'
    if row['route'] == 'hook' and binding(db, row['recipient']) is not None:
        return 'binding_changed'
    if target is not None:
        if row['route'] != 'codex' or row['target'] != target:
            return 'binding_changed'
    elif row['route'] != 'hook' or session != row['session']:
        return 'session_mismatch'
    return None


def status(db, bid, limit=50, offset=0, now=None):
    if not 1 <= limit <= 100 or offset < 0:
        raise ValueError('invalid_page')
    if not db.execute('SELECT 1 FROM broadcasts WHERE id=?', (bid,)).fetchone():
        raise ValueError('unknown_broadcast')
    now = time.time() if now is None else now
    rows = db.execute('''SELECT r.*,m.read_at,m.sender FROM broadcast_recipients r
        JOIN messages m ON m.id=r.message_id WHERE r.broadcast_id=? ORDER BY r.recipient''', (bid,)).fetchall()
    results, counts = [], {}
    for row in rows:
        state, reason = ('consumed', '') if row['read_at'] is not None else ('queued', '')
        receipt = None
        if has_table(db, 'codex_deliveries'):
            receipt = db.execute('SELECT state,reason FROM codex_deliveries WHERE message_id=?', (row['message_id'],)).fetchone()
        if receipt:
            state = 'accepted' if receipt['state'] == 'accepted' else receipt['state']
            reason = receipt['reason']
        if row['skipped_reason']:
            state, reason = 'skipped', row['skipped_reason']
        elif row['read_at'] is None and state in ('queued', 'pending'):
            bound = binding(db, row['recipient'])
            reason = child_guard(db, row['message_id'], session=row['session'],
                                 target=binding_target(bound) if bound and row['route'] == 'codex' else None, now=now) or ''
            if row['route'] == 'codex' and not reason:
                if not bound:
                    reason = 'binding_removed'
                elif not bound['enabled']:
                    reason = 'paused'
                elif bound['wakeups'] >= bound['max_wakeups']:
                    reason = 'budget_exhausted'
                elif bound['next_at'] > now:
                    reason = 'cooldown'
                else:
                    hold = db.execute("SELECT state FROM codex_deliveries WHERE recipient=? AND state IN ('blocked','unknown','sending') LIMIT 1", (row['recipient'],)).fetchone()
                    if hold:
                        reason = 'recipient_' + hold['state']
            if reason:
                state = 'held'
        reply = db.execute("SELECT id FROM messages WHERE type='reply' AND reply_to=? AND sender=? AND recipient=? ORDER BY id LIMIT 1",
                           (row['message_id'], row['recipient'], row['sender'])).fetchone()
        counts[state] = counts.get(state, 0) + 1
        results.append({'recipient': row['recipient'], 'message_id': row['message_id'],
                        'state': state, 'reason': reason, 'acknowledged': reply is not None,
                        'reply_message_id': reply['id'] if reply else None})
    return {'broadcast_id': bid, 'recipient_count': len(rows), 'counts': counts,
            'acknowledged_count': sum(r['acknowledged'] for r in results),
            'recipients': results[offset:offset + limit],
            'next_offset': offset + limit if offset + limit < len(rows) else None}
