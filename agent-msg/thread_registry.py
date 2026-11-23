"""Broker-owned exact attachments. All lifecycle writes are trusted-host only."""
import math
import time
import uuid


class Refused(ValueError):
    pass


SCHEMA = '''
CREATE TABLE IF NOT EXISTS attachments (
 session_id TEXT PRIMARY KEY REFERENCES sessions(id), id TEXT UNIQUE NOT NULL,
 instance TEXT NOT NULL, native_thread TEXT NOT NULL, evidence TEXT NOT NULL,
 epoch TEXT NOT NULL, deadline REAL NOT NULL, verified REAL NOT NULL,
 ready INTEGER NOT NULL, retired TEXT, paused INTEGER NOT NULL DEFAULT 0,
 route_revision INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS message_targets (
 message_id TEXT PRIMARY KEY REFERENCES messages(id), attachment_id TEXT NOT NULL,
 route_revision INTEGER NOT NULL, reason TEXT
);
'''


def bounded(value, maximum=128):
    if not isinstance(value, str) or not value.strip() or len(value.encode()) > maximum or '\x00' in value:
        raise Refused('invalid_request')
    return value


class Registry:
    def __init__(self, db, *, monotonic=time.monotonic, clock=time.time):
        self.db, self.monotonic, self.clock = db, monotonic, clock
        self.epoch = str(uuid.uuid4())
        version = db.execute('PRAGMA user_version').fetchone()[0]
        if version not in (0, 1):
            raise Refused('unsupported_schema')
        # Additive migration leaves credentials, grants and historical mail intact.
        db.executescript('BEGIN IMMEDIATE;\n' + SCHEMA + '\nPRAGMA user_version=1;\nCOMMIT;')

    def row(self, sid):
        return self.db.execute('SELECT * FROM attachments WHERE session_id=?', (sid,)).fetchone()

    def state(self, row):
        if row is None:
            return 'not_ready'
        if row['retired']:
            return 'retired'
        if row['epoch'] != self.epoch or row['deadline'] <= self.monotonic():
            return 'stale'
        if not row['ready']:
            return 'not_ready'
        return 'active'

    def attach(self, sid, instance, native_thread, evidence, *, ready=False, ttl=90):
        for value in (instance, native_thread):
            bounded(value, 256)
        self._ttl(ttl)
        if type(ready) is not bool or evidence not in ('supervised_hook', 'thread_attachment'):
            raise Refused('exact_attachment_required')
        if self.row(sid) is not None:
            raise Refused('new_incarnation_required')
        harness = self.db.execute('SELECT harness FROM sessions WHERE id=?', (sid,)).fetchone()[0]
        for other in self.db.execute('''SELECT a.* FROM attachments a JOIN sessions s ON s.id=a.session_id
                WHERE a.instance=? AND a.native_thread=? AND s.harness=? AND s.active=1 AND s.expires>?''',
                (instance, native_thread, harness, self.clock())):
            if self.state(other) in ('active', 'not_ready'):
                raise Refused('thread_already_attached')
        aid = str(uuid.uuid4())
        self.db.execute('''INSERT INTO attachments
            (session_id,id,instance,native_thread,evidence,epoch,deadline,verified,ready)
            VALUES (?,?,?,?,?,?,?,?,?)''',
            (sid, aid, instance, native_thread, evidence, self.epoch,
             self.monotonic() + ttl, self.clock(), int(ready)))
        return {'attachment_id': aid, 'route_revision': 1}

    @staticmethod
    def _ttl(ttl):
        if type(ttl) not in (float, int) or not math.isfinite(ttl) or not 0 < ttl <= 90:
            raise Refused('invalid_lease')

    def renew(self, sid, aid, *, ready=False, ttl=90):
        self._ttl(ttl)
        row = self.row(sid)
        if type(ready) is not bool or row is None or row['id'] != aid or self.state(row) not in ('active', 'not_ready'):
            raise Refused('attachment_expired_or_replaced')
        self.db.execute('UPDATE attachments SET deadline=?,verified=?,ready=MAX(ready,?) WHERE session_id=?',
                        (self.monotonic() + ttl, self.clock(), int(ready), sid))

    def require(self, sid, expected_revision=None):
        row = self.row(sid)
        state = self.state(row)
        if state != 'active':
            raise Refused('not_ready' if state == 'not_ready' else 'recipient_' + state)
        if expected_revision is not None and (type(expected_revision) is not int or expected_revision != row['route_revision']):
            raise Refused('stale_selection')
        if row['paused']:
            raise Refused('recipient_paused')
        return row

    def card(self, session):
        row = self.row(session['id'])
        state = self.state(row)
        if not session['active']:
            state = 'quarantined'
        elif session['expires'] <= self.clock():
            state = 'expired'
        return {**{k: session[k] for k in ('id', 'label', 'harness', 'project', 'expires')},
                'state': state, 'sendable': state == 'active' and not row['paused'],
                'instance': row['instance'] if row else None,
                'native_thread': row['native_thread'] if row else None,
                'attachment_id': row['id'] if row else None,
                'route_revision': row['route_revision'] if row else None,
                'last_verified': row['verified'] if row else None,
                'lease_remaining_seconds': max(0, row['deadline'] - self.monotonic()) if row and row['epoch'] == self.epoch else 0,
                'paused': bool(row['paused']) if row else False, 'wakeup': False}

    def message_reason(self, message):
        target = self.db.execute('SELECT * FROM message_targets WHERE message_id=?', (message['id'],)).fetchone()
        if target is None:
            return 'legacy_unpinned'
        if target['reason']:
            return target['reason']
        if message['expires'] <= self.clock():
            return 'message_expired'
        row = self.row(message['recipient'])
        if row is None or row['id'] != target['attachment_id'] or row['route_revision'] != target['route_revision']:
            return 'attachment_changed'
        state = self.state(row)
        if state != 'active':
            return 'recipient_' + state
        return 'paused' if row['paused'] else None
