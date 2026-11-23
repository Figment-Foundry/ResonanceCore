"""Version 1 private, runtime-owned invocation mailboxes. No model tools or wakeups.

The trusted runtime retains this object and enforces its run/admission fence via
the authority callback. Endpoint readiness means runtime ownership, not an agent
inbox consumer. Payloads are bounded opaque JSON pointers and are never executed.
"""
import fcntl
from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
import uuid

INTERFACE_VERSION = 1
SCHEMA = '''
CREATE TABLE IF NOT EXISTS runtime_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER, run_id TEXT);
CREATE TABLE IF NOT EXISTS endpoints(
 id TEXT PRIMARY KEY, session TEXT NOT NULL, epoch INTEGER NOT NULL, invocation TEXT NOT NULL,
 owner TEXT NOT NULL, deadline REAL NOT NULL, closed INTEGER NOT NULL DEFAULT 0,
 UNIQUE(session,epoch,invocation));
CREATE TABLE IF NOT EXISTS runtime_messages(
 id INTEGER PRIMARY KEY, sender TEXT NOT NULL REFERENCES endpoints(id), recipient TEXT NOT NULL REFERENCES endpoints(id),
 kind TEXT NOT NULL, reply_to INTEGER, body TEXT NOT NULL, key TEXT NOT NULL,
 deadline REAL NOT NULL, read_at REAL, UNIQUE(sender,key));
'''


def label(value):
    if not isinstance(value, str) or not value.strip() or len(value.encode()) > 256 or '\x00' in value:
        raise ValueError('invalid_identity')
    return value


class RuntimeMailbox:
    def __init__(self, path, run_id, *, authority=None, monotonic=time.monotonic):
        self.path, self.run_id = Path(path).absolute(), label(run_id)
        self.authority, self.now = authority, monotonic
        self.owner, self.lock, self.db = str(uuid.uuid4()), threading.RLock(), None
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.path.parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError('private_storage_required')
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise ValueError('private_storage_required')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError('runtime_already_attached') from None
            self.db = sqlite3.connect(self.path, check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            self.db.execute('PRAGMA foreign_keys=ON')
            tables = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if tables and 'runtime_meta' not in tables:
                raise ValueError('dedicated_runtime_store_required')
            self.db.executescript(SCHEMA)
            with self.db:
                row = self.db.execute('SELECT version,run_id FROM runtime_meta').fetchone()
                if row and tuple(row) != (INTERFACE_VERSION, run_id):
                    raise ValueError('runtime_identity_or_version_mismatch')
                self.db.execute('INSERT OR IGNORE INTO runtime_meta VALUES (1,?,?)', (INTERFACE_VERSION, run_id))
            self._fd = fd
        except BaseException:
            if self.db is not None:
                self.db.close()
            os.close(fd)
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def _authority(self):
        if self.db is None:
            raise ValueError('runtime_closed')
        if self.authority is not None:
            self.authority()  # Must raise if the controller's run/admission fence is lost.

    @contextmanager
    def _transaction(self):
        with self.lock:
            self._authority()
            with self.db:
                yield

    @staticmethod
    def _ttl(ttl):
        if type(ttl) not in (int, float) or not math.isfinite(ttl) or not 0 < ttl <= 3600:
            raise ValueError('invalid_lease')

    def _endpoint(self, endpoint):
        row = self.db.execute('SELECT * FROM endpoints WHERE id=?', (label(endpoint),)).fetchone()
        if row is None or row['owner'] != self.owner or row['closed'] or row['deadline'] <= self.now():
            raise ValueError('endpoint_inactive')
        return row

    def register(self, session, epoch, invocation, *, ttl=300):
        label(session), label(invocation)
        self._ttl(ttl)
        if type(epoch) is not int or not 0 <= epoch < 2**63:
            raise ValueError('invalid_epoch')
        with self._transaction():
            self._authority()
            if self.db.execute('SELECT COUNT(*) FROM endpoints').fetchone()[0] >= 1000:
                raise ValueError('capacity')
            if self.db.execute('SELECT 1 FROM endpoints WHERE session=? AND epoch=? AND invocation=?', (session, epoch, invocation)).fetchone():
                raise ValueError('invocation_already_registered')
            identifier = str(uuid.uuid4())
            self.db.execute('INSERT INTO endpoints VALUES (?,?,?,?,?,?,0)',
                            (identifier, session, epoch, invocation, self.owner, self.now() + ttl))
            return identifier

    def renew(self, endpoint, *, ttl=300):
        self._ttl(ttl)
        with self._transaction():
            self._authority()
            self._endpoint(endpoint)
            self.db.execute('UPDATE endpoints SET deadline=? WHERE id=?', (self.now() + ttl, endpoint))

    def retire(self, endpoint):
        with self._transaction():
            self._authority()
            if not self.db.execute('UPDATE endpoints SET closed=1 WHERE id=? AND owner=?', (label(endpoint), self.owner)).rowcount:
                raise ValueError('endpoint_inactive')

    def send(self, sender, recipient, payload, *, key, kind='notify', reply_to=None, ttl=300):
        self._ttl(ttl)
        label(key)
        if type(payload) is not dict or kind not in ('notify', 'request', 'reply'):
            raise ValueError('invalid_message')
        try:
            body = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)
        except (ValueError, TypeError, RecursionError):
            raise ValueError('invalid_payload') from None
        if len(body.encode()) > 4096:
            raise ValueError('payload_limit')
        if (kind == 'reply') != (reply_to is not None) or (reply_to is not None and type(reply_to) is not int):
            raise ValueError('invalid_correlation')
        with self._transaction():
            self._authority()
            source, target = self._endpoint(sender), self._endpoint(recipient)
            if reply_to is not None:
                original = self.db.execute('SELECT * FROM runtime_messages WHERE id=?', (reply_to,)).fetchone()
                if (original is None or original['sender'] != recipient or original['recipient'] != sender
                        or original['kind'] != 'request' or original['deadline'] <= self.now()):
                    raise ValueError('endpoint_inactive_or_reply_mismatch')
            prior = self.db.execute('SELECT * FROM runtime_messages WHERE sender=? AND key=?', (sender, key)).fetchone()
            if prior:
                if (prior['recipient'], prior['kind'], prior['reply_to'], prior['body']) != (recipient, kind, reply_to, body):
                    raise ValueError('key_conflict')
                return prior['id']
            if self.db.execute('SELECT COUNT(*) FROM runtime_messages').fetchone()[0] >= 10000:
                raise ValueError('capacity')
            if self.db.execute('SELECT COUNT(*) FROM runtime_messages WHERE recipient=? AND read_at IS NULL AND deadline>?', (recipient, self.now())).fetchone()[0] >= 100:
                raise ValueError('backlog_limit')
            return self.db.execute('''INSERT INTO runtime_messages(sender,recipient,kind,reply_to,body,key,deadline)
                VALUES (?,?,?,?,?,?,?)''', (sender, recipient, kind, reply_to, body, key,
                min(self.now() + ttl, source['deadline'], target['deadline']))).lastrowid

    def reply(self, sender, request_id, payload, *, key):
        with self.lock:
            self._authority()
            self._endpoint(sender)
            if type(request_id) is not int:
                raise ValueError('invalid_correlation')
            row = self.db.execute('SELECT * FROM runtime_messages WHERE id=?', (request_id,)).fetchone()
            if row is None or row['recipient'] != sender:
                raise ValueError('endpoint_inactive_or_reply_mismatch')
            return self.send(sender, row['sender'], payload, key=key, kind='reply', reply_to=request_id)

    def read(self, endpoint, *, limit=5):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('invalid_page')
        with self.lock:
            self._authority()
            self._endpoint(endpoint)
            rows = self.db.execute('''SELECT m.*,s.session AS sender_session,s.epoch AS sender_epoch,
                s.invocation AS sender_invocation,t.session AS recipient_session,t.epoch AS recipient_epoch,
                t.invocation AS recipient_invocation FROM runtime_messages m
                JOIN endpoints s ON s.id=m.sender JOIN endpoints t ON t.id=m.recipient
                WHERE m.recipient=? AND m.read_at IS NULL AND m.deadline>?
                AND s.owner=? AND s.closed=0 AND s.deadline>? ORDER BY m.id LIMIT ?''',
                (endpoint, self.now(), self.owner, self.now(), limit)).fetchall()
            return [{**dict(row), 'run_id': self.run_id, 'payload': json.loads(row['body'])} for row in rows]

    def ack(self, endpoint, ids):
        if type(ids) is not list or not 1 <= len(ids) <= 100 or any(type(i) is not int for i in ids):
            raise ValueError('invalid_ack')
        with self._transaction():
            self._authority()
            self._endpoint(endpoint)
            for mid in ids:
                row = self.db.execute('SELECT * FROM runtime_messages WHERE id=?', (mid,)).fetchone()
                if row is None or row['recipient'] != endpoint:
                    raise ValueError('ack_owner_mismatch')
                if row['read_at'] is None:
                    if row['deadline'] <= self.now():
                        raise ValueError('message_inactive')
                    self._endpoint(row['sender'])
            self.db.executemany('UPDATE runtime_messages SET read_at=COALESCE(read_at,?) WHERE id=?', [(time.time(), mid) for mid in ids])

    def receipt(self, message_id):
        with self.lock:
            self._authority()
            row = self.db.execute('SELECT * FROM runtime_messages WHERE id=?', (message_id,)).fetchone()
            if row is None:
                raise ValueError('unknown_message')
            state = 'consumed' if row['read_at'] is not None else 'queued'
            if state == 'queued':
                try:
                    self._endpoint(row['sender']), self._endpoint(row['recipient'])
                    if row['deadline'] <= self.now():
                        state = 'expired'
                except ValueError:
                    state = 'endpoint_inactive'
            return {'id': row['id'], 'sender': row['sender'], 'recipient': row['recipient'],
                    'reply_to': row['reply_to'], 'state': state}

    def close(self):
        with self.lock:
            if self.db is not None:
                try:
                    with self.db:
                        self.db.execute('UPDATE endpoints SET closed=1 WHERE owner=?', (self.owner,))
                finally:
                    self.db.close()
                    self.db = None
                    os.close(self._fd)
