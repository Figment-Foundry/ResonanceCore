"""Experimental owner-controlled local broker; no legacy database or wakeup route.

The owner API runs only in the trusted hosting process. Peer RPC exposes the
small whitelist below. Filesystem/process isolation is an external prerequisite,
not supplied by possession of a credential or by this module's file modes.
"""
import hashlib
import fcntl
import json
import os
from pathlib import Path
import secrets
import socket
import socketserver
import sqlite3
import stat
import threading
import time
import uuid
from thread_registry import Registry, Refused


SCHEMA = '''
CREATE TABLE IF NOT EXISTS sessions(
 id TEXT PRIMARY KEY, token_hash TEXT UNIQUE NOT NULL, label TEXT NOT NULL,
 harness TEXT NOT NULL, project TEXT NOT NULL, expires REAL NOT NULL, active INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS contexts(
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, expires REAL NOT NULL, active INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS members(
 context_id TEXT NOT NULL REFERENCES contexts(id), session_id TEXT NOT NULL REFERENCES sessions(id),
 active INTEGER NOT NULL, PRIMARY KEY(context_id,session_id));
CREATE TABLE IF NOT EXISTS invitations(
 code TEXT PRIMARY KEY, target TEXT NOT NULL REFERENCES sessions(id), expires REAL NOT NULL,
 source TEXT, proposal_id TEXT UNIQUE, context_id TEXT);
CREATE TABLE IF NOT EXISTS roots(
 id TEXT PRIMARY KEY, context_id TEXT NOT NULL REFERENCES contexts(id), sender TEXT NOT NULL REFERENCES sessions(id),
 key TEXT NOT NULL, intent TEXT NOT NULL, UNIQUE(context_id,sender,key));
CREATE TABLE IF NOT EXISTS messages(
 id TEXT PRIMARY KEY, root_id TEXT NOT NULL REFERENCES roots(id), context_id TEXT NOT NULL REFERENCES contexts(id),
 sender TEXT NOT NULL REFERENCES sessions(id), recipient TEXT NOT NULL REFERENCES sessions(id),
 body TEXT NOT NULL, reply_to TEXT, expires REAL NOT NULL, acked INTEGER NOT NULL DEFAULT 0,
 ordinal INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS rates(
 session_id TEXT NOT NULL, category TEXT NOT NULL, window INTEGER NOT NULL, count INTEGER NOT NULL,
 PRIMARY KEY(session_id,category));
'''


def text(value, maximum=128):
    if not isinstance(value, str) or not value or len(value.encode('utf-8')) > maximum:
        raise Refused('invalid_request')
    return value


class Broker:
    """Trusted host owns enrollment, approval, mesh creation and revocation."""
    def __init__(self, path, *, clock=time.time, monotonic=time.monotonic):
        self.path, self.clock = Path(path).absolute(), clock
        self.lock = threading.RLock()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = self.path.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.geteuid() or parent.st_mode & 0o077:
            raise Refused('private_storage_required')
        # Parent directory is required to be outside peer access. O_NOFOLLOW
        # protects the final component; it cannot substitute for that boundary.
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        self.db = None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077 or info.st_nlink != 1:
                raise Refused('private_storage_required')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise Refused('broker_already_running') from None
            self.db = sqlite3.connect(self.path, check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            self.db.execute('PRAGMA foreign_keys=ON')
            self.db.executescript(SCHEMA)
            self.registry = Registry(self.db, clock=clock, monotonic=monotonic)
            self._owner_fd = fd
        except BaseException:
            if self.db is not None:
                self.db.close()
            os.close(fd)
            raise

    def close(self):
        with self.lock:
            if self.db is not None:
                self.db.close()
                self.db = None
                os.close(self._owner_fd)

    def _session(self, sid):
        row = self.db.execute('SELECT * FROM sessions WHERE id=? AND active=1 AND expires>?',
                              (sid, self.clock())).fetchone()
        if row is None:
            raise Refused('denied')
        return row

    def enroll(self, label, harness, project, *, ttl=3600):
        """Owner-only: provision credential privately to this exact session."""
        for value in (label, harness, project):
            text(value, 4096)
        if not Path(project).is_absolute() or type(ttl) not in (int, float) or not 0 < ttl <= 28800:
            raise Refused('invalid_enrollment')
        credential, sid = secrets.token_urlsafe(32), str(uuid.uuid4())
        with self.lock, self.db:
            if self.db.execute('SELECT COUNT(*) FROM sessions').fetchone()[0] >= 1000:
                raise Refused('capacity')
            self.db.execute('INSERT INTO sessions VALUES (?,?,?,?,?,?,1)',
                            (sid, hashlib.sha256(credential.encode()).hexdigest(), label, harness, project, self.clock() + ttl))
        return {'id': sid, 'credential': credential}

    def attach(self, sid, instance, native_thread, evidence, *, ready=False, ttl=90):
        """Trusted host attests an exact receiving attachment, never a shared PID."""
        with self.lock, self.db:
            self._session(sid)
            return self.registry.attach(sid, instance, native_thread, evidence, ready=ready, ttl=ttl)

    def renew(self, sid, attachment_id, *, ready=False, ttl=90):
        with self.lock, self.db:
            self._session(sid)
            self.registry.renew(sid, attachment_id, ready=ready, ttl=ttl)

    def retire(self, sid, attachment_id, reason='closed'):
        with self.lock, self.db:
            if not self.db.execute('UPDATE attachments SET retired=?,ready=0 WHERE session_id=? AND id=?',
                                   (text(reason), text(sid), text(attachment_id))).rowcount:
                raise Refused('attachment_mismatch')

    def pause(self, sid, paused=True):
        if type(paused) is not bool:
            raise Refused('invalid_request')
        with self.lock, self.db:
            if not self.db.execute('UPDATE attachments SET paused=? WHERE session_id=?', (int(paused), text(sid))).rowcount:
                raise Refused('unavailable')

    def directory(self, *, limit=20, offset=0):
        """Owner-only global diagnostic directory, including tombstones."""
        self._page(limit, offset)
        with self.lock:
            rows = self.db.execute('SELECT * FROM sessions ORDER BY id LIMIT ? OFFSET ?', (limit + 1, offset)).fetchall()
            return {'peers': [self.registry.card(row) for row in rows[:limit]],
                    'next_offset': offset + limit if len(rows) > limit else None, 'observed_at': self.clock()}

    def receipt(self, root_id):
        """Owner-only reconciliation remains available after expiry or retirement."""
        with self.lock:
            rows = self.db.execute('SELECT * FROM messages WHERE root_id=? ORDER BY ordinal', (text(root_id),)).fetchall()
            if not rows:
                raise Refused('unavailable')
            outcomes = []
            for row in rows:
                reason = self.registry.message_reason(row)
                try:
                    members = self._members(row['context_id'], row['sender'])
                    if row['recipient'] not in members:
                        reason = 'authorization_expired_or_revoked'
                except Refused:
                    reason = 'authorization_expired_or_revoked'
                outcomes.append({'message_id': row['id'], 'recipient': row['recipient'],
                    'state': 'acknowledged' if row['acked'] else 'held' if reason == 'paused' else 'undelivered' if reason else 'queued',
                    'reason': None if row['acked'] else reason})
            return {'root_id': root_id, 'recipients': outcomes}

    @staticmethod
    def _page(limit, offset):
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise Refused('invalid_page')

    def _context(self, ids, kind):
        if not isinstance(ids, list) or not 2 <= len(ids) <= 100 or len(set(ids)) != len(ids):
            raise Refused('invalid_roster')
        sessions = [self._session(text(sid)) for sid in ids]
        if self.db.execute('SELECT COUNT(*) FROM contexts').fetchone()[0] >= 1000:
            raise Refused('capacity')
        ctx = str(uuid.uuid4())
        self.db.execute('INSERT INTO contexts VALUES (?,?,?,1)',
                        (ctx, kind, min(s['expires'] for s in sessions) if kind == 'pair' else self.clock() + 28800))
        self.db.executemany('INSERT INTO members VALUES (?,?,1)', [(ctx, sid) for sid in ids])
        return {'context_id': ctx}

    def mesh(self, ids):
        """Owner-only: exact all-to-all roster, message-only permissions."""
        with self.lock, self.db:
            return self._context(ids, 'mesh')

    def approve(self, proposal_id):
        """Owner-only: approve the frozen source/target proposal, once.

        A trusted UI must show proposal() and bind its human confirmation to the
        returned immutable ID. No peer-facing operation invokes this method.
        """
        with self.lock, self.db:
            row = self.db.execute('SELECT * FROM invitations WHERE proposal_id=?', (text(proposal_id),)).fetchone()
            if row is None:
                raise Refused('unavailable')
            if row['context_id']:
                return {'context_id': row['context_id']}
            if row['expires'] <= self.clock():
                raise Refused('unavailable')
            result = self._context([row['source'], row['target']], 'pair')
            self.db.execute('UPDATE invitations SET context_id=? WHERE proposal_id=?', (result['context_id'], proposal_id))
            return result

    def proposal(self, proposal_id):
        """Owner-only inspection: metadata, never a pending peer message body."""
        with self.lock:
            row = self.db.execute('SELECT * FROM invitations WHERE proposal_id=? AND expires>?',
                                  (text(proposal_id), self.clock())).fetchone()
            if row is None:
                raise Refused('unavailable')
            fields = ('id', 'label', 'harness', 'project', 'expires')
            return {'proposal_id': proposal_id, 'peers': [{k: self._session(sid)[k] for k in fields}
                    for sid in (row['source'], row['target'])], 'wakeup': False,
                    'operations': ['send', 'inbox', 'ack', 'announce'], 'expires': row['expires']}

    def deny(self, proposal_id):
        with self.lock, self.db:
            self.db.execute('UPDATE invitations SET expires=0 WHERE proposal_id=? AND context_id IS NULL',
                            (text(proposal_id),))

    def revoke(self, context_id):
        with self.lock, self.db:
            self.db.execute('UPDATE contexts SET active=0 WHERE id=?', (text(context_id),))

    def remove(self, context_id, session_id):
        with self.lock, self.db:
            self.db.execute('UPDATE members SET active=0 WHERE context_id=? AND session_id=?',
                            (text(context_id), text(session_id)))

    def quarantine(self, session_id):
        with self.lock, self.db:
            self.db.execute('UPDATE sessions SET active=0 WHERE id=?', (text(session_id),))

    def _rate(self, sid, category, maximum, seconds=60):
        # Commit attempt accounting independently of a refused operation, so
        # errors cannot roll back guessing/flood limits. Survives broker restart.
        window = int(self.clock() // seconds)
        with self.db:
            self.db.execute('''INSERT INTO rates VALUES (?,?,?,1) ON CONFLICT(session_id,category)
                DO UPDATE SET window=excluded.window,
                count=CASE WHEN rates.window=excluded.window THEN rates.count+1 ELSE 1 END''',
                (sid, category, window))
        count = self.db.execute('SELECT count FROM rates WHERE session_id=? AND category=?', (sid, category)).fetchone()[0]
        if count > maximum:
            raise Refused('rate_limited')

    def _members(self, context_id, actor):
        text(context_id)
        row = self.db.execute('SELECT 1 FROM contexts WHERE id=? AND active=1 AND expires>?',
                              (context_id, self.clock())).fetchone()
        members = {r['id']: r for r in self.db.execute('''SELECT s.* FROM sessions s JOIN members m
            ON m.session_id=s.id WHERE m.context_id=? AND m.active=1 AND s.active=1 AND s.expires>?''',
            (context_id, self.clock()))}
        if row is None or actor not in members:
            raise Refused('denied')
        return members

    def call(self, credential, operation, args):
        """Only peer entrypoint. Identity comes from a per-session credential."""
        allowed = {
            'whoami': (set(), set()), 'peers': ({'context_id'}, {'limit', 'offset'}),
            'resolve': ({'context_id', 'label'}, set()),
            'invite': (set(), set()), 'pair': ({'code'}, set()),
            'send': ({'context_id', 'to', 'body', 'key'}, {'reply_to', 'expected_route_revision'}),
            'announce': ({'context_id', 'body', 'key'}, set()),
            'inbox': ({'context_id'}, set()), 'ack': ({'context_id', 'message_ids'}, set()),
        }
        if not isinstance(args, dict) or not isinstance(operation, str) or operation not in allowed:
            raise Refused('invalid_request')
        required, optional = allowed[operation]
        if not required <= args.keys() or args.keys() - required - optional:
            raise Refused('invalid_request')
        digest = hashlib.sha256(text(credential).encode()).hexdigest()
        with self.lock:
            row = self.db.execute('SELECT id FROM sessions WHERE token_hash=?', (digest,)).fetchone()
            if row is None:
                raise Refused('denied')
            actor = self._session(row['id'])['id']
            self._rate(actor, 'calls', 120)
            if operation == 'pair':
                self._rate(actor, 'pair', 5, seconds=300)
            with self.db:
                if operation == 'whoami':
                    return {**self.registry.card(self._session(actor)), 'contexts': [r[0] for r in self.db.execute('''
                        SELECT m.context_id FROM members m JOIN contexts c ON c.id=m.context_id
                        WHERE m.session_id=? AND m.active=1 AND c.active=1 AND c.expires>? ORDER BY c.id''', (actor, self.clock()))]}
                if operation == 'invite':
                    if self.db.execute('SELECT COUNT(*) FROM invitations WHERE target=? AND expires>?',
                                       (actor, self.clock())).fetchone()[0] >= 3:
                        raise Refused('capacity')
                    if self.db.execute('SELECT COUNT(*) FROM invitations').fetchone()[0] >= 3000:
                        raise Refused('capacity')
                    # Invitations are identifiers, not authorization credentials.
                    alphabet = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'
                    while True:
                        raw = ''.join(secrets.choice(alphabet) for _ in range(8))
                        code = raw[:4] + '-' + raw[4:]
                        if not self.db.execute('SELECT 1 FROM invitations WHERE code=?', (code,)).fetchone():
                            break
                    self.db.execute('INSERT INTO invitations(code,target,expires) VALUES (?,?,?)',
                                    (code, actor, self.clock() + 300))
                    return {'code': code, 'expires': self.clock() + 300}
                if operation == 'pair':
                    invite = self.db.execute('SELECT * FROM invitations WHERE code=? AND expires>?',
                                             (text(args['code']), self.clock())).fetchone()
                    if invite is None or invite['target'] == actor or invite['context_id'] or invite['source'] not in (None, actor):
                        raise Refused('unavailable')
                    self._session(invite['target'])
                    pid = invite['proposal_id'] or str(uuid.uuid4())
                    self.db.execute('UPDATE invitations SET source=?,proposal_id=? WHERE code=?', (actor, pid, args['code']))
                    return {'proposal_id': pid}
                ctx = args['context_id']
                members = self._members(ctx, actor)
                if operation in ('peers', 'resolve'):
                    cards = [self.registry.card(members[sid]) for sid in sorted(members) if sid != actor]
                    if operation == 'resolve':
                        label = text(args['label'])
                        matches = [card for card in cards if card['label'] == label and card['sendable']]
                        if len(matches) != 1:
                            raise Refused('ambiguous' if len(matches) > 1 else 'unavailable')
                        return matches[0]
                    limit, offset = args.get('limit', 20), args.get('offset', 0)
                    self._page(limit, offset)
                    return {'peers': cards[offset:offset + limit], 'observed_at': self.clock(),
                            'next_offset': offset + limit if len(cards) > offset + limit else None}
                if operation in ('send', 'announce'):
                    recipients = sorted(members.keys() - {actor}) if operation == 'announce' else [text(args['to'])]
                    if not recipients or any(r not in members or r == actor for r in recipients):
                        raise Refused('denied')
                    body, key = text(args['body'], 8192), text(args['key'])
                    reply_to = args.get('reply_to')
                    if reply_to is not None:
                        original = self.db.execute('SELECT * FROM messages WHERE id=?', (text(reply_to),)).fetchone()
                        if original is None or original['context_id'] != ctx or original['recipient'] != actor or original['sender'] != recipients[0]:
                            raise Refused('denied')
                    intent = json.dumps({'op': operation, 'to': args.get('to'), 'body': body, 'reply_to': reply_to,
                                         'route_revision': args.get('expected_route_revision')}, sort_keys=True)
                    prior = self.db.execute('SELECT id,intent FROM roots WHERE context_id=? AND sender=? AND key=?', (ctx, actor, key)).fetchone()
                    if prior:
                        prior_intent = json.loads(prior['intent'])
                        prior_intent.setdefault('route_revision', None)
                        if prior_intent != json.loads(intent):
                            raise Refused('key_conflict')
                        return self._sent(prior['id'])
                    self.registry.require(actor)
                    targets = {recipient: self.registry.require(recipient, args.get('expected_route_revision')) for recipient in recipients}
                    if self.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0] + len(recipients) > 10000:
                        raise Refused('capacity')
                    for recipient in recipients:
                        backlog = self.db.execute('SELECT COUNT(*) FROM messages WHERE recipient=? AND acked=0 AND expires>?',
                                                  (recipient, self.clock())).fetchone()[0]
                        if backlog >= 100:
                            raise Refused('capacity')
                    root = str(uuid.uuid4())
                    self.db.execute('INSERT INTO roots VALUES (?,?,?,?,?)', (root, ctx, actor, key, intent))
                    for n, recipient in enumerate(recipients):
                        mid = str(uuid.uuid4())
                        self.db.execute('INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,0,?)',
                            (mid, root, ctx, actor, recipient, body, reply_to,
                             min(self.clock() + 300, members[actor]['expires'], members[recipient]['expires']), n))
                        self.db.execute('INSERT INTO message_targets VALUES (?,?,?,NULL)',
                                        (mid, targets[recipient]['id'], targets[recipient]['route_revision']))
                    return self._sent(root)
                if operation == 'inbox':
                    attachment = self.registry.row(actor)
                    # An authenticated consuming hook proves this supervised receiver works.
                    if (attachment and attachment['evidence'] == 'supervised_hook'
                            and self.registry.state(attachment) == 'not_ready'):
                        self.db.execute('UPDATE attachments SET ready=1 WHERE session_id=?', (actor,))
                    self.registry.require(actor)
                    # Reading is not consumption. A lost RPC response cannot lose mail.
                    rows = self.db.execute('''SELECT m.* FROM messages m JOIN members source
                        ON source.context_id=m.context_id AND source.session_id=m.sender
                        JOIN sessions s ON s.id=m.sender
                        WHERE m.context_id=? AND m.recipient=? AND m.acked=0 AND m.expires>?
                        AND source.active=1 AND s.active=1 AND s.expires>? ORDER BY m.rowid LIMIT 100''',
                        (ctx, actor, self.clock(), self.clock()))
                    delivered = []
                    for r in rows:
                        reason = self.registry.message_reason(r)
                        if reason:
                            if reason != 'paused':
                                self.db.execute('UPDATE message_targets SET reason=? WHERE message_id=?', (reason, r['id']))
                            continue
                        delivered.append({**dict(r), 'sender_identity': self.registry.card(members[r['sender']]),
                                          'reply_address': r['sender']})
                        if len(delivered) == 5:
                            break
                    return {'messages': delivered, 'trust': 'untrusted_peer_data'}
                ids = args['message_ids']
                if not isinstance(ids, list) or not 1 <= len(ids) <= 100:
                    raise Refused('invalid_request')
                for mid in ids:
                    row = self.db.execute('SELECT recipient,context_id FROM messages WHERE id=?', (text(mid),)).fetchone()
                    if row is None or row['recipient'] != actor or row['context_id'] != ctx:
                        raise Refused('denied')
                self.db.executemany('UPDATE messages SET acked=1 WHERE id=?', [(mid,) for mid in ids])
                return {'acked': len(set(ids))}

    def _sent(self, root):
        rows = self.db.execute('''SELECT m.id,m.recipient,t.attachment_id,t.route_revision
            FROM messages m LEFT JOIN message_targets t ON t.message_id=m.id
            WHERE m.root_id=? ORDER BY m.ordinal''', (root,)).fetchall()
        return {'root_id': root, 'message_ids': [r['id'] for r in rows],
                'targets': [dict(r) for r in rows]}


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(2)
        try:
            raw = self.rfile.readline(65537)
            if len(raw) > 65536 or not raw.endswith(b'\n'):
                raise Refused('invalid_request')
            value = json.loads(raw)
            if not isinstance(value, dict) or set(value) != {'credential', 'operation', 'args'}:
                raise Refused('invalid_request')
            result = {'result': self.server.broker.call(value['credential'], value['operation'], value['args'])}
        except Refused as error:
            result = {'error': str(error)}
        except (ValueError, TypeError, UnicodeError):
            result = {'error': 'invalid_request'}
        except (OSError, sqlite3.Error):
            result = {'error': 'unavailable'}
        try:
            self.wfile.write(json.dumps(result, ensure_ascii=True).encode() + b'\n')
        except OSError:
            pass


class BrokerServer(socketserver.UnixStreamServer):
    """Bounded sequential peer endpoint; deliberately no administrative RPC."""
    def __init__(self, path, broker):
        self.path, self.broker = Path(path), broker
        # Refuse existing paths; never unlink an arbitrary socket or symlink.
        super().__init__(str(self.path), _Handler)
        os.chmod(self.path, 0o600)
        self.timeout = 2

    def server_close(self):
        super().server_close()
        # Lifecycle cleanup belongs to the trusted host after the server stops.


def request(path, credential, operation, **args):
    raw = json.dumps({'credential': credential, 'operation': operation, 'args': args}).encode() + b'\n'
    if len(raw) > 65536:
        raise Refused('invalid_request')
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(5)
        conn.connect(str(path))
        conn.sendall(raw)
        with conn.makefile('rb') as stream:
            response = stream.readline(262145)
    if len(response) > 262144 or not response.endswith(b'\n'):
        raise Refused('invalid_response')
    result = json.loads(response)
    if 'error' in result:
        raise Refused(result['error'])
    return result['result']
