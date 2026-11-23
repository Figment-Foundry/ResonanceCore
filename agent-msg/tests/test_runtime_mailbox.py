"""Private runtime endpoints must not impersonate interactive hook sessions."""
import importlib

import pytest


def test_expired_request_rejects_late_reply_and_closed_api_is_explicit(tmp_path):
    from runtime_mailbox import RuntimeMailbox
    now = [100.0]
    box = RuntimeMailbox(tmp_path / 'private' / 'mail.db', 'run', monotonic=lambda: now[0])
    a = box.register('controller', 1, 'control')
    b = box.register('builder', 1, 'work')
    request = box.send(a, b, {}, key='request', kind='request', ttl=1)
    now[0] += 2
    assert box.receipt(request)['state'] == 'expired'
    with pytest.raises(ValueError, match='message_inactive'):
        box.ack(b, [request])
    with pytest.raises(ValueError, match='reply_mismatch'):
        box.reply(b, request, {}, key='late')
    box.close()
    with pytest.raises(ValueError, match='runtime_closed'):
        box.register('new', 2, 'new')


def test_fence_idempotency_ack_and_payload_boundaries(tmp_path):
    from runtime_mailbox import RuntimeMailbox
    allowed = [True]
    def authority():
        if not allowed[0]:
            raise ValueError('run_fenced')
    with RuntimeMailbox(tmp_path / 'private' / 'mail.db', 'run', authority=authority) as box:
        a = box.register('controller', 1, 'control')
        b = box.register('builder', 1, 'work')
        c = box.register('reviewer', 1, 'review')
        mid = box.send(a, b, {'digest': 'one'}, key='request', kind='request')
        assert box.send(a, b, {'digest': 'one'}, key='request', kind='request') == mid
        with pytest.raises(ValueError, match='key_conflict'):
            box.send(a, b, {'digest': 'two'}, key='request', kind='request')
        with pytest.raises(ValueError, match='reply_mismatch'):
            box.reply(c, mid, {}, key='wrong')
        with pytest.raises(ValueError, match='ack_owner_mismatch'):
            box.ack(b, [mid, 99999])
        assert box.receipt(mid)['state'] == 'queued'
        with pytest.raises(ValueError, match='payload_limit'):
            box.send(a, b, {'text': 'x' * 4096}, key='oversize')
        box.ack(b, [mid])
        assert box.receipt(mid)['state'] == 'consumed'
        allowed[0] = False
        for operation in (lambda: box.read(b), lambda: box.send(a, b, {}, key='fenced'),
                          lambda: box.renew(b), lambda: box.ack(b, [mid])):
            with pytest.raises(ValueError, match='run_fenced'):
                operation()


def test_private_store_exclusive_owner_run_identity_and_lease(tmp_path):
    from runtime_mailbox import RuntimeMailbox
    path = tmp_path / 'private' / 'mail.db'
    now = [100.0]
    with RuntimeMailbox(path, 'run', monotonic=lambda: now[0]) as box:
        with pytest.raises(ValueError, match='runtime_already_attached'):
            RuntimeMailbox(path, 'run')
        b = box.register('builder', 1, 'work', ttl=1)
        now[0] += 2
        with pytest.raises(ValueError, match='endpoint_inactive'):
            box.renew(b)
    with pytest.raises(ValueError, match='runtime_identity_or_version_mismatch'):
        RuntimeMailbox(path, 'other-run')
    path.chmod(0o644)
    with pytest.raises(ValueError, match='private_storage_required'):
        RuntimeMailbox(path, 'run')


def test_runtime_request_reply_is_exact_and_separate_from_peer_directory(tmp_path):
    module = importlib.import_module('runtime_mailbox')
    with module.RuntimeMailbox(tmp_path / 'private' / 'mail.db', 'run-one') as box:
        controller = box.register('controller', 1, 'control')
        role = box.register('builder', 1, 'invocation-one')
        request = box.send(controller, role, {'artifact_digest': 'a' * 64}, key='work', kind='request')
        incoming = box.read(role)
        assert incoming[0]['id'] == request
        assert incoming[0]['recipient_invocation'] == 'invocation-one'
        reply = box.reply(role, request, {'artifact_digest': 'b' * 64}, key='result')
        assert box.read(controller)[0]['id'] == reply
        box.ack(role, [request])
        assert box.read(role) == []
        assert not box.db.execute("SELECT 1 FROM sqlite_master WHERE name='participants'").fetchone()


def test_retired_runtime_endpoint_and_restart_never_inherit_historical_mail(tmp_path):
    module = importlib.import_module('runtime_mailbox')
    path = tmp_path / 'private' / 'mail.db'
    with module.RuntimeMailbox(path, 'run-one') as box:
        a = box.register('controller', 1, 'control')
        b = box.register('builder', 1, 'first')
        request = box.send(a, b, {'work': 'pointer'}, key='one', kind='request')
        box.retire(b)
        new = box.register('builder', 2, 'second')
        assert box.read(new) == []
        with pytest.raises(ValueError, match='endpoint_inactive'):
            box.reply(new, request, {'result': 'wrong incarnation'}, key='bad')
    with module.RuntimeMailbox(path, 'run-one') as restarted:
        with pytest.raises(ValueError, match='endpoint_inactive'):
            restarted.read(a)
