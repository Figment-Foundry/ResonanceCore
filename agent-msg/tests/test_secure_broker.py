"""Protected-broker contracts. These do not claim live harness integration."""
import json
import socket
import threading

import pytest
from .registry_helpers import attach

from secure_broker import Broker, BrokerServer, Refused, request


@pytest.fixture
def setup(tmp_path):
    clock = [1000.0]
    broker = Broker(tmp_path / 'control' / 'state.db', clock=lambda: clock[0])
    peers = [broker.enroll(name, harness, '/fixture/project')
             for name, harness in [('build', 'claude'), ('review', 'codex'), ('test', 'pi')]]
    for peer in peers:
        attach(broker, peer)
    yield broker, peers, clock
    broker.close()


def call(broker, peer, op, **args):
    return broker.call(peer['credential'], op, args)


def pair(broker, a, b):
    invite = call(broker, b, 'invite')
    proposal = call(broker, a, 'pair', code=invite['code'])
    return broker.approve(proposal['proposal_id'])['context_id']


def test_pair_requires_owner_then_messages_work_both_directions(setup):
    broker, (a, b, c), _ = setup
    invitation = call(broker, b, 'invite')
    proposed = call(broker, a, 'pair', code=invitation['code'])
    with pytest.raises(Refused):
        call(broker, a, 'approve', proposal_id=proposed['proposal_id'])
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=proposed['proposal_id'], to=b['id'], body='early', key='early')
    ctx = broker.approve(proposed['proposal_id'])['context_id']
    sent = call(broker, a, 'send', context_id=ctx, to=b['id'], body='review this', key='one')
    inbox = call(broker, b, 'inbox', context_id=ctx)['messages']
    assert len(inbox) == 1 and inbox[0]['sender'] == a['id']
    assert inbox[0]['id'] == sent['message_ids'][0]
    assert inbox[0]['body'] == 'review this'
    with pytest.raises(Refused):
        call(broker, c, 'inbox', context_id=ctx)
    reply = call(broker, b, 'send', context_id=ctx, to=a['id'], body='done', key='reply', reply_to=inbox[0]['id'])
    assert call(broker, a, 'inbox', context_id=ctx)['messages'][0]['id'] == reply['message_ids'][0]


def test_code_is_single_use_and_expired_code_never_approves(setup):
    broker, (a, b, c), clock = setup
    code = call(broker, b, 'invite')['code']
    proposal = call(broker, a, 'pair', code=code)
    assert call(broker, a, 'pair', code=code) == proposal
    with pytest.raises(Refused):
        call(broker, c, 'pair', code=code)
    clock[0] += 301
    with pytest.raises(Refused):
        broker.approve(proposal['proposal_id'])


def test_owner_approval_replay_never_reactivates_revoked_context(setup):
    broker, (a, b, _), _clock = setup
    proposal = call(broker, a, 'pair', code=call(broker, b, 'invite')['code'])
    approved = broker.approve(proposal['proposal_id'])
    broker.revoke(approved['context_id'])
    assert broker.approve(proposal['proposal_id']) == approved
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=approved['context_id'], to=b['id'], body='denied', key='x')


def test_denial_is_terminal_and_proposal_shows_exact_owner_metadata(setup):
    broker, (a, b, _), _clock = setup
    proposed = call(broker, a, 'pair', code=call(broker, b, 'invite')['code'])
    shown = broker.proposal(proposed['proposal_id'])
    assert [p['id'] for p in shown['peers']] == [a['id'], b['id']]
    assert shown['wakeup'] is False
    assert 'credential' not in json.dumps(shown)
    broker.deny(proposed['proposal_id'])
    with pytest.raises(Refused):
        broker.approve(proposed['proposal_id'])


def test_short_lived_mesh_member_does_not_expire_remaining_relationships(setup):
    broker, (a, b, _), clock = setup
    short = broker.enroll('short', 'codex', '/fixture/project', ttl=10)
    ctx = broker.mesh([a['id'], b['id'], short['id']])['context_id']
    clock[0] += 11
    call(broker, a, 'send', context_id=ctx, to=b['id'], body='still authorized', key='ok')
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=ctx, to=short['id'], body='expired', key='bad')


def test_broker_restart_preserves_receipt_but_fences_old_attachment(setup):
    broker, (a, b, _), clock = setup
    ctx = pair(broker, a, b)
    sent = call(broker, a, 'send', context_id=ctx, to=b['id'], body='durable', key='durable')
    broker.close()
    reopened = Broker(broker.path, clock=lambda: clock[0])
    try:
        with pytest.raises(Refused, match='recipient_stale'):
            call(reopened, b, 'inbox', context_id=ctx)
        assert call(reopened, a, 'send', context_id=ctx, to=b['id'], body='durable', key='durable') == sent
    finally:
        reopened.close()


def test_second_broker_cannot_race_owner_policy_on_same_storage(setup):
    broker, _peers, _clock = setup
    second = None
    try:
        with pytest.raises(Refused, match='broker_already_running'):
            second = Broker(broker.path)
    finally:
        if second is not None:
            second.close()


def test_mesh_every_member_can_send_without_individual_pairing(setup):
    broker, peers, _ = setup
    ctx = broker.mesh([p['id'] for p in peers])['context_id']
    for sender in peers:
        for target in peers:
            if sender != target:
                call(broker, sender, 'send', context_id=ctx, to=target['id'], body='hello', key=target['id'])
    for peer in peers:
        assert len(call(broker, peer, 'inbox', context_id=ctx)['messages']) == 2


def test_pair_relationships_do_not_become_transitive(setup):
    broker, (a, b, c), _ = setup
    ab, bc = pair(broker, a, b), pair(broker, b, c)
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=ab, to=c['id'], body='denied', key='x')
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=bc, to=c['id'], body='denied', key='x')


def test_removal_blocks_queued_mail_and_quarantine_blocks_other_contexts(setup):
    broker, (a, b, c), _ = setup
    group = broker.mesh([a['id'], b['id'], c['id']])['context_id']
    ab = pair(broker, a, b)
    call(broker, a, 'send', context_id=group, to=b['id'], body='old', key='old')
    broker.remove(group, b['id'])
    with pytest.raises(Refused):
        call(broker, b, 'inbox', context_id=group)
    call(broker, a, 'send', context_id=ab, to=b['id'], body='independent pair', key='ok')
    broker.quarantine(b['id'])
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=ab, to=b['id'], body='denied', key='denied')
    with pytest.raises(Refused):
        call(broker, b, 'inbox', context_id=ab)


def test_broadcast_snapshot_idempotency_and_ack_ownership(setup):
    broker, (a, b, c), _ = setup
    ctx = broker.mesh([a['id'], b['id'], c['id']])['context_id']
    sent = call(broker, a, 'announce', context_id=ctx, body='update', key='root')
    assert len(sent['message_ids']) == 2
    assert call(broker, a, 'announce', context_id=ctx, body='update', key='root') == sent
    with pytest.raises(Refused):
        call(broker, a, 'announce', context_id=ctx, body='changed', key='root')
    rows = call(broker, b, 'inbox', context_id=ctx)['messages']
    with pytest.raises(Refused):
        call(broker, c, 'ack', context_id=ctx, message_ids=[rows[0]['id']])
    assert len(call(broker, b, 'inbox', context_id=ctx)['messages']) == 1
    call(broker, b, 'ack', context_id=ctx, message_ids=[rows[0]['id']])
    assert call(broker, b, 'inbox', context_id=ctx)['messages'] == []


def test_sender_cannot_be_selected_and_credentials_are_per_session(setup):
    broker, (a, b, c), _ = setup
    ctx = broker.mesh([a['id'], b['id']])['context_id']
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=ctx, to=b['id'], body='x', key='x', sender=b['id'])
    with pytest.raises(Refused):
        broker.call('wrong', 'inbox', {'context_id': ctx})
    with pytest.raises(Refused):
        call(broker, c, 'send', context_id=ctx, to=b['id'], body='x', key='x')
    assert a['credential'] != b['credential']


def test_code_guessing_and_body_limits(setup):
    broker, (a, b, _), _ = setup
    for _ in range(5):
        with pytest.raises(Refused):
            call(broker, a, 'pair', code='NOT-A-CODE')
    code = call(broker, b, 'invite')['code']
    with pytest.raises(Refused, match='rate_limited'):
        call(broker, a, 'pair', code=code)
    ctx = broker.mesh([a['id'], b['id']])['context_id']
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=ctx, to=b['id'], body='x' * 8193, key='oversize')


def test_expiry_and_no_implicit_wakeup_api(setup):
    broker, (a, b, _), clock = setup
    ctx = pair(broker, a, b)
    with pytest.raises(Refused):
        call(broker, a, 'wake', context_id=ctx, to=b['id'])
    clock[0] += 3601
    with pytest.raises(Refused):
        call(broker, a, 'send', context_id=ctx, to=b['id'], body='expired', key='x')


def test_unix_transport_preserves_authorization_and_rejects_admin_api(setup, tmp_path):
    broker, (a, b, c), _ = setup
    ctx = pair(broker, a, b)
    with BrokerServer(tmp_path / 'peer.sock', broker) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = request(server.path, a['credential'], 'send', context_id=ctx, to=b['id'], body='wire', key='wire')
            assert request(server.path, b['credential'], 'inbox', context_id=ctx)['messages'][0]['id'] == result['message_ids'][0]
            with pytest.raises(Refused):
                request(server.path, c['credential'], 'inbox', context_id=ctx)
            with pytest.raises(Refused):
                request(server.path, a['credential'], 'enroll', label='owner')
            with socket.socket(socket.AF_UNIX) as raw:
                raw.connect(str(server.path)); raw.sendall(b'not json\n')
                assert json.loads(raw.recv(1024)) == {'error': 'invalid_request'}
        finally:
            server.shutdown(); thread.join(timeout=5)
