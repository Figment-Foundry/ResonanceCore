"""Opt-in T3 round trip against an installed bus and preconfigured project.

Set RESONANCE_T3_LIVE=1 and optionally override the project, bus and provider
models with RESONANCE_T3_LIVE_{PROJECT,BUS,CLAUDE_MODEL,CODEX_MODEL}.
T3 CLI v0.1.11 cannot archive threads. The test closes its bus attachment and
prints the thread ID for manual archival.
"""
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

import pytest

import agent_msg
import agent_broadcast as membership
import t3_delivery

pytestmark = pytest.mark.skipif(os.getenv('RESONANCE_T3_LIVE') != '1',
                                reason='requires a live T3 server and installed hooks')


@pytest.mark.parametrize(('harness', 'provider', 'default_model'), [
    ('claude', 'claudeAgent', 'claude-haiku-5-5'),
    ('codex', 'codex', 'gpt-6.2-sol'),
])
def test_idle_t3_peer_mail(harness, provider, default_model):
    project = Path(os.getenv('RESONANCE_T3_LIVE_PROJECT', Path(__file__).resolve().parents[2]))
    bus = os.getenv('RESONANCE_T3_LIVE_BUS', str(Path.home() / '.local/state/resonancecore/bus.db'))
    model = os.getenv(f'RESONANCE_T3_LIVE_{harness.upper()}_MODEL', default_model)
    executable = t3_delivery.executable()
    token = 'resonance-live-' + uuid.uuid4().hex

    def cli(*args):
        result = subprocess.run([executable, 'thread', *args], cwd=project, capture_output=True,
                                text=True, check=True, timeout=20)
        return json.loads(result.stdout)

    identity_check = (
        'Run agent-msg-harness --harness claude --thread <your native session ID> exec -- whoami. '
        'Include its JSON in your READY response. ' if harness == 'claude' else '')
    started = cli('start', '--json', '--provider', provider, '--model', model, '--title',
                  f'Resonance T3 {harness} test (throwaway)', str(project),
                  identity_check + 'Reply READY, then end this turn. On a later Resonance doorbell, '
                  'echo only the resonance-live token from the hook context without tools or peer replies.')
    thread = next((started.get(key) for key in ('threadId', 'id')
                   if started.get(key)), None)
    if thread is None and isinstance(started.get('thread'), dict):
        thread = started['thread'].get('threadId')
    assert t3_delivery.thread_from_environment({'T3CODE_THREAD_ID': thread}), started
    print(f'T3 test thread for manual archival: {thread}', flush=True)

    def wait_for(check):
        deadline = time.monotonic() + 100
        while time.monotonic() < deadline:
            value = check()
            if value:
                return value
            time.sleep(2)
        pytest.fail(f'{harness} T3 test timed out in {check.__name__}')

    def attachment(db):
        return db.execute('''SELECT r.* FROM t3_receivers r JOIN participants p
            ON p.recipient=r.recipient AND p.session=r.session
            WHERE r.t3_thread=? AND p.harness=? AND p.closed=0''', (thread, harness)).fetchone()

    def idle_attachment():
        info = cli('show', '--json', '--messages', '8', thread)
        if info['thread']['activeRunId'] is None and any(
                entry['role'] == 'assistant' and 'READY' in entry['text'] for entry in info['messages']):
            if harness == 'claude':
                assert any('t3_thread' in entry['text'] for entry in info['messages']
                           if entry['role'] == 'assistant'), 'Claude whoami did not confirm T3 delivery'
            return attachment(db)

    def delivered():
        info = cli('show', '--json', '--messages', '12', thread)
        messages = info['messages']
        assert all(token not in entry['text'] for entry in messages if entry['role'] == 'user')
        return (any('[Resonance] Peer mail is waiting' in entry['text'] for entry in messages
                    if entry['role'] == 'user')
                and any(token in entry['text'] for entry in messages if entry['role'] == 'assistant'))

    with agent_msg.open_db(bus) as db:
        try:
            row = wait_for(idle_attachment)
            agent_msg.send(db, 'resonance-live-fixture', row['recipient'], token,
                           expected_session=row['session'])
            assert wait_for(delivered)
            assert attachment(db)['wakeups'] == row['wakeups'] + 1
        finally:
            row = attachment(db)
            if row is not None:
                t3_delivery.control(db, row['recipient'], row['session'], 'pause')
                membership.close(db, row['recipient'], row['session'])
