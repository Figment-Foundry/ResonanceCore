"""Opt-in real Linux isolation probe, using synthetic peers (not live harnesses)."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading

import pytest

from secure_broker import Broker, BrokerServer
from .registry_helpers import attach


@pytest.mark.skipif(os.environ.get('RESONANCE_SANDBOX_TESTS') != '1',
                    reason='opt-in Linux namespace isolation test')
def test_sandboxed_peer_can_message_but_cannot_access_control_or_other_credentials(tmp_path):
    assert shutil.which('bwrap'), 'Bubblewrap required for this explicit isolation test'
    control = tmp_path / 'owner-only'
    control.mkdir(mode=0o700)
    broker = Broker(control / 'state.db')
    a = broker.enroll('fixture-a', 'fixture', '/fixture/a')
    b = broker.enroll('fixture-b', 'fixture', '/fixture/b')
    attach(broker, a)
    attach(broker, b)
    context = broker.mesh([a['id'], b['id']])['context_id']
    own, other = control / 'a.credential', control / 'b.credential'
    own.write_text(a['credential']); other.write_text(b['credential'])
    own.chmod(0o600); other.chmod(0o600)
    client = tmp_path / 'client.py'
    # Only this synthetic client's credential and the peer socket are mounted.
    client.write_text('''import json
from pathlib import Path
from secure_broker import request, Refused
for forbidden in FORBIDDEN:
    assert not Path(forbidden).exists(), 'host control state exposed'
credential = Path('/run/credential').read_text()
try:
    request('/run/peer.sock', credential, 'approve', proposal_id='fake')
except Refused:
    pass
else:
    raise AssertionError('peer could administer broker')
sent = request('/run/peer.sock', credential, 'send', context_id=CONTEXT,
               to=TARGET, body='isolated fixture', key='sandbox')
print(json.dumps({'sent': len(sent['message_ids']), 'control_hidden': True,
                  'admin_denied': True}))
'''.replace('FORBIDDEN', repr([str(control), str(other), str(broker.path)]))
       .replace('CONTEXT', repr(context)).replace('TARGET', repr(b['id'])))
    path = tmp_path / 'peer.sock'
    with BrokerServer(path, broker) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            command = ['bwrap', '--unshare-all', '--die-with-parent', '--new-session',
                       '--ro-bind', '/usr', '/usr', '--symlink', 'usr/bin', '/bin',
                       '--symlink', 'usr/lib', '/lib', '--symlink', 'usr/lib64', '/lib64',
                       '--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp',
                       '--dir', '/run', '--dir', '/app',
                       '--ro-bind', str(Path(__file__).parents[1] / 'secure_broker.py'), '/app/secure_broker.py',
                       '--ro-bind', str(Path(__file__).parents[1] / 'thread_registry.py'), '/app/thread_registry.py',
                       '--ro-bind', str(client), '/app/client.py',
                       '--ro-bind', str(own), '/run/credential',
                       '--ro-bind', str(path), '/run/peer.sock',
                       '--clearenv', '--setenv', 'PYTHONDONTWRITEBYTECODE', '1',
                       '--chdir', '/app', '/usr/bin/python3', '/app/client.py']
            result = subprocess.run(command, capture_output=True, text=True, timeout=20)
            assert result.returncode == 0, result.stderr
            assert json.loads(result.stdout) == {'sent': 1, 'control_hidden': True, 'admin_denied': True}
            received = broker.call(b['credential'], 'inbox', {'context_id': context})['messages']
            assert len(received) == 1 and received[0]['sender'] == a['id']
        finally:
            server.shutdown(); thread.join(timeout=5); broker.close()
