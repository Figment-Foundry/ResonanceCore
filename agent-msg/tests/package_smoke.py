"""Exercise only the installed wheel and an isolated temporary bus."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

import agent_msg
import agent_broadcast
import resonance_service
import desktop_attachment
import terminal_session
from runtime_mailbox import RuntimeMailbox

assert Path(agent_msg.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()), 'Use an isolated wheel installation, not the source checkout'
cli = str(Path(sys.executable).parent / 'agent-msg')
processes = []
with tempfile.TemporaryDirectory(prefix='resonance-broadcast-smoke-') as directory:
    root = Path(directory)
    assert desktop_attachment.INTERFACE_VERSION == 1
    desktop_cli = str(Path(sys.executable).parent / 'agent-msg-desktop')
    assert subprocess.run([desktop_cli, '--help'], capture_output=True, timeout=5).returncode == 0
    terminal_cli = str(Path(sys.executable).parent / 'agent-msg-terminal')
    assert subprocess.run([terminal_cli, '--help'], capture_output=True, timeout=5).returncode == 0
    assert Path(terminal_session.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    capabilities = json.loads(subprocess.check_output([cli, 'capabilities'], timeout=5))
    assert capabilities['package_version'] == '0.6.0'
    assert capabilities['native_harness_adapters'] == 1
    for name in ('agent-msg-native', 'agent-msg-native-control', 'agent-msg-harness'):
        assert subprocess.run([str(Path(sys.executable).parent/name), '--help'], capture_output=True, timeout=5).returncode == 0
    assert capabilities['project_label_resolution'] == 1
    assert capabilities['terminal_lifecycle_hooks'] == 1
    assert capabilities['desktop_owner_stdio'] == capabilities['runtime_mailbox'] == 1
    with RuntimeMailbox(root / 'private' / 'mail.db', 'wheel-test') as mailbox:
        controller = mailbox.register('controller', 1, 'control')
        role = mailbox.register('builder', 1, 'invocation')
        request = mailbox.send(controller, role, {'digest': 'a' * 64}, key='work', kind='request')
        assert mailbox.read(role)[0]['id'] == request
        reply = mailbox.reply(role, request, {'digest': 'b' * 64}, key='result')
        assert mailbox.read(controller)[0]['id'] == reply
        mailbox.ack(controller, [reply])
        assert mailbox.receipt(reply)['state'] == 'consumed'
    if go_bin := os.environ.get('RESONANCE_GO_BIN'):
        # Exercise the packaged CLI and its Python request path on a separate bus.
        native_env = dict(os.environ, RESONANCE_SERVICE_BACKEND='go',
                          RESONANCE_GO_BIN=str(Path(go_bin).resolve()),
                          RESONANCE_RUNTIME_DIR=str(root / 'run'))
        service = str(Path(sys.executable).parent / 'agent-msg-service')
        bus = str(root / 'native.db')
        start = subprocess.run([service, '--bus', bus, 'start'], cwd=root, env=native_env,
                               capture_output=True, text=True, timeout=10)
        assert start.returncode == 0, (start.stdout, start.stderr)
        try:
            with patch.dict(os.environ, native_env):
                assert resonance_service.request(bus, {'op': 'ping'}, start=False)['version'] == '0.6.0'
        finally:
            subprocess.run([service, '--bus', bus, 'stop'], cwd=root, env=native_env,
                           check=True, capture_output=True, timeout=10)
    path = root / 'bus.db'
    # Package-level discovery: stale logical alias, live concrete address.
    logical_project = root / 'resolution-project'
    logical_project.mkdir()
    with agent_msg.open_db(str(root / 'resolution.db')) as db:
        old = agent_broadcast.register(db, 'resolution-project', str(logical_project), 'fixture')
        agent_broadcast.renew(db, 'resolution-project', old, ready=True)
        agent_broadcast.close(db, 'resolution-project', old)
        live = agent_broadcast.register(db, 'codex-terminal-wheel', str(logical_project), 'fixture')
        agent_broadcast.renew(db, 'codex-terminal-wheel', live, ready=True)
    resolution_env = {k: v for k, v in os.environ.items()
                      if not k.startswith(('AGENT_MSG_', 'RESONANCE_')) and k != 'PYTHONPATH'}
    resolution_env['AGENT_MSG_DB'] = str(root / 'resolution.db')
    resolved = json.loads(subprocess.check_output(
        [cli, 'resolve', '--label', 'resolution-project', '--project', str(logical_project)],
        env=resolution_env, cwd=directory, timeout=10))
    assert resolved['recipient'] == 'codex-terminal-wheel' and resolved['session'] == live
    env = {**os.environ, 'AGENT_MSG_DB': str(path), 'AGENT_MSG_ID': 'controller'}
    env.pop('PYTHONPATH', None)
    env.pop('AGENT_MSG_SESSION', None)
    child = (
        'import os,time; from pathlib import Path; import agent_msg\n'
        'assert agent_msg.main(["drain","--as",os.environ["AGENT_MSG_ID"]]) == 0\n'
        f'while not Path({str(root / "gate")!r}).exists(): time.sleep(0.02)\n'
        'assert agent_msg.main(["drain","--as",os.environ["AGENT_MSG_ID"]]) == 0\n'
    )
    try:
        for name in ('worker', 'pi'):
            processes.append(subprocess.Popen([cli, 'run', '--as', name, '--harness', name,
                '--project', directory, '--', sys.executable, '-c', child], cwd=directory,
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with sqlite3.connect(path) as db:
                    count = db.execute('SELECT COUNT(*) FROM participants WHERE ready=1').fetchone()[0]
                if count == 2:
                    break
            except sqlite3.Error:
                pass
            time.sleep(0.05)
        else:
            raise AssertionError('installed supervisors did not become ready')
        def command(*args):
            result = subprocess.run([cli, *args], env=env, cwd=directory,
                                    capture_output=True, text=True, timeout=10)
            assert result.returncode == 0, (result.stdout, result.stderr)
            return json.loads(result.stdout)
        diagnostic = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
        diagnostic.row_factory = sqlite3.Row
        try:
            agent_broadcast.broadcast(diagnostic, 'controller', 'PACKAGED_BROADCAST_CANARY', dry_run=True)
        finally:
            diagnostic.close()
        preview = command('broadcast', '--dry-run', 'PACKAGED_BROADCAST_CANARY')
        directory_result = command('peers')
        assert {p['recipient'] for p in directory_result['peers'] if p['sendable']} == {'worker', 'pi'}
        target = command('resolve', '--label', 'pi')
        command('send', '--to', 'pi', '--session', target['session'], 'PACKAGED_DIRECT_CANARY')
        assert preview['recipient_count'] == 2
        sent = command('broadcast', '--key', 'smoke', 'PACKAGED_BROADCAST_CANARY')
        replay = command('broadcast', '--key', 'smoke', 'PACKAGED_BROADCAST_CANARY')
        assert replay['replayed'] and replay['broadcast_id'] == sent['broadcast_id']
        (root / 'gate').touch()
        for name, process in zip(('worker', 'pi'), processes):
            output, errors = process.communicate(timeout=5)
            assert process.returncode == 0, errors
            assert output.count(b'PACKAGED_BROADCAST_CANARY') == 1
            assert output.count(b'PACKAGED_DIRECT_CANARY') == (1 if name == 'pi' else 0)
        status = command('broadcast-status', sent['broadcast_id'])
        assert status['counts'] == {'consumed': 2}
        assert not status['acknowledged_count']
        print(json.dumps({'installed_wheel': True, 'recipients': ['worker', 'pi'],
                          'duplicate_children': 0, 'consumed': 2, 'acknowledged': 0}))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            process.communicate(timeout=5)
