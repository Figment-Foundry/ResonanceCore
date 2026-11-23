"""Opt-in real App Server + Unix WebSocket test against a local deterministic provider.

No signed-in model request: every generation request goes to the local fixture.
This proves framework wakeup, not connection to an existing desktop thread.
"""
import json
from contextlib import ExitStack
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import agent_msg
from .registry_helpers import hook
import codex_bridge as bridge
import terminal_session as terminal
from codex_rpc import CodexRPC
from .service_helpers import running_service


@pytest.mark.skipif(os.environ.get('RESONANCE_CODEX_LIVE') != '1', reason='opt-in real Codex protocol test')
@pytest.mark.parametrize('transport', ['unix', 'queue', 'native_policy'])
def test_real_idle_thread_wakes_from_bus_without_user_turn(tmp_path, transport, monkeypatch):
    executable = shutil.which('codex')
    assert executable, 'Codex CLI required'
    requests, arrived = [], threading.Event()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append({'body': body, 'authorization_present': 'Authorization' in self.headers})
            arrived.set()
            item = {'id': 'msg_1', 'type': 'message', 'role': 'assistant', 'status': 'completed',
                    'content': [{'type': 'output_text', 'text': 'Resonance message received.'}]}
            response = {'id': 'resp_1', 'object': 'response', 'model': body['model'],
                'status': 'completed', 'output': [item],
                'usage': {'input_tokens': 10, 'output_tokens': 5, 'total_tokens': 15}}
            events = [{'type': 'response.created', 'response': {**response, 'status': 'in_progress', 'output': []}},
                {'type': 'response.output_item.done', 'output_index': 0, 'item': item},
                {'type': 'response.completed', 'response': response}]
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            for event in events:
                self.wfile.write(('event: '+event['type']+'\ndata: '+json.dumps(event)+'\n\n').encode())
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=http.serve_forever, daemon=True)
    worker.start()
    socket = tmp_path / 'codex.sock'
    home = tmp_path / 'codex-home'
    home.mkdir()
    args = [executable, 'app-server', '--listen', 'unix://' + str(socket)]
    overrides = {'model_provider': 'fixture', 'model_providers.fixture.name': 'fixture',
        'model_providers.fixture.base_url': f'http://127.0.0.1:{http.server_port}/v1',
        'model_providers.fixture.wire_api': 'responses',
        'model_providers.fixture.requires_openai_auth': False,
        'model_providers.fixture.request_max_retries': 0,
        'model_providers.fixture.stream_max_retries': 0,
        'features.hooks': False, 'features.shell_tool': False, 'features.unified_exec': False,
        'features.apps': False, 'features.plugins': False, 'mcp_servers': {}}
    for key, value in overrides.items():
        args += ['-c', key + '=' + json.dumps(value)]
    client, process, db = None, None, None
    contexts = ExitStack()
    queue_transport = transport in ('queue', 'native_policy')
    try:
        with tempfile.TemporaryFile() as log:
            process = subprocess.Popen(args, stdout=log, stderr=log,
                                       env={**os.environ, 'CODEX_HOME': str(home)})
            deadline = time.monotonic() + 10
            while not socket.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.05)
            assert socket.exists(), 'real App Server did not expose its test socket'
            client = CodexRPC(str(socket), timeout=15)
            client.__enter__()
            created = client.call('thread/start', {'cwd': str(tmp_path), 'ephemeral': not queue_transport,
                'model': 'gpt-5.6-sol', 'modelProvider': 'fixture', 'sandbox': 'read-only',
                'approvalPolicy': 'never'})
            thread_id = created['thread']['id']
            assert not requests, 'thread creation unexpectedly started generation'
            if queue_transport:
                # Codex persists a non-ephemeral thread only after its first turn.
                seeded = client.call('turn/start', {'threadId': thread_id,
                    'input': [{'type': 'text', 'text': 'Initialize the isolated queue fixture.'}]})
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    notification = json.loads(client.connection.recv(timeout=max(0.01, deadline - time.monotonic())))
                    if (notification.get('method') == 'turn/completed'
                            and notification['params']['turn']['id'] == seeded['turn']['id']):
                        break
                else:
                    pytest.fail('queue fixture did not become idle')
                assert requests and not requests[0]['authorization_present']
                requests.clear()
                arrived.clear()
            path = str(tmp_path / 'bus.db')
            db = bridge.database(path)
            recipient = 'live-test'
            if transport == 'native_policy':
                # Controlled native attachment; actual queue and idle model execution.
                monkeypatch.setenv('HOME', str(tmp_path))
                contexts.enter_context(running_service(Path(path)))
                assert terminal.main(['wakeup', '--enable', '--executable', executable,
                                      '--codex-home', str(home), '--cooldown', '0']) == 0
                owner = lambda: (os.getpid(), list(terminal.process_identity(os.getpid())))
                event = dict(hook_event_name='UserPromptSubmit', session_id=thread_id, cwd=str(tmp_path))
                assert terminal.hook(event, path, owner_probe=owner) == 0
                recipient = 'codex-terminal-' + thread_id
                binding = db.execute('SELECT enabled,wakeups FROM codex_bindings WHERE recipient=?',
                                     (recipient,)).fetchone()
                assert binding[:] == (1, 0)
            else:
                bridge.register(db, recipient, thread_id, str(tmp_path), str(socket), cooldown=0,
                                transport=transport, executable=executable if queue_transport else None,
                                codex_home=str(home) if queue_transport else None)
                with db:
                    db.execute('UPDATE codex_bindings SET enabled=1')
                hook(db, tmp_path, recipient, route='codex')
            mid = agent_msg.send(db, 'peer-test', recipient, 'RESONANCE_IDLE_WAKE_CANARY')
            result = bridge.tick(db, path)
            assert result['state'] == 'accepted', result
            assert arrived.wait(15), 'accepted tool output did not trigger generation'
            assert not requests[0]['authorization_present']
            assert 'RESONANCE_IDLE_WAKE_CANARY' in json.dumps(requests[0]['body']['input'])
            assert result['message_id'] == mid
            assert bridge.tick(db, path)['state'] == 'idle'
    finally:
        if db:
            db.close()
        if client:
            client.close()
        contexts.close()
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        http.shutdown()
        http.server_close()
        worker.join(5)
