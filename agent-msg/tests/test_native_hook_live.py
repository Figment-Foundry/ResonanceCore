"""Actual Claude/Kimi prompt hooks with isolated homes and a local model fixture."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('hook_install_live', ROOT.parent/'tools/install_native_harnesses.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


@pytest.mark.skipif(os.environ.get('RESONANCE_NATIVE_LIVE') != '1', reason='opt-in native hook integration')
@pytest.mark.parametrize('harness', ['claude', 'kimi'])
def test_native_prompt_hook_attaches_without_wrapper(tmp_path, harness):
    requests = []
    active = []
    class Model(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append(body)
            bus = home/'.local/state/resonancecore/bus.db'
            if bus.exists():
                with sqlite3.connect(bus) as db:
                    active.extend(db.execute('SELECT harness,ready,closed FROM participants').fetchall())
            assert self.headers.get('x-api-key') == 'fixture-only'
            self.send_response(200)
            if 'count_tokens' in self.path:
                self.end_headers()
                self.wfile.write(b'{"input_tokens":50}')
                return
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            events = [
                ('message_start', {'message': {'id': 'msg_fixture', 'type': 'message', 'role': 'assistant', 'model': body['model'], 'content': [], 'stop_reason': None, 'stop_sequence': None, 'usage': {'input_tokens': 50, 'output_tokens': 0}}}),
                ('content_block_start', {'index': 0, 'content_block': {'type': 'text', 'text': ''}}),
                ('content_block_delta', {'index': 0, 'delta': {'type': 'text_delta', 'text': 'Fixture complete.'}}),
                ('content_block_stop', {'index': 0}),
                ('message_delta', {'delta': {'stop_reason': 'end_turn', 'stop_sequence': None}, 'usage': {'output_tokens': 5}}),
                ('message_stop', {}),
            ]
            for event, data in events:
                self.wfile.write(('event: '+event+'\ndata: '+json.dumps({'type': event, **data})+'\n\n').encode())
    server = ThreadingHTTPServer(('127.0.0.1', 0), Model)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    home, work, runtime = tmp_path/'home', tmp_path/'work', tmp_path/'runtime'
    home.mkdir(); work.mkdir(); (runtime/'bin').mkdir(parents=True)
    for name in ('agent-msg-native', 'agent-msg-native-control', 'agent-msg-claude-channel'):
        (runtime/'bin'/name).touch()
    driver = runtime/'bin/agent-msg-harness'
    driver.write_text(f'#!{sys.executable}\nimport sys\nsys.path.insert(0,{str(ROOT)!r})\nfrom harness_hooks import main\nraise SystemExit(main())\n')
    driver.chmod(0o755)
    installer.install(home, runtime, True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('RESONANCE_', 'AGENT_MSG_', 'ANTHROPIC_', 'CLAUDE_', 'KIMI_', 'OPENAI_'))}
    env.update(HOME=str(home), XDG_CONFIG_HOME=str(home/'.config'), XDG_DATA_HOME=str(home/'.local/share'),
               XDG_STATE_HOME=str(home/'.local/state'), XDG_CACHE_HOME=str(home/'.cache'), PYTHONPATH=str(ROOT),
               NO_PROXY='127.0.0.1,localhost', no_proxy='127.0.0.1,localhost')
    base = f'http://127.0.0.1:{server.server_port}'
    if harness == 'claude':
        env.update(ANTHROPIC_API_KEY='fixture-only', ANTHROPIC_BASE_URL=base,
                   CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC='1')
        command = [shutil.which('claude'), '-p', 'Initialize fixture.', '--model', 'claude-sonnet-4-6',
                   '--strict-mcp-config', '--mcp-config', '{"mcpServers":{}}', '--tools', '']
    else:
        config = home/'.kimi-code/config.toml'
        config.write_text('default_model = "fixture"\n'+config.read_text()+f'\n[providers.fixture]\ntype = "anthropic"\nbase_url = "{base}"\napi_key = "fixture-only"\n[models.fixture]\nprovider = "fixture"\nmodel = "claude-sonnet-4-6"\nmax_context_size = 200000\n')
        env['KIMI_CODE_HOME'] = str(home/'.kimi-code')
        check = subprocess.run([shutil.which('kimi'), 'doctor', 'config', str(config)], env=env, capture_output=True, text=True, timeout=15)
        assert check.returncode == 0, check.stdout + check.stderr
        command = [shutil.which('kimi'), '-p', 'Initialize fixture.', '-m', 'fixture']
    process = subprocess.Popen(command, cwd=work, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, start_new_session=True)
    try:
        output, _ = process.communicate(timeout=45)
        assert process.returncode == 0, output[-4000:]
        assert any('Resonance identity:' in json.dumps(body) for body in requests), output[-4000:]
        assert (harness, 1, 0) in active
        with sqlite3.connect(home/'.local/state/resonancecore/bus.db') as db:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                row = db.execute('SELECT ready,closed FROM participants').fetchone()
                if row == (0, 1):
                    break
                time.sleep(0.1)
            assert row == (0, 1)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)
        server.shutdown()
        server.server_close()
