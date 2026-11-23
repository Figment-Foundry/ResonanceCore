"""Actual native plugin idle delivery; isolated homes and a deterministic local model."""
import json
import importlib.util
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, build_opener, ProxyHandler

import pytest
import agent_msg

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('native_install_live', ROOT.parent/'tools/install_native_harnesses.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


def wait_for(check, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError('native fixture condition timed out')


@pytest.mark.skipif(os.environ.get('RESONANCE_NATIVE_LIVE') != '1', reason='opt-in real native plugin test')
@pytest.mark.parametrize('harness', ['pi', 'opencode'])
def test_native_plugin_wakes_its_existing_idle_session(tmp_path, harness):
    executable = shutil.which(harness)
    assert executable
    requests = []
    class Model(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            assert self.headers.get('Authorization') == 'Bearer fixture-only'
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append(body)
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            for delta, reason in [({'role': 'assistant', 'content': 'Fixture received.'}, None), ({}, 'stop')]:
                value = {'id': 'fixture', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'fixture',
                         'choices': [{'index': 0, 'delta': delta, 'finish_reason': reason}]}
                self.wfile.write(('data: ' + json.dumps(value) + '\n\n').encode())
            self.wfile.write(b'data: [DONE]\n\n')
    http = ThreadingHTTPServer(('127.0.0.1', 0), Model)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    home, work, bin_dir = tmp_path/'home', tmp_path/'work', tmp_path/'bin'
    for p in (home, work, bin_dir):
        p.mkdir()
    driver = bin_dir / 'agent-msg-native'
    driver.write_text(f'#!{sys.executable}\nimport sys\nsys.path.insert(0, {str(ROOT)!r})\nfrom native_delivery import main\nraise SystemExit(main())\n')
    driver.chmod(0o755)
    for name in ('agent-msg-native-control', 'agent-msg-harness', 'agent-msg-claude-channel'):
        (bin_dir/name).touch()
    installer.install(home, tmp_path, True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('RESONANCE_', 'AGENT_MSG_', 'ANTHROPIC_', 'OPENAI_', 'CLAUDE_', 'OPENCODE_'))}
    env.update(HOME=str(home), PATH=str(bin_dir)+os.pathsep+os.environ['PATH'],
               XDG_CONFIG_HOME=str(home/'.config'), XDG_DATA_HOME=str(home/'.local/share'),
               XDG_STATE_HOME=str(home/'.local/state'), XDG_CACHE_HOME=str(home/'.cache'))
    base = f'http://127.0.0.1:{http.server_port}/v1'
    if harness == 'pi':
        config = home/'.pi/agent'
        config.mkdir(parents=True, exist_ok=True)
        env['PI_CODING_AGENT_DIR'] = str(config)
        (config/'models.json').write_text(json.dumps({'providers': {'fixture': {
            'baseUrl': base, 'api': 'openai-completions', 'apiKey': 'fixture-only',
            'models': [{'id': 'fixture', 'reasoning': False, 'contextWindow': 128000, 'maxTokens': 1024}]}}}))
        command = [executable, '--mode', 'rpc', '--no-session', '--no-skills',
                   '--no-prompt-templates', '--no-themes', '--no-builtin-tools',
                   '--provider', 'fixture', '--model', 'fixture']
    else:
        # Seed isolated dependencies; the host may update its own fixture copy.
        modules = Path(executable).resolve().parents[1]/'node_modules'
        assert modules.is_dir()
        global_config = home/'.config/opencode'
        global_config.mkdir(parents=True, exist_ok=True)
        shutil.copytree(modules, global_config/'node_modules', symlinks=False)
        shutil.copy2(modules.parent/'package.json', global_config/'package.json')
        if (modules.parent/'bun.lock').exists():
            shutil.copy2(modules.parent/'bun.lock', global_config/'bun.lock')
        config = home/'opencode.json'
        config.write_text(json.dumps({'model': 'fixture/fixture', 'small_model': 'fixture/fixture',
            'share': 'disabled', 'autoupdate': False, 'enabled_providers': ['fixture'],
            'permission': {'*': 'deny'},
            'provider': {'fixture': {'npm': '@ai-sdk/openai-compatible', 'name': 'fixture',
                'options': {'baseURL': base, 'apiKey': 'fixture-only'},
                'models': {'fixture': {'name': 'fixture', 'limit': {'context': 128000, 'output': 1024}}}}}}))
        env.update(OPENCODE_CONFIG=str(config), OPENCODE_DISABLE_AUTOUPDATE='true',
                   OPENCODE_DISABLE_DEFAULT_PLUGINS='true', OPENCODE_DISABLE_MODELS_FETCH='true')
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        command = [executable, 'serve', '--hostname', '127.0.0.1', '--port', str(port)]
    bus = home/'.local/state/resonancecore/bus.db'
    process = None
    try:
        with (tmp_path/'host.log').open('w+') as log:
            process = subprocess.Popen(command, cwd=work, env=env, stdin=subprocess.PIPE,
                                       stdout=log, stderr=log, start_new_session=True)
            if harness == 'opencode':
                def api(path, body=None, method=None):
                    request = Request(f'http://127.0.0.1:{port}'+path,
                        data=json.dumps(body).encode() if body is not None else None,
                        headers={'Content-Type': 'application/json'}, method=method)
                    with build_opener(ProxyHandler({})).open(request, timeout=5 if body is None else 45) as response:
                        raw = response.read()
                        return json.loads(raw) if raw else None
                errors = []
                def running():
                    try:
                        return api('/global/health')
                    except OSError as error:
                        if str(error) not in errors:
                            errors.append(str(error))
                            print('Native HTTP readiness:', error, flush=True)
                        return None
                wait_for(running)
                session = api('/session', {})
                api('/session/'+session['id']+'/message', {'parts': [{'type': 'text', 'text': 'Initialize fixture.'}]})
            def enrolled():
                if not bus.exists():
                    return None
                with sqlite3.connect(bus) as db:
                    db.row_factory = sqlite3.Row
                    try:
                        return db.execute('SELECT * FROM participants WHERE harness=? AND ready=1 AND closed=0', (harness,)).fetchone()
                    except sqlite3.Error:
                        return None
            peer = wait_for(enrolled)
            with agent_msg.open_db(str(bus)) as db:
                mid = agent_msg.send(db, 'fixture-controller', peer['recipient'], 'NATIVE_IDLE_CANARY', expected_session=peer['session'])
                wait_for(lambda: any('NATIVE_IDLE_CANARY' in json.dumps(r.get('messages')) for r in requests))
                wait_for(lambda: db.execute("SELECT 1 FROM native_deliveries WHERE message_id=? AND state='accepted'", (mid,)).fetchone())
                assert db.execute('SELECT COUNT(*) FROM native_deliveries WHERE message_id=?', (mid,)).fetchone()[0] == 1
                if harness == 'opencode':
                    api('/session/'+session['id'], {'time': {'archived': int(time.time()*1000)}}, method='PATCH')
                    wait_for(lambda: db.execute('SELECT closed FROM participants WHERE recipient=?', (peer['recipient'],)).fetchone()[0])
            assert process.poll() is None, 'native session exited instead of remaining attached'
    except BaseException:
        if (tmp_path/'host.log').exists():
            print((tmp_path/'host.log').read_text(errors='replace')[-5000:])
        raise
    finally:
        if process:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate(timeout=5)
        http.shutdown()
        http.server_close()
