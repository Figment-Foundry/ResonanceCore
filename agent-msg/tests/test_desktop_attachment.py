"""Native owner-subscription lifecycle, not process or loaded-history inference."""
import importlib
import uuid
import pytest

import codex_bridge


def test_stdio_preserves_large_frames_and_child_failure(tmp_path):
    import json
    from pathlib import Path
    import subprocess
    import sys
    import desktop_attachment
    command = [sys.executable, str(Path(desktop_attachment.__file__)), '--db', str(tmp_path / 'bus.db'),
               '--', sys.executable, '-c',
               'import json,sys; print(json.dumps({"result":"x"*200000}),flush=True); sys.exit(7)']
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        frame = process.stdout.readline()
        assert json.loads(frame) == {'result': 'x' * 200000}
        assert process.wait(timeout=5) == 7
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


def test_two_threads_one_owner_detach_only_retires_exact_thread(tmp_path):
    module = importlib.import_module('desktop_attachment')
    db = codex_bridge.database(str(tmp_path / 'bus.db'))
    ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    for n, tid in enumerate(ids):
        codex_bridge.register(db, f'peer-{n}', tid, str(tmp_path), '/unused.sock')
    host = module.DesktopAttachment(db)
    try:
        for n, tid in enumerate(ids):
            host.client({'id': n, 'method': 'thread/resume', 'params': {'threadId': tid}})
            host.server({'id': n, 'result': {'thread': {'id': tid, 'cwd': str(tmp_path)}}})
        host.heartbeat()
        assert db.execute('SELECT COUNT(*) FROM participants WHERE ready=1 AND closed=0').fetchone()[0] == 2
        # Loaded status can remain idle after unsubscribe; owner detach wins immediately.
        host.client({'id': 3, 'method': 'thread/unsubscribe', 'params': {'threadId': ids[0]}})
        host.server({'method': 'thread/status/changed', 'params': {'threadId': ids[0], 'status': {'type': 'idle'}}})
        host.heartbeat()
        states = {r['recipient']: r['closed'] for r in db.execute('SELECT * FROM participants')}
        assert states == {'peer-0': 1, 'peer-1': 0}
    finally:
        host.close()
        db.close()


def test_history_notifications_and_late_resume_do_not_create_attachment(tmp_path):
    from desktop_attachment import DesktopAttachment
    db = codex_bridge.database(str(tmp_path / 'bus.db'))
    tid = str(uuid.uuid4())
    codex_bridge.register(db, 'peer', tid, str(tmp_path), '/unused.sock')
    host = DesktopAttachment(db)
    try:
        host.client({'id': 1, 'method': 'thread/read', 'params': {'threadId': tid}})
        host.server({'id': 1, 'result': {'thread': {'id': tid, 'cwd': str(tmp_path)}}})
        host.server({'method': 'thread/started', 'params': {'thread': {'id': tid, 'cwd': str(tmp_path)}}})
        assert not host.probe({'thread_id': tid, 'project': str(tmp_path)})
        host.client({'id': 2, 'method': 'thread/resume', 'params': {'threadId': tid}})
        host.client({'id': 3, 'method': 'thread/unsubscribe', 'params': {'threadId': tid}})
        host.server({'id': 2, 'result': {'thread': {'id': tid, 'cwd': str(tmp_path)}}})
        host.heartbeat()
        assert db.execute('SELECT COUNT(*) FROM participants').fetchone()[0] == 0
    finally:
        host.close()
        db.close()


def test_native_reconnect_mints_new_session_and_binding_change_retires_old(tmp_path):
    from desktop_attachment import DesktopAttachment
    import agent_msg
    from agent_directory import guard
    db = codex_bridge.database(str(tmp_path / 'bus.db'))
    tid = str(uuid.uuid4())
    codex_bridge.register(db, 'peer', tid, str(tmp_path), '/unused.sock')
    with db:
        db.execute('UPDATE codex_bindings SET enabled=1')
    host = DesktopAttachment(db)
    def attach(n):
        host.client({'id': n, 'method': 'thread/resume', 'params': {'threadId': tid}})
        host.server({'id': n, 'result': {'thread': {'id': tid, 'cwd': str(tmp_path)}}})
    try:
        attach(1)
        original = db.execute('SELECT session FROM participants').fetchone()[0]
        mid = agent_msg.send(db, 'sender', 'peer', 'old')
        host.client({'id': 2, 'method': 'thread/unsubscribe', 'params': {'threadId': tid}})
        attach(3)
        assert db.execute('SELECT session FROM participants').fetchone()[0] != original
        assert guard(db, mid) == 'session_replaced'
        with db:
            db.execute('UPDATE codex_bindings SET thread_id=?', (str(uuid.uuid4()),))
        host.heartbeat()
        assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1
    finally:
        host.close()
        db.close()


@pytest.mark.parametrize('event', ['thread/closed', 'thread/archived', 'thread/deleted'])
def test_terminal_native_events_retire_even_when_owner_process_survives(tmp_path, event):
    from desktop_attachment import DesktopAttachment
    db = codex_bridge.database(str(tmp_path / 'bus.db'))
    tid = str(uuid.uuid4())
    codex_bridge.register(db, 'peer', tid, str(tmp_path), '/unused.sock')
    host = DesktopAttachment(db)
    host.client({'id': 1, 'method': 'thread/resume', 'params': {'threadId': tid}})
    host.server({'id': 1, 'result': {'thread': {'id': tid, 'cwd': str(tmp_path)}}})
    host.server({'method': event, 'params': {'threadId': tid}})
    assert db.execute('SELECT closed FROM participants').fetchone()[0] == 1
    host.close()
    db.close()


def test_real_codex_owner_stdio_two_threads_detach_and_idle(tmp_path):
    """Real App Server, no model requests, no existing desktop or user history."""
    import json
    import os
    from pathlib import Path
    import queue
    import shutil
    import subprocess
    import sys
    import threading
    import time
    import agent_msg
    import desktop_attachment

    codex = shutil.which('codex')
    if not codex:
        pytest.skip('Codex binary unavailable')
    path = str(tmp_path / 'bus.db')
    home = tmp_path / 'home'
    home.mkdir()
    command = [sys.executable, str(Path(desktop_attachment.__file__)), '--db', path, '--interval', '.1', '--',
               codex, '-c', 'model_provider="fixture"', '-c', 'model_providers.fixture.name="fixture"',
               '-c', 'model_providers.fixture.base_url="http://127.0.0.1:1/v1"',
               '-c', 'model_providers.fixture.wire_api="responses"',
               '-c', 'model_providers.fixture.requires_openai_auth=false', 'app-server']
    log = (tmp_path / 'stderr').open('w+')
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                               text=True, env={**os.environ, 'CODEX_HOME': str(home)})
    messages = queue.Queue()
    def reader():
        for line in process.stdout:
            messages.put(json.loads(line))
    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    seq = 0
    def call(method, params):
        nonlocal seq
        seq += 1
        process.stdin.write(json.dumps({'id': seq, 'method': method, 'params': params}) + '\n')
        process.stdin.flush()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                result = messages.get(timeout=max(.1, deadline - time.monotonic()))
            except queue.Empty:
                log.flush(); log.seek(0)
                pytest.fail('native response timeout: ' + log.read(3000))
            if result.get('id') == seq:
                assert 'error' not in result, result
                return result['result']
        pytest.fail('native response timeout')
    db = codex_bridge.database(path)
    try:
        call('initialize', {'clientInfo': {'name': 'resonance-test', 'version': '0.4.0'}, 'capabilities': {'experimentalApi': True}})
        process.stdin.write('{"method":"initialized","params":{}}\n'); process.stdin.flush()
        tids = [call('thread/start', {'cwd': str(tmp_path), 'ephemeral': True,
                'model': 'fixture', 'modelProvider': 'fixture', 'approvalPolicy': 'never'})['thread']['id'] for _ in range(2)]
        for n, tid in enumerate(tids):
            codex_bridge.register(db, f'peer-{n}', tid, str(tmp_path), transport='queue', executable=codex, codex_home=str(home))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if db.execute('SELECT COUNT(*) FROM participants WHERE ready=1 AND closed=0').fetchone()[0] == 2:
                break
            time.sleep(.03)
        assert db.execute('SELECT COUNT(*) FROM participants WHERE ready=1 AND closed=0').fetchone()[0] == 2
        before = db.execute("SELECT expires FROM participants WHERE recipient='peer-1'").fetchone()[0]
        call('thread/unsubscribe', {'threadId': tids[0]})
        # Native App Server may retain the detached thread in memory; no poll inference.
        loaded = call('thread/loaded/list', {})['data']
        assert tids[1] in loaded
        time.sleep(.25)
        assert db.execute("SELECT closed FROM participants WHERE recipient='peer-0'").fetchone()[0] == 1
        survivor = db.execute("SELECT closed,expires FROM participants WHERE recipient='peer-1'").fetchone()
        assert not survivor['closed'] and survivor['expires'] > before
        process.stdin.close()
        assert process.wait(timeout=5) == 0
        assert db.execute('SELECT COUNT(*) FROM participants WHERE closed=0').fetchone()[0] == 0
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        thread.join(2)
        process.stdout.close()
        log.close()
        db.close()
