import os

import pytest

import terminal_session as terminal


pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows ancestor evidence')


def test_windows_owner_accepts_interactive_harness_through_shell(monkeypatch):
    rows = {100: (50, 'powershell.exe', 'powershell hook'),
            50: (1, 'codex.exe', 'codex')}
    monkeypatch.setattr(os, 'getppid', lambda: 100)
    monkeypatch.setattr(terminal, 'process_identity', lambda pid: ('windows', 'created'))
    assert terminal.terminal_owner(windows_probe=lambda pid: rows[pid]) == (
        50, ['windows', 'created'])


@pytest.mark.parametrize('command', ['codex exec', 'codex app-server', 'codex mcp-server',
                                  'codex "exec"', 'codex "app-server"', 'codex "mcp-server"'])
def test_windows_owner_rejects_noninteractive_codex_modes(monkeypatch, command):
    monkeypatch.setattr(os, 'getppid', lambda: 50)
    monkeypatch.setattr(terminal, 'process_identity', lambda pid: ('windows', 'created'))
    with pytest.raises(ValueError, match='native_terminal_hook_required'):
        terminal.terminal_owner(windows_probe=lambda pid: (1, 'codex.exe', command))


def test_windows_owner_rejects_unexpected_ancestor(monkeypatch):
    monkeypatch.setattr(os, 'getppid', lambda: 50)
    with pytest.raises(ValueError, match='native_terminal_hook_required'):
        terminal.terminal_owner(windows_probe=lambda pid: (1, 'python.exe', 'python tool.py'))


def test_windows_metadata_probe_uses_exact_stable_pid(monkeypatch):
    identities = iter([('windows', 'created'), ('windows', 'created')])
    monkeypatch.setattr(terminal, 'process_identity', lambda pid: next(identities))
    observed = {}
    class Result:
        returncode = 0
        stdout = '{"ProcessId":42,"ParentProcessId":7,"ExecutablePath":"C:\\\\codex.exe","CommandLine":"codex"}'
    def run(argv, **kwargs):
        observed.update(kwargs)
        assert argv[-1] != '42'
        return Result()
    monkeypatch.setattr(terminal.subprocess, 'run', run)
    assert terminal._windows_process(42) == (7, 'codex.exe', 'codex')
    assert observed['env']['RESONANCE_PROBE_PROCESS_ID'] == '42'


def test_windows_metadata_probe_rejects_pid_reuse(monkeypatch):
    identities = iter([('windows', 'first'), ('windows', 'second')])
    monkeypatch.setattr(terminal, 'process_identity', lambda pid: next(identities))
    monkeypatch.setattr(terminal.subprocess, 'run', lambda *a, **k: type('Result', (), {
        'returncode': 0,
        'stdout': '{"ProcessId":42,"ParentProcessId":7,"ExecutablePath":"C:\\\\codex.exe","CommandLine":"codex"}'
    })())
    with pytest.raises(ValueError, match='native_terminal_hook_required'):
        terminal._windows_process(42)
