import importlib.util
import json
from pathlib import Path
import tomllib

import pytest

spec = importlib.util.spec_from_file_location('native_install', Path(__file__).resolve().parents[2]/'tools/install_native_harnesses.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


def runtime_at(root):
    (root/'bin').mkdir(parents=True, exist_ok=True)
    for name in ('agent-msg-native', 'agent-msg-native-control', 'agent-msg-harness', 'agent-msg-claude-channel'):
        (root/'bin'/name).touch()
    return root


def test_install_preserves_settings_and_is_idempotent(tmp_path):
    home = tmp_path/'home'
    runtime = runtime_at(tmp_path/'runtime')
    (home/'.claude').mkdir(parents=True)
    prior = {'permissions': {'deny': ['Read(private)']}, 'hooks': {'SessionStart': [{'hooks': [{'type': 'command', 'command': 'existing-hook'}]}]}}
    (home/'.claude/settings.json').write_text(json.dumps(prior))
    (home/'.kimi-code').mkdir()
    (home/'.kimi-code/config.toml').write_text('# keep comment\nmodel = "local"\n[[hooks]]\nevent = "Stop"\ncommand = "existing-hook"\n')
    planned = installer.install(home, runtime)
    assert not (home/'.pi').exists()
    assert installer.install(home, runtime, True) == planned
    assert installer.install(home, runtime, True) == []
    settings = json.loads((home/'.claude/settings.json').read_text())
    assert settings['permissions'] == prior['permissions']
    assert settings['hooks']['SessionStart'][0] == prior['hooks']['SessionStart'][0]
    kimi = (home/'.kimi-code/config.toml').read_text()
    assert kimi.startswith('# keep comment')
    assert len(tomllib.loads(kimi)['hooks']) == 4
    assert len(list((home/'.local/state/resonancecore/install-backups').glob('*/.claude/settings.json'))) == 1
    assert str(runtime/'bin/agent-msg-native') in (home/'.local/share/resonancecore/integrations/native-peer.mjs').read_text()


def test_install_refuses_unmanaged_extension_before_writes(tmp_path):
    home = tmp_path/'home'
    runtime = runtime_at(tmp_path/'runtime')
    folder = home/'.pi/agent/extensions'
    folder.mkdir(parents=True)
    (folder/'resonance.ts').write_text('owner code')
    with pytest.raises(ValueError, match='unmanaged'):
        installer.install(home, runtime, True)
    assert not (home/'.claude').exists()
