"""Preview/apply owner-local native adapters; preserve unrelated harness settings."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import tempfile
import time
import tomllib

ROOT = Path(__file__).resolve().parents[1]
MARKER = '// Managed by ResonanceCore native harness installer.\n'
BEGIN = '# BEGIN ResonanceCore native hooks'
END = '# END ResonanceCore native hooks'


def plan(home, runtime):
    files, links = {}, {}
    integration = home/'.local/share/resonancecore/integrations'
    for name in ('native-peer.mjs', 'pi-extension.ts', 'opencode-plugin.ts'):
        content = (ROOT/'agent-msg/integrations'/name).read_text()
        if name == 'native-peer.mjs':
            content = content.replace('options.executable ?? "agent-msg-native"',
                                      'options.executable ?? ' + json.dumps(str(runtime/'bin/agent-msg-native')))
        files[integration/name] = MARKER + content
    files[home/'.pi/agent/extensions/resonance.ts'] = MARKER + 'export { default } from ' + json.dumps(str(integration/'pi-extension.ts')) + '\n'
    files[home/'.config/opencode/plugins/resonance.ts'] = MARKER + 'export { AgentMsgBus } from ' + json.dumps(str(integration/'opencode-plugin.ts')) + '\n'
    # OpenCode itself installs its matching plugin SDK in this config directory.
    links[integration/'node_modules'] = home/'.config/opencode/node_modules'
    for name in ('agent-msg-native', 'agent-msg-native-control', 'agent-msg-harness', 'agent-msg-claude-channel'):
        if not (runtime/'bin'/name).is_file():
            raise ValueError('Build and install the wheel before installing native adapters: ' + name)
        links[home/'.local/bin'/name] = runtime/'bin'/name
    settings_path = home/'.claude/settings.json'
    settings = json.loads(settings_path.read_text()) if settings_path.exists() else {}
    hooks = settings.setdefault('hooks', {})
    command = shlex.quote(str(runtime/'bin/agent-msg-harness')) + ' --harness claude hook'
    for event in ('SessionStart', 'UserPromptSubmit', 'SessionEnd'):
        entries = hooks.setdefault(event, [])
        # Remove only our exact previously installed command; leave all others.
        entries[:] = [entry for entry in entries if entry != {'hooks': [{'type': 'command', 'command': command, 'timeout': 10}]}]
        entries.append({'hooks': [{'type': 'command', 'command': command, 'timeout': 10}]})
    files[settings_path] = json.dumps(settings, indent=2) + '\n'
    kimi_path = home/'.kimi-code/config.toml'
    content = kimi_path.read_text() if kimi_path.exists() else ''
    if BEGIN in content:
        before, after = content.split(BEGIN, 1)
        if END not in after:
            raise ValueError('Incomplete managed Kimi hook block')
        content = before.rstrip() + after.split(END, 1)[1]
    command = shlex.quote(str(runtime/'bin/agent-msg-harness')) + ' --harness kimi hook'
    block = '\n\n' + BEGIN + '\n'
    for event in ('SessionStart', 'UserPromptSubmit', 'SessionEnd'):
        block += '\n[[hooks]]\nevent = ' + json.dumps(event) + '\ncommand = ' + json.dumps(command) + '\ntimeout = 10\n'
    files[kimi_path] = content.rstrip() + block + END + '\n'
    tomllib.loads(files[kimi_path])
    # Channels need native per-launch consent. Do not enable them globally.
    channel_path = home/'.config/resonancecore/claude-channel.json'
    files[channel_path] = json.dumps({'mcpServers': {'resonance': {
        'command': str(runtime/'bin/agent-msg-claude-channel'), 'args': []}}}, indent=2) + '\n'
    for path, content in files.items():
        if path in (settings_path, kimi_path, channel_path) or not path.exists():
            continue
        if not path.read_text().startswith(MARKER):
            raise ValueError('Refusing to replace unmanaged integration: ' + str(path))
    if channel_path.exists() and channel_path.read_text() != files[channel_path]:
        raise ValueError('Refusing to replace a different Claude channel configuration')
    for path, target in links.items():
        if path.exists() or path.is_symlink():
            if not path.is_symlink() or path.resolve() != target.resolve():
                raise ValueError('Refusing to replace existing path: ' + str(path))
    return files, links


def install(home, runtime, apply=False):
    files, links = plan(home, runtime)
    changes = []
    backup = home/'.local/state/resonancecore/install-backups'/str(time.time_ns())
    for path, content in files.items():
        if path.exists() and path.read_text() == content:
            continue
        changes.append(str(path))
        if not apply:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            saved = backup/path.relative_to(home)
            saved.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(path, saved)
            saved.chmod(0o600)
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.resonance-')
        with os.fdopen(fd, 'w') as stream:
            stream.write(content)
        os.replace(temporary, path)
    for path, target in links.items():
        if path.is_symlink():
            continue
        changes.append(str(path))
        if apply:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.symlink_to(target)
    return changes


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--home', type=Path, default=Path.home())
    parser.add_argument('--runtime', type=Path, default=ROOT/'.venv')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps({'apply': args.apply, 'paths': install(args.home.resolve(), args.runtime.resolve(), args.apply)}, indent=2))
