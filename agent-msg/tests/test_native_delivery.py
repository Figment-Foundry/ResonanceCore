import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import pytest

import agent_msg
import agent_directory
import native_delivery
from native_delivery import NativeSession


def test_native_delivery_is_exclusive_and_acceptance_is_not_a_reply(tmp_path, capsys):
    with NativeSession(str(tmp_path / 'bus.db'), 'pi', 'native-one', str(tmp_path), cooldown=0) as peer:
        mid = agent_msg.send(peer.db, 'sender', peer.recipient, 'wake me')
        item = peer.claim()
        assert item['message_id'] == mid and 'wake me' in item['content']
        assert peer.claim() is None
        args = argparse.Namespace(as_=peer.recipient, session=peer.session, keep_unread=False)
        assert agent_msg.cmd_drain(peer.db, args) == 2
        assert peer.db.execute('SELECT read_at FROM messages').fetchone()[0] is None
        peer.finish(mid, 'accepted')
        assert peer.db.execute('SELECT read_at FROM messages').fetchone()[0] is not None
        assert peer.claim() is None
        assert peer.status()['deliveries'][0]['state'] == 'accepted'
        assert 'native adapter' in capsys.readouterr().err


def test_native_unknown_holds_later_messages_and_cannot_be_replayed(tmp_path):
    with NativeSession(str(tmp_path / 'bus.db'), 'opencode', 'ses_fixture', str(tmp_path), cooldown=0) as peer:
        mid = agent_msg.send(peer.db, 'sender', peer.recipient, 'one')
        peer.claim()
        peer.finish(mid, 'unknown')
        assert peer.identity()['wakeup'] is False
        agent_msg.send(peer.db, 'sender', peer.recipient, 'two')
        assert peer.claim() is None
        with pytest.raises(ValueError):
            peer.finish(mid, 'accepted')


def test_native_session_replacement_never_receives_old_mail(tmp_path):
    path = str(tmp_path / 'bus.db')
    with NativeSession(path, 'pi', 'same-thread', str(tmp_path)) as old:
        old_session = old.session
        mid = agent_msg.send(old.db, 'sender', old.recipient, 'old')
        old.claim()
    with NativeSession(path, 'pi', 'same-thread', str(tmp_path)) as new:
        assert new.session != old_session
        assert new.claim() is None
        assert agent_directory.guard(new.db, mid, session=new.session) == 'session_replaced'


def test_native_presence_budget_and_pause(tmp_path):
    with NativeSession(str(tmp_path / 'bus.db'), 'pi', 'native', str(tmp_path), max_wakeups=1, cooldown=0) as peer:
        assert peer.identity()['wakeup'] is True
        peer.pause()
        assert peer.identity()['wakeup'] is False
        peer.resume()
        mid = agent_msg.send(peer.db, 'sender', peer.recipient, 'one')
        peer.claim()
        peer.finish(mid, 'accepted')
        agent_msg.send(peer.db, 'sender', peer.recipient, 'two')
        assert peer.claim() is None
        assert peer.identity()['wakeup'] is False
        with peer.db:
            peer.db.execute('UPDATE participants SET expires=0')
        with pytest.raises(ValueError, match='expired'):
            peer.renew()


def test_native_send_cannot_impersonate_another_session(tmp_path):
    with NativeSession(str(tmp_path / 'bus.db'), 'pi', 'one', str(tmp_path)) as one:
        with NativeSession(str(tmp_path / 'bus.db'), 'opencode', 'two', str(tmp_path)) as two:
            result = one.invoke('send', {'to': two.recipient, 'session': two.session, 'body': 'hello'})
            assert two.claim()['message_id'] == result['message_id']
            with pytest.raises(ValueError):
                one.invoke('send', {'from': 'spoof', 'to': two.recipient, 'session': two.session, 'body': 'hello'})


def test_native_owner_controls_are_session_pinned_and_preserve_unknown(tmp_path):
    from native_delivery import control_main
    path = str(tmp_path/'bus.db')
    with NativeSession(path, 'pi', 'one', str(tmp_path), cooldown=0) as peer:
        args = ['--bus', path, '--recipient', peer.recipient, '--session', peer.session]
        assert control_main(args + ['pause']) == 0
        assert not peer.identity()['wakeup']
        assert control_main(args + ['resume']) == 0
        mid = agent_msg.send(peer.db, 'sender', peer.recipient, 'one')
        peer.claim()
        peer.finish(mid, 'unknown')
        assert control_main(args + ['resume']) == 0
        assert peer.claim() is None
        assert control_main(args[:-1] + ['wrong', 'resume']) == 1


def _owner_after_update(tmp_path, folder, *, prune):
    """Run native_owner under a host binary that a self-update has since superseded."""
    versions = tmp_path / folder
    versions.mkdir()
    shutil.copy2('/usr/bin/dash', versions / '1.0')
    (versions / '2.0').write_bytes(b'')
    (versions / '2.0').chmod(0o755)
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    (bin_dir / 'fakehost').symlink_to(versions / '2.0')
    probe = (f'cd {Path(native_delivery.__file__).parent} && '
             f'{sys.executable} -c "import native_delivery; print(native_delivery.native_owner(\'fakehost\'))"')
    script = ('rm -f "$0"; ' if prune else '') + probe + '; exit $?'
    result = subprocess.run([str(versions / '1.0'), '-c', script, str(versions / '1.0')],
                            env={**os.environ, 'PATH': f'{bin_dir}:{os.environ["PATH"]}'},
                            capture_output=True, text=True, timeout=30)
    return result


@pytest.mark.parametrize('prune', [False, True])
def test_native_owner_survives_host_self_update(tmp_path, prune):
    result = _owner_after_update(tmp_path, 'versions', prune=prune)
    assert result.returncode == 0, result.stderr
    assert int(result.stdout) > 1


def test_native_owner_sibling_rule_is_limited_to_versions_directories(tmp_path):
    result = _owner_after_update(tmp_path, 'bin-shared', prune=False)
    assert 'native_host_required' in result.stderr
