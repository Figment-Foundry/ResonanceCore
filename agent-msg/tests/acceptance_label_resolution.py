"""Controller-owned offline acceptance for logical discovery; no live bus access."""
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import agent_msg as bus
import agent_broadcast as membership


class LabelResolutionAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.project = self.root / 'worker'
        self.project.mkdir()
        self.path = self.root / 'bus.db'
        self.db = bus.open_db(str(self.path))

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def join(self, name, project=None, ready=True):
        session = membership.register(self.db, name, str(project or self.project), 'codex')
        membership.renew(self.db, name, session, ready=ready)
        return session

    def resolve(self, label='worker', project=None):
        args = ['resolve', '--label', label]
        if project is not None:
            args += ['--project', str(project)]
        out, err = io.StringIO(), io.StringIO()
        env = {k: v for k, v in os.environ.items() if not k.startswith(('AGENT_MSG_', 'RESONANCE_'))}
        env['AGENT_MSG_DB'] = str(self.path)
        with patch.dict(os.environ, env, clear=True), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = bus.main(args)
        return code, json.loads(out.getvalue()) if code == 0 else err.getvalue()

    def test_stale_alias_resolves_unique_live_project_member(self):
        old = self.join('worker')
        membership.close(self.db, 'worker', old)
        current = self.join('codex-terminal-current')
        code, result = self.resolve()
        self.assertEqual(code, 0, result)
        self.assertEqual(result['recipient'], 'codex-terminal-current')
        self.assertEqual(result['session'], current)
        self.assertTrue(result['sendable'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], 0)

    def test_no_historical_alias_needed_and_exact_lookup_still_works(self):
        current = self.join('codex-terminal-current')
        for label in ('worker', 'codex-terminal-current'):
            code, result = self.resolve(label, self.project)
            self.assertEqual(code, 0, result)
            self.assertEqual(result['session'], current)

    def test_multiple_live_sessions_are_ambiguous_even_with_exact_project(self):
        self.join('codex-terminal-a')
        self.join('codex-terminal-b')
        for project in (None, self.project):
            code, result = self.resolve(project=project)
            self.assertEqual(code, 2, result)
            self.assertIn('ambiguous', result)

    def test_colliding_project_basenames_require_scope(self):
        other = self.root / 'elsewhere' / 'worker'
        other.mkdir(parents=True)
        self.join('codex-terminal-a')
        expected = self.join('codex-terminal-b', other)
        code, result = self.resolve()
        self.assertEqual(code, 2, result)
        self.assertIn('ambiguous', result)
        code, result = self.resolve(project=other)
        self.assertEqual(code, 0, result)
        self.assertEqual(result['session'], expected)

    def test_closed_expired_and_unready_do_not_hide_live_member(self):
        old = self.join('old')
        membership.close(self.db, 'old', old)
        self.join('expired')
        with self.db:
            self.db.execute('UPDATE participants SET expires=? WHERE recipient=?', (time.time() - 1, 'expired'))
        self.join('unready', ready=False)
        expected = self.join('current')
        code, result = self.resolve()
        self.assertEqual(code, 0, result)
        self.assertEqual(result['session'], expected)

    def test_retirement_refreshes_discovery_but_never_retargets_mail(self):
        first = self.join('codex-terminal-a')
        code, selected = self.resolve()
        self.assertEqual(code, 0, selected)
        bus.send(self.db, 'sender', selected['recipient'], 'old mail', expected_session=selected['session'])
        membership.close(self.db, 'codex-terminal-a', first)
        replacement = self.join('codex-terminal-b')
        code, refreshed = self.resolve()
        self.assertEqual(code, 0, refreshed)
        self.assertEqual(refreshed['session'], replacement)
        self.assertEqual(bus.fetch_unread(self.db, 'codex-terminal-b', session=replacement), [])
        with self.assertRaises(ValueError):
            bus.send(self.db, 'sender', selected['recipient'], 'stale selection', expected_session=selected['session'])

    def test_no_match_is_explicit_and_wrong_project_never_falls_back(self):
        self.join('current')
        other = self.root / 'other'
        other.mkdir()
        code, result = self.resolve(project=other)
        self.assertEqual(code, 2, result)
        self.assertTrue('unavailable' in result or 'not_found' in result, result)


if __name__ == '__main__':
    unittest.main(verbosity=2)
