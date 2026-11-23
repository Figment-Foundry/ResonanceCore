"""Public CLI acceptance and concrete-address regression coverage."""
from . import acceptance_label_resolution as acceptance


class ControllerEdges(acceptance.LabelResolutionAcceptance):
    def test_protected_project_filter_is_not_silently_ignored(self):
        import contextlib
        import io
        from unittest.mock import patch
        for command in ('resolve', 'peers'):
            err = io.StringIO()
            env = {'RESONANCE_PEER_SOCKET': str(self.root / 'missing.sock'),
                   'AGENT_MSG_DB': str(self.root / 'must-not-create.db')}
            with patch.dict(acceptance.os.environ, env, clear=True), contextlib.redirect_stderr(err):
                code = acceptance.bus.main([command, '--context', 'private',
                                            '--label', 'worker', '--project', str(self.project)])
            self.assertEqual(code, 2)
            self.assertIn('protected_project_filter_unsupported', err.getvalue())
            self.assertFalse((self.root / 'must-not-create.db').exists())

    def test_unrelated_label_with_project_never_selects_arbitrary_peer(self):
        self.join('current')
        code, result = self.resolve('unrelated', self.project)
        self.assertEqual(code, 2, result)

    def test_saved_terminal_identity_never_becomes_project_lookup(self):
        label = 'codex-terminal-11111111-1111-4111-8111-111111111111'
        old = self.join(label)
        acceptance.membership.close(self.db, label, old)
        collision = self.root / label
        collision.mkdir()
        self.join('replacement', collision)
        code, result = self.resolve(label)
        self.assertEqual(code, 2, result)

    def test_exact_recipient_honors_project_filter(self):
        self.join('concrete')
        wrong = self.root / 'wrong'
        wrong.mkdir()
        code, result = self.resolve('concrete', wrong)
        self.assertEqual(code, 2, result)

    def test_discovery_does_not_use_only_first_page(self):
        for i in range(23):
            name = f'old-{i:02}'
            sid = self.join(name)
            acceptance.membership.close(self.db, name, sid)
        current = self.join('zz-current')
        code, result = self.resolve()
        self.assertEqual(code, 0, result)
        self.assertEqual(result['session'], current)

    def test_live_exact_recipient_precedes_logical_fallback(self):
        concrete = self.join('worker')
        self.join('another')
        code, result = self.resolve()
        self.assertEqual(code, 0, result)
        self.assertEqual(result['session'], concrete)
