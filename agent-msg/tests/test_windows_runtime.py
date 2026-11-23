"""Unavailable native sessions must refuse cleanly without initializing a bus."""
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(os.name == "nt", "Windows native transport regression")
class ResonanceWindowsRuntime(unittest.TestCase):
    def assert_unavailable_refusal(self, program, arguments, exit_code):
        executable = shutil.which(program)
        self.assertIsNotNone(executable, f"Installed {program} is required")
        with tempfile.TemporaryDirectory() as temporary:
            bus = Path(temporary) / "absent" / "bus.db"
            environment = dict(os.environ, AGENT_MSG_DB=str(bus),
                               CODEX_THREAD_ID="00000000-0000-0000-0000-000000000000",
                               RESONANCE_RUNTIME_DIR=str(Path(temporary)/"run"))
            result = subprocess.run(
                [executable, '--bus', str(bus), *arguments],
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, exit_code, result.stdout + result.stderr)
            self.assertIn("service_unavailable", result.stderr)
            self.assertNotIn("Traceback", result.stderr)
            self.assertFalse(bus.parent.exists(), "Refusal must not initialize a bus")

    def test_terminal_command_refuses_unattached_session(self):
        self.assert_unavailable_refusal("agent-msg-terminal", ["exec", "--", "whoami"], 2)

    def test_service_status_does_not_start_an_absent_service(self):
        self.assert_unavailable_refusal("agent-msg-service", ["status"], 1)


if __name__ == "__main__":
    unittest.main()
