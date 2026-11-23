"""Native Windows process identity and isolated service lifecycle regressions."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

MODULES = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULES))
from agent_session import process_identity


@unittest.skipUnless(os.name == "nt", "Windows native transport boundary")
class ResonanceWindowsNative(unittest.TestCase):
    def test_native_process_metadata_probe_returns_current_process(self):
        import terminal_session
        parent, executable, command = terminal_session._windows_process(os.getpid())
        self.assertEqual(parent, os.getppid())
        self.assertEqual(executable, Path(sys.executable).name.lower())
        self.assertIsInstance(command, str)

    def test_terminal_interface_is_available_on_windows(self):
        executable = shutil.which("agent-msg-terminal")
        result = subprocess.run([executable, "--help"], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("hook", result.stdout)

    def test_current_windows_process_has_stable_identity(self):
        identity = process_identity(os.getpid())
        self.assertIsNotNone(identity)
        self.assertEqual(len(identity), 2)
        self.assertEqual(identity, process_identity(os.getpid()))
        self.assertIsNone(process_identity(2_000_000_000))

    def test_isolated_windows_service_start_status_stop(self):
        executable = shutil.which("agent-msg-service")
        self.assertIsNotNone(executable)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bus = root / "bus.db"
            environment = dict(os.environ, RESONANCE_RUNTIME_DIR=str(root / "run"))
            def call(operation):
                return subprocess.run([executable, "--bus", str(bus), operation],
                                      env=environment, capture_output=True, text=True, timeout=20)
            try:
                start = call("start")
                self.assertEqual(start.returncode, 0, start.stdout + start.stderr)
                status = call("status")
                self.assertEqual(status.returncode, 0, status.stdout + status.stderr)
                self.assertIsInstance(json.loads(status.stdout), dict)
            finally:
                call("stop")


if __name__ == "__main__":
    unittest.main()
