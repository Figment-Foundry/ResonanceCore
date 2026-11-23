import os
from pathlib import Path
import shutil
import signal
import tempfile

import pytest


@pytest.fixture(autouse=True)
def isolated_service_runtime(monkeypatch):
    """Never reach the owner's real service or start a systemd unit from a test."""
    # Short path: Unix socket names are limited to 108 bytes.
    runtime = tempfile.mkdtemp(prefix='rs-')
    os.chmod(runtime, 0o700)
    monkeypatch.setenv('RESONANCE_RUNTIME_DIR', runtime)
    monkeypatch.setenv('RESONANCE_SERVICE_SPAWN', 'process')
    # Tests run from T3-hosted agents too; never reach the owner's real T3 server.
    monkeypatch.delenv('T3CODE_THREAD_ID', raising=False)
    yield Path(runtime)
    for pidfile in Path(runtime).glob('*.pid'):
        try:
            os.kill(int(pidfile.read_text().strip()), signal.SIGTERM)
        except (ValueError, OSError):
            pass
    shutil.rmtree(runtime, ignore_errors=True)
