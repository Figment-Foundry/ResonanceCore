"""Run a real Resonance service in-process on the test's private runtime socket."""
from contextlib import contextmanager
import threading

import resonance_service as service


@contextmanager
def running_service(bus, **options):
    instance = service.Service(str(bus), **options)
    server = service.serve(instance, service.socket_path(str(bus)))
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05}, daemon=True)
    thread.start()
    try:
        yield instance
    finally:
        server.shutdown()
        server.server_close()
        service.socket_path(str(bus)).unlink(missing_ok=True)
        thread.join(timeout=5)
        instance.db.close()
