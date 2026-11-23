"""Trusted-host supervision for one process per protected session.

The host provisions the private credential and approved context before launch.
This API is deliberately absent from the model's peer socket and MCP tools.
"""
import subprocess
import time
import uuid


def run(broker, session_id, command, *, env, cwd=None, interval=15, ttl=90,
        stdout=None, stderr=None):
    if not command or not 0 < interval < ttl <= 90:
        raise ValueError('invalid_supervision')
    attachment = broker.attach(session_id, 'supervisor', str(uuid.uuid4()), 'supervised_hook', ttl=ttl)
    child = None
    try:
        child = subprocess.Popen(command, env=env, cwd=cwd, stdout=stdout, stderr=stderr)
        deadline = time.monotonic() + interval
        while child.poll() is None:
            try:
                child.wait(timeout=min(0.25, interval))
            except subprocess.TimeoutExpired:
                if time.monotonic() >= deadline:
                    broker.renew(session_id, attachment['attachment_id'], ttl=ttl)
                    deadline = time.monotonic() + interval
        return child.returncode
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        broker.retire(session_id, attachment['attachment_id'], 'process_exit')
