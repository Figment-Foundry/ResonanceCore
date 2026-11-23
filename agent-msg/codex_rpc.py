"""Bounded JSON-RPC over Codex's documented Unix WebSocket transport."""
import json
import time


class TransportError(RuntimeError):
    pass


class Rejected(TransportError):
    def __init__(self, reason, method=None, code=None):
        super().__init__(reason)
        self.method = method
        self.code = code if type(code) is int and -(2**31) <= code < 2**31 else None


class CodexRPC:
    def __init__(self, socket_path, timeout=10):
        self.socket_path, self.timeout = socket_path, timeout
        self.connection = None
        self.manager = None
        self.sequence = 0

    def __enter__(self):
        try:
            from websockets.sync.client import unix_connect
            self.manager = unix_connect(self.socket_path, open_timeout=self.timeout,
                close_timeout=2, max_size=1_048_576, max_queue=8, compression=None)
            self.connection = self.manager.__enter__()
            self.call('initialize', {'clientInfo': {'name': 'resonance', 'version': '0.2'},
                'capabilities': {'experimentalApi': True}})
            self.notify('initialized', {})
        except Exception as error:
            self.close()
            if isinstance(error, TransportError):
                raise
            raise TransportError('connection_unavailable') from None
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        manager, connection = self.manager, self.connection
        self.manager = self.connection = None
        if manager is not None and connection is not None:
            manager.__exit__(None, None, None)

    def _send(self, value):
        text = json.dumps(value, separators=(',', ':'))
        if len(text.encode()) > 65_536:
            raise TransportError('request_limit')
        try:
            self.connection.send(text)
        except Exception:
            raise TransportError('connection_lost') from None

    def notify(self, method, params):
        self._send({'method': method, 'params': params})

    def call(self, method, params):
        self.sequence += 1
        expected = self.sequence
        self._send({'id': expected, 'method': method, 'params': params})
        deadline, observed = time.monotonic() + self.timeout, 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransportError('rpc_timeout')
            try:
                raw = self.connection.recv(timeout=remaining)
            except TimeoutError:
                raise TransportError('rpc_timeout') from None
            except Exception:
                raise TransportError('connection_lost') from None
            observed += len(raw.encode() if isinstance(raw, str) else raw)
            if observed > 1_048_576:
                raise TransportError('response_limit')
            try:
                response = json.loads(raw)
            except (ValueError, UnicodeError):
                raise TransportError('invalid_json') from None
            if not isinstance(response, dict):
                raise TransportError('invalid_response')
            if response.get('id') == expected:
                if 'error' in response:
                    error = response['error']
                    code = error.get('code') if isinstance(error, dict) else None
                    raise Rejected('rpc_rejected', method, code)
                if 'result' not in response:
                    raise TransportError('invalid_response')
                return response['result']


def connect(binding):
    return CodexRPC(binding['socket']).__enter__()
