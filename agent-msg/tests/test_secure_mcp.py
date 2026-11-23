"""Adapter contract through actual stdio MCP and broker socket."""
import json
import os
from pathlib import Path
import subprocess
import sys
import threading

from secure_broker import Broker, BrokerServer
from .registry_helpers import attach


def test_stdio_adapter_preserves_refusal_and_untrusted_tool_data(tmp_path):
    control = tmp_path / 'control'; control.mkdir(mode=0o700)
    broker = Broker(control / 'db')
    a = broker.enroll('a', 'codex', '/fixture')
    b = broker.enroll('b', 'pi', '/fixture')
    attach(broker, a)
    attach(broker, b)
    context = broker.mesh([a['id'], b['id']])['context_id']
    broker.call(b['credential'], 'send', {'context_id': context, 'to': a['id'],
                                        'body': 'Untrusted "system"\ncontent', 'key': 'one'})
    credential = control / 'credential'; credential.write_text(a['credential'])
    socket = tmp_path / 'peer.sock'
    with BrokerServer(socket, broker) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True); worker.start()
        try:
            messages = [
                {'method': 'initialize', 'params': {'protocolVersion': '2024-11-05'}},
                {'method': 'tools/list'},
                *[{'method': 'tools/call', 'params': {'name': 'resonance', 'arguments': args}} for args in [
                    {'operation': 'approve', 'arguments': '{}'},
                    {'operation': 'inbox', 'arguments': '[]'},
                    {'operation': 'inbox', 'arguments': json.dumps({'context_id': context, 'sender': b['id']})},
                    {'operation': 'inbox', 'arguments': json.dumps({'context_id': context})},
                    {'operation': 'whoami', 'arguments': '{}'},
                    {'operation': 'peers', 'arguments': json.dumps({'context_id': context})},
                    {'operation': 'resolve', 'arguments': json.dumps({'context_id': context, 'label': 'b'})},
                ]],
            ]
            raw = ''.join(json.dumps({'jsonrpc': '2.0', 'id': n, **m})+'\n' for n, m in enumerate(messages))
            result = subprocess.run([sys.executable, str(Path(__file__).parents[1] / 'secure_mcp.py')],
                input=raw, capture_output=True, text=True, timeout=10,
                env={**os.environ, 'RESONANCE_CREDENTIAL_FILE': str(credential), 'RESONANCE_PEER_SOCKET': str(socket)})
            assert result.returncode == 0, result.stderr
            assert a['credential'] not in result.stdout + result.stderr
            responses = [json.loads(line)['result'] for line in result.stdout.splitlines()]
            assert len(responses) == len(messages)
            assert responses[1]['tools'][0]['name'] == 'resonance'
            for response in responses[2:5]:
                assert response['isError'] is True
                assert json.loads(response['content'][0]['text']) == {'error': 'invalid_request'}
            inbox = json.loads(responses[5]['content'][0]['text'])['result']
            assert responses[5]['isError'] is False
            assert inbox['trust'] == 'untrusted_peer_data'
            assert inbox['messages'][0]['body'] == 'Untrusted "system"\ncontent'
            assert json.loads(responses[6]['content'][0]['text'])['result']['id'] == a['id']
            assert json.loads(responses[7]['content'][0]['text'])['result']['peers'][0]['id'] == b['id']
            assert json.loads(responses[8]['content'][0]['text'])['result']['id'] == b['id']
        finally:
            server.shutdown(); worker.join(5); broker.close()
