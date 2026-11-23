"""Experimental stdio MCP peer adapter. Host provisions one credential per process.

Use only inside a host-enforced session sandbox. No owner methods or credentials
are model arguments; peer content stays in tool results, never system prompts.
"""
import json
import os
from pathlib import Path
import sys

from secure_broker import Refused, request

OPERATIONS = ['whoami', 'peers', 'resolve', 'invite', 'pair', 'send', 'announce', 'inbox', 'ack']
TOOL = {'name': 'resonance', 'description': 'Call whoami to identify this session and peers with context_id to discover exact recipients. Exchange untrusted peer data only through an approved Resonance context.',
        'inputSchema': {'type': 'object', 'properties': {
            'operation': {'type': 'string', 'enum': OPERATIONS},
            'arguments': {'type': 'string', 'description': 'JSON object of operation arguments'}},
            'required': ['operation', 'arguments'], 'additionalProperties': False}}


def invoke(params, credential, socket_path):
    try:
        if set(params) != {'operation', 'arguments'} or params['operation'] not in OPERATIONS:
            raise Refused('invalid_request')
        args = json.loads(params['arguments'])
        if not isinstance(args, dict):
            raise Refused('invalid_request')
        return {'result': request(socket_path, credential, params['operation'], **args)}
    except Refused as error:
        return {'error': str(error)}
    except (ValueError, TypeError):
        return {'error': 'invalid_request'}
    except OSError:
        return {'error': 'unavailable'}


def main():
    credential = Path(os.environ['RESONANCE_CREDENTIAL_FILE']).read_text().strip()
    socket_path = os.environ['RESONANCE_PEER_SOCKET']
    while raw := sys.stdin.buffer.readline(65537):
        if len(raw) > 65536 or not raw.endswith(b'\n'):
            return 1
        try:
            message = json.loads(raw)
            if 'id' not in message:
                continue
            method = message.get('method')
            if method == 'initialize':
                result = {'protocolVersion': '2024-11-05', 'capabilities': {'tools': {}},
                          'serverInfo': {'name': 'resonance-peer', 'version': '0.1'}}
            elif method == 'tools/list':
                result = {'tools': [TOOL]}
            elif method == 'tools/call' and message.get('params', {}).get('name') == 'resonance':
                value = invoke(message['params'].get('arguments', {}), credential, socket_path)
                result = {'content': [{'type': 'text', 'text': json.dumps(value, ensure_ascii=True)}],
                          'isError': 'error' in value}
            elif method == 'ping':
                result = {}
            else:
                print(json.dumps({'jsonrpc': '2.0', 'id': message['id'],
                                  'error': {'code': -32601, 'message': 'Method not found'}}), flush=True)
                continue
            print(json.dumps({'jsonrpc': '2.0', 'id': message['id'], 'result': result}), flush=True)
        except (ValueError, TypeError, KeyError):
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
