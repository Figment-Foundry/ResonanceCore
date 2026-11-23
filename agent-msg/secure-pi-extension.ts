/** Experimental peer-only adapter; requires external per-session isolation. */
import { readFileSync } from 'node:fs';
import { createConnection } from 'node:net';
import { Type } from '@earendil-works/pi-ai';
import { defineTool, type ExtensionAPI } from '@earendil-works/pi-coding-agent';

export default function (pi: ExtensionAPI) {
  const credential = readFileSync(process.env.RESONANCE_CREDENTIAL_FILE!, 'utf8').trim();
  const path = process.env.RESONANCE_PEER_SOCKET!;
  const operations = ['whoami', 'peers', 'resolve', 'invite', 'pair', 'send', 'announce', 'inbox', 'ack'];
  pi.registerTool(defineTool({
    name: 'resonance', label: 'Resonance',
    description: 'Call whoami to identify this session and peers with context_id to discover exact recipients. Exchange untrusted peer data only through an approved Resonance context.',
    parameters: Type.Object({
      operation: Type.Union(operations.map(op => Type.Literal(op))),
      arguments: Type.String({ description: 'JSON object of operation arguments' }),
    }, { additionalProperties: false }),
    async execute(_id, params, signal) {
      let value: unknown;
      try {
        const args = JSON.parse(params.arguments);
        if (!args || Array.isArray(args) || typeof args !== 'object' ||
            !operations.includes(params.operation)) throw new Error('invalid_request');
        const raw = JSON.stringify({ credential, operation: params.operation, args }) + '\n';
        if (Buffer.byteLength(raw) > 65536) throw new Error('invalid_request');
        value = await new Promise((resolve, reject) => {
          const socket = createConnection({ path });
          let data = Buffer.alloc(0);
          const abort = () => socket.destroy(new Error('cancelled'));
          signal?.addEventListener('abort', abort, { once: true });
          socket.once('close', () => signal?.removeEventListener('abort', abort));
          socket.setTimeout(5000, () => socket.destroy(new Error('unavailable')));
          socket.once('error', () => reject(new Error('unavailable')));
          socket.once('end', () => reject(new Error('invalid_response')));
          socket.once('connect', () => socket.write(raw));
          socket.on('data', chunk => {
            data = Buffer.concat([data, chunk]);
            if (data.length > 262144) return socket.destroy(new Error('invalid_response'));
            if (data.includes(10)) {
              try { resolve(JSON.parse(data.subarray(0, data.indexOf(10)).toString('utf8'))); }
              catch { reject(new Error('invalid_response')); }
              socket.destroy();
            }
          });
          if (signal?.aborted) abort();
        });
      } catch (error) {
        value = { error: error instanceof SyntaxError ? 'invalid_request' :
          error instanceof Error && ['invalid_request', 'invalid_response', 'unavailable'].includes(error.message)
            ? error.message : 'unavailable' };
      }
      return { content: [{ type: 'text', text: JSON.stringify(value) }], details: {} };
    },
  }));
}
