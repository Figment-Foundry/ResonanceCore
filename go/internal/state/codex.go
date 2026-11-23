package state

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"time"

	"github.com/gorilla/websocket"
	"resonancecore/internal/transport"
)

// Verification only reads the explicitly named thread. It never resumes a thread.
func VerifyCodex(socket, project, thread string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second, EnableCompression: false, NetDialContext: func(_ context.Context, _, _ string) (net.Conn, error) { return transport.Dial(socket, 5*time.Second) }}
	conn, _, e := dialer.DialContext(ctx, "ws://localhost/", nil)
	if e != nil {
		return refusal("codex_daemon_unavailable")
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	conn.SetReadDeadline(deadline)
	conn.SetWriteDeadline(deadline)
	conn.SetReadLimit(1048576)
	sequence := 0
	rpc := func(method string, params Map) (Map, error) {
		sequence++
		expected := sequence
		if e := conn.WriteJSON(Map{"id": expected, "method": method, "params": params}); e != nil {
			return nil, e
		}
		seen := 0
		for {
			_, raw, e := conn.ReadMessage()
			if e != nil {
				return nil, e
			}
			seen += len(raw)
			if seen > 1048576 {
				return nil, refusal("codex_daemon_unavailable")
			}
			var response Map
			if json.Unmarshal(raw, &response) != nil {
				return nil, refusal("codex_daemon_unavailable")
			}
			if response["id"] == float64(expected) {
				if _, exists := response["error"]; exists {
					return nil, refusal("codex_thread_not_loaded")
				}
				result, ok := response["result"].(map[string]any)
				if !ok {
					return nil, refusal("codex_daemon_unavailable")
				}
				return result, nil
			}
		}
	}
	if _, e = rpc("initialize", Map{"clientInfo": Map{"name": "resonance", "version": "0.2"}, "capabilities": Map{"experimentalApi": true}}); e != nil {
		return refusal("codex_daemon_unavailable")
	}
	if conn.WriteJSON(Map{"method": "initialized", "params": Map{}}) != nil {
		return refusal("codex_daemon_unavailable")
	}
	result, e := rpc("thread/read", Map{"threadId": thread})
	if e != nil {
		if e.Error() == "codex_thread_not_loaded" {
			return e
		}
		return refusal("codex_daemon_unavailable")
	}
	found, ok := result["thread"].(map[string]any)
	if !ok || found["id"] != thread {
		return refusal("codex_thread_not_loaded")
	}
	status, ok := found["status"].(map[string]any)
	if !ok || status["type"] == nil || status["type"] == "notLoaded" {
		return refusal("codex_thread_not_loaded")
	}
	cwd, ok := found["cwd"].(string)
	if !ok {
		return refusal("codex_project_mismatch")
	}
	resolved, e := filepath.EvalSymlinks(cwd)
	if e != nil || filepath.Clean(resolved) != project {
		return refusal("codex_project_mismatch")
	}
	return nil
}
