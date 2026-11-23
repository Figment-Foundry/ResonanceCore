package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixture(t *testing.T) *Service {
	s, e := NewService(filepath.Join(t.TempDir(), "bus.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Store.Close)
	return s
}
func queryOwner(t *testing.T, s *Service, thread, project, event string) (Map, error) {
	owner := Owner{Kind: "pid", PID: os.Getpid(), Identity: ProcessIdentity(os.Getpid())}
	b, _ := json.Marshal(owner)
	var m Map
	json.Unmarshal(b, &m)
	return s.Dispatch(Map{"op": "attach", "harness": "codex", "thread": thread, "project": project, "event": event, "owner": m})
}
func TestAttachmentIncarnationScopeAndSupervision(t *testing.T) {
	s := fixture(t)
	project := t.TempDir()
	first, e := queryOwner(t, s, "fixture", project, "SessionStart")
	if e != nil {
		t.Fatal(e)
	}
	second, e := queryOwner(t, s, "fixture", project, "UserPromptSubmit")
	if e != nil || first["session"] != second["session"] {
		t.Fatal(second, e)
	}
	if _, e = queryOwner(t, s, "fixture", t.TempDir(), "SessionStart"); e == nil || e.Error() != "session_scope_changed" {
		t.Fatal(e)
	}
	lookup, e := s.Dispatch(Map{"op": "lookup", "harness": "codex", "thread": "fixture"})
	if e != nil || lookup["session"] != first["session"] {
		t.Fatal(lookup, e)
	}
	s.Store.DB.Exec(`UPDATE service_attachments SET owner=?`, `{"kind":"pid","pid":2000000000,"identity":["wrong","wrong"]}`)
	if e = s.Tick(); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Dispatch(Map{"op": "lookup", "harness": "codex", "thread": "fixture"}); e == nil {
		t.Fatal("dead owner accepted")
	}
	third, e := queryOwner(t, s, "fixture", project, "SessionStart")
	if e != nil || third["session"] == first["session"] {
		t.Fatal(third, e)
	}
	ended, e := queryOwner(t, s, "fixture", project, "SessionEnd")
	if e != nil || ended["closed"] != true {
		t.Fatal(ended, e)
	}
}

func TestUnreachableDaemonRetiresAfterTwoFailures(t *testing.T) {
	s := fixture(t)
	insertDaemonAttachment(t, s, "daemon-session")
	s.VerifyOwner = func(Owner, string, string) error { return refusal("codex_daemon_unavailable") }
	if e := s.Tick(); e != nil {
		t.Fatal(e)
	}
	p, _ := part(s.Store.DB, "codex-terminal-daemon")
	if p.Closed != 0 {
		t.Fatal("first transient failure retired owner")
	}
	if e := s.Tick(); e != nil {
		t.Fatal(e)
	}
	p, _ = part(s.Store.DB, "codex-terminal-daemon")
	if p.Closed != 1 {
		t.Fatal("unreachable daemon remains active after second failure")
	}
}

func insertDaemonAttachment(t *testing.T, s *Service, session string) {
	t.Helper()
	project := t.TempDir()
	if _, e := s.Store.DB.Exec(`INSERT INTO participants(recipient,session,project,harness,route,target,expires,ready,closed,evidence) VALUES('codex-terminal-daemon',?,?,'codex','hook','{}',?,1,0,'supervised_hook') ON CONFLICT(recipient) DO UPDATE SET session=excluded.session,project=excluded.project,expires=excluded.expires,closed=0`, session, project, now()+90); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Store.DB.Exec(`INSERT OR REPLACE INTO service_attachments VALUES('codex-terminal-daemon','codex','daemon',?,?,?,?)`, session, project, `{"kind":"codex_thread","socket":"/nonexistent-resonance-fixture.sock"}`, now()); e != nil {
		t.Fatal(e)
	}
}

func TestDaemonFailureCounterResetsAfterSuccess(t *testing.T) {
	s := fixture(t)
	insertDaemonAttachment(t, s, "daemon-session")
	results := []error{refusal("codex_daemon_unavailable"), nil, refusal("codex_daemon_unavailable"), refusal("codex_daemon_unavailable")}
	s.VerifyOwner = func(Owner, string, string) error {
		e := results[0]
		results = results[1:]
		return e
	}
	for i := 0; i < 3; i++ {
		if e := s.Tick(); e != nil {
			t.Fatal(e)
		}
	}
	p, _ := part(s.Store.DB, "codex-terminal-daemon")
	if p.Closed != 0 {
		t.Fatal("a success did not reset consecutive failure count")
	}
	if e := s.Tick(); e != nil {
		t.Fatal(e)
	}
	p, _ = part(s.Store.DB, "codex-terminal-daemon")
	if p.Closed != 1 {
		t.Fatal("two failures after reset did not retire owner")
	}
}

func TestReplacementDoesNotInheritDaemonFailures(t *testing.T) {
	s := fixture(t)
	insertDaemonAttachment(t, s, "first-session")
	s.VerifyOwner = func(Owner, string, string) error { return refusal("codex_daemon_unavailable") }
	if e := s.Tick(); e != nil {
		t.Fatal(e)
	}
	insertDaemonAttachment(t, s, "replacement-session")
	if e := s.Tick(); e != nil {
		t.Fatal(e)
	}
	p, _ := part(s.Store.DB, "codex-terminal-daemon")
	if p.Closed != 0 || p.Session != "replacement-session" {
		t.Fatal("replacement inherited prior incarnation's failure", p)
	}
}

func TestExplicitThreadFailureRetiresImmediately(t *testing.T) {
	s := fixture(t)
	insertDaemonAttachment(t, s, "daemon-session")
	s.VerifyOwner = func(Owner, string, string) error { return refusal("codex_thread_not_loaded") }
	if e := s.Tick(); e != nil {
		t.Fatal(e)
	}
	p, _ := part(s.Store.DB, "codex-terminal-daemon")
	if p.Closed != 1 {
		t.Fatal("explicit thread failure did not retire owner")
	}
}
func participants(t *testing.T, s *Service) {
	for _, r := range []string{"sender", "recipient"} {
		_, e := s.Store.DB.Exec(`INSERT INTO participants(recipient,session,project,harness,route,target,expires,ready,closed,evidence) VALUES(?,?,?,'codex','hook','{}',?,1,0,'supervised_hook')`, r, r+"-session", t.TempDir(), now()+90)
		if e != nil {
			t.Fatal(e)
		}
	}
}
func TestDirectMessagePinningAndAck(t *testing.T) {
	s := fixture(t)
	participants(t, s)
	sent, e := s.Dispatch(Map{"op": "send", "sender": "sender", "recipient": "recipient", "body": "fixture 空间", "type": "request"})
	if e != nil {
		t.Fatal(e)
	}
	id := float64(sent["message_id"].(int64))
	inbox, e := s.Dispatch(Map{"op": "drain", "recipient": "recipient", "session": "recipient-session"})
	if e != nil || len(inbox["messages"].([]Map)) != 1 {
		t.Fatal(inbox, e)
	}
	var read any
	s.Store.DB.QueryRow(`SELECT read_at FROM messages WHERE id=?`, id).Scan(&read)
	if read != nil {
		t.Fatal("consumed before caller acknowledgement")
	}
	if _, e = s.Dispatch(Map{"op": "ack", "recipient": "recipient", "session": "wrong", "message_ids": []any{id}}); e == nil {
		t.Fatal("wrong session consumed")
	}
	ack, e := s.Dispatch(Map{"op": "ack", "recipient": "recipient", "session": "recipient-session", "message_ids": []any{id}})
	if e != nil || ack["acknowledged"] != 1 {
		t.Fatal(ack, e)
	}
	if _, e = s.Dispatch(Map{"op": "send", "sender": "recipient", "recipient": "sender", "body": "reply", "type": "reply", "reply_to": id}); e != nil {
		t.Fatal(e)
	}
	s.Store.DB.Exec(`UPDATE participants SET session='replacement' WHERE recipient='recipient'`)
	if _, e = s.Dispatch(Map{"op": "send", "sender": "recipient", "recipient": "sender", "body": "old reply", "type": "reply", "reply_to": id}); e == nil {
		t.Fatal("replaced source replied to old incarnation")
	}
}
func TestNativeReaderCannotBeAcknowledgedThroughHook(t *testing.T) {
	s := fixture(t)
	participants(t, s)
	s.Store.DB.Exec(`CREATE TABLE native_receivers(recipient TEXT,session TEXT)`)
	s.Store.DB.Exec(`INSERT INTO native_receivers VALUES('recipient','recipient-session')`)
	if _, e := s.Dispatch(Map{"op": "ack", "recipient": "recipient", "session": "recipient-session", "message_ids": []any{}}); e == nil {
		t.Fatal("native adapter ownership bypassed")
	}
}

func TestMalformedAcknowledgementIsAllOrNone(t *testing.T) {
	s := fixture(t)
	participants(t, s)
	sent, e := s.Dispatch(Map{"op": "send", "sender": "sender", "recipient": "recipient", "body": "fixture"})
	if e != nil {
		t.Fatal(e)
	}
	id := sent["message_id"].(int64)
	if _, e = s.Dispatch(Map{"op": "ack", "recipient": "recipient", "session": "recipient-session", "message_ids": []any{float64(id), "malformed"}}); e == nil || e.Error() != "invalid_request" {
		t.Fatal("malformed acknowledgement accepted", e)
	}
	var read any
	if e = s.Store.DB.QueryRow(`SELECT read_at FROM messages WHERE id=?`, id).Scan(&read); e != nil {
		t.Fatal(e)
	}
	if read != nil {
		t.Fatal("message acknowledged before malformed request rolled back")
	}
}

func TestT3ThreadKeepsSessionAtTurnEnd(t *testing.T) {
	s := fixture(t)
	s.VerifyOwner = func(Owner, string, string) error { return nil }
	m := Map{"op": "attach", "harness": "codex", "thread": "native", "project": t.TempDir(),
		"event": "SessionStart", "owner": Map{"kind": "t3", "thread": "mcp:22222222-2222-4222-8222-222222222222"}}
	first, err := s.Dispatch(m)
	if err != nil || first["delivery"] != "t3_thread" {
		t.Fatal(first, err)
	}
	m["event"] = "SessionEnd"
	if ended, err := s.Dispatch(m); err != nil || ended["closed"] != false {
		t.Fatal(ended, err)
	}
	m["event"] = "UserPromptSubmit"
	if next, err := s.Dispatch(m); err != nil || next["session"] != first["session"] {
		t.Fatal(next, err)
	}
}

func TestT3UpgradeClaimAndUnreachablePolls(t *testing.T) {
	s := fixture(t)
	project := t.TempDir()
	first, err := queryOwner(t, s, "native", project, "SessionStart")
	if err != nil {
		t.Fatal(err)
	}
	s.VerifyOwner = func(Owner, string, string) error { return nil }
	m := Map{"op": "attach", "harness": "codex", "thread": "native", "project": project,
		"event": "SessionStart", "owner": Map{"kind": "t3", "thread": "mcp:22222222-2222-4222-8222-222222222222",
			"host_pid": os.Getpid(), "host_identity": ProcessIdentity(os.Getpid())}}
	upgraded, err := s.Dispatch(m)
	if err != nil || upgraded["session"] != first["session"] {
		t.Fatal(upgraded, err)
	}
	m["thread"] = "replacement"
	replacement, err := s.Dispatch(m)
	if err != nil {
		t.Fatal(err)
	}
	old, err := part(s.Store.DB, "codex-terminal-native")
	if err != nil || old.Closed != 1 {
		t.Fatal(old, err)
	}
	recipient := replacement["recipient"].(string)
	if _, err := s.Store.DB.Exec(`INSERT INTO messages(ts,sender,recipient,type,body) VALUES(?, 'sender', ?, 'notify', 'peer body')`, now(), recipient); err != nil {
		t.Fatal(err)
	}
	firstClaim, err := s.claimT3()
	if err != nil || len(firstClaim) != 1 || firstClaim[0].recipient != recipient {
		t.Fatal(firstClaim, err)
	}
	if extra, err := s.claimT3(); err != nil || len(extra) != 0 {
		t.Fatal(extra, err)
	}
	s.VerifyOwner = func(Owner, string, string) error { return refusal("t3_thread_unavailable") }
	for poll := 1; poll <= 4; poll++ {
		s.lastT3 = time.Time{}
		if err := s.Tick(); err != nil {
			t.Fatal(err)
		}
		if err := s.Tick(); err != nil {
			t.Fatal(err)
		}
		p, err := part(s.Store.DB, recipient)
		if err != nil || (p.Closed == 1) != (poll == 4) {
			t.Fatalf("poll %d: %+v %v", poll, p, err)
		}
	}
}
