package state

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Map = map[string]any
type Service struct {
	Store       *Store
	Bus         string
	Started     float64
	mu          sync.Mutex
	tickMu      sync.Mutex
	VerifyOwner func(Owner, string, string) error
	failures    map[string]int
	lastT3      time.Time
}

func NewService(bus string) (*Service, error) {
	s, e := Open(bus)
	if e != nil {
		return nil, e
	}
	if _, e := s.DB.Exec(t3Schema); e != nil {
		s.Close()
		return nil, e
	}
	return &Service{Store: s, Bus: bus, Started: now(), VerifyOwner: Verify, failures: map[string]int{}}, nil
}
func (s *Service) verify(owner Owner, project, thread string) error {
	return s.VerifyOwner(owner, project, thread)
}
func failureKey(a attachment) string { return a.Recipient + "\x00" + a.Session }
func now() float64                   { return float64(time.Now().UnixNano()) / 1e9 }
func refusal(code string) error      { return errors.New(code) }
func ErrorCode(e error) string {
	if e != nil && regexp.MustCompile(`^[a-z_]{1,64}$`).MatchString(e.Error()) {
		return e.Error()
	}
	return "internal_error"
}
func text(m Map, key string) string { v, _ := m[key].(string); return v }
func sessionID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	s := hex.EncodeToString(b)
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func Project(value string) (string, error) {
	if !filepath.IsAbs(value) {
		return "", refusal("project_must_be_existing_absolute_directory")
	}
	st, e := os.Stat(value)
	if e != nil || !st.IsDir() {
		return "", refusal("project_must_be_existing_absolute_directory")
	}
	return filepath.EvalSymlinks(filepath.Clean(value))
}
func threadKey(harness, thread string) error {
	if harness != "codex" && harness != "claude" && harness != "kimi" {
		return refusal("unsupported_native_harness")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$`).MatchString(thread) {
		return refusal("invalid_thread_identity")
	}
	return nil
}
func parseOwner(value any) (Owner, error) {
	b, e := json.Marshal(value)
	if e != nil {
		return Owner{}, refusal("owner_evidence_required")
	}
	var owner Owner
	if json.Unmarshal(b, &owner) != nil {
		return owner, refusal("owner_evidence_required")
	}
	if owner.Kind == "pid" {
		if owner.PID <= 1 || len(owner.Identity) != 2 || len(owner.Identity[0]) > 64 || len(owner.Identity[1]) > 64 {
			return owner, refusal("owner_evidence_required")
		}
		owner.Socket = ""
		owner.Thread = ""
		return owner, nil
	}
	if owner.Kind == "t3" && canonicalT3Thread.MatchString(owner.Thread) {
		return Owner{Kind: "t3", Thread: owner.Thread}, nil
	}
	if owner.Kind == "codex_thread" && strings.HasPrefix(owner.Socket, "/") && len(owner.Socket) <= 200 {
		return Owner{Kind: owner.Kind, Socket: owner.Socket}, nil
	}
	return owner, refusal("owner_evidence_required")
}
func Verify(owner Owner, project, thread string) error {
	if owner.Kind == "pid" {
		if !reflect.DeepEqual(ProcessIdentity(owner.PID), owner.Identity) {
			return refusal("owner_not_live")
		}
		return nil
	}
	if owner.Kind == "t3" {
		return verifyT3(owner.Thread)
	}
	return VerifyCodex(owner.Socket, project, thread)
}

type attachment struct {
	Recipient, Harness, Thread, Session, Project, Encoded string
	Owner                                                 Owner
	Created                                               float64
}
type participant struct {
	Recipient, Session, Project, Harness, Route, Target, Evidence string
	Expires                                                       float64
	Ready, Closed                                                 int
}
type querier interface {
	QueryRow(string, ...any) *sql.Row
	Exec(string, ...any) (sql.Result, error)
}

func part(q querier, recipient string) (*participant, error) {
	p := participant{}
	e := q.QueryRow(`SELECT recipient,session,project,harness,route,target,expires,ready,closed,evidence FROM participants WHERE recipient=?`, recipient).Scan(&p.Recipient, &p.Session, &p.Project, &p.Harness, &p.Route, &p.Target, &p.Expires, &p.Ready, &p.Closed, &p.Evidence)
	if e == sql.ErrNoRows {
		return nil, nil
	}
	return &p, e
}
func attached(q querier, recipient string) (*attachment, error) {
	a := attachment{}
	e := q.QueryRow(`SELECT recipient,harness,thread,session,project,owner,created FROM service_attachments WHERE recipient=?`, recipient).Scan(&a.Recipient, &a.Harness, &a.Thread, &a.Session, &a.Project, &a.Encoded, &a.Created)
	if e == sql.ErrNoRows {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	if json.Unmarshal([]byte(a.Encoded), &a.Owner) != nil {
		return nil, refusal("owner_evidence_required")
	}
	return &a, nil
}
func live(p *participant, session string) bool {
	return p != nil && p.Closed == 0 && p.Expires > now() && (session == "" || p.Session == session)
}
func retire(q querier, a *attachment) error {
	if _, e := q.Exec(`UPDATE participants SET closed=1,ready=0 WHERE recipient=? AND session=?`, a.Recipient, a.Session); e != nil {
		return e
	}
	_, e := q.Exec(`DELETE FROM service_attachments WHERE recipient=? AND session=?`, a.Recipient, a.Session)
	return e
}
func hasTable(q querier, name string) bool {
	var n int
	return q.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&n) == nil
}
func binding(q querier, recipient string) (Map, error) {
	if !hasTable(q, "codex_bindings") {
		return nil, nil
	}
	var thread, project, socket, transport, exe, home string
	var enabled int
	e := q.QueryRow(`SELECT thread_id,project,socket,transport,executable,codex_home,enabled FROM codex_bindings WHERE recipient=?`, recipient).Scan(&thread, &project, &socket, &transport, &exe, &home, &enabled)
	if e == sql.ErrNoRows {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	return Map{"thread_id": thread, "project": project, "socket": socket, "transport": transport, "executable": exe, "codex_home": home, "enabled": enabled}, nil
}
func targetEqual(target string, bound Map) bool {
	var m Map
	if json.Unmarshal([]byte(target), &m) != nil {
		return false
	}
	for _, k := range []string{"thread_id", "project", "socket", "transport", "executable", "codex_home"} {
		if m[k] != bound[k] {
			return false
		}
	}
	return len(m) == 6
}
func boundTarget(bound Map) string {
	keys := []string{"codex_home", "executable", "project", "socket", "thread_id", "transport"}
	parts := []string{}
	for _, k := range keys {
		b, _ := json.Marshal(bound[k])
		parts = append(parts, `"`+k+`": `+asciiJSON(b))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
func asciiJSON(b []byte) string {
	var out strings.Builder
	for _, r := range string(b) {
		if r <= 127 {
			out.WriteRune(r)
		} else if r <= 65535 {
			fmt.Fprintf(&out, `\u%04x`, r)
		} else {
			r -= 65536
			fmt.Fprintf(&out, `\u%04x\u%04x`, 0xd800+(r>>10), 0xdc00+(r&1023))
		}
	}
	return out.String()
}
func (s *Service) attach(m Map) (Map, error) {
	h, t, event := text(m, "harness"), text(m, "thread"), text(m, "event")
	if e := threadKey(h, t); e != nil {
		return nil, e
	}
	if event != "SessionStart" && event != "UserPromptSubmit" && event != "SessionEnd" {
		return nil, refusal("unsupported_hook")
	}
	owner, e := parseOwner(m["owner"])
	if e != nil {
		return nil, e
	}
	project, e := Project(text(m, "project"))
	if e != nil {
		return nil, e
	}
	if event != "SessionEnd" {
		if e = s.verify(owner, project, t); e != nil {
			return nil, e
		}
	}
	r := h + "-terminal-" + t
	a, e := attached(s.Store.DB, r)
	if e != nil {
		return nil, e
	}
	if event == "SessionEnd" && (owner.Kind == "t3" || a != nil && a.Owner.Kind == "t3") {
		return Map{"recipient": r, "closed": false}, nil
	}
	p, e := part(s.Store.DB, r)
	if e != nil {
		return nil, e
	}
	retired := false
	if a != nil {
		if live(p, a.Session) {
			matchingOwner := reflect.DeepEqual(a.Owner, owner) || t3Upgrade(a.Owner, owner, m["owner"])
			if matchingOwner && a.Project != project {
				return nil, refusal("session_scope_changed")
			}
			if !matchingOwner {
				if a.Owner.Kind == "t3" || s.verify(a.Owner, a.Project, a.Thread) == nil {
					return nil, refusal("thread_already_attached")
				}
				if e = retire(s.Store.DB, a); e != nil {
					return nil, e
				}
				a = nil
				retired = true
			}
		} else {
			if e = retire(s.Store.DB, a); e != nil {
				return nil, e
			}
			a = nil
			retired = true
		}
	}
	if event == "SessionEnd" {
		if a != nil {
			if e = retire(s.Store.DB, a); e != nil {
				return nil, e
			}
		}
		return Map{"recipient": r, "closed": a != nil || retired}, nil
	}
	tx, e := s.Store.DB.Begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	p, e = part(tx, r)
	if e != nil {
		return nil, e
	}
	bound, e := binding(tx, r)
	if e != nil {
		return nil, e
	}
	sid, route := "", "hook"
	if a != nil {
		sid = a.Session
		if !live(p, sid) {
			return nil, refusal("session_expired_or_replaced")
		}
		route = p.Route
		_, e = tx.Exec(`UPDATE participants SET expires=? WHERE recipient=? AND session=? AND closed=0`, now()+90, r, sid)
	} else {
		if live(p, "") {
			if p.Harness != h || p.Project != project {
				return nil, refusal("recipient_already_active")
			}
			sid = p.Session
			route = p.Route
		} else {
			target, evidence := "{}", "supervised_hook"
			if bound != nil {
				if bound["thread_id"] != t || bound["project"] != project || bound["transport"] != "queue" {
					return nil, refusal("native_binding_mismatch")
				}
				route = "codex"
				target = boundTarget(bound)
				evidence = "thread_attachment"
			}
			sid = sessionID()
			_, e = tx.Exec(`INSERT INTO participants(recipient,session,project,harness,route,target,expires,ready,closed,evidence) VALUES(?,?,?,?,?,?,?,0,0,?) ON CONFLICT(recipient) DO UPDATE SET session=excluded.session,project=excluded.project,harness=excluded.harness,route=excluded.route,target=excluded.target,expires=excluded.expires,ready=0,closed=0,evidence=excluded.evidence`, r, sid, project, h, route, target, now()+90, evidence)
		}
		if e != nil {
			return nil, e
		}
		encoded, _ := json.Marshal(owner)
		_, e = tx.Exec(`INSERT OR REPLACE INTO service_attachments VALUES(?,?,?,?,?,?,?)`, r, h, t, sid, project, string(encoded), now())
	}
	if e != nil {
		return nil, e
	}
	if event == "UserPromptSubmit" && route == "codex" {
		p, e = part(tx, r)
		if e != nil {
			return nil, e
		}
		if bound == nil || bound["thread_id"] != t || bound["project"] != project || !targetEqual(p.Target, bound) {
			return nil, refusal("native_binding_mismatch")
		}
		_, e = tx.Exec(`UPDATE participants SET ready=1 WHERE recipient=? AND session=?`, r, sid)
		if e != nil {
			return nil, e
		}
	}
	result := Map{"recipient": r, "session": sid, "route": route, "project": project}
	if owner.Kind == "t3" {
		if e = bindT3(tx, r, sid, owner, event, route); e != nil {
			return nil, e
		}
		if route == "hook" {
			result["delivery"] = "t3_thread"
		}
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	if owner.Kind == "t3" {
		delete(s.failures, r+"\x00"+sid)
	}
	return result, nil
}
func (s *Service) attachments() ([]attachment, error) {
	rows, e := s.Store.DB.Query(`SELECT recipient,harness,thread,session,project,owner,created FROM service_attachments ORDER BY recipient`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	all := []attachment{}
	for rows.Next() {
		a := attachment{}
		if e = rows.Scan(&a.Recipient, &a.Harness, &a.Thread, &a.Session, &a.Project, &a.Encoded, &a.Created); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(a.Encoded), &a.Owner); e != nil {
			return nil, e
		}
		all = append(all, a)
	}
	return all, rows.Err()
}
func (s *Service) Tick() error {
	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, e := s.attachments()
	if e != nil {
		return e
	}
	pollT3 := time.Since(s.lastT3) >= 30*time.Second
	if pollT3 {
		s.lastT3 = time.Now()
	}
	for _, a := range rows {
		p, e := part(s.Store.DB, a.Recipient)
		if e != nil {
			return e
		}
		if !live(p, a.Session) {
			if _, e = s.Store.DB.Exec(`DELETE FROM service_attachments WHERE recipient=? AND session=?`, a.Recipient, a.Session); e != nil {
				return e
			}
			delete(s.failures, failureKey(a))
			continue
		}
		e = nil
		if a.Owner.Kind != "t3" {
			e = s.verify(a.Owner, a.Project, a.Thread)
		} else if pollT3 {
			// The probe can block. Discard its result if a hook renewed or
			// replaced this incarnation while it was running.
			s.mu.Unlock()
			probe := s.verify(a.Owner, a.Project, a.Thread)
			s.mu.Lock()
			fresh, err := part(s.Store.DB, a.Recipient)
			if err != nil {
				return err
			}
			if !live(fresh, a.Session) || fresh.Expires != p.Expires {
				continue
			}
			e = probe
		}
		key := failureKey(a)
		if e != nil {
			if a.Owner.Kind == "t3" && e.Error() == "t3_thread_unavailable" || a.Owner.Kind == "codex_thread" && e.Error() == "codex_daemon_unavailable" {
				limit := 2
				if a.Owner.Kind == "t3" {
					limit = 4
				}
				s.failures[key]++
				if s.failures[key] >= limit {
					if e = retire(s.Store.DB, &a); e != nil {
						return e
					}
					delete(s.failures, key)
					continue
				}
				if _, e = s.Store.DB.Exec(`UPDATE participants SET expires=? WHERE recipient=? AND session=? AND closed=0`, now()+90, a.Recipient, a.Session); e != nil {
					return e
				}
				continue
			}
			delete(s.failures, key)
			if e = retire(s.Store.DB, &a); e != nil {
				return e
			}
		} else {
			if a.Owner.Kind != "t3" || pollT3 {
				delete(s.failures, key)
			}
			if _, e = s.Store.DB.Exec(`UPDATE participants SET expires=? WHERE recipient=? AND session=? AND closed=0`, now()+90, a.Recipient, a.Session); e != nil {
				return e
			}
		}
	}
	return nil
}
func (s *Service) Dispatch(m Map) (Map, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch text(m, "op") {
	case "ping":
		return Map{"version": "0.6.0"}, nil
	case "stop":
		return Map{"stopping": true}, nil
	case "attach":
		return s.attach(m)
	case "lookup":
		h, t := text(m, "harness"), text(m, "thread")
		if e := threadKey(h, t); e != nil {
			return nil, e
		}
		r := h + "-terminal-" + t
		a, e := attached(s.Store.DB, r)
		if e != nil {
			return nil, e
		}
		p, e := part(s.Store.DB, r)
		if e != nil {
			return nil, e
		}
		if a == nil || !live(p, a.Session) || s.verify(a.Owner, a.Project, a.Thread) != nil {
			return nil, refusal("native_session_not_attached")
		}
		return Map{"recipient": r, "session": a.Session, "project": a.Project, "bus": s.Bus, "owner": a.Owner.Kind}, nil
	case "status":
		all, e := s.attachments()
		if e != nil {
			return nil, e
		}
		rows := []Map{}
		for i, a := range all {
			if i >= 100 {
				break
			}
			rows = append(rows, Map{"recipient": a.Recipient, "owner": a.Owner.Kind, "project": a.Project})
		}
		return Map{"pid": os.Getpid(), "version": "0.6.0", "bus": s.Bus, "started": s.Started, "attachments": rows}, nil
	case "send":
		return s.send(m)
	case "ack":
		return s.ack(m)
	case "drain", "list":
		return s.inbox(m, text(m, "op") == "drain")
	}
	return nil, refusal("unsupported_operation")
}
