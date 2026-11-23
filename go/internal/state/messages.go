package state

import (
	"database/sql"
	"encoding/json"
	"strings"
)

func bounded(v string, limit int) bool {
	return strings.TrimSpace(v) != "" && len(v) <= limit && !strings.ContainsRune(v, 0)
}
func sendable(q querier, p *participant) bool {
	if !live(p, "") || p.Ready == 0 {
		return false
	}
	b, e := binding(q, p.Recipient)
	if e != nil {
		return false
	}
	if p.Route == "codex" {
		return p.Evidence == "thread_attachment" && b != nil && b["enabled"] == 1 && targetEqual(p.Target, b)
	}
	return b == nil
}
func (s *Service) send(m Map) (Map, error) {
	sender, recipient, body := text(m, "sender"), text(m, "recipient"), text(m, "body")
	typ := text(m, "type")
	if typ == "" {
		typ = "notify"
	}
	if !bounded(sender, 128) || !bounded(recipient, 128) || !bounded(body, 60000) || (typ != "notify" && typ != "request" && typ != "reply") {
		return nil, refusal("invalid_message")
	}
	tx, e := s.Store.DB.Begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	target, e := part(tx, recipient)
	if e != nil {
		return nil, e
	}
	if !sendable(tx, target) {
		return nil, refusal("recipient_unavailable")
	}
	if expected := text(m, "expected_session"); expected != "" && expected != target.Session {
		return nil, refusal("session_replaced")
	}
	source, e := part(tx, sender)
	if e != nil {
		return nil, e
	}
	var senderSession any
	if live(source, "") {
		senderSession = source.Session
	}
	var reply any
	if value, ok := m["reply_to"]; ok && value != nil {
		n, ok := value.(float64)
		if !ok || n <= 0 || n != float64(int64(n)) {
			return nil, refusal("reply_session_mismatch")
		}
		reply = int64(n)
		var originalSender, originalRecipient string
		if tx.QueryRow(`SELECT sender,recipient FROM messages WHERE id=?`, reply).Scan(&originalSender, &originalRecipient) != nil {
			return nil, refusal("reply_session_mismatch")
		}
		var originSender sql.NullString
		var originTarget string
		e = tx.QueryRow(`SELECT sender_session,session FROM direct_targets WHERE message_id=?`, reply).Scan(&originSender, &originTarget)
		if e == sql.ErrNoRows {
			e = tx.QueryRow(`SELECT o.sender_session,r.session FROM broadcast_recipients r JOIN broadcast_origins o ON o.broadcast_id=r.broadcast_id WHERE r.message_id=?`, reply).Scan(&originSender, &originTarget)
		}
		if e != nil || originalSender != recipient || originalRecipient != sender || !originSender.Valid || originSender.String != target.Session || senderSession != originTarget {
			return nil, refusal("reply_session_mismatch")
		}
	}
	timestamp := now()
	result, e := tx.Exec(`INSERT INTO messages(ts,sender,recipient,type,reply_to,body) VALUES(?,?,?,?,?,?)`, timestamp, sender, recipient, typ, reply, body)
	if e != nil {
		return nil, e
	}
	id, e := result.LastInsertId()
	if e != nil {
		return nil, e
	}
	_, e = tx.Exec(`INSERT INTO direct_targets(message_id,session,route,target,sender_session,expires) VALUES(?,?,?,?,?,?)`, id, target.Session, target.Route, target.Target, senderSession, timestamp+300)
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return Map{"message_id": id}, nil
}
func guard(q querier, id int64, recipient, session string) bool {
	p, e := part(q, recipient)
	if e != nil || !live(p, session) || session == "" || p.Ready == 0 || p.Route != "hook" {
		return false
	}
	b, e := binding(q, recipient)
	if e != nil || b != nil {
		return false
	}
	var pinned, route, target string
	var reason sql.NullString
	var expires float64
	e = q.QueryRow(`SELECT session,route,target,reason,expires FROM direct_targets WHERE message_id=?`, id).Scan(&pinned, &route, &target, &reason, &expires)
	if e == sql.ErrNoRows {
		var skip sql.NullString
		e = q.QueryRow(`SELECT session,route,target,skipped_reason FROM broadcast_recipients WHERE message_id=? AND recipient=?`, id, recipient).Scan(&pinned, &route, &target, &skip)
		if e != nil || skip.Valid {
			return false
		}
	} else if e != nil || reason.Valid || expires <= now() {
		return false
	}
	return pinned == p.Session && route == p.Route && target == p.Target
}
func (s *Service) inbox(m Map, consume bool) (Map, error) {
	recipient, session := text(m, "recipient"), text(m, "session")
	if !bounded(recipient, 128) {
		return nil, refusal("invalid_recipient")
	}
	tx, e := s.Store.DB.Begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	if consume {
		p, e := part(tx, recipient)
		if e != nil {
			return nil, e
		}
		if !live(p, session) || session == "" {
			return nil, refusal("session_expired_or_replaced")
		}
		bound, e := binding(tx, recipient)
		if e != nil {
			return nil, e
		}
		if bound != nil || p.Route != "hook" {
			return nil, refusal("recipient_managed_by_codex_bridge")
		}
		if hasTable(tx, "native_receivers") {
			var n int
			if tx.QueryRow(`SELECT 1 FROM native_receivers WHERE recipient=? AND session=?`, recipient, session).Scan(&n) == nil {
				return nil, refusal("recipient_managed_by_native_adapter")
			}
		}
		if _, e = tx.Exec(`UPDATE participants SET expires=? WHERE recipient=? AND session=? AND closed=0`, now()+90, recipient, session); e != nil {
			return nil, e
		}
	}
	rows, e := tx.Query(`SELECT m.id,m.ts,m.sender,m.type,m.reply_to,m.body,r.broadcast_id,d.sender_session,d.session FROM messages m LEFT JOIN broadcast_recipients r ON r.message_id=m.id LEFT JOIN direct_targets d ON d.message_id=m.id WHERE m.recipient=? AND m.read_at IS NULL ORDER BY m.id LIMIT 1000`, recipient)
	if e != nil {
		return nil, e
	}
	all := []Map{}
	for rows.Next() {
		var id int64
		var ts float64
		var sender, typ, body string
		var reply sql.NullInt64
		var bid, senderSession, recipientSession sql.NullString
		if e = rows.Scan(&id, &ts, &sender, &typ, &reply, &body, &bid, &senderSession, &recipientSession); e != nil {
			rows.Close()
			return nil, e
		}
		var r, b, ss, rs any
		if reply.Valid {
			r = reply.Int64
		}
		if bid.Valid {
			b = bid.String
		}
		if senderSession.Valid {
			ss = senderSession.String
		}
		if recipientSession.Valid {
			rs = recipientSession.String
		}
		all = append(all, Map{"id": id, "ts": ts, "sender": sender, "type": typ, "reply_to": r, "body": body, "broadcast_id": b, "sender_session": ss, "recipient_session": rs})
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	selected := []Map{}
	for _, message := range all {
		if !consume || guard(tx, message["id"].(int64), recipient, session) {
			selected = append(selected, message)
		}
	}
	result := Map{"messages": selected, "recipient": recipient, "session": session}
	b, _ := json.Marshal(result)
	if len(b) > 1000000 {
		return nil, refusal("response_limit")
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return result, nil
}
func (s *Service) ack(m Map) (Map, error) {
	recipient, session := text(m, "recipient"), text(m, "session")
	ids, ok := m["message_ids"].([]any)
	if !ok || len(ids) > 1000 {
		return nil, refusal("invalid_request")
	}
	tx, e := s.Store.DB.Begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	p, e := part(tx, recipient)
	if e != nil {
		return nil, e
	}
	if !live(p, session) || session == "" || p.Route != "hook" {
		return nil, refusal("session_expired_or_replaced")
	}
	bound, e := binding(tx, recipient)
	if e != nil {
		return nil, e
	}
	if bound != nil {
		return nil, refusal("recipient_managed_by_codex_bridge")
	}
	if hasTable(tx, "native_receivers") {
		var n int
		if tx.QueryRow(`SELECT 1 FROM native_receivers WHERE recipient=? AND session=?`, recipient, session).Scan(&n) == nil {
			return nil, refusal("recipient_managed_by_native_adapter")
		}
	}
	for _, v := range ids {
		n, ok := v.(float64)
		if !ok || n <= 0 || n != float64(int64(n)) {
			return nil, refusal("invalid_request")
		}
		id := int64(n)
		var exact string
		if tx.QueryRow(`SELECT recipient FROM messages WHERE id=?`, id).Scan(&exact) != nil || exact != recipient || !guard(tx, id, recipient, session) {
			return nil, refusal("session_replaced")
		}
		if _, e = tx.Exec(`UPDATE messages SET read_at=? WHERE id=? AND recipient=? AND read_at IS NULL`, now(), id, recipient); e != nil {
			return nil, e
		}
	}
	if _, e = tx.Exec(`UPDATE participants SET ready=1,expires=? WHERE recipient=? AND session=? AND closed=0`, now()+90, recipient, session); e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return Map{"acknowledged": len(ids)}, nil
}
