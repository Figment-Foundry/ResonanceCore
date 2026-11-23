package state

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type Store struct{ DB *sql.DB }
type Owner struct {
	Kind     string   `json:"kind"`
	PID      int      `json:"pid,omitempty"`
	Identity []string `json:"identity,omitempty"`
	Thread   string   `json:"thread,omitempty"`
	Socket   string   `json:"socket,omitempty"`
}
type Attachment struct {
	Recipient, Harness, Thread, Session, Project string
	Owner                                        Owner
}

func Open(path string) (*Store, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY,ts REAL NOT NULL,sender TEXT NOT NULL,recipient TEXT NOT NULL,type TEXT NOT NULL CHECK(type IN ('notify','request','reply')),reply_to INTEGER,body TEXT NOT NULL,read_at REAL); CREATE INDEX IF NOT EXISTS idx_inbox ON messages(recipient,read_at) WHERE read_at IS NULL; CREATE INDEX IF NOT EXISTS idx_reply ON messages(reply_to) WHERE reply_to IS NOT NULL; CREATE TABLE IF NOT EXISTS participants(recipient TEXT PRIMARY KEY,session TEXT NOT NULL UNIQUE,project TEXT NOT NULL,harness TEXT NOT NULL,route TEXT NOT NULL CHECK(route IN ('hook','codex')),target TEXT NOT NULL,expires REAL NOT NULL,ready INTEGER NOT NULL DEFAULT 0,closed INTEGER NOT NULL DEFAULT 0,evidence TEXT NOT NULL DEFAULT 'unverified'); CREATE TABLE IF NOT EXISTS service_attachments(recipient TEXT PRIMARY KEY,harness TEXT NOT NULL,thread TEXT NOT NULL,session TEXT NOT NULL,project TEXT NOT NULL,owner TEXT NOT NULL,created REAL NOT NULL);`)
	if e != nil {
		db.Close()
		return nil, e
	}
	rows, e := db.Query("PRAGMA table_info(participants)")
	if e != nil {
		db.Close()
		return nil, e
	}
	hasEvidence := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var defaultValue any
		rows.Scan(&cid, &name, &typ, &notnull, &defaultValue, &pk)
		if name == "evidence" {
			hasEvidence = true
		}
	}
	rows.Close()
	if !hasEvidence {
		if _, e = db.Exec("ALTER TABLE participants ADD COLUMN evidence TEXT NOT NULL DEFAULT 'unverified'"); e != nil {
			db.Close()
			return nil, e
		}
	}
	_, e = db.Exec(`CREATE TABLE IF NOT EXISTS direct_targets(message_id INTEGER PRIMARY KEY REFERENCES messages(id),session TEXT NOT NULL,route TEXT NOT NULL,target TEXT NOT NULL,sender_session TEXT,expires REAL NOT NULL,reason TEXT);CREATE TABLE IF NOT EXISTS broadcasts(id TEXT PRIMARY KEY,sender TEXT NOT NULL,key TEXT NOT NULL,ts REAL NOT NULL,body TEXT NOT NULL,project TEXT,UNIQUE(sender,key));CREATE TABLE IF NOT EXISTS broadcast_recipients(broadcast_id TEXT NOT NULL REFERENCES broadcasts(id),recipient TEXT NOT NULL,message_id INTEGER NOT NULL UNIQUE REFERENCES messages(id),session TEXT NOT NULL,route TEXT NOT NULL,target TEXT NOT NULL,skipped_reason TEXT,PRIMARY KEY(broadcast_id,recipient));CREATE TABLE IF NOT EXISTS broadcast_origins(broadcast_id TEXT PRIMARY KEY,sender_session TEXT);`)
	if e != nil {
		db.Close()
		return nil, e
	}
	return &Store{db}, nil
}
func (s *Store) Close() { s.DB.Close() }
