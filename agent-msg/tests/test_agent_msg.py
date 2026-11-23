import agent_msg
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from .registry_helpers import hook, session

from agent_msg import (
    fetch_unread,
    format_inbox,
    mark_read,
    open_db,
    resolve_db_path,
    send,
    wait_for_reply,
)


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / 'bus.db')
    with open_db(path) as db:
        for name in ('a', 'b', 'c', 'claude-x', 'codex-y', 'codex-auth', 'kimi-ui'):
            hook(db, tmp_path, name)
    return path


def test_resolve_db_path_prefers_explicit(db_path, monkeypatch):
    monkeypatch.setenv("AGENT_MSG_DB", "/tmp/env-override.db")
    assert resolve_db_path(db_path) == db_path


def test_resolve_db_path_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_MSG_DB", str(tmp_path / "env.db"))
    monkeypatch.chdir(tmp_path)
    assert resolve_db_path(None) == str(tmp_path / "env.db")


def test_resolve_db_path_default(monkeypatch, tmp_path):
    monkeypatch.delenv("AGENT_MSG_DB", raising=False)
    monkeypatch.chdir(tmp_path)
    assert resolve_db_path(None) == str(tmp_path / ".agent-msg" / "bus.db")


def test_open_db_creates_schema_and_wal(db_path):
    conn = open_db(db_path)
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)").fetchall()}
    assert cols == {"id", "ts", "sender", "recipient", "type", "reply_to", "body", "read_at"}
    idx = {r[1] for r in conn.execute("PRAGMA index_list(messages)").fetchall()}
    assert "idx_inbox" in idx and "idx_reply" in idx
    conn.close()


def test_open_db_creates_parent_dirs(tmp_path):
    deep = str(tmp_path / "a" / "b" / "bus.db")
    conn = open_db(deep)
    assert os.path.exists(deep)
    conn.close()


def test_open_db_enforces_type_check(db_path):
    conn = open_db(db_path)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO messages (ts, sender, recipient, type, body) VALUES (?,?,?,?,?)",
            (1.0, "a", "b", "bogus", "x"),
        )
    conn.close()


def test_send_returns_incrementing_ids(db_path):
    conn = open_db(db_path)
    id1 = send(conn, "a", "b", "hello")
    id2 = send(conn, "a", "b", "again")
    assert id2 == id1 + 1
    conn.close()


def test_send_stores_all_fields(db_path):
    conn = open_db(db_path)
    mid = send(conn, "claude-x", "codex-y", "do the thing",
               mtype="request", reply_to=None)
    row = conn.execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
    assert row["sender"] == "claude-x"
    assert row["recipient"] == "codex-y"
    assert row["type"] == "request"
    assert row["body"] == "do the thing"
    assert row["read_at"] is None
    assert row["ts"] > 0
    conn.close()


def test_send_reply_with_correlation(db_path):
    conn = open_db(db_path)
    req = send(conn, "a", "b", "question?")
    rep = send(conn, "b", "a", "answer!", mtype="reply", reply_to=req)
    row = conn.execute("SELECT * FROM messages WHERE id=?", (rep,)).fetchone()
    assert row["reply_to"] == req
    conn.close()


def test_fetch_unread_only_mine_only_unread_ordered(db_path):
    conn = open_db(db_path)
    send(conn, "a", "b", "first")
    send(conn, "a", "c", "not mine")
    second = send(conn, "a", "b", "second")
    mark_read(conn, [second])
    send(conn, "a", "b", "third")

    rows = fetch_unread(conn, "b", session=session(conn, "b"))
    assert [r["body"] for r in rows] == ["first", "third"]
    conn.close()


def test_format_inbox_empty_when_no_mail(db_path):
    assert format_inbox([]) == ""


def test_format_inbox_shows_body_and_reply_hint(db_path):
    conn = open_db(db_path)
    send(conn, "codex-auth", "kimi-ui", "Does module X export parseConfig?",
         mtype="request")
    rows = fetch_unread(conn, "kimi-ui", session=session(conn, "kimi-ui"))
    block = format_inbox(rows)
    assert "1 unread" in block
    assert "codex-auth" in block
    assert "parseConfig" in block
    assert '--reply-to %d' % rows[0]["id"] in block
    conn.close()


def test_mark_read_is_persistent(db_path):
    conn = open_db(db_path)
    mid = send(conn, "a", "b", "hello")
    mark_read(conn, [mid])
    row = conn.execute("SELECT read_at FROM messages WHERE id=?", (mid,)).fetchone()
    assert row["read_at"] is not None
    assert fetch_unread(conn, "b", session=session(conn, "b")) == []
    conn.close()


def test_crash_before_mark_read_keeps_mail(db_path):
    """Simulates a crash between printing and marking read: mail survives."""
    conn = open_db(db_path)
    send(conn, "a", "b", "important")
    rows = fetch_unread(conn, "b", session=session(conn, "b"))           # fetched...
    _block = format_inbox(rows)              # printed...
    # ...crash! mark_read never ran.
    assert len(fetch_unread(conn, "b", session=session(conn, "b"))) == 1  # redelivery, not loss
    conn.close()


def test_crash_between_print_and_mark_read_redelivers(db_path, monkeypatch):
    """If the process dies after formatting but before mark_read commits,
    the mail must still be unread on the next drain."""
    conn = open_db(db_path)
    send(conn, "a", "b", "important")
    rows = fetch_unread(conn, "b", session=session(conn, "b"))
    block = format_inbox(rows)
    assert "important" in block

    def exploding_mark_read(c, ids, ts=None):
        raise RuntimeError("simulated crash before ack")

    monkeypatch.setattr("agent_msg.mark_read", exploding_mark_read)

    # the drain caller's sequence: print(block) then mark_read -> crashes here.
    # Mail must remain undelivered for redelivery. Call via the module so the
    # patched (crashing) mark_read is exercised.
    with pytest.raises(RuntimeError):
        agent_msg.mark_read(conn, [r["id"] for r in rows])
    assert len(fetch_unread(conn, "b", session=session(conn, "b"))) == 1
    conn.close()


def test_wait_for_reply_returns_when_reply_lands(db_path):
    conn = open_db(db_path)
    writer = open_db(db_path)
    req = send(conn, "a", "b", "status?")

    def reply_later():
        time.sleep(0.3)
        send(writer, "b", "a", "all green", mtype="reply", reply_to=req)

    t = threading.Thread(target=reply_later)
    t.start()
    row = wait_for_reply(conn, req, timeout_s=5.0)
    t.join()
    assert row is not None
    assert row["sender"] == "b"
    assert row["body"] == "all green"
    conn.close()
    writer.close()


def test_wait_for_reply_times_out_returning_none(db_path):
    conn = open_db(db_path)
    req = send(conn, "a", "b", "anyone?")
    start = time.monotonic()
    row = wait_for_reply(conn, req, timeout_s=0.6)
    assert row is None
    assert time.monotonic() - start >= 0.5
    conn.close()


def test_wait_for_reply_ignores_other_correlations(db_path):
    conn = open_db(db_path)
    writer = open_db(db_path)
    mine = send(conn, "a", "b", "mine?")
    other = send(conn, "a", "c", "other?")

    def wrong_reply_then_right():
        time.sleep(0.2)
        send(writer, "c", "a", "wrong thread!", mtype="reply", reply_to=other)
        time.sleep(0.2)
        send(writer, "b", "a", "right thread!", mtype="reply", reply_to=mine)

    t = threading.Thread(target=wrong_reply_then_right)
    t.start()
    row = wait_for_reply(conn, mine, timeout_s=5.0)
    t.join()
    assert row["body"] == "right thread!"
    conn.close()
    writer.close()


SCRIPT = Path(__file__).resolve().parents[1] / "agent_msg.py"


def run_cli(args, env_extra=None, check=True):
    env = {**os.environ, "AGENT_MSG_ID": "", **(env_extra or {})}
    r = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True, text=True, env=env,
    )
    if check:
        assert r.returncode == 0, f"rc={r.returncode} stderr={r.stderr}"
    return r


def base_env(tmp_path):
    path = str(tmp_path / 'bus.db')
    with open_db(path) as db:
        for name in ('a', 'b', 'c', 'hub', 'ghost'):
            hook(db, tmp_path, name)
        sid = session(db, 'b')
    return {'AGENT_MSG_DB': path, 'AGENT_MSG_SESSION': sid}


def test_cli_send_prints_message_id(tmp_path):
    r = run_cli(["send", "--to", "b", "--from", "a", "hi"], base_env(tmp_path))
    assert r.stdout.strip().isdigit()


def test_cli_sender_requires_from_or_env(tmp_path):
    r = run_cli(["send", "--to", "b", "hi"], base_env(tmp_path), check=False)
    assert r.returncode == 2
    assert "sender unknown" in r.stderr


def test_cli_env_var_supplies_sender(tmp_path):
    env = {**base_env(tmp_path), "AGENT_MSG_ID": "claude-1"}
    r = run_cli(["send", "--to", "b", "hi"], env)
    assert r.stdout.strip().isdigit()


def test_cli_drain_prints_and_marks_read(tmp_path):
    env = base_env(tmp_path)
    run_cli(["send", "--to", "b", "--from", "a", "hello b"], env)
    out = run_cli(["drain", "--as", "b"], env).stdout
    assert "hello b" in out and "1 unread" in out
    # second drain: empty (marked read)
    assert run_cli(["drain", "--as", "b"], env).stdout == ""


def test_cli_drain_keep_unread_flag(tmp_path):
    env = base_env(tmp_path)
    run_cli(["send", "--to", "b", "--from", "a", "hello b"], env)
    run_cli(["drain", "--as", "b", "--keep-unread"], env)
    out = run_cli(["drain", "--as", "b"], env).stdout
    assert "hello b" in out  # still there


def test_cli_drain_empty_inbox_prints_nothing(tmp_path):
    assert run_cli(["drain", "--as", "nobody"], {**base_env(tmp_path), "AGENT_MSG_SESSION": ""}).stdout == ""


def test_cli_list_filters(tmp_path):
    env = base_env(tmp_path)
    run_cli(["send", "--to", "b", "--from", "a", "one"], env)
    run_cli(["send", "--to", "c", "--from", "a", "two"], env)
    out = run_cli(["list", "--to", "b"], env).stdout
    assert "one" in out and "two" not in out


def test_cli_sync_roundtrip_end_to_end(tmp_path):
    env = base_env(tmp_path)

    def replier():
        import sqlite3 as s3
        for _ in range(400):
            try:
                c = s3.connect(env["AGENT_MSG_DB"])
                row = c.execute(
                    "SELECT id FROM messages WHERE recipient='b' AND type='request'"
                ).fetchone()
                c.close()
            except s3.OperationalError:
                row = None
            if row:
                break
            time.sleep(0.05)
        run_cli(["send", "--to", "a", "--from", "b", "--type", "reply",
                 "--reply-to", str(row[0]), "yes, exported"], env)

    t = threading.Thread(target=replier)
    t.start()
    r = run_cli(["send", "--to", "b", "--from", "a", "--wait",
                 "--timeout", "15", "does parseConfig exist?"], env)
    t.join()
    assert "yes, exported" in r.stdout
    assert re.search(r"\[\d+\]", r.stdout)  # "[<req-id>] reply from b:"


def test_cli_wait_timeout_exit_code_3(tmp_path):
    r = run_cli(["send", "--to", "ghost", "--from", "a", "--wait",
                 "--timeout", "1", "you there?"], base_env(tmp_path), check=False)
    assert r.returncode == 3
    assert "no reply" in r.stderr
    assert "ghost" in r.stderr


def test_cli_drain_stdout_failure_keeps_mail_unread(tmp_path):
    """SIGPIPE/closed-stdout during drain must not consume mail."""
    env = base_env(tmp_path)
    run_cli(["send", "--to", "b", "--from", "a", "survive me"], env)
    # Run drain with stdout wired to a pipe whose read end is already
    # closed: the write fails (BrokenPipeError) before ack can happen.
    r_fd, w_fd = os.pipe()
    os.close(r_fd)
    try:
        proc = subprocess.Popen(
            [sys.executable, str(SCRIPT), "drain", "--as", "b"],
            stdout=w_fd,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "AGENT_MSG_ID": "", **env},
        )
        rc = proc.wait(timeout=30)
    finally:
        os.close(w_fd)
    assert rc != 0
    # Mail was NOT acknowledged -> still unread and redelivered next time.
    out = run_cli(["drain", "--as", "b"], env).stdout
    assert "survive me" in out


def test_concurrent_senders_no_lost_writes(tmp_path):
    """15 parallel CLI senders (the production fleet shape) lose no messages."""
    n = 15
    procs = []
    for i in range(n):
        procs.append(subprocess.Popen(
            [sys.executable, str(SCRIPT), "send",
             "--to", "hub", "--from", f"agent-{i}", f"payload {i}"],
            env={**os.environ, "AGENT_MSG_ID": "", **base_env(tmp_path)},
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ))
    results = [p.communicate() for p in procs]
    codes = [p.returncode for p in procs]
    failed = [(c, err) for c, (out, err) in zip(codes, results) if c != 0]
    assert not failed, f"failed senders: {failed}"

    out = run_cli(["list", "--to", "hub"], base_env(tmp_path)).stdout
    received = [line for line in out.splitlines() if line.strip()]
    assert len(received) == n
    for i in range(n):
        assert any(f"payload {i}" in line for line in received), \
            f"lost write: payload {i} not in list output"
