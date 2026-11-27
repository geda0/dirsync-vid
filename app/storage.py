import json
import sqlite3
import threading
from dataclasses import dataclass

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  external_id TEXT PRIMARY KEY,
  email TEXT,
  first_name TEXT,
  last_name TEXT,
  department TEXT,
  title TEXT,
  status TEXT NOT NULL,
  provider_last_updated TEXT NOT NULL,
  synced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  external_id TEXT NOT NULL,
  action TEXT NOT NULL,
  details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT,
  started_at TEXT,
  finished_at TEXT,
  cursor_saved TEXT,
  pages INTEGER,
  users_seen INTEGER
);
"""

FIELDS = ("email", "first_name", "last_name", "department", "title", "status")


@dataclass(frozen=True)
class User:
    external_id: str
    email: str
    first_name: str
    last_name: str
    department: str
    title: str
    status: str
    provider_last_updated: str


class Storage:
    def __init__(self, path=":memory:"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.conn.executescript(SCHEMA)

    def query(self, sql, params=()):
        with self.lock:
            return self.conn.execute(sql, params).fetchall()

    def execute(self, sql, params=()):
        with self.lock:
            self.conn.execute(sql, params)
            self.conn.commit()

    def upsert(self, user, now):
        with self.lock:
            exists = self.conn.execute(
                "SELECT 1 FROM users WHERE external_id = ?", (user.external_id,)
            ).fetchone()
            self.conn.execute(
                """
                INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(external_id) DO UPDATE SET
                  email = excluded.email,
                  first_name = excluded.first_name,
                  last_name = excluded.last_name,
                  department = excluded.department,
                  title = excluded.title,
                  status = excluded.status,
                  provider_last_updated = excluded.provider_last_updated,
                  synced_at = excluded.synced_at
                """,
                (user.external_id, *(getattr(user, f) for f in FIELDS), user.provider_last_updated, now),
            )
            self.conn.execute(
                "INSERT INTO audit_log (at, external_id, action, details) VALUES (?, ?, ?, ?)",
                (now, user.external_id, "user.updated" if exists else "user.created",
                 json.dumps({f: getattr(user, f) for f in FIELDS})),
            )
            self.conn.commit()

    def get(self, external_id):
        rows = self.query("SELECT * FROM users WHERE external_id = ?", (external_id,))
        return User(**{k: rows[0][k] for k in User.__dataclass_fields__}) if rows else None

    def all(self):
        return {r["external_id"]: User(**{k: r[k] for k in User.__dataclass_fields__})
                for r in self.query("SELECT * FROM users")}

    def page(self, search, limit, offset):
        like = f"%{search.lower()}%"
        where = "WHERE lower(email || ' ' || first_name || ' ' || last_name || ' ' || department || ' ' || title) LIKE ?"
        rows = self.query(f"SELECT * FROM users {where} ORDER BY external_id LIMIT ? OFFSET ?", (like, limit, offset))
        total = self.query(f"SELECT count(*) AS n FROM users {where}", (like,))[0]["n"]
        return [dict(r) for r in rows], total

    def audit(self, external_id):
        return [r["action"] for r in self.query(
            "SELECT action FROM audit_log WHERE external_id = ? ORDER BY id", (external_id,))]

    def audit_count(self):
        return self.query("SELECT count(*) AS n FROM audit_log")[0]["n"]

    def record_run(self, kind, started_at, finished_at, cursor_saved, pages, users_seen):
        self.execute(
            "INSERT INTO sync_runs (kind, started_at, finished_at, cursor_saved, pages, users_seen) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (kind, started_at, finished_at, cursor_saved, pages, users_seen),
        )

    def runs(self):
        return [dict(r) for r in self.query("SELECT * FROM sync_runs ORDER BY id")]

    def cursor(self):
        rows = self.query("SELECT cursor_saved FROM sync_runs ORDER BY id DESC LIMIT 1")
        return rows[0]["cursor_saved"] if rows else None
