import sqlite3
from datetime import datetime, timezone

from models import EnforcementAction

DB_PATH = "seen_actions.db"


class DiffEngine:
    def __init__(self, db_path: str = DB_PATH, check_same_thread: bool = True):
        self.conn = sqlite3.connect(db_path, check_same_thread=check_same_thread)
        self._init_db()

    def _init_db(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_actions (
                fingerprint TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                title TEXT,
                url TEXT,
                date TEXT,
                first_seen TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def check_new(self, actions: list[EnforcementAction]) -> list[EnforcementAction]:
        if not actions:
            return []

        new_actions = []
        for action in actions:
            cursor = self.conn.execute(
                "SELECT 1 FROM seen_actions WHERE fingerprint = ?",
                (action.fingerprint,)
            )
            if cursor.fetchone() is None:
                new_actions.append(action)
        return new_actions

    def mark_seen(self, actions: list[EnforcementAction]):
        now = datetime.now(timezone.utc).isoformat()
        for action in actions:
            self.conn.execute(
                """INSERT OR IGNORE INTO seen_actions
                   (fingerprint, source, title, url, date, first_seen)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (action.fingerprint, action.source, action.title,
                 action.url, action.date, now)
            )
        self.conn.commit()

    def count(self) -> int:
        cursor = self.conn.execute("SELECT COUNT(*) FROM seen_actions")
        return cursor.fetchone()[0]

    def get_sources(self) -> list[str]:
        cursor = self.conn.execute(
            "SELECT DISTINCT source FROM seen_actions ORDER BY source"
        )
        return [row[0] for row in cursor.fetchall()]

    def search(
        self,
        text: str = "",
        sources: list[str] | None = None,
        date_from: str = "",
        date_to: str = "",
        limit: int = 1000,
    ) -> list[dict]:
        conditions = []
        params = []

        if text:
            conditions.append("(title LIKE ? OR source LIKE ? OR url LIKE ?)")
            wildcard = f"%{text}%"
            params.extend([wildcard, wildcard, wildcard])

        if sources:
            placeholders = ",".join("?" for _ in sources)
            conditions.append(f"source IN ({placeholders})")
            params.extend(sources)

        if date_from:
            conditions.append("first_seen >= ?")
            params.append(date_from)

        if date_to:
            conditions.append("first_seen <= ?")
            params.append(date_to + "T23:59:59")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT fingerprint, source, title, url, date, first_seen
            FROM seen_actions {where}
            ORDER BY first_seen DESC
            LIMIT ?
        """
        params.append(limit)

        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.execute(query, params)
        rows = [dict(row) for row in cursor.fetchall()]
        self.conn.row_factory = None
        return rows

    def close(self):
        self.conn.close()
