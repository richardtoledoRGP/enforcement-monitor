import re
import sqlite3
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

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
                first_seen TEXT NOT NULL,
                summary TEXT DEFAULT '',
                ai_overview TEXT DEFAULT ''
            )
        """)
        # Add columns if the table already existed without them
        for col in ("summary", "ai_overview"):
            try:
                self.conn.execute(f"ALTER TABLE seen_actions ADD COLUMN {col} TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # Column already exists
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
                   (fingerprint, source, title, url, date, first_seen, summary, ai_overview)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (action.fingerprint, action.source, action.title,
                 action.url, action.date, now,
                 getattr(action, "summary", "") or "",
                 getattr(action, "ai_overview", "") or "")
            )
        self.conn.commit()

    def count(self) -> int:
        cursor = self.conn.execute("SELECT COUNT(*) FROM seen_actions")
        return cursor.fetchone()[0]

    def last_updated(self) -> str:
        cursor = self.conn.execute("SELECT MAX(first_seen) FROM seen_actions")
        row = cursor.fetchone()
        return row[0] if row and row[0] else ""

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
            SELECT fingerprint, source, title, url, date, first_seen, summary, ai_overview
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

    def get_recent_actions(self, days: int = 7, limit: int = 500) -> list[dict]:
        """Return actions issued in the last N days OR first discovered in the last N days.

        To avoid the initial backfill flooding "new actions," we find the earliest
        first_seen date in the DB (the seed date). Actions loaded on that date are
        only included if they have a parseable action date within the window.
        Actions loaded AFTER the seed date are always included (they're genuinely new).
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        cutoff_str = cutoff.strftime("%Y-%m-%d")

        # Find the seed date (earliest first_seen, truncated to date)
        cursor = self.conn.execute("SELECT MIN(first_seen) FROM seen_actions")
        seed_row = cursor.fetchone()
        seed_date = seed_row[0][:10] if seed_row and seed_row[0] else ""

        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.execute(
            "SELECT fingerprint, source, title, url, date, first_seen, summary, ai_overview FROM seen_actions"
        )
        all_rows = [dict(row) for row in cursor.fetchall()]
        self.conn.row_factory = None

        recent = {}
        for row in all_rows:
            fp = row["fingerprint"]
            first_seen_date = row["first_seen"][:10]
            is_seed_day = (first_seen_date == seed_date)
            is_recent_discovery = (first_seen_date >= cutoff_str and not is_seed_day)
            parsed = _parse_date(row["date"])
            is_recent_action = (parsed is not None and parsed >= cutoff)

            if is_recent_discovery or is_recent_action:
                row["parsed_date"] = parsed.strftime("%Y-%m-%d") if parsed else ""
                recent[fp] = row

        result = list(recent.values())
        result.sort(key=lambda r: r["parsed_date"] or r["first_seen"][:10], reverse=True)
        return result[:limit]

    def close(self):
        self.conn.close()


def _parse_date(date_str: str) -> datetime | None:
    """Best-effort parse of the various date formats across sources."""
    if not date_str:
        return None

    # ISO format: 2025-01-17T00:00:00
    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

    # RFC 2822: "Fri, 03 Apr 2026 09:05:47 -0500" or "Thu, 9 Apr 2026 15:00:00 GMT"
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        pass

    # US format: "3/15/2026" or "03/15/2026"
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None
