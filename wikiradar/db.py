from dataclasses import asdict
from datetime import datetime

import psycopg

from wikiradar import config
from wikiradar.features import FEATURE_NAMES
from wikiradar.schema import Edit

WINDOW_COLUMNS = (
    "window_start", "window_end", "n_events",
    *(f for f in FEATURE_NAMES if not f.startswith("hour_")),
    "top_title", "score", "is_anomaly", "reason", "has_synthetic",
)


def connect() -> psycopg.Connection:
    return psycopg.connect(**config.POSTGRES)


def insert_edits(conn: psycopg.Connection, edits: list[Edit]):
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO edits (event_id, dt, title, title_url, user_name, is_bot, is_anon, is_new,
                                  is_revert, byte_delta, rev_id, synthetic)
               VALUES (%(event_id)s, %(dt)s, %(title)s, %(title_url)s, %(user)s, %(is_bot)s, %(is_anon)s,
                       %(is_new)s, %(is_revert)s, %(byte_delta)s, %(rev_id)s, %(synthetic)s)
               ON CONFLICT (event_id) DO NOTHING""",
            [asdict(e) for e in edits],
        )


def upsert_window(conn: psycopg.Connection, row: dict):
    columns = ", ".join(WINDOW_COLUMNS)
    values = ", ".join(f"%({c})s" for c in WINDOW_COLUMNS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in WINDOW_COLUMNS[1:])
    conn.execute(
        f"INSERT INTO window_metrics ({columns}) VALUES ({values}) "
        f"ON CONFLICT (window_start) DO UPDATE SET {updates}",
        row,
    )


def update_current_state(conn: psycopg.Connection, as_of: datetime, late_dropped: int, last_score: float):
    """Dashboard tiles, computed from window_metrics relative to the latest window end (event time)."""
    conn.execute(
        """INSERT INTO current_state (id, updated_at, edits_last_min, bot_pct_last_5m, anon_pct_last_5m,
                                      anomalies_last_hour, total_events, late_dropped, last_window_score)
           SELECT 1, now(),
                  coalesce(sum(n_events) FILTER (WHERE window_end > %(as_of)s - interval '1 minute'), 0),
                  100 * sum(bot_ratio * n_events) FILTER (WHERE window_end > %(as_of)s - interval '5 minutes')
                      / nullif(sum(n_events) FILTER (WHERE window_end > %(as_of)s - interval '5 minutes'), 0),
                  100 * sum(anon_ratio * n_events) FILTER (WHERE window_end > %(as_of)s - interval '5 minutes')
                      / nullif(sum(n_events) FILTER (WHERE window_end > %(as_of)s - interval '5 minutes'), 0),
                  count(*) FILTER (WHERE is_anomaly),
                  (SELECT coalesce(sum(n_events), 0) FROM window_metrics), %(late)s, %(score)s
           FROM window_metrics
           WHERE window_end > %(as_of)s - interval '1 hour' AND window_end <= %(as_of)s
           ON CONFLICT (id) DO UPDATE SET
             updated_at = EXCLUDED.updated_at, edits_last_min = EXCLUDED.edits_last_min,
             bot_pct_last_5m = EXCLUDED.bot_pct_last_5m, anon_pct_last_5m = EXCLUDED.anon_pct_last_5m,
             anomalies_last_hour = EXCLUDED.anomalies_last_hour, total_events = EXCLUDED.total_events,
             late_dropped = EXCLUDED.late_dropped, last_window_score = EXCLUDED.last_window_score""",
        {"as_of": as_of, "late": late_dropped, "score": last_score},
    )


def purge_old_edits(conn: psycopg.Connection):
    conn.execute("DELETE FROM edits WHERE dt < now() - interval '24 hours'")


def last_window_end(conn: psycopg.Connection) -> datetime | None:
    return conn.execute("SELECT max(window_end) FROM window_metrics").fetchone()[0]


def late_dropped(conn: psycopg.Connection) -> int:
    row = conn.execute("SELECT late_dropped FROM current_state").fetchone()
    return row[0] if row else 0
