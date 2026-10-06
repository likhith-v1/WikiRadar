"""Truncate edits, window_metrics and current_state."""

from wikiradar import db

with db.connect() as conn:
    conn.execute("TRUNCATE edits, window_metrics, current_state")
print("tables truncated")
