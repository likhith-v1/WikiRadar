"""Truncate edits, window_metrics and current_state."""

import argparse

from wikiradar import db

argparse.ArgumentParser(description=__doc__).parse_args()
with db.connect() as conn:
    conn.execute("TRUNCATE edits, window_metrics, current_state")
print("tables truncated")
