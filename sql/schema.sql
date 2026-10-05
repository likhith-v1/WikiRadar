CREATE TABLE IF NOT EXISTS edits (
  event_id    TEXT PRIMARY KEY,
  dt          TIMESTAMPTZ NOT NULL,
  title       TEXT NOT NULL,
  title_url   TEXT,
  user_name   TEXT,
  is_bot      BOOLEAN, is_anon BOOLEAN, is_new BOOLEAN, is_revert BOOLEAN,
  byte_delta  INTEGER,
  rev_id      BIGINT,
  synthetic   BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS edits_dt_idx ON edits (dt);

CREATE TABLE IF NOT EXISTS window_metrics (
  window_start      TIMESTAMPTZ PRIMARY KEY,
  window_end        TIMESTAMPTZ NOT NULL,
  n_events          INTEGER NOT NULL,
  edit_count        REAL, bot_ratio REAL, anon_ratio REAL, new_page_count REAL,
  mean_abs_delta    REAL, min_delta REAL, revert_ratio REAL,
  top_title_share   REAL, unique_users_ratio REAL,
  top_title         TEXT,
  score             REAL NOT NULL,
  is_anomaly        BOOLEAN NOT NULL,
  reason            TEXT,
  has_synthetic     BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS current_state (
  id                  INTEGER PRIMARY KEY CHECK (id = 1),
  updated_at          TIMESTAMPTZ,
  edits_last_min      INTEGER,
  bot_pct_last_5m     REAL,
  anon_pct_last_5m    REAL,
  anomalies_last_hour INTEGER,
  total_events        BIGINT,
  late_dropped        BIGINT,
  last_window_score   REAL
);
