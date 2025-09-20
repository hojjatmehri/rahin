-- 020_error_analysis.sql
-- تحلیل خطا + ضدتکرار پردازش/ارسال

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

BEGIN;

-- بینش خطاها
CREATE TABLE IF NOT EXISTS rahin_error_insights (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at    TEXT NOT NULL,
  log_file      TEXT,
  error_ts      TEXT,
  error_line    TEXT,
  file_path     TEXT,
  line_no       INTEGER,
  short_summary TEXT,
  root_cause    TEXT,
  fix_steps     TEXT,
  code_patch    TEXT,
  model         TEXT,
  tokens_in     INTEGER DEFAULT 0,
  tokens_out    INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_err_created ON rahin_error_insights(created_at);
CREATE INDEX IF NOT EXISTS idx_err_file    ON rahin_error_insights(file_path);

-- ضدتکرار پردازش خطا
CREATE TABLE IF NOT EXISTS rahin_error_sent (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  error_hash    TEXT UNIQUE,
  first_seen_at TEXT DEFAULT (datetime('now','localtime'))
);
