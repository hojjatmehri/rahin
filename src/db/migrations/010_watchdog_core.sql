-- 010_watchdog_core.sql
-- جداول مخصوص واچ‌داگ + KPI + لاگ ارسال

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

BEGIN;

-- خروجی تحلیل دوگانه (مدیریتی/فنی)
CREATE TABLE IF NOT EXISTS rahin_dual_insights (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at   TEXT NOT NULL,
  run_key      TEXT NOT NULL,
  period_date  TEXT,
  metrics_json TEXT NOT NULL,
  mgmt_json    TEXT NOT NULL,
  tech_json    TEXT NOT NULL,
  model        TEXT NOT NULL,
  tokens_in    INTEGER DEFAULT 0,
  tokens_out   INTEGER DEFAULT 0,
  latency_ms   INTEGER DEFAULT 0,
  error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_dual_created ON rahin_dual_insights(created_at);
CREATE INDEX IF NOT EXISTS idx_dual_runkey  ON rahin_dual_insights(run_key);

-- KPI روزانه برای گارد تصمیم
CREATE TABLE IF NOT EXISTS rahin_kpi_snapshots (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at      TEXT NOT NULL,
  day             TEXT NOT NULL,
  tx_today_cnt    INTEGER,
  tx_7d_cnt       INTEGER,
  tx_7d_sell_sum  REAL,
  wa_today_cnt    INTEGER,
  wa_7d_cnt       INTEGER,
  ig_today_cnt    INTEGER,
  ig_7d_cnt       INTEGER,
  pdf_today_cnt   INTEGER,
  pdf_7d_cnt      INTEGER,
  tx_last_ts      TEXT,
  wa_last_ts      TEXT,
  ig_last_ts      TEXT,
  pdf_last_ts     TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_kpi_day ON rahin_kpi_snapshots(day);
CREATE INDEX IF NOT EXISTS idx_kpi_created ON rahin_kpi_snapshots(created_at);

-- تجمیع مالی روزانه
CREATE TABLE IF NOT EXISTS finance_daily (
  day          TEXT PRIMARY KEY,
  total_sell   NUMERIC,
  total_buy    NUMERIC,
  total_profit NUMERIC,
  orders_count INTEGER
);

-- سلامت سنکرون مالی
CREATE TABLE IF NOT EXISTS finance_health (
  day                 TEXT PRIMARY KEY,
  has_transactions    INTEGER,
  last_sync_at        TEXT,
  source_lag_minutes  INTEGER
);

-- لاگ ارسال برای ضدتکرار پیام (run_key + channel + to_number)
CREATE TABLE IF NOT EXISTS rahin_sent_log (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  run_key    TEXT NOT NULL,
  channel    TEXT NOT NULL,
  to_number  TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(run_key, channel, to_number)
);
