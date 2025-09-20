-- 020_error_analysis.sql
-- تحلیل خطا + ضدتکرار پردازش/ارسال

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

CREATE TABLE IF NOT EXISTS rahin_error_sent (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  error_hash    TEXT UNIQUE,
  first_seen_at TEXT DEFAULT (datetime('now','localtime'))
);

-- SEED اختیاری
INSERT INTO rahin_error_insights (
  created_at, log_file, error_ts, error_line, file_path, line_no, short_summary, root_cause, fix_steps, code_patch, model, tokens_in, tokens_out
)
SELECT
  datetime('now','localtime'),
  'C:/logs/app.log',
  datetime('now','-5 minutes','localtime'),
  '[2025-09-20 12:34:56] [ERROR] Sample error | stack=at C:\\app\\src\\index.js:42:10',
  'C:/app/src/index.js',
  42,
  'خطای نمونه برای تست گزارش‌گیری',
  'Nil reference در فانکشن main',
  'گام ۱: بررسی مقدار ورودی | گام ۲: افزودن گارد نال | گام ۳: نوشتن تست',
  '',
  'gpt-4o',
  0, 0
WHERE NOT EXISTS (SELECT 1 FROM rahin_error_insights);
