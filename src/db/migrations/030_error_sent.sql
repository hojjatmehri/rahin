-- 030_error_sent.sql
-- ذخیره هش خطاهای پردازش‌شده برای ضدتکرار

CREATE TABLE IF NOT EXISTS rahin_error_sent (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  error_hash TEXT NOT NULL UNIQUE,
  created_at DATETIME DEFAULT (datetime('now','localtime'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_error_sent_hash 
  ON rahin_error_sent(error_hash);
