-- 001_core.sql
-- هسته‌ی جداول ورودی (upstream/اپ‌استریم) + داده‌ی تست اختیاری

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

BEGIN;

-- تراکنش‌ها
CREATE TABLE IF NOT EXISTS transactions (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  serviceTitle    TEXT,
  sellAmount      REAL,
  buyAmount       REAL,
  profit          REAL,
  regDate         TEXT,  -- ISO/localtime
  payType1        TEXT,
  payDate1        TEXT,
  paidAmount1     REAL,
  payType2        TEXT,
  payDate2        TEXT,
  paidAmount2     REAL,
  customerDebt    REAL
);
CREATE INDEX IF NOT EXISTS idx_tx_regDate ON transactions(regDate);

-- پیام‌های واتساپ
CREATE TABLE IF NOT EXISTS whatsapp_new_msg (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  mobile      TEXT,
  text        TEXT,
  ttime       TEXT,  -- قدیمی
  created_at  TEXT   -- جدید
);
CREATE INDEX IF NOT EXISTS idx_wa_created ON whatsapp_new_msg(COALESCE(created_at, ttime));

-- رویدادهای اینستاگرام
CREATE TABLE IF NOT EXISTS atigh_instagram_dev (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type  TEXT,
  payload     TEXT,
  created_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_ig_created ON atigh_instagram_dev(created_at);

-- لاگ ارسال PDF
CREATE TABLE IF NOT EXISTS wa_pdf_dispatch_log (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  contact_id  TEXT,
  wa_status   TEXT,
  created_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pdf_created ON wa_pdf_dispatch_log(created_at);

-- لاگ کلیک‌ها
CREATE TABLE IF NOT EXISTS click_logs (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  visitor_id  TEXT,
  page_url    TEXT,
  target_url  TEXT,
  utm_source  TEXT,
  click_type  TEXT,
  clicked_at  TEXT DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_click_type ON click_logs(click_type);
CREATE INDEX IF NOT EXISTS idx_click_time ON click_logs(clicked_at);

-- دفترچه تماس بازدیدکنندگان
CREATE TABLE IF NOT EXISTS visitor_contacts (
  visitor_id TEXT NOT NULL,
  mobile     TEXT NOT NULL,
  source     TEXT,
  confidence INTEGER,
  last_seen  TEXT DEFAULT (datetime('now','localtime')),
  PRIMARY KEY (visitor_id, mobile)
);
CREATE INDEX IF NOT EXISTS idx_visitor_mobile ON visitor_contacts(mobile);
CREATE INDEX IF NOT EXISTS idx_visitor_seen   ON visitor_contacts(last_seen);

