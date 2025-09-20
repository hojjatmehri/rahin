-- 001_core.sql
-- جداول ورودی (upstream/اپ‌استریم) + داده‌ی تست اختیاری

-- تراکنش‌ها
CREATE TABLE IF NOT EXISTS transactions (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  serviceTitle    TEXT,
  sellAmount      REAL,
  buyAmount       REAL,
  profit          REAL,
  regDate         TEXT,
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

-- =============== SEED (اختیاری) ===============
INSERT INTO transactions (serviceTitle,sellAmount,buyAmount,profit,regDate,payType1,payDate1,paidAmount1,customerDebt)
SELECT 'تور استانبول ۳ شب', 150000000, 120000000, 30000000, datetime('now','-1 day','localtime'), 'card', date('now','-1 day','localtime'), 150000000, 0
WHERE NOT EXISTS (SELECT 1 FROM transactions);

INSERT INTO transactions (serviceTitle,sellAmount,buyAmount,profit,regDate,payType1,payDate1,paidAmount1,customerDebt)
SELECT 'تور دبی لحظه‌ای',   90000000,  80000000, 10000000, datetime('now','localtime'),           'cash', date('now','localtime'),           90000000,  0
WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE date(regDate)=date('now','localtime'));

-- پیام واتساپ امروز ✅ بدون ستون text
-- اولویت با created_at اگر وجود داشته باشد
INSERT INTO whatsapp_new_msg (mobile, created_at)
SELECT '989123456789', datetime('now','localtime')
WHERE NOT EXISTS (SELECT 1 FROM whatsapp_new_msg)
  AND EXISTS (SELECT 1 FROM pragma_table_info('whatsapp_new_msg') WHERE name='created_at');

-- اگر created_at نبود، ولی ttime هست، از ttime استفاده کن
INSERT INTO whatsapp_new_msg (mobile, ttime)
SELECT '989123456789', datetime('now','localtime')
WHERE NOT EXISTS (SELECT 1 FROM whatsapp_new_msg)
  AND NOT EXISTS (SELECT 1 FROM pragma_table_info('whatsapp_new_msg') WHERE name='created_at')
  AND EXISTS (SELECT 1 FROM pragma_table_info('whatsapp_new_msg') WHERE name='ttime');


-- اینستاگرام: یک رویداد تست ✅ سازگار با اسکیماهای مختلف جدول atigh_instagram_dev

-- حالت 1) جدول ستون‌های event_type + payload + created_at دارد
INSERT INTO atigh_instagram_dev (event_type, payload, created_at)
SELECT 'webhook:test', '{"demo":true}', datetime('now','localtime')
WHERE NOT EXISTS (SELECT 1 FROM atigh_instagram_dev)
  AND EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='event_type')
  AND EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='payload')
  AND EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='created_at');

-- حالت 2) event_type نیست، ولی payload + created_at هست
INSERT INTO atigh_instagram_dev (payload, created_at)
SELECT '{"demo":true}', datetime('now','localtime')
WHERE NOT EXISTS (SELECT 1 FROM atigh_instagram_dev)
  AND NOT EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='event_type')
  AND EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='payload')
  AND EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='created_at');

-- حالت 3) فقط created_at هست
INSERT INTO atigh_instagram_dev (created_at)
SELECT datetime('now','localtime')
WHERE NOT EXISTS (SELECT 1 FROM atigh_instagram_dev)
  AND NOT EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='event_type')
  AND NOT EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='payload')
  AND EXISTS (SELECT 1 FROM pragma_table_info('atigh_instagram_dev') WHERE name='created_at');


INSERT INTO wa_pdf_dispatch_log (contact_id,wa_status,created_at)
SELECT 'cust-1001','SENT', datetime('now','localtime')
WHERE NOT EXISTS (SELECT 1 FROM wa_pdf_dispatch_log);

INSERT INTO click_logs (visitor_id,page_url,target_url,utm_source,click_type,clicked_at)
SELECT 'v-1','/tour/istanbul','https://wa.me/989123456789?text=Hi', 'google','whatsapp', datetime('now','localtime')
WHERE NOT EXISTS (SELECT 1 FROM click_logs WHERE click_type='whatsapp');
