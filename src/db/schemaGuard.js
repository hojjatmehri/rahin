// src/db/schemaGuard.js
// ایجاد/بررسی حداقل اسکیما و ایندکس‌ها + اعمال PRAGMAهای ضروری

import { exec as dbExec, run as dbRun, get as dbGet, runMigrations, ensurePragmas } from './db.js';

/* ----------------------------------
   Helpers
---------------------------------- */
export async function tableExists(name) {
  const row = await dbGet(
    `SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
    [name]
  );
  return !!row;
}

async function createTable(sql) {
  await dbExec(sql);
}

async function ensureIndexes(indexSqlArray = []) {
  for (const sql of indexSqlArray) {
    await dbExec(sql);
  }
}

/* ----------------------------------
   Core schema (idempotent)
---------------------------------- */
async function ensureCoreTables() {
  // اسنپ‌شات و خروجی تحلیل دوگانه (مدیریتی/فنی)
  await createTable(`
    CREATE TABLE IF NOT EXISTS rahin_dual_insights (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at     TEXT NOT NULL,
      run_key        TEXT NOT NULL,
      period_date    TEXT,
      metrics_json   TEXT NOT NULL,
      mgmt_json      TEXT NOT NULL,
      tech_json      TEXT NOT NULL,
      model          TEXT NOT NULL,
      tokens_in      INTEGER DEFAULT 0,
      tokens_out     INTEGER DEFAULT 0,
      latency_ms     INTEGER DEFAULT 0,
      error          TEXT
    );
  `);
  await ensureIndexes([
    `CREATE INDEX IF NOT EXISTS idx_dual_insights_created_at ON rahin_dual_insights(created_at);`,
    `CREATE INDEX IF NOT EXISTS idx_dual_insights_run_key   ON rahin_dual_insights(run_key);`
  ]);

  // KPI روزانه برای گارد تصمیم
  await createTable(`
    CREATE TABLE IF NOT EXISTS rahin_kpi_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
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
  `);
  await ensureIndexes([
    `CREATE UNIQUE INDEX IF NOT EXISTS ux_kpi_day ON rahin_kpi_snapshots(day);`,
    `CREATE INDEX IF NOT EXISTS idx_kpi_created_at ON rahin_kpi_snapshots(created_at);`
  ]);

  // تجمیع ساده مالی روزانه
  await createTable(`
    CREATE TABLE IF NOT EXISTS finance_daily (
      day TEXT PRIMARY KEY,
      total_sell    NUMERIC,
      total_buy     NUMERIC,
      total_profit  NUMERIC,
      orders_count  INTEGER
    );
  `);

  // سلامت سنکرون مالی
  await createTable(`
    CREATE TABLE IF NOT EXISTS finance_health (
      day TEXT PRIMARY KEY,
      has_transactions INTEGER,
      last_sync_at     TEXT,
      source_lag_minutes INTEGER
    );
  `);

  // تحلیل خطاهای اخیر + ضدتکرار ارسال
  await createTable(`
    CREATE TABLE IF NOT EXISTS rahin_error_insights (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at   TEXT NOT NULL,
      log_file     TEXT,
      error_ts     TEXT,
      error_line   TEXT,
      file_path    TEXT,
      line_no      INTEGER,
      short_summary TEXT,
      root_cause    TEXT,
      fix_steps     TEXT,
      code_patch    TEXT,
      model         TEXT,
      tokens_in     INTEGER DEFAULT 0,
      tokens_out    INTEGER DEFAULT 0
    );
  `);
  await ensureIndexes([
    `CREATE INDEX IF NOT EXISTS idx_err_ins_created ON rahin_error_insights(created_at);`,
    `CREATE INDEX IF NOT EXISTS idx_err_ins_file    ON rahin_error_insights(file_path);`
  ]);

  await createTable(`
    CREATE TABLE IF NOT EXISTS rahin_error_sent (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      error_hash    TEXT UNIQUE,
      first_seen_at TEXT DEFAULT (datetime('now','localtime'))
    );
  `);

  // لاگ ارسال کانال‌ها (برای جلوگیری از ارسال تکراری run_key)
  await createTable(`
    CREATE TABLE IF NOT EXISTS rahin_sent_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_key   TEXT NOT NULL,
      channel   TEXT NOT NULL,
      to_number TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(run_key, channel, to_number)
    );
  `);

  // دفترچه تماس بازدیدکنندگان (برای enrich از کلیک‌ها)
  await createTable(`
    CREATE TABLE IF NOT EXISTS visitor_contacts (
      visitor_id TEXT NOT NULL,
      mobile     TEXT NOT NULL,
      source     TEXT,
      confidence INTEGER,
      last_seen  TEXT DEFAULT (datetime('now','localtime')),
      PRIMARY KEY (visitor_id, mobile)
    );
  `);
  await ensureIndexes([
    `CREATE INDEX IF NOT EXISTS idx_visitor_contacts_mobile ON visitor_contacts(mobile);`,
    `CREATE INDEX IF NOT EXISTS idx_visitor_contacts_seen   ON visitor_contacts(last_seen);`
  ]);

  // جدول اختیاری کلیک‌ها (اگر پروژه‌ات دارد، این اسکیمای مینیمال جواب می‌دهد)
  await createTable(`
    CREATE TABLE IF NOT EXISTS click_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      visitor_id  TEXT,
      page_url    TEXT,
      target_url  TEXT,
      utm_source  TEXT,
      click_type  TEXT,
      clicked_at  TEXT DEFAULT (datetime('now','localtime'))
    );
  `);
  await ensureIndexes([
    `CREATE INDEX IF NOT EXISTS idx_click_logs_type ON click_logs(click_type);`,
    `CREATE INDEX IF NOT EXISTS idx_click_logs_time ON click_logs(clicked_at);`
  ]);
}

/* ----------------------------------
   ورودی‌های خارجی (اختیاری)
   این‌ها را نمی‌سازیم چون معمولاً upstream هستند؛
   فقط اگر دوست داری می‌توانی اسکیمای مینیمال بگذاری.
   - transactions
   - whatsapp_new_msg
   - atigh_instagram_dev
   - wa_pdf_dispatch_log
---------------------------------- */

/* ----------------------------------
   Public API
---------------------------------- */
export async function ensureMinimalSchema({ runSqlMigrations = true } = {}) {
  // PRAGMAها (اگر قبلاً هم اجرا شده باشند idempotent است)
  await ensurePragmas();

  // جداول داخلی Watchdog
  await ensureCoreTables();

  // اجرای فایل‌های migrations در صورت وجود
  if (runSqlMigrations) {
    await runMigrations(); // مسیر پیش‌فرض: ./src/db/migrations
  }
}
