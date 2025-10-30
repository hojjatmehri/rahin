// ============================================================
// File: src/lib/db/dbSingleton.js
// Purpose: Shared persistent SQLite connection across all Rahin modules
// Author: Hojjat Mehri
// ============================================================

import Database from 'better-sqlite3';
import path from 'path';
import 'dotenv/config';

// ============================================================
// تنظیم مسیر پایگاه داده (ثابت و هماهنگ با پروژه اصلی)
// ============================================================
const DB_PATH = 'E:/Projects/AtighgashtAI/db_atigh.sqlite';

// ============================================================
// ساخت Singleton Connection
// ============================================================
if (!globalThis.__RAHIN_SHARED_DB__) {
  try {
    const db = new Database(DB_PATH, { verbose: null });
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
    db.pragma('busy_timeout = 30000'); // صبر تا ۳۰ ثانیه هنگام قفل شدن
    db.pragma('synchronous = NORMAL');
    db.pragma('temp_store = MEMORY');
    db.pragma('cache_size = -8000'); // حدود ۸ مگ کش RAM

    globalThis.__RAHIN_SHARED_DB__ = db;
    console.log(`[DB] ✅ Shared SQLite connection opened → ${DB_PATH}`);
  } catch (err) {
    console.error(`[DB] ❌ Failed to open shared connection: ${err.message}`);
    process.exit(1);
  }
} else {
  console.log('[DB] ♻️ Reusing existing shared SQLite connection.');
}

// ============================================================
// اکسپورت برای استفاده در تمام ماژول‌ها
// ============================================================
export const db = globalThis.__RAHIN_SHARED_DB__;

// ============================================================
// تست سلامت اتصال (اختیاری)
// ============================================================
try {
  const row = db.prepare("SELECT datetime('now','localtime') AS now").get();
  console.log(`[DB] 🕓 Connection test OK at ${row.now}`);
} catch (e) {
  console.error('[DB] ⚠️ Connection test failed:', e.message);
}
