// src/db/db.js
// لایهٔ ساده و امن برای کار با sqlite3 (Promise-based helpers)

import sqlite3 from 'sqlite3';
import { promisify } from 'util';
import fs from 'fs/promises';
import path from 'path';
import env from '../config/env.js';

sqlite3.verbose();

/**
 * ایجاد یا باز کردن دیتابیس
 * گزینه‌ها: OPEN_READWRITE | OPEN_CREATE
 */
const DB_PATH = env.SQLITE_DB_PATH || './db_atigh.sqlite';
const mode = sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE;
const rawDb = new sqlite3.Database(DB_PATH, mode, (err) => {
  if (err) {
    console.error(`[db] خطا در باز کردن sqlite DB (${DB_PATH}):`, err.message);
    // در مرحلهٔ اجرا ممکن است بخواهی process.exit(1) کنی؛ فعلاً فقط لاگ می‌زنیم.
  } else {
    console.log(`[db] اتصال sqlite باز شد: ${DB_PATH}`);
  }
});

// دقت: sqlite3 در حالت serialized باعث می‌شود دستورات پشت سر هم اجرا شوند.
rawDb.serialize();

/* -----------------------
   Promise-wrapper helpers
   ----------------------- */
function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    rawDb.run(sql, params, function (err) {
      if (err) return reject(err);
      // this.lastID و this.changes قابل استفاده‌اند
      resolve({ lastID: this.lastID, changes: this.changes });
    });
  });
}

function get(sql, params = []) {
  return new Promise((resolve, reject) => {
    rawDb.get(sql, params, (err, row) => {
      if (err) return reject(err);
      resolve(row);
    });
  });
}

function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    rawDb.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

function exec(sql) {
  return new Promise((resolve, reject) => {
    rawDb.exec(sql, (err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

function each(sql, params = [], onRow) {
  return new Promise((resolve, reject) => {
    try {
      rawDb.each(
        sql,
        params,
        (err, row) => {
          if (err) return reject(err);
          try { onRow(row); } catch (e) { /* swallow onRow errors */ }
        },
        (err, count) => {
          if (err) return reject(err);
          resolve(count);
        }
      );
    } catch (e) { reject(e); }
  });
}

function close() {
  return new Promise((resolve, reject) => {
    rawDb.close((err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

/* -----------------------
   تراکنش (transaction helper)
   استفاده:
     await withTransaction(async () => {
       await run(...);
       await run(...);
     });
   ----------------------- */
async function withTransaction(fn) {
  await run('BEGIN TRANSACTION;');
  try {
    const res = await fn();
    await run('COMMIT;');
    return res;
  } catch (e) {
    try { await run('ROLLBACK;'); } catch (_) { /* ignore */ }
    throw e;
  }
}

/* -----------------------
   PRAGMAهای پیشنهادی
   - WAL برای هم‌زمانی بهتر
   - foreign_keys = ON
   ----------------------- */
async function ensurePragmas() {
  try {
    await exec('PRAGMA journal_mode = WAL;');
    await exec('PRAGMA synchronous = NORMAL;');
    await exec('PRAGMA foreign_keys = ON;');
    console.log('[db] PRAGMAها اعمال شد: WAL, foreign_keys=ON, synchronous=NORMAL');
  } catch (e) {
    console.warn('[db] خطا در اعمال PRAGMAها:', e.message);
  }
}

/* -----------------------
   اجرای migrationها از یک پوشه
   فایل‌ها به ترتیب حروفی/عددی اجرا می‌شوند.
   ----------------------- */
async function runMigrations(migrationsDir = path.resolve('./src/db/migrations')) {
  try {
    const entries = await fs.readdir(migrationsDir, { withFileTypes: true });
    const files = entries
      .filter(e => e.isFile() && /\.(sql|SQL)$/.test(e.name))
      .map(e => e.name)
      .sort();

    for (const f of files) {
      const full = path.join(migrationsDir, f);
      const sql = await fs.readFile(full, 'utf8');
      console.log(`[db:migrate] اجرای migration: ${f}`);
      // هر فایل ممکن است شامل چندین دستور باشد؛ از exec استفاده می‌کنیم
      await exec(sql);
    }
    console.log('[db:migrate] تمام migrationها اجرا شد.');
  } catch (e) {
    if (e.code === 'ENOENT') {
      console.log('[db:migrate] پوشهٔ migrations پیدا نشد؛ از آن چشم‌پوشی شد.');
      return;
    }
    throw e;
  }
}

/* -----------------------
   اجرای یک فایل SQL منفرد (برای debugging)
   ----------------------- */
async function runSqlFile(filePath) {
  const sql = await fs.readFile(filePath, 'utf8');
  await exec(sql);
}

/* -----------------------
   Export ها
   ----------------------- */
export {
  rawDb as db,
  run,
  get,
  all,
  exec,
  each,
  close,
  withTransaction,
  ensurePragmas,
  runMigrations,
  runSqlFile,
};

// اجرای اولیه: PRAGMAها را تنظیم کن (به محض لود شدن ماژول)
ensurePragmas().catch(err => {
  console.warn('[db] warn: ensurePragmas failed', err?.message || err);
});
