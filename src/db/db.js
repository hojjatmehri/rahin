// src/db/db.js
// لایهٔ ساده و امن برای کار با sqlite3 (Promise-based helpers)

import sqlite3 from 'sqlite3';
import path from 'path';
import fs from 'fs/promises';
import env from '../config/env.js';

sqlite3.verbose();

/* ----------------------- مسیر DB ----------------------- */
function resolveDbPath() {
  // اولویت با ENV سیستم؛ سپس env.js
  const DB_FILE =
    process.env.SQLITE_DB_PATH ||
    process.env.DB_PATH ||
    env?.SQLITE_DB_PATH ||
    './db_atigh.sqlite';

  const abs = path.isAbsolute(DB_FILE) ? DB_FILE : path.resolve(process.cwd(), DB_FILE);
  return abs;
}

export const DB_PATH = resolveDbPath();

/* ----------------------- اتصال ----------------------- */
const mode = sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE;
const rawDb = new sqlite3.Database(DB_PATH, mode, (err) => {
  if (err) {
    console.error(`[db] خطا در باز کردن sqlite DB (${DB_PATH}):`, err.message);
  } else {
    console.log(`[db] اتصال sqlite باز شد: ${DB_PATH}`);
  }
});

// کاهش خطای BUSY در بار همزمان
try { rawDb.configure?.('busyTimeout', 5000); } catch { /* ignore */ }

// serialize: اجرای دستورات به‌ترتیب
rawDb.serialize();

// ⬅️ برای سازگاری با کدهای قدیمی
export const db = rawDb;

/* ----------------------- Promise wrappers ----------------------- */
export function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    rawDb.run(sql, params, function (err) {
      if (err) return reject(err);
      resolve({ lastID: this.lastID, changes: this.changes });
    });
  });
}

export function get(sql, params = []) {
  return new Promise((resolve, reject) => {
    rawDb.get(sql, params, (err, row) => {
      if (err) return reject(err);
      resolve(row);
    });
  });
}

export function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    rawDb.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

export function exec(sql) {
  return new Promise((resolve, reject) => {
    rawDb.exec(sql, (err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

export function each(sql, params = [], onRow) {
  return new Promise((resolve, reject) => {
    try {
      rawDb.each(
        sql,
        params,
        (err, row) => {
          if (err) return reject(err);
          try { onRow(row); } catch { /* swallow */ }
        },
        (err, count) => {
          if (err) return reject(err);
          resolve(count);
        }
      );
    } catch (e) { reject(e); }
  });
}

export function close() {
  return new Promise((resolve, reject) => {
    rawDb.close((err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

/* ----------------------- تراکنش ----------------------- */
export async function withTransaction(fn) {
  await run('BEGIN TRANSACTION;');
  try {
    const res = await fn();
    await run('COMMIT;');
    return res;
  } catch (e) {
    try { await run('ROLLBACK;'); } catch { /* ignore */ }
    throw e;
  }
}

/* ----------------------- PRAGMAs ----------------------- */
export async function ensurePragmas() {
  try {
    await exec('PRAGMA journal_mode = WAL;');
    await exec('PRAGMA synchronous = NORMAL;');
    await exec('PRAGMA foreign_keys = ON;');
    await exec('PRAGMA busy_timeout = 5000;');
    console.log('[db] PRAGMAها اعمال شد: WAL, foreign_keys=ON, synchronous=NORMAL');
  } catch (e) {
    console.warn('[db] خطا در اعمال PRAGMAها:', e.message);
  }
}

/* ----------------------- Migrations ----------------------- */
export async function runMigrations(migrationsDir = path.resolve('./src/db/migrations')) {
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

export async function runSqlFile(filePath) {
  const sql = await fs.readFile(filePath, 'utf8');
  await exec(sql);
}

/* ----------------------- Init PRAGMAs on load ----------------------- */
ensurePragmas().catch(err => {
  console.warn('[db] warn: ensurePragmas failed', err?.message || err);
});
