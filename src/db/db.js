import 'file:///E:/Projects/rahin/logger.js';
// ============================================================
// File: src/db/db.js
// لایهٔ ایمن و پایدار برای کار با SQLite (با پشتیبانی از auto-reopen)
// Author: Hojjat Mehri
// ============================================================

import sqlite3 from 'sqlite3';
import path from 'path';
import fs from 'fs/promises';
import env from '../config/env.js';

sqlite3.verbose();

let _overrideDb = null;        // اگر ست شود یعنی از better-sqlite3 استفاده می‌شود
let _rawDb = null;             // اتصال فعال sqlite3
const mode = sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE;

/* ----------------------- مسیر DB ----------------------- */
function resolveDbPath() {
  const DB_FILE =
    process.env.SQLITE_DB_PATH ||
    process.env.DB_PATH ||
    env?.SQLITE_DB_PATH ||
    './db_atigh.sqlite';

  return path.isAbsolute(DB_FILE) ? DB_FILE : path.resolve(process.cwd(), DB_FILE);
}
const SILENT_RECONNECT = true;

export const DB_PATH = resolveDbPath();

/* ----------------------- تابع بازگشایی خودکار ----------------------- */
function ensureOpenConnection() {
  if (_overrideDb) return _overrideDb;

  // اگر هنوز باز نشده یا بسته شده
  if (!_rawDb || !_rawDb.open) {
    try {
      if (!SILENT_RECONNECT)
        console.warn(`[db] Connection closed or missing. Reopening: ${DB_PATH}`);
      
      _rawDb = new sqlite3.Database(DB_PATH, mode, (err) => {
        if (err) console.error('[db] Failed to reopen:', err.message);
        else console.log('[db] Reconnected to SQLite:', DB_PATH);
      });
      _rawDb.configure?.('busyTimeout', 5000);
      _rawDb.serialize();
    } catch (e) {
      console.error('[db] Critical: reopen failed =>', e.message);
      throw e;
    }
  }
  return _rawDb;
}

/* ----------------------- پشتیبانی از better-sqlite3 ----------------------- */
export function useBetterSqlite3(dbInstance) {
  _overrideDb = dbInstance || null;
  if (_overrideDb) console.log('[db] better-sqlite3 mode enabled.');
}

/* ----------------------- Promise helpers ----------------------- */
export function run(sql, params = []) {
  if (_overrideDb) {
    return new Promise((resolve, reject) => {
      try {
        const stmt = _overrideDb.prepare(sql);
        const info = stmt.run(...params);
        resolve({ lastID: info.lastInsertRowid, changes: info.changes });
      } catch (e) { reject(e); }
    });
  }

  const dbConn = ensureOpenConnection();
  return new Promise((resolve, reject) => {
    dbConn.run(sql, params, function (err) {
      if (err) return reject(err);
      resolve({ lastID: this.lastID, changes: this.changes });
    });
  });
}

export function get(sql, params = []) {
  if (_overrideDb) {
    return new Promise((resolve, reject) => {
      try {
        const stmt = _overrideDb.prepare(sql);
        resolve(stmt.get(...params));
      } catch (e) { reject(e); }
    });
  }

  const dbConn = ensureOpenConnection();
  return new Promise((resolve, reject) => {
    dbConn.get(sql, params, (err, row) => err ? reject(err) : resolve(row));
  });
}

export function all(sql, params = []) {
  if (_overrideDb) {
    return new Promise((resolve, reject) => {
      try {
        const stmt = _overrideDb.prepare(sql);
        resolve(stmt.all(...params));
      } catch (e) { reject(e); }
    });
  }

  const dbConn = ensureOpenConnection();
  return new Promise((resolve, reject) => {
    dbConn.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows));
  });
}

export function exec(sql) {
  if (_overrideDb) {
    return new Promise((resolve, reject) => {
      try { _overrideDb.exec(sql); resolve(); } catch (e) { reject(e); }
    });
  }

  const dbConn = ensureOpenConnection();
  return new Promise((resolve, reject) => {
    dbConn.exec(sql, (err) => err ? reject(err) : resolve());
  });
}

export function each(sql, params = [], onRow) {
  const dbConn = ensureOpenConnection();
  if (_overrideDb) {
    return new Promise((resolve, reject) => {
      try {
        const stmt = _overrideDb.prepare(sql);
        let count = 0;
        for (const row of stmt.iterate(...params)) {
          try { onRow?.(row); } catch { /* ignore consumer error */ }
          count++;
        }
        resolve(count);
      } catch (e) { reject(e); }
    });
  }

  return new Promise((resolve, reject) => {
    dbConn.each(
      sql,
      params,
      (err, row) => {
        if (err) return reject(err);
        try { onRow(row); } catch { /* swallow */ }
      },
      (err, count) => err ? reject(err) : resolve(count)
    );
  });
}

/* ----------------------- بستن اتصال ----------------------- */
export function close() {
  if (_overrideDb) {
    return new Promise((resolve, reject) => {
      try { _overrideDb.close(); resolve(); } catch (e) { reject(e); }
    });
  }

  if (!_rawDb) return Promise.resolve();
  return new Promise((resolve, reject) => {
    _rawDb.close((err) => {
      if (err) return reject(err);
      console.log('[db] Connection closed.');
      _rawDb = null;
      resolve();
    });
  });
}

/* ----------------------- تراکنش ----------------------- */
export async function withTransaction(fn) {
  await run('BEGIN TRANSACTION;');
  try {
    const result = await fn();
    await run('COMMIT;');
    return result;
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

/* ----------------------- Init on load ----------------------- */
ensureOpenConnection();
ensurePragmas().catch(err => {
  console.warn('[db] warn: ensurePragmas failed', err?.message || err);
});

/* ----------------------- Global Error Guard ----------------------- */
process.on('uncaughtException', (err) => {
  if (err?.message?.includes('SQLITE_BUSY') || err?.message?.includes('not open')) {
    console.warn('[db] Transient SQLite error ignored:', err.message);
  } else {
    console.error('[db] Uncaught exception:', err);
  }
});

export const db = ensureOpenConnection();
