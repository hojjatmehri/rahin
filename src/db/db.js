import 'file:///E:/Projects/rahin/logger.js';
// ============================================================
// File: src/db/db.js
// Purpose: Thin wrapper around shared dbSingleton connection
// Author: Hojjat Mehri
// ============================================================

import { db as sharedDb } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';

// از اینجا به بعد تمام توابع از sharedDb استفاده می‌کنند

/* ----------------------- Promise Helpers ----------------------- */
export function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    try {
      const stmt = sharedDb.prepare(sql);
      const info = stmt.run(...params);
      resolve({ lastID: info.lastInsertRowid, changes: info.changes });
    } catch (e) {
      reject(e);
    }
  });
}

export function get(sql, params = []) {
  return new Promise((resolve, reject) => {
    try {
      const stmt = sharedDb.prepare(sql);
      const row = stmt.get(...params);
      resolve(row);
    } catch (e) {
      reject(e);
    }
  });
}

export function all(sql, params = []) {
  return new Promise((resolve, reject) => {
    try {
      const stmt = sharedDb.prepare(sql);
      const rows = stmt.all(...params);
      resolve(rows);
    } catch (e) {
      reject(e);
    }
  });
}

export function exec(sql) {
  return new Promise((resolve, reject) => {
    try {
      sharedDb.exec(sql);
      resolve();
    } catch (e) {
      reject(e);
    }
  });
}

/* ----------------------- تراکنش ----------------------- */
export async function withTransaction(fn) {
  await exec('BEGIN TRANSACTION;');
  try {
    const result = await fn();
    await exec('COMMIT;');
    return result;
  } catch (e) {
    try { await exec('ROLLBACK;'); } catch { /* ignore */ }
    throw e;
  }
}

/* ----------------------- Export shared DB ----------------------- */
export const db = sharedDb;

console.log('[db] ✅ Connected via shared singleton (better-sqlite3).');
