// src/scripts/print_schema.mjs
// چاپ اسکیما (جداول/ستون‌ها/ایندکس‌ها/کلیدهای خارجی) + شمارش ردیف برای هر جدول
// Usage:
//   node src/scripts/print_schema.mjs
// یا با مسیر سفارشی:
//   MAIN_DB_PATH="C:/path/to/main.sqlite" ARCHIVE_DB_PATH="C:/path/to/archive.sqlite" node src/scripts/print_schema.mjs

import 'dotenv/config';
import path from 'path';
import fs from 'fs';
import Database from 'better-sqlite3';

const MAIN_DB_PATH    = process.env.MAIN_DB_PATH    || process.env.SQLITE_DB_PATH || 'db_atigh.sqlite';
const ARCHIVE_DB_PATH = process.env.ARCHIVE_DB_PATH || '';

function abs(p) {
  return path.isAbsolute(p) ? p : path.resolve(process.cwd(), p);
}
function qident(name) {
  return `"${String(name).replaceAll('"','""')}"`;
}

/* ---------- DB helpers ---------- */
function openDb(file) {
  const filename = abs(file);
  if (!fs.existsSync(filename)) {
    throw new Error(`DB not found: ${filename}`);
  }
  const db = new Database(filename, { fileMustExist: true, readonly: true });
  // فقط خواندن وضعیت پرگماها (بدون تغییر)
  return db;
}

function listTables(db) {
  const sql = `
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
    ORDER BY name;
  `;
  return db.prepare(sql).all();
}

function tableInfo(db, table) {
  const cols = db.prepare(`PRAGMA table_info(${qident(table)});`).all();
  let fks = [];
  let idxs = [];
  try { fks  = db.prepare(`PRAGMA foreign_key_list(${qident(table)});`).all(); } catch {}
  try { idxs = db.prepare(`PRAGMA index_list(${qident(table)});`).all(); } catch {}

  // شمارش ردیف‌ها
  let rowCount = null;
  try {
    const r = db.prepare(`SELECT COUNT(*) AS c FROM ${qident(table)};`).get();
    rowCount = r?.c ?? null;
  } catch {}

  // جزئیات ایندکس‌ها
  const idxDetails = [];
  for (const i of idxs) {
    try {
      const cols = db.prepare(`PRAGMA index_info(${qident(i.name)});`).all();
      idxDetails.push({
        name: i.name,
        unique: !!i.unique,
        origin: i.origin,
        partial: !!i.partial,
        cols
      });
    } catch {}
  }

  return { cols, fks, idxs: idxDetails, rowCount };
}

function printTableSchema(dbLabel, table, info) {
  console.log(`\n# [${dbLabel}] جدول: ${table}`);
  console.log(`ردیف‌ها: ${info.rowCount ?? 'نامشخص'}`);

  // ستون‌ها
  console.log(`\nستون‌ها:`);
  console.log(`| نام ستون | نوع | PK | Nullable | Default |`);
  console.log(`|---|---|:--:|:--:|---|`);
  for (const c of info.cols) {
    const pk = c.pk ? '✓' : '';
    const notnull = c.notnull ? 'NO' : 'YES';
    const defv = (c.dflt_value ?? '').toString();
    console.log(`| ${c.name} | ${c.type || ''} | ${pk} | ${notnull} | ${defv} |`);
  }

  // کلیدهای خارجی
  console.log(`\nکلیدهای خارجی:`);
  if (!info.fks?.length) {
    console.log(`(ندارد)`);
  } else {
    console.log(`| id | جدول مقصد | از ستون | به ستون | on_update | on_delete |`);
    console.log(`|---:|---|---|---|---|---|`);
    for (const f of info.fks) {
      console.log(`| ${f.id} | ${f.table} | ${f.from} | ${f.to} | ${f.on_update} | ${f.on_delete} |`);
    }
  }

  // ایندکس‌ها
  console.log(`\nایندکس‌ها:`);
  if (!info.idxs?.length) {
    console.log(`(ندارد)`);
  } else {
    for (const i of info.idxs) {
      console.log(`- ${i.name} ${i.unique ? '(UNIQUE)' : ''} [origin=${i.origin}${i.partial ? ', partial' : ''}]`);
      const cols = (i.cols || []).map(c => c.name).join(', ');
      console.log(`  ستون‌ها: ${cols || '(?)'}`);
    }
  }
  console.log('');
}

function dumpSchema(dbPath, label) {
  const db = openDb(dbPath);

  console.log(`\n==============================`);
  console.log(`اسکیما: ${label}`);
  console.log(`فایل: ${abs(dbPath)}`);
  console.log(`==============================\n`);

  // وضعیت پرگماها
  try {
    const jm = db.pragma('journal_mode', { simple: true });
    const fk = db.pragma('foreign_keys', { simple: true });
    console.log(`PRAGMA journal_mode: ${jm}`);
    console.log(`PRAGMA foreign_keys: ${fk}\n`);
  } catch {}

  const tables = listTables(db);
  if (!tables.length) {
    console.log('(هیچ جدولی یافت نشد)');
    db.close();
    return;
  }

  for (const t of tables) {
    const info = tableInfo(db, t.name);
    printTableSchema(label, t.name, info);
  }
  db.close();
}

/* ---------- run ---------- */
try {
  dumpSchema(MAIN_DB_PATH, 'MAIN');
  if (ARCHIVE_DB_PATH) {
    dumpSchema(ARCHIVE_DB_PATH, 'ARCHIVE');
  } else {
    console.log('\n[هشدار] ARCHIVE_DB_PATH تنظیم نشده. فقط MAIN چاپ شد.\n');
  }
} catch (e) {
  console.error('❌ خطا در چاپ اسکیما:', e?.message || e);
  process.exit(1);
}
