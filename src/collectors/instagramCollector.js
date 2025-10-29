import 'file:///E:/Projects/rahin/logger.js';
// src/collectors/instagramCollector.js
// گردآوری آمار اینستاگرام از سه جدول: comment, reply, atigh_instagram_new
// - ستون زمان به‌صورت هوشمند از بین چند کاندیدا انتخاب می‌شود.
// - خروجی شامل آمار امروز/۷روز/آخرین زمان برای هر جدول + جمع کل است.

import { get as dbGet, all as dbAll } from '../db/db.js';

/* =========================
 * Utilities
 * ========================= */

async function tableExists(name) {
  const row = await dbGet(
    `SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
    [name]
  );
  return !!row;
}

async function columnExists(table, col) {
  const rows = await dbAll(`PRAGMA table_info("${table}")`);
  return Array.isArray(rows) && rows.some(r => (r?.name || '').toLowerCase() === col.toLowerCase());
}

// نرمال‌سازی زمان به datetime (محلی)
// - اعداد یونیکس (ثانیه/میلی‌ثانیه) → datetime محلی
// - رشته تاریخ → تلاش برای parse توسط sqlite
function normalizedDatetimeExpr(col) {
  const c = `"${col}"`;
  return `
    CASE
      WHEN typeof(${c})='integer' THEN datetime(${c}, 'unixepoch', 'localtime')
      WHEN CAST(${c} AS INTEGER) > 10000000000 THEN datetime(CAST(${c} AS INTEGER)/1000, 'unixepoch', 'localtime')
      WHEN CAST(${c} AS INTEGER) BETWEEN 1000000000 AND 5000000000 THEN datetime(CAST(${c} AS INTEGER), 'unixepoch', 'localtime')
      ELSE datetime(${c}) -- تلاش برای parse رشته تاریخ
    END
  `;
}

// شمارش با یک ستون مشخصِ زمان
async function countWithColumn(table, col) {
  const dt = normalizedDatetimeExpr(col);

  const todayRow = await dbGet(
    `SELECT COUNT(*) AS cnt FROM "${table}" WHERE date(${dt}) = date('now','localtime')`
  );

  const weekRow = await dbGet(
    `SELECT COUNT(*) AS cnt FROM "${table}" WHERE datetime(${dt}) >= datetime('now','-7 days','localtime')`
  );

  const lastRow = await dbGet(
    `SELECT ${dt} AS last_dt FROM "${table}" ORDER BY ${dt} DESC LIMIT 1`
  );

  return {
    ok: true,
    today: Number(todayRow?.cnt || 0),
    week: Number(weekRow?.cnt || 0),
    last_dt: lastRow?.last_dt || null,
    col,
  };
}

// تلاش روی چند کاندیدای ستون زمان تا یکی جواب بدهد
async function tryCount(table, candidateCols, outErrors) {
  for (const col of candidateCols) {
    // فقط اگر ستون واقعاً وجود دارد تلاش کن
    // (اگر جدول خیلی بزرگ است و PRAGMA کند است، می‌توان این چک را حذف کرد.)
    // ولی برای ایمنی ساختاری، نگه می‌داریم.
    try {
      const hasCol = await columnExists(table, col);
      if (!hasCol) continue;
      return await countWithColumn(table, col);
    } catch (e) {
      outErrors.push({ table, col, error: e?.message || String(e) });
      // ادامه بده با ستون بعدی
    }
  }
  return { ok: false };
}

function maxDatetime(a, b) {
  if (!a) return b || null;
  if (!b) return a || null;
  // مقایسهٔ لغوی برای ISO-like datetime در SQLite معمولاً کافی است
  return a > b ? a : b;
}

/* =========================
 * Main collector
 * ========================= */

export async function collectInstagram() {
  // سه جدول هدف و کاندیداهای محتمل برای ستون زمان
  const targets = [
    { name: 'comment', candidates: ['created_at', 'ttime', 'timestamp', 'time', 'date', 'occurred_at', 'published_at'] },
    { name: 'reply', candidates: ['created_at', 'ttime', 'timestamp', 'time', 'date', 'occurred_at', 'published_at'] },
    { name: 'atigh_instagram_new', candidates: ['created_at', 'ttime', 'timestamp', 'time', 'date', 'occurred_at', 'published_at'] },
  ];

  const errors = [];
  const table_stats = [];
  let total_today = 0;
  let total_week = 0;
  let overall_last_dt = null;

  for (const t of targets) {
    const exists = await tableExists(t.name);
    if (!exists) {
      errors.push({ table: t.name, error: 'TABLE_NOT_FOUND' });
      table_stats.push({
        table: t.name, ok: false, today: 0, week: 0, last_dt: null, chosen_time_col: null, note: 'table not found'
      });
      continue;
    }

    const res = await tryCount(t.name, t.candidates, errors);
    if (!res.ok) {
      table_stats.push({
        table: t.name, ok: false, today: 0, week: 0, last_dt: null, chosen_time_col: null, note: 'no usable time column'
      });
      continue;
    }

    total_today += res.today;
    total_week += res.week;
    overall_last_dt = maxDatetime(overall_last_dt, res.last_dt);

    table_stats.push({
      table: t.name,
      ok: true,
      today: res.today,
      week: res.week,
      last_dt: res.last_dt,
      chosen_time_col: res.col
    });
  }

  // خروجی نهایی: آمار جدول‌به‌جدول + مجموع‌ها
  return {
    ok: true,
    total: {
      today: total_today,
      last7d: total_week,
      last_dt: overall_last_dt
    },
    tables: table_stats,
    errors // برای دیباگ؛ اگر خالی بود یعنی مشکلی نبوده یا هندل شده
  };
}

