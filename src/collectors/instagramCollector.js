// src/collectors/instagramCollector.js
// گردآوری آمار جداول اینستاگرام: atigh_instagram_dev, reply, comment

import { get as dbGet, all as dbAll } from '../db/db.js';

async function tableExists(name) {
  const row = await dbGet(
    `SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
    [name]
  );
  return !!row;
}

// SQL snippet برای نرمال‌سازی زمان به datetime (لوکال)
// - اگر عدد یونیکس (ثانیه) یا میلی‌ثانیه باشد، به datetime تبدیل می‌کند
// - اگر رشتهٔ تاریخ باشد (ISO/SQL)، همان را datetime() می‌کند
function normalizedDatetimeExpr(col) {
  const c = `"${col}"`;
  return `
    CASE
      WHEN typeof(${c})='integer' THEN datetime(${c}, 'unixepoch', 'localtime')
      WHEN CAST(${c} AS INTEGER) > 10000000000 THEN datetime(CAST(${c} AS INTEGER)/1000, 'unixepoch', 'localtime')
      WHEN CAST(${c} AS INTEGER) BETWEEN 1000000000 AND 5000000000 THEN datetime(CAST(${c} AS INTEGER), 'unixepoch', 'localtime')
      ELSE datetime(${c}) -- سعی در پارس رشتهٔ تاریخ
    END
  `;
}

// تلاش برای شمارش با یک ستون مشخصِ زمان
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

// روی چند نامِ کاندید برای ستون زمان تلاش می‌کنیم تا یکی جواب بدهد
async function tryCount(table, candidateCols, outErrors) {
  for (const col of candidateCols) {
    try {
      return await countWithColumn(table, col);
    } catch (e) {
      outErrors.push({ table, col, error: e?.message || String(e) });
      // ادامه بده با ستون بعدی
    }
  }
  return { ok: false };
}

export async function collectInstagram() {
  const today = Number((await dbGet(`
    SELECT COUNT(*) AS cnt
    FROM interactions
    WHERE channel='instagram' AND date(occurred_at)=date('now','localtime')
  `))?.cnt || 0);

  const week = Number((await dbGet(`
    SELECT COUNT(*) AS cnt
    FROM interactions
    WHERE channel='instagram' AND datetime(occurred_at) >= datetime('now','-7 days','localtime')
  `))?.cnt || 0);

  // اختیاری: آمار به تفکیک نوع رویداد (برای دیباگ/دایگنستیک)
  const byType = await dbAll(`
    SELECT event_type, COUNT(*) AS cnt
    FROM interactions
    WHERE channel='instagram' AND date(occurred_at)=date('now','localtime')
    GROUP BY event_type
    ORDER BY cnt DESC
  `);

  return { dev_events_today: today, dev_events_7d: week, by_type: byType };
}

