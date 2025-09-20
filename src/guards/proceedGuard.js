// src/guards/proceedGuard.js
// گارد تصمیم برای ادامه‌ی اجرای تحلیل بر اساس تغییرات KPIها

import { get as dbGet, run as dbRun } from '../db/db.js';

/* ---------------------------
   Helpers
--------------------------- */
function newer(a, b) {
  if (!a && b) return false;
  if (a && !b) return true;
  if (!a && !b) return false;
  return new Date(a).getTime() > new Date(b).getTime();
}

function relDiff(curr, prev) {
  const a = Number(curr) || 0;
  const b = Number(prev) || 0;
  if (a === 0 && b === 0) return 0;
  return Math.abs(a - b) / Math.max(b, 1);
}

async function tableExists(name) {
  const row = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, [name]);
  return !!row;
}

/* ---------------------------
   امضای فعلی وضعیت سیستم
   خروجی: شیء snapshot
--------------------------- */
export async function readCurrentSignature() {
  const hasPDF = await tableExists('wa_pdf_dispatch_log');
  const hasIG  = await tableExists('atigh_instagram_dev');
  const hasWA  = await tableExists('whatsapp_new_msg');

  const dayRow = await dbGet(`SELECT date('now','localtime') AS d`);
  const today  = dayRow?.d;
  const since7d = (await dbGet(`SELECT datetime('now','-7 days','localtime') AS t`))?.t;

  // Transactions
  const tx_today = await dbGet(`
    SELECT COUNT(*) AS c
    FROM transactions
    WHERE regDate >= datetime(? || ' 00:00:00')
      AND regDate <  datetime(? || ' 00:00:00','+1 day')
  `, [today, today]);

  const tx_7d = await dbGet(`
    SELECT COUNT(*) AS c, IFNULL(SUM(sellAmount),0) AS s
    FROM transactions
    WHERE regDate >= ?
  `, [since7d]);

  const tx_last = await dbGet(`SELECT MAX(regDate) AS ts FROM transactions`);

  // WhatsApp
  let wa_today = { c: 0 }, wa_7d = { c: 0 }, wa_last = { ts: null };
  if (hasWA) {
    wa_today = await dbGet(`
      SELECT COUNT(*) AS c
      FROM whatsapp_new_msg
      WHERE COALESCE(created_at, ttime) >= datetime(? || ' 00:00:00')
        AND COALESCE(created_at, ttime) <  datetime(? || ' 00:00:00','+1 day')
    `, [today, today]);

    wa_7d = await dbGet(`
      SELECT COUNT(*) AS c
      FROM whatsapp_new_msg
      WHERE COALESCE(created_at, ttime) >= ?
    `, [since7d]);

    wa_last = await dbGet(`SELECT MAX(COALESCE(created_at, ttime)) AS ts FROM whatsapp_new_msg`);
  }

  // Instagram
  let ig_today = { c: 0 }, ig_7d = { c: 0 }, ig_last = { ts: null };
  if (hasIG) {
    ig_today = await dbGet(`
      SELECT COUNT(*) AS c
      FROM atigh_instagram_dev
      WHERE created_at >= datetime(? || ' 00:00:00')
        AND created_at <  datetime(? || ' 00:00:00','+1 day')
    `, [today, today]);

    ig_7d = await dbGet(`
      SELECT COUNT(*) AS c
      FROM atigh_instagram_dev
      WHERE created_at >= ?
    `, [since7d]);

    ig_last = await dbGet(`SELECT MAX(created_at) AS ts FROM atigh_instagram_dev`);
  }

  // PDF
  let pdf_today = { c: 0 }, pdf_7d = { c: 0 }, pdf_last = { ts: null };
  if (hasPDF) {
    pdf_today = await dbGet(`
      SELECT COUNT(*) AS c
      FROM wa_pdf_dispatch_log
      WHERE created_at >= datetime(? || ' 00:00:00')
        AND created_at <  datetime(? || ' 00:00:00','+1 day')
    `, [today, today]);

    pdf_7d = await dbGet(`
      SELECT COUNT(*) AS c
      FROM wa_pdf_dispatch_log
      WHERE created_at >= ?
    `, [since7d]);

    pdf_last = await dbGet(`SELECT MAX(created_at) AS ts FROM wa_pdf_dispatch_log`);
  }

  return {
    day: today,

    tx_today_cnt: Number(tx_today?.c || 0),
    tx_7d_cnt: Number(tx_7d?.c || 0),
    tx_7d_sell_sum: Number(tx_7d?.s || 0),

    wa_today_cnt: Number(wa_today?.c || 0),
    wa_7d_cnt: Number(wa_7d?.c || 0),

    ig_today_cnt: Number(ig_today?.c || 0),
    ig_7d_cnt: Number(ig_7d?.c || 0),

    pdf_today_cnt: Number(pdf_today?.c || 0),
    pdf_7d_cnt: Number(pdf_7d?.c || 0),

    tx_last_ts: tx_last?.ts || null,
    wa_last_ts: wa_last?.ts || null,
    ig_last_ts: ig_last?.ts || null,
    pdf_last_ts: pdf_last?.ts || null,
  };
}

/* ---------------------------
   آخرین اسنپ‌شات ذخیره‌شده
--------------------------- */
export async function readLastSignature() {
  return await dbGet(`
    SELECT *
    FROM rahin_kpi_snapshots
    ORDER BY datetime(created_at) DESC
    LIMIT 1
  `);
}

/* ---------------------------
   معیار تصمیم برای ادامه
   - اگر روز عوض شده: پایهٔ امروز را ذخیره و ادامه بده
   - اگر داده‌ی جدید (با تکیه بر max timestampها) آمده: ادامه بده
   - در غیر اینصورت اگر تغییر نسبی KPIها ≥ threshold بود: ادامه بده
--------------------------- */
export function shouldProceed(curr, prev, threshold = 0.10) {
  if (!curr) return { hasNew: false, changeRatio: 0, pass: false };
  if (!prev) return { hasNew: true, changeRatio: 1, pass: true };

  // اگر روز عوض شده، baseline امروز را ثبت کن
  if (curr.day && prev.day && curr.day !== prev.day) {
    return { hasNew: true, changeRatio: 1, pass: true };
  }

  // وجود دادهٔ جدید بر اساس timestamps
  const hasNewByTs =
    newer(curr.tx_last_ts,  prev.tx_last_ts)  ||
    newer(curr.wa_last_ts,  prev.wa_last_ts)  ||
    newer(curr.ig_last_ts,  prev.ig_last_ts)  ||
    newer(curr.pdf_last_ts, prev.pdf_last_ts);

  // بیشترین تغییر نسبی روی KPIهای کلیدی
  const keys = ['tx_7d_sell_sum', 'tx_7d_cnt', 'wa_7d_cnt', 'ig_7d_cnt', 'pdf_7d_cnt'];
  const ratios = keys.map(k => relDiff(curr[k], prev[k]));
  const changeRatio = ratios.length ? Math.max(...ratios) : 0;

  const pass = hasNewByTs || (changeRatio >= threshold);
  return { hasNew: hasNewByTs, changeRatio, pass };
}

/* ---------------------------
   ذخیره اسنپ‌شات امروز (upsert روی day)
--------------------------- */
export async function saveSignature(sig) {
  await dbRun(`
    INSERT INTO rahin_kpi_snapshots
      (day, created_at,
       tx_today_cnt, tx_7d_cnt, tx_7d_sell_sum,
       wa_today_cnt, wa_7d_cnt,
       ig_today_cnt, ig_7d_cnt,
       pdf_today_cnt, pdf_7d_cnt,
       tx_last_ts, wa_last_ts, ig_last_ts, pdf_last_ts)
    VALUES
      (date('now','localtime'), datetime('now','localtime'),
       ?, ?, ?,
       ?, ?,
       ?, ?,
       ?, ?,
       ?, ?, ?, ?)
    ON CONFLICT(day) DO UPDATE SET
      created_at = excluded.created_at,
      tx_today_cnt = excluded.tx_today_cnt,
      tx_7d_cnt = excluded.tx_7d_cnt,
      tx_7d_sell_sum = excluded.tx_7d_sell_sum,
      wa_today_cnt = excluded.wa_today_cnt,
      wa_7d_cnt = excluded.wa_7d_cnt,
      ig_today_cnt = excluded.ig_today_cnt,
      ig_7d_cnt = excluded.ig_7d_cnt,
      pdf_today_cnt = excluded.pdf_today_cnt,
      pdf_7d_cnt = excluded.pdf_7d_cnt,
      tx_last_ts = excluded.tx_last_ts,
      wa_last_ts = excluded.wa_last_ts,
      ig_last_ts = excluded.ig_last_ts,
      pdf_last_ts = excluded.pdf_last_ts
  `, [
    sig.tx_today_cnt, sig.tx_7d_cnt, sig.tx_7d_sell_sum,
    sig.wa_today_cnt, sig.wa_7d_cnt,
    sig.ig_today_cnt, sig.ig_7d_cnt,
    sig.pdf_today_cnt, sig.pdf_7d_cnt,
    sig.tx_last_ts, sig.wa_last_ts, sig.ig_last_ts, sig.pdf_last_ts,
  ]);
}

// در صورت نیاز اگر بخواهی بیرون هم استفاده کنی
export { tableExists, newer, relDiff };
