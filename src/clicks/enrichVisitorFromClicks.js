// src/clicks/enrichVisitorFromClicks.js
// استخراج موبایل از کلیک‌های واتساپ و ذخیره آن برای visitorها

import { get as dbGet, all as dbAll, run as dbRun, exec as dbExec } from '../db/db.js';

/* ---------------------------------------
   Helpers: mobile normalization & parsing
--------------------------------------- */

/**
 * نرمال‌سازی شماره موبایل ایران به فرمت 98xxxxxxxxxx
 * @param {string} m
 * @returns {string|null}
 */
export function normalizeMobile(m) {
  const digits = String(m || '').replace(/[^\d]/g, '');
  if (!digits) return null;
  if (digits.startsWith('98')) return digits;
  if (digits.startsWith('0'))  return '98' + digits.slice(1);
  if (digits.startsWith('9'))  return '98' + digits;
  return digits;
}

/**
 * استخراج شماره موبایل از URL واتساپ
 * پشتیبانی از:
 *  - https://wa.me/98XXXXXXXXXX
 *  - https://api.whatsapp.com/send?phone=98XXXXXXXXXX
 *  - ...?text=... (گاهی شماره داخل متن است)
 * @param {string} u
 * @returns {string|null} mobile in 98xxxxxxxxxx or null
 */
export function extractMobileFromWhatsAppUrl(u = '') {
  try {
    const url = new URL(u);
    const host = url.hostname.toLowerCase();
    const isWa = /(^|\.)wa\.me$|(^|\.)whatsapp\.com$/.test(host);
    if (!isWa) return null;

    // 1) مسیر مانند /98xxxxxxxxxx
    const pathNum = url.pathname.replace(/^\/+/, '').match(/^\d{9,15}$/);
    if (pathNum) return normalizeMobile(pathNum[0]);

    // 2) پارامتر phone
    const p = url.searchParams.get('phone');
    if (p) return normalizeMobile(p);

    // 3) شماره در متن پیام
    const t = url.searchParams.get('text') || '';
    const m = t.match(/(?:\+?98|0)?9\d{9}/);
    if (m) return normalizeMobile(m[0]);

    return null;
  } catch {
    return null;
  }
}

/* ---------------------------------------
   DB: ensure & upsert
--------------------------------------- */

async function ensureVisitorContactsTable() {
  await dbExec(`
    CREATE TABLE IF NOT EXISTS visitor_contacts (
      visitor_id TEXT NOT NULL,
      mobile     TEXT NOT NULL,
      source     TEXT,
      confidence INTEGER,
      last_seen  TEXT DEFAULT (datetime('now','localtime')),
      PRIMARY KEY (visitor_id, mobile)
    );
  `);
  await dbExec(`CREATE INDEX IF NOT EXISTS idx_visitor_contacts_mobile ON visitor_contacts(mobile);`);
}

/**
 * ذخیره/به‌روزرسانی موبایل برای visitor
 * @param {string} visitor_id
 * @param {string} mobile - normalized (98xxxxxxxxxx)
 * @param {string} source
 * @param {number} confidence
 */
export async function upsertVisitorMobile(visitor_id, mobile, source = 'whatsapp_click', confidence = 95) {
  if (!visitor_id || !mobile) return;
  await ensureVisitorContactsTable();
  await dbRun(`
    INSERT INTO visitor_contacts (visitor_id, mobile, source, confidence, last_seen)
    VALUES (?, ?, ?, ?, datetime('now','localtime'))
    ON CONFLICT(visitor_id, mobile) DO UPDATE SET
      last_seen  = datetime('now','localtime'),
      source     = CASE WHEN excluded.confidence >= visitor_contacts.confidence THEN excluded.source ELSE visitor_contacts.source END,
      confidence = MAX(visitor_contacts.confidence, excluded.confidence)
  `, [visitor_id, mobile, source, confidence]);
}

/* ---------------------------------------
   Main enricher
--------------------------------------- */

/**
 * همه کلیک‌های واتساپ را از click_logs می‌خواند، موبایل را از URL استخراج کرده و
 * برای هر visitor_id ذخیره می‌کند. خروجی: تعداد upsert موفق.
 * نیازمند جدول‌های: click_logs(visitor_id, target_url, click_type, clicked_at)
 */
export async function enrichVisitorMobilesFromClicks() {
  // بررسی وجود click_logs
  const tbl = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name='click_logs'`);
  if (!tbl) return 0;

  const rows = await dbAll(`
    SELECT id, visitor_id, target_url, clicked_at
    FROM click_logs
    WHERE click_type='whatsapp' AND target_url IS NOT NULL
  `);

  let inserted = 0;
  for (const r of rows || []) {
    const mobile = extractMobileFromWhatsAppUrl(r.target_url);
    if (!mobile || !r.visitor_id) continue;
    await upsertVisitorMobile(r.visitor_id, mobile, 'whatsapp_click', 95);
    inserted++;
  }
  return inserted;
}

/* ---------------------------------------
   Optional: tiny self-test (run manually)
--------------------------------------- */
// if (process.env.NODE_ENV === 'development') {
//   (async () => {
//     const n = await enrichVisitorMobilesFromClicks();
//     console.log(`[enrichVisitorFromClicks] inserted/updated: ${n}`);
//   })().catch(console.error);
// }
