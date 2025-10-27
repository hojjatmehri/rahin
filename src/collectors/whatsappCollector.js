import '../../logger.js';
// src/collectors/whatsappCollector.js
// گردآوری آمار واتساپ + بینش کلیک‌های واتساپ

import { get as dbGet, all as dbAll } from '../db/db.js';

async function tableExists(name) {
  const row = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, [name]);
  return !!row;
}

export async function collectWhatsApp() {
  const out = { inbound_today: 0, unique_contacts_today: 0, inbound_7d: 0, mapped_visitors_7d: 0 };

  try {
    if (!(await tableExists('whatsapp_new_msg'))) return out;
  } catch (e) {
    out._wa0 = e.message;
    return out;
  }

  try {
    out.inbound_today = Number(
      (await dbGet(`
        SELECT COUNT(*) AS cnt
        FROM whatsapp_new_msg
        WHERE date(created_at) = date('now','localtime')
      `))?.cnt || 0
    );
  } catch (e) {
    out._wa1 = e.message;
  }

  try {
    out.unique_contacts_today = Number(
      (await dbGet(`
        SELECT COUNT(DISTINCT mobile) AS uniq_cnt
        FROM whatsapp_new_msg
        WHERE date(created_at) = date('now','localtime')
      `))?.uniq_cnt || 0
    );
  } catch (e) {
    out._wa2 = e.message;
  }

  try {
    out.inbound_7d = Number(
      (await dbGet(`
        SELECT COUNT(*) AS cnt
        FROM whatsapp_new_msg
        WHERE datetime(created_at) >= datetime('now','-7 days','localtime')
      `))?.cnt || 0
    );
  } catch (e) {
    out._wa3 = e.message;
  }

  try {
    if (await tableExists('visitor_contacts')) {
      out.mapped_visitors_7d = Number(
        (await dbGet(`
          SELECT COUNT(DISTINCT visitor_id) AS c
          FROM visitor_contacts
          WHERE datetime(last_seen) >= datetime('now','-7 days','localtime')
        `))?.c || 0
      );
    }
  } catch (e) {
    out._wa4 = e.message;
  }

  return out;
}

export async function whatsappClickInsightsShort() {
  const out = { top_sources: [], top_pages: [], wa_click_rate: 0 };

  try {
    if (!(await tableExists('click_logs'))) return out;
  } catch (e) {
    out._click0 = e.message;
    return out;
  }

  const topSources = await dbAll(`
    SELECT IFNULL(utm_source,'(na)') AS src, COUNT(*) AS cnt
    FROM click_logs
    WHERE click_type='whatsapp'
    GROUP BY src
    ORDER BY cnt DESC
    LIMIT 5
  `);

  const topPages = await dbAll(`
    SELECT page_url, COUNT(*) AS cnt
    FROM click_logs
    WHERE click_type='whatsapp' AND page_url IS NOT NULL
    GROUP BY page_url
    ORDER BY cnt DESC
    LIMIT 5
  `);

  const allClicks = await dbGet(`SELECT COUNT(*) AS c FROM click_logs`);
  const waClicks  = await dbGet(`SELECT COUNT(*) AS c FROM click_logs WHERE click_type='whatsapp'`);
  const rate = Number(waClicks?.c || 0) / Math.max(1, Number(allClicks?.c || 0));

  out.top_sources = (Array.isArray(topSources) ? topSources : [])
     .filter(r => r && (r.src ?? '(na)') !== null)
     .map(r => ({ source: r.src ?? '(na)', cnt: Number(r.cnt || 0) }));
   out.top_pages = (Array.isArray(topPages) ? topPages : [])
     .filter(r => r && r.page_url != null)
     .map(r => ({ page: String(r.page_url || '(na)'), cnt: Number(r.cnt || 0) }));
  out.wa_click_rate = Number((rate * 100).toFixed(1));

  return out;
}

