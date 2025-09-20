// src/collectors/pdfCollector.js
// آمار ارسال PDF: وضعیت‌ها امروز/۷روز و مخاطبان برتر امروز

import { get as dbGet, all as dbAll } from '../db/db.js';

async function tableExists(name) {
  const row = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, [name]);
  return !!row;
}

export async function collectPDF() {
  const out = { today_by_status: [], last_7d_by_status: [], top_contacts_today: [] };

  try {
    if (!(await tableExists('wa_pdf_dispatch_log'))) return out;
  } catch (e) {
    out._pdf0 = e.message;
    return out;
  }

  try {
    const t = await dbAll(`
      SELECT wa_status, COUNT(*) AS cnt
      FROM wa_pdf_dispatch_log
      WHERE date(created_at)=date('now','localtime')
      GROUP BY wa_status
      ORDER BY cnt DESC
    `);
    out.today_by_status = (t || []).map(r => ({
      status: r.wa_status,
      cnt: Number(r.cnt || 0),
    }));
  } catch (e) {
    out._pdf1 = e.message;
  }

  try {
    const s7 = await dbAll(`
      SELECT wa_status, COUNT(*) AS cnt
      FROM wa_pdf_dispatch_log
      WHERE datetime(created_at)>=datetime('now','-7 days','localtime')
      GROUP BY wa_status
      ORDER BY cnt DESC
    `);
    out.last_7d_by_status = (s7 || []).map(r => ({
      status: r.wa_status,
      cnt: Number(r.cnt || 0),
    }));
  } catch (e) {
    out._pdf2 = e.message;
  }

  try {
    const top = await dbAll(`
      SELECT contact_id, COUNT(*) AS cnt
      FROM wa_pdf_dispatch_log
      WHERE date(created_at)=date('now','localtime')
      GROUP BY contact_id
      ORDER BY cnt DESC
      LIMIT 10
    `);
    out.top_contacts_today = (top || []).map(r => ({
      contact_id: r.contact_id,
      cnt: Number(r.cnt || 0),
    }));
  } catch (e) {
    out._pdf3 = e.message;
  }

  return out;
}
