// src/collectors/instagramCollector.js
// گردآوری آمار جدول atigh_instagram_dev

import { get as dbGet } from '../db/db.js';

async function tableExists(name) {
  const row = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, [name]);
  return !!row;
}

export async function collectInstagram() {
  const out = { dev_events_today: 0, dev_events_7d: 0 };

  try {
    if (!(await tableExists('atigh_instagram_dev'))) return out;
  } catch (e) {
    out._ig0 = e.message;
    return out;
  }

  try {
    out.dev_events_today = Number(
      (await dbGet(`
        SELECT COUNT(*) AS cnt
        FROM atigh_instagram_dev
        WHERE date(created_at) = date('now','localtime')
      `))?.cnt || 0
    );
  } catch (e) {
    out._ig1 = e.message;
  }

  try {
    out.dev_events_7d = Number(
      (await dbGet(`
        SELECT COUNT(*) AS cnt
        FROM atigh_instagram_dev
        WHERE datetime(created_at) >= datetime('now','-7 days','localtime')
      `))?.cnt || 0
    );
  } catch (e) {
    out._ig2 = e.message;
  }

  return out;
}
