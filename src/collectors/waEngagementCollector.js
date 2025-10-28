// ========================================================
// File: src/collectors/waEngagementCollector.js
// Purpose: Update person_unified_profile with WhatsApp engagement stats
// Author: Hojjat Mehri
// ========================================================

import '../../logger.js';
import Database from 'better-sqlite3';
import moment from 'moment-timezone';

const MOD = '[waEngagementCollector]';
const TZ = process.env.APP_TZ || 'Asia/Tehran';
const DB_PATH = process.env.MAIN_DB_PATH || 'E:/Projects/AtighgashtAI/db_atigh.sqlite';

// اتصال به دیتابیس
function openDB() {
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.pragma('synchronous = NORMAL');
  return db;
}

// شمارش پیام‌های ۳۰ روز اخیر از جداول واتساپ
export function collectWAEngagement() {
  const db = openDB();
  const cutoff = moment().tz(TZ).subtract(30, 'days').format('YYYY-MM-DD HH:mm:ss');

  console.log(`${MOD} 🚀 Collector started at ${moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss')}`);

  // اطمینان از وجود جداول
  const hasInbound = db.prepare(`
    SELECT name FROM sqlite_master WHERE type='table' AND name='whatsapp_new_msg';
  `).get();
  const hasOutbound = db.prepare(`
    SELECT name FROM sqlite_master WHERE type='table' AND name='message_log';
  `).get();
  const hasProfile = db.prepare(`
    SELECT name FROM sqlite_master WHERE type='table' AND name='person_unified_profile';
  `).get();

  if (!hasInbound || !hasOutbound || !hasProfile) {
    console.warn(`${MOD} ⚠️ Required tables missing.`);
    db.close();
    return;
  }

  // دریافت آمار پیام‌های دریافتی (inbound)
  const inboundRows = db.prepare(`
    SELECT
      REPLACE(REPLACE(REPLACE(mobile,'+',''),' ',''),
      '0098','98') AS mobile,
      MAX(created_at) AS last_in_at,
      COUNT(*) AS in_count_30d
    FROM whatsapp_new_msg
    WHERE datetime(created_at) >= datetime(?)
    GROUP BY mobile
  `).all(cutoff);

  // دریافت آمار پیام‌های ارسالی (outbound)
  const outboundRows = db.prepare(`
    SELECT
      REPLACE(REPLACE(REPLACE(mobile,'+',''),' ',''),
      '0098','98') AS mobile,
      MAX(sent_at) AS last_out_at,
      COUNT(*) AS out_count_30d
    FROM message_log
    WHERE channel='wa' AND datetime(sent_at) >= datetime(?)
    GROUP BY mobile
  `).all(cutoff);

  const inboundMap = new Map();
  inboundRows.forEach(r => inboundMap.set(r.mobile, r));

  let updated = 0;
  const updateStmt = db.prepare(`
    UPDATE person_unified_profile
    SET
      last_in_at = COALESCE(@last_in_at, last_in_at),
      last_out_at = COALESCE(@last_out_at, last_out_at),
      in_count_30d = COALESCE(@in_count_30d, in_count_30d),
      out_count_30d = COALESCE(@out_count_30d, out_count_30d),
      last_touch_ts = COALESCE(
        MAX(@last_in_at, @last_out_at, last_touch_ts),
        last_touch_ts
      )
    WHERE REPLACE(REPLACE(REPLACE(mobile,'+',''),' ',''),'0098','98') = @mobile
  `);

  db.transaction(() => {
    for (const out of outboundRows) {
      const inData = inboundMap.get(out.mobile);
      const params = {
        mobile: out.mobile,
        last_in_at: inData?.last_in_at || null,
        last_out_at: out.last_out_at || null,
        in_count_30d: inData?.in_count_30d || 0,
        out_count_30d: out.out_count_30d || 0
      };
      const res = updateStmt.run(params);
      if (res.changes > 0) updated++;
    }

    // inboundهایی که outbound ندارند
    for (const [mobile, inData] of inboundMap.entries()) {
      const existsOut = outboundRows.find(o => o.mobile === mobile);
      if (!existsOut) {
        const params = {
          mobile,
          last_in_at: inData.last_in_at || null,
          last_out_at: null,
          in_count_30d: inData.in_count_30d || 0,
          out_count_30d: 0
        };
        const res = updateStmt.run(params);
        if (res.changes > 0) updated++;
      }
    }
  })();

  console.log(`${MOD} ✅ Completed. Updated ${updated} profiles with WA engagement data.`);
  db.close();
  console.log(`${MOD} 🏁 Collector finished.`);
}
