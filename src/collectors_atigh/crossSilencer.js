// ========================================================
// File: src/collectors/crossSilencer.js
// Author: Hojjat Mehri
// Role: Cross-channel silence controller to prevent over-contacting
// ========================================================

import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import moment from 'moment-timezone';

const MOD = '[CrossSilencer]';
const TZ = 'Asia/Tehran';
const DB_PATH = 'E:/Projects/AtighgashtAI/db_atigh.sqlite';

// در دقیقه — مثلاً ۳۶۰ یعنی ۶ ساعت سکوت بعد از هر تماس در هر کانال
const SILENCE_WINDOW_MIN = Number(process.env.CROSS_SILENCE_MIN || 360);

function now() {
  return moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
}
function log(...a) {
  console.log(`${MOD} ${now()} |`, ...a);
}
function warn(...a) {
  console.warn(`${MOD} ${now()} ⚠️ |`, ...a);
}
function err(...a) {
  console.error(`${MOD} ${now()} ❌ |`, ...a);
}

/**
 * Returns last activity timestamp (epoch ms) for a given mobile number,
 * across both Didar timeline and WhatsApp messages.
 */
export async function getLastActivityTimestamp(db, mobile) {
  if (!mobile) return 0;

  // نرمال‌سازی شماره موبایل برای جست‌وجوی دقیق
  const normalized = String(mobile)
    .replace('@c.us', '')
    .replace(/^98/, '0')
    .replace(/\D+/g, '')
    .slice(-10);

  const likePattern = `%${normalized}%`;

  // --- منبع ۱: WhatsApp ---
  const [wa] = await db.all(
    `SELECT MAX(ts_millis) AS t
     FROM wa_messages_valid
     WHERE phone_norm LIKE ?`,
    [likePattern]
  );

  // --- منبع ۲: Didar Contacts ---
  const [crm] = await db.all(
    `SELECT MAX(COALESCE(last_touch_ts, MAX(last_in, last_out))) AS t
     FROM didar_contacts
     WHERE phone_norm LIKE ? OR mobile_norm LIKE ?`,
    [likePattern, likePattern]
  );

  const waTs = Number(wa?.t) || 0;
  const crmTs = Number(crm?.t) || 0;

  // بیشترین زمان بین واتساپ و دیدار
  return Math.max(waTs, crmTs);
}




/**
 * Determines if a contact should be silenced due to recent activity.
 * @returns {Promise<{shouldSilence: boolean, minutesAgo: number, lastTs: number}>}
 */
export async function checkCrossSilence(db, mobile) {
  const lastTs = await getLastActivityTimestamp(db, mobile);
  if (!lastTs) return { shouldSilence: false, minutesAgo: Infinity, lastTs: 0 };

  const diffMin = (Date.now() - lastTs) / 60000;
  const shouldSilence = diffMin < SILENCE_WINDOW_MIN;

  return {
    shouldSilence,
    minutesAgo: diffMin,
    lastTs,
    lastAt: new Date(lastTs).toLocaleString('fa-IR', { timeZone: TZ })
  };
}

/**
 * Scans the message_queue and flags messages that should be silenced
 * (recent contact in any channel).
 */
export async function applyCrossSilence() {
  const db = await open({ filename: DB_PATH, driver: sqlite3.Database });
  log('✅ Connected to SQLite:', DB_PATH);

  // فیلدهای لازم در message_queue: id, mobile, planned_for, status
  const queued = await db.all(`
    SELECT id, mobile, planned_for
    FROM message_queue
    WHERE status = 'queued' OR status = 'pending'
  `);

  if (!queued.length) {
    log('🎯 No queued messages to evaluate.');
    await db.close();
    return;
  }

  let silenced = 0;
  let kept = 0;

  for (const row of queued) {
    const { shouldSilence, minutesAgo } = await checkCrossSilence(db, row.mobile);
    if (shouldSilence) {
      await db.run(
        `UPDATE message_queue 
         SET status = 'silenced', 
             meta = json_set(COALESCE(meta,'{}'),
                             '$.silenced_at',?,
                             '$.silenced_reason',?)
         WHERE id = ?`,
        [now(), `recent activity ${minutesAgo.toFixed(1)}min ago`, row.id]
      );
      silenced++;
      if (silenced % 50 === 0) log(`...silenced ${silenced} so far`);
    } else {
      kept++;
    }
  }

  log(`✅ Cross-silence applied. Silenced=${silenced}, Kept=${kept}, Total=${queued.length}`);
  await db.close();
  log('🧱 Database connection closed cleanly.');
}
/**
 * Lightweight API for Rahin pipelines.
 * Returns { active: boolean, until: string, reason: string }
 */
export async function shouldSilence(mobile) {
  try {
    const db = await open({ filename: DB_PATH, driver: sqlite3.Database });
    const { shouldSilence: active, minutesAgo, lastAt } = await checkCrossSilence(db, mobile);
    await db.close();

    if (active) {
      const until = moment().tz(TZ).add(SILENCE_WINDOW_MIN - minutesAgo, 'minutes').format('YYYY-MM-DD HH:mm:ss');
      return {
        active: true,
        until,
        reason: `recent contact ${minutesAgo.toFixed(1)} min ago (last at ${lastAt})`
      };
    }

    return { active: false, until: null, reason: null };
  } catch (e) {
    err('shouldSilence failed:', e);
    return { active: false, until: null, reason: 'error' };
  }
}

// CLI entry
if (import.meta.url === `file://${process.argv[1]}`) {
  applyCrossSilence().catch(e => err('applyCrossSilence failed:', e.message || e));
}
