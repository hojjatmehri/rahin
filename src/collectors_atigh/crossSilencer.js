// ========================================================
// File: src/collectors/crossSilencer.js
// Author: Hojjat Mehri
// Role: Cross-channel silence controller to prevent over-contacting
// ========================================================

import moment from 'moment-timezone';
import { all, run } from 'file:///E:/Projects/AtighgashtAI/lib/db/db.js'; // ← async-safe wrapper
import { withDbRetry } from 'file:///E:/Projects/AtighgashtAI/lib/db/dbRetryQueue.js';
import { db } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';

const MOD = '[CrossSilencer]';
const TZ = 'Asia/Tehran';
const SILENCE_WINDOW_MIN = Number(process.env.CROSS_SILENCE_MIN || 360);

function now() { return moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss'); }
const log  = (...a) => console.log(`${MOD} ${now()} |`, ...a);
const warn = (...a) => console.warn(`${MOD} ${now()} ⚠️ |`, ...a);
const err  = (...a) => console.error(`${MOD} ${now()} ❌ |`, ...a);

/**
 * Returns last activity timestamp (epoch ms) for a given mobile number,
 * across both Didar timeline and WhatsApp messages.
 */
export async function getLastActivityTimestamp(mobile) {
  if (!mobile) return 0;

  // نرمال‌سازی شماره موبایل برای جست‌وجوی دقیق
  const normalized = String(mobile)
    .replace('@c.us', '')
    .replace(/^98/, '0')
    .replace(/\D+/g, '')
    .slice(-10);

  const likePattern = `%${normalized}%`;

  // --- منبع ۱: WhatsApp ---
  const [wa] = await all(`
    SELECT MAX(ts_millis) AS t
    FROM wa_messages_valid
    WHERE phone_norm LIKE ?;
  `, [likePattern]);

  // --- منبع ۲: Didar Contacts ---
  const [crm] = await all(`
    SELECT MAX(COALESCE(last_touch_ts, MAX(last_in, last_out))) AS t
    FROM didar_contacts
    WHERE phone_norm LIKE ? OR mobile_norm LIKE ?;
  `, [likePattern, likePattern]);

  const waTs = Number(wa?.t) || 0;
  const crmTs = Number(crm?.t) || 0;

  // بیشترین زمان بین واتساپ و دیدار
  return Math.max(waTs, crmTs);
}

/**
 * Determines if a contact should be silenced due to recent activity.
 */
export async function checkCrossSilence(mobile) {
  const lastTs = await getLastActivityTimestamp(mobile);
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
 * Scans message_queue and flags messages that should be silenced
 * (recent contact in any channel).
 */
export async function applyCrossSilence() {
  const queued = await all(`
    SELECT id, mobile, planned_for
    FROM message_queue
    WHERE status IN ('queued','pending');
  `);

  if (!queued.length) {
    log('🎯 No queued messages to evaluate.');
    return;
  }

  let silenced = 0;
  let kept = 0;

  for (const row of queued) {
    const { shouldSilence, minutesAgo } = await checkCrossSilence(row.mobile);
    if (shouldSilence) {
      await withDbRetry(async () => {
        run(`
          UPDATE message_queue
          SET status = 'silenced',
              meta = json_set(COALESCE(meta,'{}'),
                              '$.silenced_at',?,
                              '$.silenced_reason',?)
          WHERE id = ?;
        `, [now(), `recent activity ${minutesAgo.toFixed(1)}min ago`, row.id]);
      }, { jobName: 'crossSilenceUpdate', retries: 3 });
      silenced++;
      if (silenced % 50 === 0) log(`...silenced ${silenced} so far`);
    } else kept++;
  }

  log(`✅ Cross-silence applied. Silenced=${silenced}, Kept=${kept}, Total=${queued.length}`);
}

/**
 * Lightweight API for Rahin pipelines.
 * Returns { active: boolean, until: string, reason: string }
 */
export async function shouldSilence(mobile) {
  try {
    const { shouldSilence: active, minutesAgo, lastAt } = await checkCrossSilence(mobile);
    if (active) {
      const until = moment().tz(TZ)
        .add(SILENCE_WINDOW_MIN - minutesAgo, 'minutes')
        .format('YYYY-MM-DD HH:mm:ss');
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
