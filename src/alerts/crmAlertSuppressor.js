// ========================================================
// File: src/alerts/crmAlertSuppressor.js
// Author: Hojjat Mehri
// Role: Smart suppression for CRM alerts (cross-channel + recency + auto-extend)
// ========================================================

import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import moment from 'moment-timezone';

const DB_PATH = 'C:/Users/Administrator/Desktop/Projects/AtighgashtAI/db_atigh.sqlite';
const TZ = 'Asia/Tehran';

// سکوت متقاطع (اگر در این مدت فعالیتی بوده، هشدار نسازد)
const CROSS_CHANNEL_HOURS = 72;

// اگر هشدار مشابهی در این بازه وجود دارد → تمدید suppress_until
const RECENT_ALERT_HOURS = 72;

// مدت تمدید suppress (در ساعت)
const EXTEND_HOURS = 48;

function log(...a) {
  console.log(`[AlertSuppressor ${moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss')}]`, ...a);
}

// ========================================================
// ۱. بررسی cross-channel silence
// ========================================================
export async function shouldSuppressCrossChannel(contactId) {
  const db = await open({ filename: DB_PATH, driver: sqlite3.Database });

  try {
    const cutoff = Date.now() - CROSS_CHANNEL_HOURS * 3600 * 1000;
    const recentActivity = await db.get(
      `
      SELECT event_type, event_date
      FROM didar_activity_timeline
      WHERE contact_id = ?
        AND ts_millis > ?
      ORDER BY ts_millis DESC
      LIMIT 1
      `,
      [contactId, cutoff]
    );

    if (recentActivity) {
      return {
        suppressed: true,
        reason: `active ${recentActivity.event_type} within ${CROSS_CHANNEL_HOURS}h`,
      };
    }

    return { suppressed: false };
  } catch (err) {
    log('❌ Error in shouldSuppressCrossChannel:', err.message);
    return { suppressed: false };
  } finally {
    await db.close();
  }
}

// ========================================================
// ۲. بررسی تکرار آلارم + تمدید suppress_until
// ========================================================
export async function shouldSuppressRecentAlert(db, contactId, alertType) {
  try {
    const since = moment().tz(TZ).subtract(RECENT_ALERT_HOURS, 'hours').format('YYYY-MM-DD HH:mm:ss');

    // آیا هشدار مشابهی اخیراً وجود دارد؟
    const alert = await db.get(
      `
      SELECT id, created_at, suppress_until
      FROM didar_crm_alerts
      WHERE contact_id = ? AND alert_type = ? AND created_at >= ?
      ORDER BY created_at DESC
      LIMIT 1
      `,
      [contactId, alertType, since]
    );

    if (!alert) return { suppressed: false };

    const now = moment().tz(TZ);
    const suppressUntil = moment(alert.suppress_until || alert.created_at).tz(TZ);

    // اگر هنوز suppress_until نگذشته → فقط تمدید کن
    if (now.isBefore(suppressUntil)) {
      const newUntil = suppressUntil.clone().add(EXTEND_HOURS, 'hours').format('YYYY-MM-DD HH:mm:ss');
      await db.run(
        `UPDATE didar_crm_alerts SET suppress_until = ?, meta_json = json_set(COALESCE(meta_json, '{}'), '$.extended_at', ?, '$.extended_by', ?) WHERE id = ?`,
        [newUntil, now.format('YYYY-MM-DD HH:mm:ss'), 'crmAlertSuppressor', alert.id]
      );
      log(`🕓 Extended alert for contact ${contactId} until ${newUntil}`);
      return { suppressed: true, reason: `extended existing alert id=${alert.id}` };
    }

    // اگر suppress_until گذشته → اجازه ساخت هشدار جدید بده
    return { suppressed: false };
  } catch (err) {
    log('❌ Error in shouldSuppressRecentAlert:', err.message);
    return { suppressed: false };
  }
}
