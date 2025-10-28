// ========================================================
// File: src/jobs/job_crm_alerts_daily.js
// Author: Hojjat Mehri
// Role: Nightly job for CRM alert collection and WhatsApp summary
// Schedule: Every night at 02:00 Tehran time
// ========================================================
import dotenv from 'dotenv';
dotenv.config({ path: 'E:/Projects/AtighgashtAI/.env' });

import '../../logger.js';
import 'dotenv/config';
import cron from 'node-cron';
import moment from 'moment-timezone';
import { collectCrmAlerts } from '../alerts/crmAlertCollector.js';

const TZ = 'Asia/Tehran';
const MOD = '[JobCRMAlertsDaily]';

const log = (...a) => console.log(MOD, ...a);
const warn = (...a) => console.warn(MOD, ...a);
const err = (...a) => console.error(MOD, ...a);

// ========================================================
// اجرای اصلی یک‌باره
// ========================================================
async function runJobOnce() {
  log(`🕑 Starting CRM Alerts Daily Job — ${moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss')}`);
  try {
    await collectCrmAlerts();
    log(`✅ CRM Alerts job completed at ${moment().tz(TZ).format('HH:mm:ss')}`);
  } catch (e) {
    err(`❌ CRM Alerts job failed: ${e.message}`);
  }
}

// ========================================================
// زمان‌بندی خودکار (هر شب ساعت ۲:۰۰ به وقت تهران)
// ========================================================
export function scheduleCrmAlertsJob() {
  const scheduleExpr = '0 2 * * *'; // هر روز ساعت 02:00
  cron.schedule(scheduleExpr, () => {
    log(`🗓 Triggered CRM alerts job (scheduled run)`);
    runJobOnce();
  }, {
    timezone: TZ
  });
  log(`📆 Job scheduled for ${TZ} time at 02:00 every day`);
}

// ========================================================
// اگر مستقیم اجرا شود (مثلاً با node job_crm_alerts_daily.js)
// ========================================================
if (process.argv.includes('--now') || process.argv.includes('--force')) {
  await runJobOnce();
} else {
  scheduleCrmAlertsJob();
}
