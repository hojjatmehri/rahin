// ============================================================
// File: src/jobs/job_crm_alerts_daily.js
// Purpose: جمع‌آوری و ارسال آلارم‌های CRM هر شب ساعت ۲:۰۰
// Author: Hojjat Mehri (Stable v4 - Scheduler Compatible)
// ============================================================

import 'dotenv/config';
import 'file:///E:/Projects/rahin/logger.js';
import moment from 'moment-timezone';
import { collectCrmAlerts } from '../alerts/crmAlertCollector.js';
import WhatsAppService from '../WhatsAppService.js';

const TZ = 'Asia/Tehran';
const MOD = '[JobCRMAlertsDaily]';
const MANAGER_MOBILE = process.env.DEV_ALERT_MOBILE || '09134052885';
const DRY_RUN = String(process.env.DRY_RUN || '0') === '1';

const log = (...a) => console.log(MOD, ...a);
const err = (...a) => console.error(MOD, ...a);

export async function main() {
  const start = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
  log(`🕑 CRM Alerts Daily Job started at ${start}`);

  try {
    // اجرای Collector
    const result = await collectCrmAlerts();
    const end = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
    log(`✅ CRM Alerts job completed successfully at ${end}`);

    // ساخت پیام گزارش کوتاه
    const msg =
      `⚠️ اجرای آلارم‌های CRM تکمیل شد\n` +
      `شروع: ${start}\nپایان: ${end}\n` +
      (result?.createdCount
        ? `تعداد آلارم‌های جدید: ${result.createdCount}`
        : `بدون آلارم جدید`);

    // ارسال پیام واتساپ یا چاپ
    if (DRY_RUN) {
      log(`${MOD} [DRY_RUN] پیام واتساپ ارسال نمی‌شود.\n` + msg);
    } else {
      await WhatsAppService.sendMessage(MANAGER_MOBILE, msg);
      log(`${MOD} ✅ گزارش برای ${MANAGER_MOBILE} ارسال شد.`);
    }

    log(`${MOD} 🏁 Job finished.`);
  } catch (e) {
    err(`${MOD} ❌ CRM Alerts job failed: ${e.message}`);
  }
}
