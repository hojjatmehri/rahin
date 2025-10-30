// ============================================================
// File: src/jobs/job_customer_value_daily.js
// Purpose: اجرای روزانه collector ارزش مشتری و ارسال گزارش به واتساپ
// Author: Hojjat Mehri (Stable v6 – Scheduler Compatible)
// ============================================================

import 'file:///E:/Projects/rahin/logger.js';
import moment from 'moment-timezone';
import { db } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';
import { withDbRetry } from 'file:///E:/Projects/AtighgashtAI/lib/db/dbRetryQueue.js';
import { collectCustomerValue } from '../collectors/customerValueCollector.js';
import { syncUnifiedProfiles } from '../../../AtighgashtAI/src/collectors/personUnifiedFromDidar.js';
import WhatsAppService from '../WhatsAppService.js';
import { acquireGlobalLock, releaseGlobalLock } from '../lib/db/jobLock.js';

const MOD = '[JobCustomerValueDaily]';
const TZ = 'Asia/Tehran';
const MANAGER_MOBILE = process.env.DEV_ALERT_MOBILE || '09134052885';
const DRY_RUN = String(process.env.DRY_RUN || '1') === '1';

export async function main() {
  const now = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
  console.log(`${MOD} 🚀 Started at ${now}`);

  // جلوگیری از اجرای همزمان
  if (!acquireGlobalLock(MOD)) {
    console.warn(`${MOD} 🔒 Skipped due to global DB lock.`);
    return;
  }

  try {
    // --- مرحله ۰: همگام‌سازی پروفایل‌های دیدار و فرم‌افزار ---
    try {
      await syncUnifiedProfiles();
      console.log(`${MOD} ✅ Unified profiles synced from Didar & Formafzar.`);
    } catch (e) {
      console.warn(`${MOD} ⚠️ syncUnifiedProfiles failed:`, e.message);
    }

    // --- مرحله ۱: اجرای Collector (با retry داخلی) ---
    await withDbRetry(() => collectCustomerValue(), {
      jobName: 'collectCustomerValue',
      retries: 15,
      initialDelayMs: 1000,
      backoffFactor: 1.6,
    });
    console.log(`${MOD} ✅ Customer value recalculated.`);

    // --- مرحله ۲: بازسازی View ---
    db.exec(`
      DROP VIEW IF EXISTS v_customer_value_ranked;
      CREATE VIEW v_customer_value_ranked AS
      SELECT
        cv.mobile,
        COALESCE(pup.contact_name, 'درج نشده') AS contact_name,
        cv.value_score,
        cv.whatsapp_score,
        cv.crm_stage_score,
        cv.financial_score,
        cv.total_interactions,
        cv.total_amount,
        cv.payments_count,
        cv.last_payment_at,
        cv.updated_at,
        cv.rank_label,
        CAST((julianday('now') - julianday(cv.last_payment_at)) AS INT) AS recency_days
      FROM customer_value cv
      LEFT JOIN person_unified_profile pup
        ON pup.mobile = cv.mobile
      ORDER BY cv.value_score DESC;
    `);
    console.log(`${MOD} ✅ View v_customer_value_ranked rebuilt.`);

    // --- مرحله ۳: انتخاب ۱۰ مشتری برتر ---
    const topCustomers = db
      .prepare(`
        SELECT mobile, contact_name, ROUND(value_score, 1) AS value_score, recency_days
        FROM v_customer_value_ranked
        ORDER BY value_score DESC
        LIMIT 10
      `)
      .all();

    // --- مرحله ۴: ساخت گزارش ---
    let report = `📊 گزارش ارزش مشتری‌ها (${moment().tz(TZ).format('YYYY-MM-DD HH:mm')})\n\n`;
    if (topCustomers.length === 0) {
      report += 'هیچ داده‌ای برای نمایش وجود ندارد.';
    } else {
      report += '🏆 ۱۰ مشتری برتر بر اساس امتیاز ارزش:\n\n';
      topCustomers.forEach((c, i) => {
        const name = c.contact_name || 'بدون‌نام';
        report += `${i + 1}. ${name} (${c.mobile}) — امتیاز: ${c.value_score} — آخرین فعالیت: ${c.recency_days} روز قبل\n`;
      });
    }

    // --- مرحله ۵: ارسال واتساپ یا چاپ ---
    if (DRY_RUN) {
      console.log(`${MOD} 📄 DRY_RUN فعال است. گزارش فقط چاپ می‌شود:\n${report}`);
    } else {
      await WhatsAppService.sendMessage(MANAGER_MOBILE, report);
      console.log(`${MOD} ✅ گزارش برای ${MANAGER_MOBILE} ارسال شد.`);
    }

    console.log(`${MOD} 🏁 Job completed successfully.`);
  } catch (e) {
    console.error(`${MOD} ❌ خطا در اجرای job:`, e.message);
  } finally {
    releaseGlobalLock(MOD);
  }
}
