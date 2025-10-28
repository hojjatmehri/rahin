// ============================================================
// File: src/jobs/job_customer_value_daily.js
// Purpose: اجرای روزانه collector ارزش مشتری و ارسال گزارش به واتساپ
// Author: Hojjat Mehri
// ============================================================

import '../../logger.js';
import Database from 'better-sqlite3';
import moment from 'moment-timezone';
import WhatsAppService from '../WhatsAppService.js';
import { collectCustomerValue } from '../collectors/customerValueCollector.js';
import { syncUnifiedProfiles } from '../../../AtighgashtAI/src/collectors/personUnifiedFromDidar.js';

const MOD = '[JobCustomerValueDaily]';
const TZ = 'Asia/Tehran';
const DB_PATH = 'E:/Projects/AtighgashtAI/db_atigh.sqlite';
const MANAGER_MOBILE = process.env.DEV_ALERT_MOBILE || '09134052885';
const DRY_RUN = String(process.env.DRY_RUN || '1') === '1';

const log = (...a) => console.log(MOD, ...a);
const err = (...a) => console.error(MOD, ...a);

(async () => {
  try {
    const now = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
    log(`🚀 Job started at ${now}`);

    // --- مرحله ۰: همگام‌سازی پروفایل‌های دیدار و فرم‌افزار ---
    try {
      syncUnifiedProfiles();
      log('✅ Unified profiles synced from Didar & Formafzar.');
    } catch (e) {
      log('⚠️ syncUnifiedProfiles failed:', e.message);
    }

    // --- مرحله ۱: اجرای Collector ---
    await collectCustomerValue();
    log('✅ Customer value recalculated.');

    // --- مرحله ۲: اتصال به دیتابیس ---
    const db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');

    // --- مرحله ۳: بازسازی ویو برای اطمینان از وجود contact_name ---
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
    log('✅ View v_customer_value_ranked rebuilt successfully.');

    // --- مرحله ۴: دریافت ۱۰ مشتری برتر ---
    const topCustomers = db
      .prepare(`
        SELECT mobile, contact_name, ROUND(value_score, 1) AS value_score, recency_days
        FROM v_customer_value_ranked
        ORDER BY value_score DESC
        LIMIT 10
      `)
      .all();

    db.close();

    // --- مرحله ۵: ساخت گزارش ---
    let report = `📊 گزارش ارزش مشتری‌ها (${moment().tz(TZ).format('YYYY-MM-DD HH:mm')})\n\n`;
    if (topCustomers.length === 0) {
      report += 'هیچ داده‌ای برای نمایش وجود ندارد.';
    } else {
      report += '🏆 ۱۰ مشتری برتر بر اساس امتیاز ارزش:\n\n';
      topCustomers.forEach((c, i) => {
        const name = c.contact_name || 'بدون‌نام';
        const line = `${i + 1}. ${name} (${c.mobile}) — امتیاز: ${c.value_score} — آخرین فعالیت: ${c.recency_days} روز قبل`;
        report += line + '\n';
      });
    }

    // --- مرحله ۶: ارسال واتساپ یا چاپ ---
    if (DRY_RUN) {
      log('📄 DRY_RUN فعال است. گزارش فقط چاپ می‌شود:\n' + report);
    } else {
      await WhatsAppService.sendMessage(MANAGER_MOBILE, report);
      log(`✅ گزارش برای ${MANAGER_MOBILE} ارسال شد.`);
    }

    log('🏁 Job completed successfully.');
  } catch (e) {
    err('❌ خطا در اجرای job:', e.message);
  }
})();
