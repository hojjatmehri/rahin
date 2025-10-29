// ============================================================
// File: src/jobs/job_financial_score_daily.js
// Purpose: اجرای روزانه مدل امتیاز مالی (FinancialScoreDynamic)
// Author: Hojjat Mehri (v1 - 2025-10-28)
// ============================================================

import 'file:///E:/Projects/rahin/logger.js';
import Database from 'better-sqlite3';
import moment from 'moment-timezone';
import WhatsAppService from '../WhatsAppService.js';
import { computeFinancialScoreDynamic } from '../collectors/financialScoreDynamic.js';
import { collectCustomerValue } from '../collectors/customerValueCollector.js';

const MOD = '[JobFinancialScoreDaily]';
const TZ = 'Asia/Tehran';
const DB_PATH = 'E:/Projects/AtighgashtAI/db_atigh.sqlite';
const MANAGER_MOBILE = process.env.DEV_ALERT_MOBILE || '09134052885';
const DRY_RUN = String(process.env.DRY_RUN || '0') === '1';

const log = (...a) => console.log(MOD, ...a);
const err = (...a) => console.error(MOD, ...a);

(async () => {
    try {
        const now = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
        log(`🚀 Job started at ${now}`);

        // --- مرحله ۱: اجرای مدل مالی پویا ---
        await computeFinancialScoreDynamic();
        log('✅ Financial score recalculated successfully.');

        // --- مرحله ۲: اجرای collector ارزش مشتری ---
        await collectCustomerValue();
        log('✅ Customer value updated based on new financial scores.');

        // --- مرحله ۳: اتصال به دیتابیس برای خلاصهٔ جدید ---
        const db = new Database(DB_PATH);
        db.pragma('journal_mode = WAL');
        db.pragma('synchronous = NORMAL');

        const summary = db
            .prepare(`
        SELECT
          rank_label,
          COUNT(*) AS total_customers,
          ROUND(AVG(value_score),1) AS avg_value_score,
          ROUND(AVG(total_amount),0) AS avg_total_amount,
          ROUND(AVG(payments_count),1) AS avg_payments_count
        FROM customer_value
        GROUP BY rank_label
        ORDER BY avg_value_score DESC;
      `)
            .all();

        db.close();

        // --- مرحله ۴: ساخت گزارش ---
        let report = `📊 گزارش روزانه امتیاز مالی (${moment().tz(TZ).format('YYYY-MM-DD HH:mm')})\n\n`;
        if (summary.length === 0) {
            report += 'هیچ داده‌ای برای نمایش وجود ندارد.';
        } else {
            for (const s of summary) {
                report += `${s.rank_label || 'نامشخص'} → ${s.total_customers} نفر | میانگین ارزش: ${s.avg_value_score} | میانگین خرید: ${Number(s.avg_total_amount).toLocaleString()} تومان | میانگین پرداخت‌ها: ${s.avg_payments_count}\n`;
            }
        }

        // --- مرحله ۵: ارسال واتساپ ---
        if (DRY_RUN) {
            log('📄 DRY_RUN فعال است. گزارش فقط چاپ می‌شود:\n' + report);
        } else {
            await WhatsAppService.sendMessage('98' + MANAGER_MOBILE.replace(/^0/, ''), report);
            log(`✅ گزارش برای ${MANAGER_MOBILE} ارسال شد.`);
        }

        log('🏁 Job completed successfully.');
    } catch (e) {
        err('❌ خطا در اجرای job:', e.message);
    }
})();
