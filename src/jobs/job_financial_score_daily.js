// ============================================================
// File: src/jobs/job_financial_score_daily.js
// Purpose: اجرای روزانه مدل امتیاز مالی (FinancialScoreDynamic)
// Author: Hojjat Mehri (Stable v4 – Scheduler Compatible)
// ============================================================

import 'file:///E:/Projects/rahin/logger.js';
import { db } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';
import moment from 'moment-timezone';
import WhatsAppService from '../WhatsAppService.js';
import { computeFinancialScoreDynamic } from '../collectors/financialScoreDynamic.js';
import { collectCustomerValue } from '../collectors/customerValueCollector.js';
import { withDbRetry } from 'file:///E:/Projects/AtighgashtAI/lib/db/dbRetryQueue.js';

const MOD = '[JobFinancialScoreDaily]';
const TZ = 'Asia/Tehran';
const MANAGER_MOBILE = process.env.DEV_ALERT_MOBILE || '09134052885';
const DRY_RUN = String(process.env.DRY_RUN || '0') === '1';

const log = (...a) => console.log(MOD, ...a);
const err = (...a) => console.error(MOD, ...a);

export async function main() {
  try {
    const start = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
    log(`🚀 Job started at ${start}`);

    // ========================================================
    // مرحله ۱: اجرای مدل مالی پویا با Retry
    // ========================================================
    await withDbRetry(
      async () => {
        await computeFinancialScoreDynamic();
      },
      { jobName: 'computeFinancialScoreDynamic', retries: 5, initialDelayMs: 5000 }
    );
    log('✅ Financial score recalculated successfully.');

    // ========================================================
    // مرحله ۲: Collector ارزش مشتری
    // ========================================================
    await withDbRetry(
      async () => {
        await collectCustomerValue();
      },
      { jobName: 'collectCustomerValue', retries: 5, initialDelayMs: 5000 }
    );
    log('✅ Customer value updated based on new financial scores.');

    // ========================================================
    // مرحله ۳: خلاصهٔ گزارش روز
    // ========================================================
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

    // ========================================================
    // مرحله ۴: ساخت متن گزارش
    // ========================================================
    let report = `📊 گزارش روزانه امتیاز مالی (${moment()
      .tz(TZ)
      .format('YYYY-MM-DD HH:mm')})\n\n`;

    if (summary.length === 0) {
      report += 'هیچ داده‌ای برای نمایش وجود ندارد.';
    } else {
      for (const s of summary) {
        report += `${s.rank_label || 'نامشخص'} → ${s.total_customers} نفر | میانگین ارزش: ${s.avg_value_score} | میانگین خرید: ${Number(
          s.avg_total_amount
        ).toLocaleString()} تومان | میانگین پرداخت‌ها: ${s.avg_payments_count}\n`;
      }
    }

    // ========================================================
    // مرحله ۵: ارسال گزارش به واتساپ
    // ========================================================
    if (DRY_RUN) {
      log('📄 DRY_RUN فعال است. گزارش فقط چاپ می‌شود:\n' + report);
    } else {
      await WhatsAppService.sendMessage('98' + MANAGER_MOBILE.replace(/^0/, ''), report);
      log(`✅ گزارش برای ${MANAGER_MOBILE} ارسال شد.`);
    }

    const end = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
    log(`🏁 Job completed successfully. (${start} → ${end})`);
  } catch (e) {
    err('❌ خطا در اجرای job:', e.message);
  }
}
