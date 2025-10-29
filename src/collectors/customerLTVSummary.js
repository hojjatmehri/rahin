// ============================================================
// File: src/collectors/customerLTVSummary.js
// Purpose: خلاصه‌سازی روزانه ارزش مشتریان از View v_customer_segments و ارسال به واتساپ
// Author: Hojjat Mehri (v3 – Job Style)
// ============================================================

import 'file:///E:/Projects/rahin/logger.js';
import Database from 'better-sqlite3';
import moment from 'moment-timezone';
import WhatsAppService from '../WhatsAppService.js';

const MOD = '[JobCustomerLTVSummary]';
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

    // --- مرحله ۱: اتصال به DB ---
    const db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');

    // --- مرحله ۲: خواندن داده‌ها از View ---
    const segments = db.prepare('SELECT * FROM v_customer_segments;').all();
    const totalRow = db.prepare('SELECT COUNT(*) AS c FROM customer_value;').get();
    const total = totalRow?.c || 0;
    db.close();

    if (!segments.length) {
      log('⚠️ No segment data found.');
      return;
    }

    // --- مرحله ۳: ساخت گزارش ---
    let report = `📊 گزارش ارزش مشتریان (${moment().tz(TZ).format('YYYY-MM-DD HH:mm')})\n\n`;
    for (const s of segments) {
      const rank = s.rank_label || 'نامشخص';
      const avgBuyM = s.avg_total_amount ? (s.avg_total_amount / 1_000_000).toFixed(1) + 'M' : '-';
      report += `${rank}: ${s.total_customers} نفر | امتیاز ${s.avg_value_score ?? '-'} | میانگین خرید ${avgBuyM} | پرداخت ${s.avg_payments_count ?? '-'}\n`;
    }
    report += `\nمجموع مشتریان: ${total}\nزمان: ${moment().tz(TZ).format('HH:mm')}`;

    // --- مرحله ۴: ارسال یا چاپ ---
    if (DRY_RUN) {
      log('📄 DRY_RUN فعال است. گزارش فقط چاپ می‌شود:\n' + report);
    } else {
      await WhatsAppService.sendMessage(MANAGER_MOBILE, report);
      log(`✅ WhatsApp report sent to ${MANAGER_MOBILE}`);
    }

    log('🏁 Job completed successfully.');
  } catch (e) {
    err('❌ خطا در اجرای job:', e.message);
  }
})();
