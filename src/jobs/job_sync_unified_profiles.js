// ============================================================
// File: src/jobs/job_sync_unified_profiles.js
// Purpose: اجرای روزانه همگام‌سازی پروفایل‌ها از دیدار و تراکنش‌ها (Formafzar)
// Author: Hojjat Mehri
// ============================================================

import '../../logger.js';
import moment from 'moment-timezone';
import wa from '../WhatsAppService.js'; // ← خروجی آماده

import { syncUnifiedProfiles } from '../../../AtighgashtAI/src/collectors/personUnifiedFromDidar.js';
import { collectFormafzar } from '../../../AtighgashtAI/src/collectors/formafzarCollector.js';

const MOD = '[JobSyncUnifiedProfiles]';
const TZ = 'Asia/Tehran';
const DRY_RUN = String(process.env.DRY_RUN || '0') === '1';
const MANAGER_MOBILE = process.env.DEV_ALERT_MOBILE || '09134052885';

(async () => {
  try {
    const start = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
    console.log(`${MOD} 🚀 Sync job started at ${start}`);

    // --- مرحله ۱: دیدار ---
    await syncUnifiedProfiles();
    console.log(`${MOD} ✅ Didar contacts synced.`);

    // --- مرحله ۲: تراکنش‌ها ---
    await collectFormafzar();
    console.log(`${MOD} ✅ Transaction-based profiles synced.`);

    const done = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
    const msg = `🧩 همگام‌سازی پروفایل‌ها انجام شد.\nشروع: ${start}\nپایان: ${done}`;

    if (DRY_RUN) {
      console.log(`${MOD} [DRY_RUN] پیام واتساپ ارسال نمی‌شود.`);
      console.log(msg);
    } else {
      await wa.sendMessage(MANAGER_MOBILE, msg);
      console.log(`${MOD} ✅ گزارش برای ${MANAGER_MOBILE} ارسال شد.`);
    }

    console.log(`${MOD} 🏁 Job completed successfully.`);
  } catch (e) {
    console.error(`${MOD} ❌ Error during sync:`, e.message);
  }
})();
