// ============================================================
// File: lib/db/jobLock.js
// Author: Hojjat Mehri
// Purpose: کنترل قفل سراسری بین تمام Jobها برای جلوگیری از Database Lock
// Version: Stable v3 – Non-Blocking Safe
// ============================================================

import fs from "fs";

const GLOBAL_LOCK = "E:/Projects/rahin/tmp/global_db.lock";

// اطمینان از وجود مسیر tmp
fs.mkdirSync("E:/Projects/rahin/tmp", { recursive: true });

/**
 * گرفتن قفل سراسری
 * در صورت انتظار بیش از زمان مجاز، job فقط skip می‌شود و خطا تولید نمی‌کند
 */
export function acquireGlobalLock(mod = "UnknownJob", maxWaitMs = 60000) {
  const start = Date.now();

  while (fs.existsSync(GLOBAL_LOCK)) {
    const elapsed = Date.now() - start;
    if (elapsed > maxWaitMs) {
      console.warn(`[${mod}] ⚠️ Skipped — global lock still active after ${Math.round(elapsed / 1000)}s.`);
      return false; // فقط skip، نه خطا
    }

    // انتظار کوتاه برای جلوگیری از busy loop
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 250);
  }

  try {
    fs.writeFileSync(GLOBAL_LOCK, String(Date.now()));
    console.log(`[${mod}] 🔏 Global DB lock acquired.`);
    return true;
  } catch (e) {
    console.error(`[${mod}] ❌ Error acquiring lock: ${e.message}`);
    return false;
  }
}

/**
 * آزادسازی قفل سراسری (همیشه در finally صدا زده شود)
 */
export function releaseGlobalLock(mod = "UnknownJob") {
  try {
    if (fs.existsSync(GLOBAL_LOCK)) {
      fs.unlinkSync(GLOBAL_LOCK);
      console.log(`[${mod}] 🔓 Global DB lock released.`);
    }
  } catch (e) {
    console.error(`[${mod}] ⚠️ Error releasing global lock: ${e.message}`);
  }
}
