// ========================================================
// File: src/lib/whatsapp/sendWhatsapp.js
// Author: Hojjat Mehri
// Role: Wrapper for WhatsApp message sending with DEV/PROD switch
// ========================================================

import { CONFIG } from "../../config/Config.js";
import dotenv from "dotenv";
dotenv.config();

const MOD = "[sendWhatsapp]";
const log = (...a) => console.log(MOD, ...a);
const err = (...a) => console.error(MOD, ...a);

const DEV_MOBILE = process.env.DEV_ALERT_MOBILE || "";
const SEND_REAL = String(process.env.SEND_REAL_ALERTS || "0") === "1";

/**
 * ارسال پیام واتساپ با حالت تست و لاگ فارسی
 * @param {string} to شماره مقصد (می‌تونه واقعی باشه)
 * @param {string} body متن پیام
 * @returns {Promise<void>}
 */
export async function sendWhatsapp(to, body) {
  try {
    const normalizedTo = SEND_REAL ? to : DEV_MOBILE;
    if (!normalizedTo) {
      log("⚠️ DEV_ALERT_MOBILE تنظیم نشده. پیام ارسال نشد.");
      return;
    }

    log(`📤 در حال ارسال پیام به ${SEND_REAL ? "مخاطب واقعی" : "شماره تستی"}: ${normalizedTo}`);
    log("───── پیام:");
    log(body);
    log("─────");

    const res = await CONFIG.waService.sendMessage(normalizedTo, body);

    if (res?.sent || res?.id) {
      log(`✅ پیام با موفقیت ارسال شد. message_id=${res?.id || "?"}`);
    } else {
      log(`⚠️ پاسخ UltraMsg غیرمنتظره بود:`, res);
    }
  } catch (e) {
    err(`❌ خطا در sendWhatsapp: ${e.message}`);
  }
}
