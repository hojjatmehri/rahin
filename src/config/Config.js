// ============================================================
// File: src/config/Config.js
// Purpose: پیکربندی مرکزی پروژه Rahin (env + clients + db)
// Author: Hojjat Mehri
// ============================================================

import '../../logger.js';
import env from './env.js';
import OpenAI from 'openai';
import WhatsAppService from '../WhatsAppService.js'; // ← خروجی instance آماده است
import { db } from '../db/db.js';

const MOD = '[Config]';
const log = (...a) => console.log(MOD, ...a);
const err = (...a) => console.error(MOD, ...a);

// ============================================================
// OpenAI Client
// ============================================================
export const openai = new OpenAI({ apiKey: env.OPENAI_API_KEY });

// ============================================================
// WhatsApp Client (Singleton)
// ============================================================
// چون WhatsAppService در نسخه جدید خودش instance است، نیازی به new نیست
export const waService = WhatsAppService;

// ============================================================
// Named re-exports (برای سازگاری با فایل‌های قدیمی)
// ============================================================
export const {
  OPENAI_API_KEY,
  NOVINHUB_API_KEY,
  DIDAR_API_KEY,
  DIDAR_BASE_URL,
  KAVENEGAR_API_KEY,
  ULTRAMSG_INSTANCE_ID,
  ULTRAMSG_TOKEN,
  NOVINHUB_BASE_URL,
  KAVENEGAR_BASE_URL,
  ULTRAMSG_BASE_URL,
  WEBHOOK_SECRET,
  WHATSAPP_GROUP_ID,
  WHATSAPP_DEST_MOBILE,
  TABLE_NAME,
  SQLITE_DB_PATH,
  GOOGLE_SHEET_ID,
  GOOGLE_SHEET_AUTH_EMAIL,
  GOOGLE_SHEET_AUTH_KEY,
  RAHIN_INTERVAL_MIN,
  RAHIN_MODEL,
  RAHIN_LOG_FILE,
  RAHIN_LOGS_DIR,
} = env;

// ============================================================
// آبجکت مرکزی پیکربندی
// ============================================================
export const CONFIG = {
  ...env,        // تمام متغیرهای محیطی
  db,            // اتصال دیتابیس مرکزی
  openai,        // کلاینت OpenAI
  waService,     // سرویس WhatsApp (UltraMsg)
};

// ============================================================
// Health log
// ============================================================
log(`✅ CONFIG loaded (env=${env.NODE_ENV || 'unknown'})`);
log(`🔌 WhatsAppService instance ready: ${env.ULTRAMSG_INSTANCE_ID || '(unset)'}`);

export default CONFIG;
