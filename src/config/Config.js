// src/config/Config.js
// پیکربندی مرکزی پروژه: env + کلاینت‌ها + db

import env from './env.js';
import OpenAI from 'openai';
import WhatsAppService from '../WhatsAppService.js';
import { db } from '../db/db.js';

// ---------- کلاینت‌ها ----------
export const openai = new OpenAI({ apiKey: env.OPENAI_API_KEY });

export const waService = new WhatsAppService(
  env.ULTRAMSG_INSTANCE_ID,
  env.ULTRAMSG_TOKEN,
  env.ULTRAMSG_BASE_URL || 'https://api.ultramsg.com'
);

// ---------- re-export متغیرهای env به صورت نام‌دار ----------
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
  WHATSAPP_DEST_MOBILE,   // اگر در .env گذاشتی
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

// ---------- آبجکت مرکزی ----------
export const CONFIG = {
  ...env,        // همه‌ی کلیدهای env
  db,            // اتصال دیتابیس
  openai,        // کلاینت OpenAI
  waService,     // سرویس واتساپ
};

export default CONFIG;
