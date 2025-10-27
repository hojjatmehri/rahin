import '../../logger.js';
// src/config/env.js
// بارگذاری و اعتبارسنجی متغیرهای محیطی بر اساس نیاز Rahin Ops + سرویس‌ها

import dotenv from 'dotenv';
dotenv.config({ path: process.env.ENV_FILE || '.env' });

/* --------------------- Helpers --------------------- */
function asString(val, { name, required = false, def = undefined } = {}) {
  const v = (val ?? def);
  if (required && (v === undefined || v === null || v === '')) {
    throw new Error(`ENV ${name} الزامی است و مقدار ندارد.`);
  }
  return v === undefined ? undefined : String(v);
}

function asInt(val, { name, def = undefined, min, max } = {}) {
  const raw = (val ?? def);
  if (raw === undefined || raw === null || raw === '') return undefined;
  const n = Number.parseInt(String(raw), 10);
  if (Number.isNaN(n)) throw new Error(`ENV ${name} باید عدد صحیح باشد. مقدار فعلی: ${raw}`);
  if (min !== undefined && n < min) throw new Error(`ENV ${name} باید ≥ ${min} باشد. مقدار فعلی: ${n}`);
  if (max !== undefined && n > max) throw new Error(`ENV ${name} باید ≤ ${max} باشد. مقدار فعلی: ${n}`);
  return n;
}

function asBool(val, { name, def = undefined } = {}) {
  const raw = (val ?? def);
  if (raw === undefined || raw === null || raw === '') return undefined;
  const s = String(raw).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(s)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(s)) return false;
  throw new Error(`ENV ${name} باید boolean باشد (true/false). مقدار فعلی: ${raw}`);
}

// برای سازگاری (اگر جایی شماره فردی باشد؛ در داده فعلی شما Group ID واتساپ است)
function normalizeIranMobile(m) {
  if (!m) return undefined;
  const digits = String(m).replace(/[^\d]/g, '');
  if (!digits) return undefined;
  if (digits.startsWith('98')) return digits;
  if (digits.startsWith('0')) return '98' + digits.slice(1);
  if (digits.startsWith('9')) return '98' + digits;
  return digits;
}

// بازسازی کلید خصوصی Google (اگر در .env با \n ذخیره شده باشد)
function normalizeGServiceKey(key) {
  if (!key) return undefined;
  const k = String(key);
  if (k.includes('-----BEGIN') && k.includes('\\n')) {
    return k.replace(/\\n/g, '\n');
  }
  return k;
}

/* --------------------- Load & Validate --------------------- */
let errors = [];
function safe(get) { try { return get(); } catch (e) { errors.push(e.message); return undefined; } }

const env = {
  // محیط
  NODE_ENV: safe(() => asString(process.env.NODE_ENV, { name: 'NODE_ENV', def: 'production' })),
  PORT: safe(() => asInt(process.env.PORT, { name: 'PORT', def: 3000, min: 1 })),

  // OpenAI
  OPENAI_API_KEY: safe(() => asString(process.env.OPENAI_API_KEY, { name: 'OPENAI_API_KEY', required: true })),
  RAHIN_MODEL: safe(() => asString(process.env.RAHIN_MODEL, { name: 'RAHIN_MODEL', def: 'gpt-4o' })),

  // زمان‌بندی و لاگ راهنگار
  RAHIN_INTERVAL_MIN: safe(() => asInt(process.env.RAHIN_INTERVAL_MIN, { name: 'RAHIN_INTERVAL_MIN', def: 60, min: 5 })),
  RAHIN_LOG_FILE: safe(() => asString(process.env.RAHIN_LOG_FILE, { name: 'RAHIN_LOG_FILE', def: './rahin_ops.log' })),
  RAHIN_LOGS_DIR: safe(() => asString(process.env.RAHIN_LOGS_DIR, { name: 'RAHIN_LOGS_DIR', def: './logs' })),
  DEBUG_PROCESS_ERRORS: safe(() => asBool(process.env.DEBUG_PROCESS_ERRORS, { name: 'DEBUG_PROCESS_ERRORS', def: false })),

  // پایگاه داده
  SQLITE_DB_PATH: safe(() => asString(process.env.SQLITE_DB_PATH, { name: 'SQLITE_DB_PATH', def: './db_atigh.sqlite' })),
  TABLE_NAME: safe(() => asString(process.env.TABLE_NAME, { name: 'TABLE_NAME', def: 'atigh_instagram_dev' })),

  // کلیدها و سرویس‌ها (مطابق داده‌های شما)
  NOVINHUB_API_KEY: safe(() => asString(process.env.NOVINHUB_API_KEY, { name: 'NOVINHUB_API_KEY' })),
  DIDAR_API_KEY: safe(() => asString(process.env.DIDAR_API_KEY, { name: 'gwjwiso8f78l0ohwqx4hm65ft2f9j4zv' })),
  KAVENEGAR_API_KEY: safe(() => asString(process.env.KAVENEGAR_API_KEY, { name: 'KAVENEGAR_API_KEY' })),
  ULTRAMSG_INSTANCE_ID: safe(() => asString(process.env.ULTRAMSG_INSTANCE_ID, { name: 'ULTRAMSG_INSTANCE_ID', required: true })),
  ULTRAMSG_TOKEN: safe(() => asString(process.env.ULTRAMSG_TOKEN, { name: 'ULTRAMSG_TOKEN', required: true })),

  // URL های پایه (پیش‌فرض طبق داده‌های شما)
  NOVINHUB_BASE_URL: safe(() => asString(process.env.NOVINHUB_BASE_URL, { name: 'NOVINHUB_BASE_URL', def: 'https://api.novinhub.com/token/v2' })),
  DIDAR_BASE_URL: safe(() => asString(process.env.DIDAR_BASE_URL, { name: 'DIDAR_BASE_URL', def: 'https://app.didar.me/api' })),
  KAVENEGAR_BASE_URL: safe(() => asString(process.env.KAVENEGAR_BASE_URL, { name: 'KAVENEGAR_BASE_URL', def: 'https://api.kavenegar.com/v1' })),
  ULTRAMSG_BASE_URL: safe(() => asString(process.env.ULTRAMSG_BASE_URL, { name: 'ULTRAMSG_BASE_URL', def: 'https://api.ultramsg.com' })),

  // وبهوک و واتساپ
  WEBHOOK_SECRET: safe(() => asString(process.env.WEBHOOK_SECRET, { name: 'WEBHOOK_SECRET', required: true })),
  WHATSAPP_GROUP_ID: safe(() => asString(process.env.WHATSAPP_GROUP_ID, { name: 'WHATSAPP_GROUP_ID', required: true })),
  // اگر مقصد پیام به‌صورت موبایل بود (نه گروه)، از این استفاده کنید:
  WHATSAPP_DEST_MOBILE: safe(() => {
    const raw = asString(process.env.WHATSAPP_DEST_MOBILE, { name: 'WHATSAPP_DEST_MOBILE', def: '' });
    return raw ? normalizeIranMobile(raw) : '';
  }),

  // Google Sheets
  // Google Sheets (اختیاری - فقط اگر REQUIRE_GOOGLE_SHEET=1)
  GOOGLE_SHEET_ID: safe(() => {
    const required = process.env.REQUIRE_GOOGLE_SHEET === '1';
    return asString(process.env.GOOGLE_SHEET_ID, {
      name: 'GOOGLE_SHEET_ID',
      required,
    });
  }),
  GOOGLE_SHEET_AUTH_EMAIL: safe(() => {
    const required = process.env.REQUIRE_GOOGLE_SHEET === '1';
    return asString(process.env.GOOGLE_SHEET_AUTH_EMAIL, {
      name: 'GOOGLE_SHEET_AUTH_EMAIL',
      required,
    });
  }),
  GOOGLE_SHEET_AUTH_KEY: safe(() => {
    const required = process.env.REQUIRE_GOOGLE_SHEET === '1';
    const k = asString(process.env.GOOGLE_SHEET_AUTH_KEY, {
      name: 'GOOGLE_SHEET_AUTH_KEY',
      required,
    });
    return normalizeGServiceKey(k);
  }),

};

if (errors.length) {
  const msg = `خطای پیکربندی ENV:\n- ${errors.join('\n- ')}\n` +
    `لطفاً فایل .env را کامل کنید.`;
  throw new Error(msg);
}

export default env;

// اکسپورت‌های نام‌دار در صورت نیاز
export const {
  NODE_ENV,
  PORT,
  OPENAI_API_KEY,
  RAHIN_MODEL,
  RAHIN_INTERVAL_MIN,
  RAHIN_LOG_FILE,
  RAHIN_LOGS_DIR,
  DEBUG_PROCESS_ERRORS,

  SQLITE_DB_PATH,
  TABLE_NAME,

  NOVINHUB_API_KEY,
  DIDAR_API_KEY,
  KAVENEGAR_API_KEY,
  ULTRAMSG_INSTANCE_ID,
  ULTRAMSG_TOKEN,

  NOVINHUB_BASE_URL,
  DIDAR_BASE_URL,
  KAVENEGAR_BASE_URL,
  ULTRAMSG_BASE_URL,

  WEBHOOK_SECRET,
  WHATSAPP_GROUP_ID,
  WHATSAPP_DEST_MOBILE,

  GOOGLE_SHEET_ID,
  GOOGLE_SHEET_AUTH_EMAIL,
  GOOGLE_SHEET_AUTH_KEY,
} = env;

