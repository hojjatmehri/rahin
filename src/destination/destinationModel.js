// src/destination/destinationModel.js
// بازسازی و نگه‌داری دیتای مقصدها بر اساس تراکنش‌ها + کش فایل

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { all as dbAll } from '../db/db.js';

// مسیر کش
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.resolve(process.cwd(), 'data');
const CACHE_FILE = path.join(DATA_DIR, 'destination_dictionary.json');

// لیست بذر (seed) مقاصد رایج — قابل گسترش
// ساختار: { code, keywords: [ ... ] }
const SEED_DESTINATIONS = [
  { code: 'ISTANBUL', keywords: ['استانبول', 'istanbul'] },
  { code: 'ANTALYA',  keywords: ['آنتالیا', 'antalya'] },
  { code: 'DUBAI',     keywords: ['دبی', 'dubai'] },
  { code: 'THAILAND',  keywords: ['تایلند', 'thailand', 'پوکت', 'phuket', 'پاتایا', 'pattaya'] },
  { code: 'ARMENIA',   keywords: ['ایروان', 'armenia', 'yerevan', 'ارمنستان'] },
];

// دیکشنری داخل حافظه
let MEM_DICT = null;

/* -----------------------------------
   ابزارهای متنی ساده
----------------------------------- */
function normLower(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

// برش کلمات فارسی/انگلیسی و عدد؛ حذف علائم
function tokenize(title) {
  const t = String(title || '').toLowerCase();
  // کلمات فارسی/لاتین/اعداد
  const tokens = t.match(/[\p{L}\p{N}]+/gu) || [];
  // حذف توکن‌های خیلی کوتاه (۱-۲ کاراکتر)
  return tokens.filter(w => w.length >= 3);
}

// آیا عنوان شامل یکی از کلیدواژه‌های مقصد است؟
function titleHasAny(title, keywords = []) {
  const t = normLower(title);
  return keywords.some(kw => t.includes(normLower(kw)));
}

/* -----------------------------------
   I/O کش فایل
----------------------------------- */
function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function saveCache(dict) {
  try {
    ensureDataDir();
    fs.writeFileSync(CACHE_FILE, JSON.stringify(dict, null, 2), 'utf8');
  } catch (e) {
    // فقط لاگ کن؛ عدم توانایی در ذخیره کش نباید اجرای برنامه را متوقف کند
    console.warn('[destinationModel] warn: saveCache failed:', e.message);
  }
}

function loadCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const raw = fs.readFileSync(CACHE_FILE, 'utf8');
      return JSON.parse(raw);
    }
  } catch (e) {
    console.warn('[destinationModel] warn: loadCache failed:', e.message);
  }
  return null;
}

/* -----------------------------------
   ساخت دیکشنری از تراکنش‌ها
   خروجی نمونه:
   {
     ISTANBUL: { code: 'ISTANBUL', keywords: ['استانبول','istanbul','تور','لحظه‌ای'], support: 123 },
     DUBAI:    { code: 'DUBAI',    keywords: [...], support: 98 },
     ...
   }
----------------------------------- */
function buildDictFromRows(rows, seed = SEED_DESTINATIONS, { minKeywordFreq = 3, maxKeywords = 20 } = {}) {
  // 1) با seed شروع می‌کنیم
  const dict = new Map(seed.map(d => [d.code, { code: d.code, keywords: new Set(d.keywords), support: 0 }]));

  // 2) ردیف‌ها را برچسب می‌زنیم: هر سطر ممکن است به یک مقصد مَچ شود
  const tokenFreqByCode = new Map(); // code -> Map(token -> cnt)

  for (const r of rows || []) {
    const title = r.serviceTitle || '';
    for (const dest of seed) {
      if (titleHasAny(title, dest.keywords)) {
        const rec = dict.get(dest.code);
        rec.support++;
        // توکن‌های عنوان را بشمار
        const toks = tokenize(title);
        if (!tokenFreqByCode.has(dest.code)) tokenFreqByCode.set(dest.code, new Map());
        const freqMap = tokenFreqByCode.get(dest.code);
        for (const tk of toks) {
          // توکن‌های خیلی عمومی را فیلتر کن
          if (['تور', 'بلیط', 'هتل', 'پرواز', 'رفت', 'برگشت', 'لحظه‌ای', 'off', 'offer'].includes(tk)) continue;
          freqMap.set(tk, (freqMap.get(tk) || 0) + 1);
        }
        break; // به‌محض اولین مچ مقصد، از حلقه خارج شو
      }
    }
  }

  // 3) برای هر مقصد، توکن‌های پرتکرار را به‌عنوان کلیدواژه جدید اضافه کن
  for (const [code, freqMap] of tokenFreqByCode.entries()) {
    // مرتب‌سازی نزولی بر اساس فراوانی
    const topTokens = [...freqMap.entries()]
      .filter(([, c]) => c >= minKeywordFreq)
      .sort((a, b) => b[1] - a[1])
      .slice(0, maxKeywords)
      .map(([tk]) => tk);

    const rec = dict.get(code);
    topTokens.forEach(tk => rec.keywords.add(tk));
  }

  // 4) تبدیل Set به Array و خروجی شیء plain
  const out = {};
  for (const [code, rec] of dict.entries()) {
    out[code] = {
      code,
      keywords: Array.from(rec.keywords),
      support: rec.support,
    };
  }
  return out;
}

/* -----------------------------------
   API عمومی
----------------------------------- */

/**
 * بازسازی دیکشنری مقصدها از تراکنش‌ها
 * @param {{days?: number, minKeywordFreq?: number, maxKeywords?: number}} options
 * @returns {Promise<object>} دیکشنری مقصدها
 */
export async function rebuildDestinationDictionary(options = {}) {
  const {
    days = 90,
    minKeywordFreq = 3,
    maxKeywords = 20,
  } = options;

  // تراکنش‌های بازه زمانی
  let rows = [];
  try {
    rows = await dbAll(
      `
        SELECT serviceTitle
        FROM transactions
        WHERE datetime(regDate) >= datetime('now', ? , 'localtime')
          AND serviceTitle IS NOT NULL AND TRIM(serviceTitle) <> ''
      `,
      [`-${days} days`]
    );
  } catch (e) {
    console.warn('[destinationModel] warn: cannot read transactions:', e.message);
    // در صورت خطا، فقط از seed استفاده می‌کنیم
  }

  const dict = buildDictFromRows(rows, SEED_DESTINATIONS, { minKeywordFreq, maxKeywords });

  // کش در حافظه و فایل
  MEM_DICT = dict;
  saveCache(dict);

  return dict;
}

/**
 * دریافت دیکشنری در حافظه (اگر نبود از کش فایل یا seed)
 */
export function getDestinationDictionary() {
  if (MEM_DICT) return MEM_DICT;
  const cached = loadCache();
  if (cached) {
    MEM_DICT = cached;
    return MEM_DICT;
  }
  // fallback: فقط seed
  MEM_DICT = Object.fromEntries(
    SEED_DESTINATIONS.map(d => [d.code, { code: d.code, keywords: d.keywords.slice(), support: 0 }])
  );
  return MEM_DICT;
}

/**
 * افزودن/بروزرسانی دستی مقصد سفارشی در دیکشنری
 * (و ذخیره در کش)
 */
export function upsertDestination(code, keywords = [], support = 0) {
  const dict = getDestinationDictionary();
  const rec = dict[code] || { code, keywords: [], support: 0 };
  const kwSet = new Set([...(rec.keywords || []), ...(keywords || [])]);
  dict[code] = { code, keywords: Array.from(kwSet), support: Math.max(rec.support || 0, Number(support) || 0) };
  MEM_DICT = dict;
  saveCache(dict);
  return dict[code];
}
