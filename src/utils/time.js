import 'file:///E:/Projects/rahin/logger.js';
// src/utils/time.js
// همهٔ زمان‌ها به وقت تهران

export const APP_TZ = process.env.APP_TZ || 'Asia/Tehran';

// ساخت یک formatter پایدار برای تهران
function parts(d = new Date(), tz = APP_TZ) {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, hour12: false,
    year:'numeric', month:'2-digit', day:'2-digit',
    hour:'2-digit', minute:'2-digit', second:'2-digit',
  });
  const map = {};
  for (const p of fmt.formatToParts(d)) map[p.type] = p.value;
  return map; // {year,month,day,hour,minute,second}
}

// YYYY-MM-DD
export function todayYMD(tz = APP_TZ) {
  const p = parts(new Date(), tz);
  return `${p.year}-${p.month}-${p.day}`;
}

// HH:mm:ss
export function timeHMS(tz = APP_TZ) {
  const p = parts(new Date(), tz);
  return `${p.hour}:${p.minute}:${p.second}`;
}

// YYYY-MM-DD HH:mm:ss  (برای لاگ)
export function nowStamp(tz = APP_TZ) {
  const p = parts(new Date(), tz);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

// ISO-مانند بدون Z (لوکال تهران): YYYY-MM-DDTHH:mm:ss
export function nowStampLocal(tz = APP_TZ) {
  const p = parts(new Date(), tz);
  return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}`;
}

// برای نام‌گذاری فایل‌های لاگ روزانه
export function ymdOf(date = new Date(), tz = APP_TZ) {
  const p = parts(date, tz);
  return `${p.year}-${p.month}-${p.day}`;
}

// ابزار زمان‌سنجی ساده
export function t0(){ return Date.now(); }
export function took(ms){ return `${ms}ms`; }

// بازه‌ی امروز تهران برای کوئری‌های SQL
// خروجی: { start: 'YYYY-MM-DD 00:00:00', end: 'YYYY-MM-DD 00:00:00 (+1 day)' }
export function sqlTodayRange(tz = APP_TZ) {
  const p = parts(new Date(), tz);
  const start = `${p.year}-${p.month}-${p.day} 00:00:00`;
  // فردای تهران:
  const d = new Date();
  // اضافه کردن یک روز منطقی: 24h کفایت می‌کند چون فقط برای مرزبندی روزی است
  const d2 = new Date(d.getTime() + 24*3600*1000);
  const p2 = parts(d2, tz);
  const end = `${p2.year}-${p2.month}-${p2.day} 00:00:00`;
  return { start, end };
}

// ۷ روز قبل از «الان تهران» (برای بازه‌های ۷ روزه)
export function sqlSince7d(tz = APP_TZ) {
  const now = new Date();
  const since = new Date(now.getTime() - 7*24*3600*1000);
  const p = parts(since, tz);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

