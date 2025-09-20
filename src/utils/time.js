// src/utils/time.js
// ابزارهای زمانی ساده

/**
 * تاریخ/زمان فعلی به ISO (UTC)
 */
export function nowIso() {
    return new Date().toISOString();
  }
  
  /**
   * تاریخ امروز به فرمت YYYY-MM-DD (UTC)
   * اگر به زمان محلی نیاز داری، با new Date() و offset محلی مدیریت کن.
   */
  export function todayYMD() {
    return new Date().toISOString().slice(0, 10);
  }
  
  /**
   * شروع تایمر ساده
   */
  export function t0() {
    return Date.now();
  }
  
  /**
   * نمایش مدت زمان برحسب ms با پسوند
   */
  export function took(msOrStart) {
    if (typeof msOrStart === 'number' && msOrStart > 1e12) {
      // اگر ورودی timestamp شروع باشد
      return `${Date.now() - msOrStart}ms`;
    }
    return `${msOrStart}ms`;
  }
  