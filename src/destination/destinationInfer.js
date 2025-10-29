import 'file:///E:/Projects/rahin/logger.js';
// src/destination/destinationInfer.js
// تخمین مقصد بر اساس عنوان سرویس یا توضیح

// نمونه دیکشنری مقاصد (قابل گسترش)
const DESTINATION_DICT = [
    { code: "ISTANBUL", keywords: ["استانبول", "istanbul"] },
    { code: "ANTALYA", keywords: ["آنتالیا", "antalya"] },
    { code: "DUBAI", keywords: ["دبی", "dubai"] },
    { code: "THAILAND", keywords: ["تایلند", "پوکت", "پاتایا", "thailand", "phuket", "pattaya"] },
    { code: "ARMENIA", keywords: ["ایروان", "ارمنستان", "yerevan", "armenia"] },
  ];
  
  /**
   * ساده‌ترین نرمالایزر متن (حذف فاصله و کوچک‌سازی)
   */
  function norm(s) {
    return String(s || "")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
  }
  
  /**
   * بر اساس عنوان سرویس مقصد رو حدس می‌زنه
   * @param {string} title - عنوان سرویس (مثلاً پرواز یا تور)
   * @returns {object} { destination_code, confidence, source }
   */
  export function inferDestination(title = "") {
    const t = norm(title);
  
    for (const dest of DESTINATION_DICT) {
      for (const kw of dest.keywords) {
        if (t.includes(norm(kw))) {
          return {
            destination_code: dest.code,
            confidence: 90,
            source: "keyword-match",
          };
        }
      }
    }
  
    // اگر چیزی پیدا نشد
    return {
      destination_code: null,
      confidence: 0,
      source: "none",
    };
  }
  
  export default { inferDestination };
  
