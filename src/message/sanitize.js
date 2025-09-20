// src/message/sanitize.js
// ابزارهای پاکسازی و تقسیم متن برای واتساپ

/**
 * پاکسازی متن برای ارسال در واتساپ
 * - حذف بلاک‌های کد ```...```
 * - حذف خطوطی که شبیه SQL/JS/CLI هستند
 * - تمیزکاری فاصله‌های اضافه
 */
export function sanitizeForWhatsApp(msg) {
    if (!msg) return "";
  
    let m = String(msg);
  
    // حذف بلاک‌های کد ```...```
    m = m.replace(/```[\s\S]*?```/g, "");
  
    // حذف خطوطی که بوی کد می‌دهند
    const codey =
      /^(?=.*(;|\{|\}|\(|\)|=))(?:.*\b(SELECT|INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|JOIN|INDEX|FROM|WHERE|axios|fetch|curl|await|function|const|let|var|class|npm|yarn|pnpm|node|sql)\b).*$/i;
  
    m = m
      .split("\n")
      .filter((line) => !codey.test(line))
      .join("\n");
  
    // حذف تیترهای خالی مربوط به کد
    m = m.replace(/^—\s*کد پیشنهادی:\s*$/gm, "");
  
    // تمیزکاری فاصله‌های خالی مضاعف
    m = m.replace(/\n{3,}/g, "\n\n").trim();
  
    return m;
  }
  
  /**
   * تقسیم متن به بخش‌های کوچک
   * @param {string} txt متن ورودی
   * @param {number} maxLen طول حداکثر هر بخش
   * @returns {string[]} بخش‌ها
   */
  export function chunkText(txt, maxLen = 1200) {
    if (!txt) return [];
    const chunks = [];
    let i = 0;
    while (i < txt.length) {
      chunks.push(txt.slice(i, i + maxLen));
      i += maxLen;
    }
    return chunks;
  }
  
  export default { sanitizeForWhatsApp, chunkText };
  