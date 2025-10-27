import '../../logger.js';
// src/message/sanitize.js
// ابزارهای پاکسازی و تقسیم متن برای واتساپ
import { openai } from '../config/Config.js';

export function looksPersian(s = '') {
  return /[\u0600-\u06FF]/.test(String(s));
}

/** اگر متن فارسی نیست، ترجمهٔ سریع به فارسی انجام می‌دهد (در صورت خطا، متن اصلی را برمی‌گرداند) */
export async function forcePersianText(text) {
  const s = String(text ?? '').trim();
  if (!s) return s;
  if (looksPersian(s)) return s;

  try {
    const resp = await openai.chat.completions.create({
      model: process.env.RAHIN_MODEL || 'gpt-4o-mini',
      temperature: 0,
      max_tokens: 800,
      messages: [
        { role: 'system', content: 'فقط ترجمهٔ دقیق و روان فارسی برگردان. هیچ توضیح اضافه ننویس.' },
        { role: 'user', content: s }
      ]
    });
    const out = resp?.choices?.[0]?.message?.content?.trim();
    return out || s;
  } catch (e) {
    console.warn('[forcePersianText] translate failed:', e?.message || e);
    return s;
  }
}


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
  
  export default { sanitizeForWhatsApp, chunkText , forcePersianText };
  
