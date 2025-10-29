import 'file:///E:/Projects/rahin/logger.js';
// src/utils/files.js
// ابزارهای فایل: خواندن امن خطوط و استخراج اسنیپت کد اطراف یک شماره خط

import fs from 'fs';

/**
 * خواندن امن فایل متنی و برگرداندن آرایه خطوط.
 * در صورت خطا، آرایه خالی برمی‌گرداند.
 */
export function readLinesSafe(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    return content.split(/\r?\n/);
  } catch {
    return [];
  }
}

/**
 * استخراج اسنیپت کد از فایل در اطراف شماره خط موردنظر.
 * اگر lineNo مشخص نباشد، ۸۰ خط اول را برمی‌گرداند.
 * @param {string} filePath
 * @param {number} lineNo - ۱ مبنا (شماره خط انسانی)
 * @param {number} pad - تعداد خطوط قبل/بعد
 */
export function readCodeSnippet(filePath, lineNo, pad = 12) {
  if (!filePath) return '';
  const lines = readLinesSafe(filePath);
  if (!lines.length) return '';

  if (Number.isInteger(lineNo) && lineNo > 0) {
    // ایندکس آرایه ۰ مبناست
    const startIdx = Math.max(0, lineNo - 1 - pad);
    const endIdx = Math.min(lines.length, lineNo - 1 + pad + 1);
    return lines.slice(startIdx, endIdx).join('\n');
  }

  // پیش‌فرض: ۸۰ خط اول
  return lines.slice(0, 80).join('\n');
}

