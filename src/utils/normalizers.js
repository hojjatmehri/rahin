import '../../logger.js';
// src/utils/normalizers.js
// نرمال‌سازی متن/داده و پس‌پردازش خروجی‌های فنی AI

import crypto from 'crypto';

/**
 * نرمال‌سازی رشته فارسی/لاتین:
 * - حروف کوچک
 * - ادغام فاصله و نیم‌فاصله
 * - حذف علائم (فقط حروف/اعداد/فاصله باقی می‌ماند)
 */
export function norm(s = '') {
  return String(s)
    .toLowerCase()
    .replace(/[\u200c\s]+/g, ' ')
    .replace(/[^\p{L}\p{N}\s]/gu, '')
    .trim();
}

/**
 * تولید کلید شبه‌منحصر‌به‌فرد از آیتم‌های «title/problem»
 * برای جلوگیری از تکرار در آرایه‌ها
 */
export function keyFromItem(it = {}) {
  const t = norm(it.title || '');
  const p = norm(it.problem || '');
  return `${t}|${p}`;
}

/**
 * نرمال‌سازی شماره موبایل ایران به فرمت 98xxxxxxxxxx
 */
export function normalizeMobile(m) {
  const digits = String(m || '').replace(/[^\d]/g, '');
  if (!digits) return null;
  if (digits.startsWith('98')) return digits;
  if (digits.startsWith('0')) return '98' + digits.slice(1);
  if (digits.startsWith('9')) return '98' + digits;
  return digits;
}

/**
 * فیلتر کیفیت برای آیتم‌های فنی که کد/منطق معنادار ندارند.
 * (به‌ویژه برای خروجی‌های AI)
 */
export function filterLowValueItems(arr = []) {
  const CODE_RE = /(insert|update|select|alter|create|axios|fetch|queue|cron|setInterval|rate[_-]?limit|join|index)/i;
  return arr.filter(it => {
    const title = (it?.title || '').trim();
    const prob = (it?.problem || '').trim();
    if (!title || !prob) return false;

    const code = (it?.code_snippet || it?.pseudo_code || '').trim();
    if (code && !CODE_RE.test(code)) {
      return false;
    }
    return true;
  });
}

/**
 * اعمال فیلتر کیفیت روی بخش‌های مختلف خروجی فنی
 */
export function applyQualityFilters(tech = {}) {
  const cleaned = { ...tech };
  cleaned.connectors_improvements = filterLowValueItems(cleaned.connectors_improvements || []);
  cleaned.data_model_changes     = filterLowValueItems(cleaned.data_model_changes || []);
  cleaned.automation_rules       = filterLowValueItems(cleaned.automation_rules || []);
  cleaned.code_tasks             = filterLowValueItems(cleaned.code_tasks || []);
  return cleaned;
}

/**
 * حذف تکرار درون‌بخشی و بین‌بخشی آیتم‌های فنی.
 * اگر یک ایده هم در automation_rules و هم در code_tasks بود،
 * نسخه code_tasks مرجع است و در rule فقط ارجاع می‌دهیم.
 */
export function dedupeTechAcrossSections(tech = {}) {
  const cleaned = { ...tech };
  const sections = [
    'connectors_improvements',
    'data_model_changes',
    'automation_rules',
    'code_tasks',
  ];

  // تضمین آرایه بودن و حذف تکرار درون هر بخش
  for (const s of sections) {
    const arr = Array.isArray(cleaned[s]) ? cleaned[s] : [];
    const seen = new Set();
    cleaned[s] = arr.filter(it => {
      if (!it) return false;
      const title = it.title || String(it).trim?.() || '';
      if (!title) return false;
      const k = keyFromItem(it);
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }

  // نقشه کد-تسک‌ها
  const taskMap = new Map(cleaned.code_tasks.map(t => [keyFromItem(t), t]));

  // ارجاع‌دهی در قوانین اتوماسیون اگر مشابه code_tasks باشد
  cleaned.automation_rules = cleaned.automation_rules.map(rule => {
    const k = keyFromItem(rule);
    if (taskMap.has(k)) {
      const t = taskMap.get(k);
      return {
        title: rule.title || t.title,
        problem: rule.problem || t.problem,
        reasons: rule.reasons || t.reasons,
        solution: rule.solution || t.solution,
        reference_task: t.title,
      };
    }
    return rule;
  });

  return cleaned;
}

/**
 * تزریق قانون اجباری اتوماسیون (در صورت نبود)
 */
export function injectMandatoryAutomationRule(tech = {}) {
  const title = 'ایجاد قوانین خودکار برای ارسال پیام‌های تبلیغاتی در شبکه‌های اجتماعی';
  const exists = Array.isArray(tech.automation_rules)
    ? tech.automation_rules.some(it =>
        (typeof it === 'string' && it.includes(title)) ||
        (typeof it === 'object' && (it.title?.includes(title) || String(it).includes(title)))
      )
    : false;

  if (!exists) {
    const item = {
      title,
      problem: 'عدم وجود فرآیند خودکار برای فعال‌سازی کمپین‌های مناسبتی/پرتکرار در واتساپ و اینستاگرام.',
      reasons: 'وابستگی به اقدام دستی باعث تأخیر و ناهماهنگی پیام می‌شود.',
      solution: 'تعریف قوانین زمان‌مند/رویدادمحور با صف‌ ارسال و محدودسازی نرخ.',
      code_snippet:
`// نمونه اسکچ:
enqueueCampaign({
  channel: 'whatsapp',
  template: 'promo_x',
  audience: 'recent_inquirers',
  send_at: '2025-09-20 10:00'
});`
    };
    if (!Array.isArray(tech.automation_rules)) tech.automation_rules = [];
    tech.automation_rules.unshift(item);
  }
  return tech;
}

/**
 * هش کمکی (برای ضدتکرار لاگ‌ها و خطاها)
 */
export function hashLine(s) {
  return crypto.createHash('sha256').update(String(s)).digest('hex');
}

