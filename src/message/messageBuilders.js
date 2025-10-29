import 'file:///E:/Projects/rahin/logger.js';
// src/message/messageBuilders.js
// ساخت پیام‌های کوتاه و خوانا برای ارسال در واتساپ (فارسی)

function cap(s, n = 4) {
    // کمک: اولین n خط غیرخالی از متن
    if (!s) return '';
    return String(s).trim().split('\n').filter(Boolean).slice(0, n).join('\n');
  }
  
  import { forcePersianText } from "./sanitize.js";

  /**
   * پیام مدیریتی (خلاصهٔ تحلیل + ۳ ایده)
   * @param {string} runKey
   * @param {string} date - YYYY-MM-DD
   * @param {object} management - { analysis, ideas: [{title,why,how,...}], ... }
   * @returns {string}
   */
  export function buildManagementMessage(runKey, date, management = {}) {
    const l = [];
    l.push(`گزارش مدیریتی راهنگار – ${date}`);
  
    if (management?.analysis) {
      l.push('');
      l.push('تحلیل کوتاه:');
      l.push(cap(management.analysis, 4));
    }
  
    if (Array.isArray(management?.ideas) && management.ideas.length) {
      l.push('');
      l.push('۳ ایده اولویت‌دار:');
      management.ideas.slice(0, 3).forEach((it, i) => {
        const idx = i + 1;
        l.push(`${idx}) ${it.title || '-'}`);
        if (it.why) l.push(`- چرا: ${it.why}`);
        if (it.how) l.push(`- چگونه: ${it.how}`);
      });
    }
  
    l.push('');
    l.push(`run_key: ${runKey}`);
    return l.join('\n');
  }
  
  /**
   * پیام خلاصهٔ فنی (نما + ۳ مورد کلیدی از بخش‌های مختلف)
   * @param {string} runKey
   * @param {string} date - YYYY-MM-DD
   * @param {object} tech
   * @returns {string}
   */
  export function buildTechSummaryMessage(runKey, date, tech = {}) {
    const l = [];
    l.push(`گزارش فنی راهنگار – ${date}`);
  
    if (tech?.overview) {
      l.push('');
      l.push('نما:');
      l.push(cap(tech.overview, 4));
    }
  
    // انتخاب سه مورد کلیدی از بخش‌های مختلف
    const picks = [];
    const pick3 = (arr, label) => {
      if (!Array.isArray(arr)) return;
      for (const it of arr) {
        if (picks.length >= 3) break;
        const t = (it?.title || String(it || '')).toString().trim();
        if (t) picks.push(`${label}: ${t}`);
      }
    };
  
    pick3(tech?.connectors_improvements, 'کانکتور');
    pick3(tech?.data_model_changes, 'مدل داده');
    pick3(tech?.automation_rules, 'اتوماسیون');
    pick3(tech?.code_tasks, 'تسک');
  
    if (picks.length) {
      l.push('');
      l.push('۳ تغییر کلیدی:');
      picks.slice(0, 3).forEach((x, i) => l.push(`${i + 1}) ${x}`));
    }
  
    l.push('');
    l.push(`run_key: ${runKey}`);
    return l.join('\n');
  }
  
  /**
   * پیام‌های جزئی برای هر آیتم فنی (بدون کد)
   * - برای قوانین ارجاعی (reference_task) توضیح ارجاع می‌دهد.
   * @param {string} date - YYYY-MM-DD
   * @param {object} tech
   * @returns {string[]} لیستی از پیام‌ها
   */
  export function buildTechItemMessages(date, tech = {}) {
    const messages = [];
  
    const emit = (sectionTitle, it) => {
      // اگر rule فقط ارجاع است، جزئیات را تکرار نکن
      if (sectionTitle === 'قوانین اتوماسیون' && it.reference_task) {
        messages.push(
          `🧩 ${sectionTitle} – ${date}\n` +
          `عنوان: ${it.title || '-'}\n` +
          `— این قانون به تسک «${it.reference_task}» متصل است و از همان مسیر اجرا/پیگیری می‌شود.`
        );
        return;
      }
  
      const title = it.title || it.name || (typeof it === 'string' ? it : '-');
      const problem = it.problem || '';
      const reasons = it.reasons || '';
      const solution = it.solution || '';
  
      const lines = [];
      lines.push(`🧩 ${sectionTitle} – ${date}`);
      lines.push(`عنوان: ${title}`);
      if (problem) { lines.push('— مشکل/علت:'); lines.push(problem); }
      if (reasons) { lines.push('— دلایل/اثرات:'); lines.push(reasons); }
      if (solution) { lines.push('— راهکار:'); lines.push(solution); }
  
      // ⚠️ عمداً کد یا pseudo_code ارسال نمی‌کنیم (واتساپ)
      messages.push(lines.join('\n'));
    };
  
    const pack = (sectionTitle, arr = []) =>
      (arr || []).forEach(it =>
        emit(sectionTitle, (typeof it === 'object' ? it : { title: String(it) }))
      );
  
    pack('بهبود کانکتورها', tech?.connectors_improvements);
    pack('تغییرات مدل داده', tech?.data_model_changes);
    pack('قوانین اتوماسیون', tech?.automation_rules);
    if (Array.isArray(tech?.code_tasks)) tech.code_tasks.forEach(t => emit('تسک کدنویسی', t));
  
    return messages;
  }
  
  export default {
    buildManagementMessage,
    buildTechSummaryMessage,
    buildTechItemMessages,
  };
  
