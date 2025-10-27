import '../../logger.js';
// src/ai/dualInsights.js
// تولید «تحلیل دوگانه» (مدیریتی + فنی) از داده‌های جمع‌آوری‌شده

import { openai } from '../config/Config.js';
import env from '../config/env.js';
import {
  applyQualityFilters,
  dedupeTechAcrossSections,
  injectMandatoryAutomationRule,
} from '../utils/normalizers.js';

/* ---------------------------------------
   تنظیمات مدل
--------------------------------------- */
const MODEL = env.RAHIN_MODEL || 'gpt-4o';
const TEMPERATURE = 0.2;
const MAX_TOKENS = 1600; // متناسب با اندازهٔ خروجی JSON هدف
// اگر RAHIN_FORCE_FA=1 باشد، حتی اگر انگلیسی تشخیص داده نشود هم فارسی‌سازی اجرا می‌شود
const FORCE_FA = String(env.RAHIN_FORCE_FA || '1') === '1';

/* ---------------------------------------
   اسکیمای مورد انتظار (برای راهنمایی مدل)
   — شامل «قید تعداد» برای پر بودن خروجی
--------------------------------------- */
const schemaGuide = `
خروجی فقط و فقط یک JSON معتبر باشد (بدون توضیح اضافی). کل خروجی فارسی باشد.

ساختار دقیق و تعداد آیتم‌ها:
{
  "management": {
    "analysis": "string",
    "ideas": [
      { "title":"string","why":"string","how":"string","priority":0,"expected_impact":"string","eta_days":0,"risk":"string" },
      { "title":"string","why":"string","how":"string","priority":0,"expected_impact":"string","eta_days":0,"risk":"string" },
      { "title":"string","why":"string","how":"string","priority":0,"expected_impact":"string","eta_days":0,"risk":"string" }
    ],
    "risks": ["string","string"],
    "action_checklist_24h": ["string","string","string"],
    "missing_data": ["string"]
  },
  "tech": {
    "overview": "string",
    "connectors_improvements": [
      { "title":"string","problem":"string","reasons":"string","solution":"string","code_snippet":"string" },
      { "title":"string","problem":"string","reasons":"string","solution":"string","code_snippet":"string" }
    ],
    "data_model_changes": [
      { "title":"string","problem":"string","reasons":"string","solution":"string","code_snippet":"string" },
      { "title":"string","problem":"string","reasons":"string","solution":"string","code_snippet":"string" }
    ],
    "automation_rules": [
      { "title":"string","problem":"string","reasons":"string","solution":"string","code_snippet":"string" }
    ],
    "code_tasks": [
      {
        "title":"string",
        "rationale":"string",
        "acceptance_criteria":["string","string"],
        "pseudo_code":"string",
        "problem":"string",
        "reasons":"string",
        "solution":"string",
        "code_snippet":"string"
      },
      {
        "title":"string",
        "rationale":"string",
        "acceptance_criteria":["string","string"],
        "pseudo_code":"string",
        "problem":"string",
        "reasons":"string",
        "solution":"string",
        "code_snippet":"string"
      },
      {
        "title":"string",
        "rationale":"string",
        "acceptance_criteria":["string","string"],
        "pseudo_code":"string",
        "problem":"string",
        "reasons":"string",
        "solution":"string",
        "code_snippet":"string"
      }
    ]
  }
}
کلیدها و ساختار دقیقاً همین باشد. همه‌ی متن‌ها فارسی باشند. اگر کد لازم است می‌تواند JS/Node یا SQL باشد.
`;

/* ---------------------------------------
   ابزارهای کمکی
--------------------------------------- */
function safeJSONStringify(obj, space = 2, maxLen = 4000) {
  let s;
  try { s = JSON.stringify(obj, null, space); } catch { s = '{}'; }
  if (s.length <= maxLen) return s;
  // اگر خیلی بزرگ است، کوتاه می‌کنیم
  return s.slice(0, maxLen) + '\n/* truncated */';
}

function countOf(x) {
  if (!x) return 0;
  if (Array.isArray(x)) return x.length;
  if (typeof x === 'object') return Object.keys(x).length;
  return 1;
}

/**
 * ساخت خلاصهٔ کوچک از ورودی برای مدل (به‌جای ارسال خامِ سنگین)
 * شامل شمارنده‌ها + چند نمونهٔ کوچک
 */
function makeCompactInputSummary(input) {
  const pickSample = (arr, n = 3) => (Array.isArray(arr) ? arr.slice(0, n) : arr);

  const compact = {
    period: input?.period || {},
    metrics_summary: {
      finance_items: countOf(input?.finance),
      whatsapp_items: countOf(input?.whatsapp),
      instagram_items: countOf(input?.instagram),
      pdf_dispatch_items: countOf(input?.pdf_dispatch),
      whatsapp_clicks_items: countOf(input?.whatsapp_clicks),
    },
    samples: {
      finance: pickSample(input?.finance, 2),
      whatsapp: pickSample(input?.whatsapp, 3),
      instagram: pickSample(input?.instagram, 3),
      pdf_dispatch: pickSample(input?.pdf_dispatch, 2),
      whatsapp_clicks: pickSample(input?.whatsapp_clicks, 3),
    },
    notes: input?.notes || [],
  };

  return compact;
}

/* ---------------------------------------
   ساخت پرامپت ورودی مدل (فشرده و دقیق)
--------------------------------------- */
export function buildPrompt(input) {
  const compact = makeCompactInputSummary(input);

  return `
شما باید دو خروجی «management» و «tech» را دقیقاً در قالب JSON تولید کنید.
قوانین:
- زبانِ تمام متن‌ها فقط «فارسی» باشد.
- هیچ متن اضافه یا توضیح خارج از JSON ننویس.
- فقط یک شیء JSON معتبر با کلیدهای مشخص برگردان.
- منابع داده‌ی شما فقط این‌ها هستند: معاملات مالی از transactions، پیام‌های واتساپ از whatsapp_new_msg،
  رویدادهای اینستاگرام از atigh_instagram_dev، لاگ ارسال PDF از wa_pdf_dispatch_log، و آمار کلیک‌ها از click_logs.
- در «tech.automation_rules» حتماً حداقل یک قاعده درباره «ایجاد قوانین خودکار برای ارسال پیام‌های تبلیغاتی در شبکه‌های اجتماعی» بده.
- تعداد اقلام هر بخش دقیقاً مطابق schema باشد (ideas=3، code_tasks=3، …).

خلاصهٔ داده‌های امروز (فشرده):
${safeJSONStringify(compact, 2, 3500)}
`;
}

/* ---------------------------------------
   تشخیص آمیختگی انگلیسی
--------------------------------------- */
function looksEnglish(s = '') {
  const m = String(s).match(/[A-Za-z]/g);
  return m && m.length > 10;
}
function objectHasEnglish(o) {
  if (o == null) return false;
  if (typeof o === 'string') return looksEnglish(o);
  if (Array.isArray(o)) return o.some(objectHasEnglish);
  if (typeof o === 'object') return Object.values(o).some(objectHasEnglish);
  return false;
}

/* ---------------------------------------
   نگهبان کدها در ترجمه (Mask/Unmask)
--------------------------------------- */
function maskCodeSnippets(obj) {
  const stash = [];
  const isCodeLike = (txt) =>
    /(\bfunction\b|\bconst\b|\blet\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\(|\)|\{|\}|;|=>|\bFROM\b|\bWHERE\b)/.test(txt);

  const walk = (o) => {
    if (!o || typeof o !== 'object') return;
    for (const k of Object.keys(o)) {
      const v = o[k];
      if (typeof v === 'string' && isCodeLike(v)) {
        const token = `__CODE_${stash.length}__`;
        stash.push(v);
        o[k] = token;
      } else if (typeof v === 'object') {
        walk(v);
      }
    }
  };
  const clone = JSON.parse(JSON.stringify(obj));
  walk(clone);
  return { clone, stash };
}
function unmaskCodeSnippets(obj, stash) {
  const walk = (o) => {
    if (!o || typeof o !== 'object') return;
    for (const k of Object.keys(o)) {
      const v = o[k];
      if (typeof v === 'string' && /^__CODE_\d+__$/.test(v)) {
        const idx = Number(v.match(/\d+/)[0]);
        o[k] = stash[idx] ?? '';
      } else if (typeof v === 'object') {
        walk(v);
      }
    }
  };
  walk(obj);
}

/* ---------------------------------------
   فارسی‌سازی مقادیر متنی JSON (بدون تغییر ساختار)
   — ایمن برای کدها
--------------------------------------- */
export async function ensurePersianJSON(jsonObj) {
  if (!FORCE_FA && !objectHasEnglish(jsonObj)) return jsonObj;

  const { clone, stash } = maskCodeSnippets(jsonObj);

  const instr = `
این JSON را صرفاً به فارسی بازنویسی کن، اما ساختار و کلیدها دقیقاً همین بمانند.
هیچ کلید یا فیلدی حذف/اضافه نکن. فقط مقادیر متنی را فارسی کن. فقط همان JSON را برگردان.
`;

  const resp = await openai.chat.completions.create({
    model: MODEL,
    response_format: { type: 'json_object' },
    temperature: TEMPERATURE,
    max_tokens: MAX_TOKENS,
    messages: [
      { role: 'system', content: 'مترجم ساختاری: فقط مقادیر متنی را فارسی کن، ساختار JSON را تغییر نده.' },
      { role: 'system', content: 'اگر توکنی مانند __CODE_0__ دیدی، همان را بدون تغییر نگه دار.' },
      { role: 'user', content: instr },
      { role: 'user', content: JSON.stringify(clone) },
    ],
  });

  const raw = resp?.choices?.[0]?.message?.content || '{}';
  try {
    const obj = JSON.parse(raw);
    unmaskCodeSnippets(obj, stash);
    return obj;
  } catch {
    // اگر نشد، نسخهٔ اصلی را برگردان
    return jsonObj;
  }
}

/* ---------------------------------------
   پس‌پردازش بخش فنی خروجی مدل
   - تزریق قانون اجباری اتوماسیون در صورت نبود
   - حذف تکرار بین‌بخشی
   - فیلتر کیفیت
--------------------------------------- */
export function postProcessTech(tech) {
  let out = tech || {};
  out = injectMandatoryAutomationRule(out);
  out = dedupeTechAcrossSections(out);
  out = applyQualityFilters(out);
  return out;
}

/* ---------------------------------------
   اجبار شکل خروجی (Coercion) و پر کردن کمبودها
--------------------------------------- */
function ensureArrayLen(arr, n, filler) {
  const out = Array.isArray(arr) ? arr.slice(0, n) : [];
  while (out.length < n) out.push(filler(out.length));
  return out;
}

function coerceDualShape(d) {
  const defIdea = (i) => ({
    title: `ایده جایگزین ${i + 1}`,
    why: 'به‌علت ورودی محدود، پیشنهاد عمومی.',
    how: 'بازبینی قیف امروز + اجرای تست A/B.',
    priority: 2,
    expected_impact: 'متوسط',
    eta_days: 2,
    risk: 'پایین',
  });

  const defConn = (i) => ({
    title: `بهبود کانکتور ${i + 1}`,
    problem: 'جزئیات ناکافی از لاگ/داده.',
    reasons: 'ورودی محدود یا خطای جمع‌آوری.',
    solution: 'افزودن لاگ DEBUG و تست سلامت کوئری.',
    code_snippet: '',
  });

  const defDM = (i) => ({
    title: `تغییر مدل داده ${i + 1}`,
    problem: 'عدم کفایت ایندکس/کلیدها.',
    reasons: 'فیلتر تاریخ/تایم‌زون یا کارایی پایین.',
    solution: 'ایندکس‌های ضروری + نرمال‌سازی تاریخ.',
    code_snippet: '',
  });

  const defRule = () => ({
    title: 'اتوماسیون پیام تبلیغاتی',
    problem: 'تعامل کم در ساعات کم‌ترافیک.',
    reasons: 'عدم زمان‌بندی هوشمند/سگمنت‌بندی.',
    solution: 'قواعد ارسال مبتنی بر سکوت ۲۴ساعت و مقصد.',
    code_snippet: '',
  });

  const defTask = (i) => ({
    title: `تسک فنی ${i + 1}`,
    rationale: 'پوشش خلا داده و مشاهده‌پذیری.',
    acceptance_criteria: ['لاگ کامل', 'اجرای موفق کران'],
    pseudo_code: '',
    problem: 'کمبود ورودی برای تحلیل دقیق.',
    reasons: 'عدم ثبت رویداد در بازه امروز.',
    solution: 'بهبود کالکتور و ایندکس‌ها.',
    code_snippet: '',
  });

  const m = d?.management || {};
  const t = d?.tech || {};

  return {
    management: {
      analysis: m.analysis || 'تحلیل معنادار محدود به‌علت ورودی کم.',
      ideas: ensureArrayLen(m.ideas, 3, defIdea),
      risks: Array.isArray(m.risks) ? m.risks : [],
      action_checklist_24h: Array.isArray(m.action_checklist_24h) ? m.action_checklist_24h : [],
      missing_data: Array.isArray(m.missing_data) ? m.missing_data : [],
    },
    tech: {
      overview: t.overview || 'مرور فنی محدود به‌علت ورودی کم.',
      connectors_improvements: ensureArrayLen(t.connectors_improvements, 2, defConn),
      data_model_changes: ensureArrayLen(t.data_model_changes, 2, defDM),
      automation_rules: ensureArrayLen(t.automation_rules, 1, defRule),
      code_tasks: ensureArrayLen(t.code_tasks, 3, defTask),
    },
  };
}

/* ---------------------------------------
   فراخوانی مدل و تحویل خروجی JSON معتبر
   — با Retry در صورت شکست JSON
   خروجی: { data, latency, tokensIn, tokensOut }
--------------------------------------- */
export async function getDualInsights(input) {
  const prompt = buildPrompt(input);
  const t0 = Date.now();

  let resp;
  try {
    resp = await openai.chat.completions.create({
      model: MODEL,
      response_format: { type: 'json_object' },
      temperature: TEMPERATURE,
      max_tokens: MAX_TOKENS,
      messages: [
        { role: 'system', content: 'تو تحلیلگر و توسعه‌دهنده هستی. تمام خروجی‌ها باید فارسی باشند.' },
        { role: 'system', content: schemaGuide },
        { role: 'user', content: prompt },
      ],
    });
  } catch (e) {
    // اگر فراخوانی شکست خورد، تلاش دوم با پرامپت کوتاه
    resp = await openai.chat.completions.create({
      model: MODEL,
      response_format: { type: 'json_object' },
      temperature: TEMPERATURE,
      max_tokens: MAX_TOKENS,
      messages: [
        { role: 'system', content: 'تو تحلیلگر و توسعه‌دهنده هستی. تمام خروجی‌ها باید فارسی باشند.' },
        { role: 'system', content: schemaGuide },
        { role: 'user', content: 'داده عملیاتی محدود بود؛ بر اساس داده کم، خروجی کامل مطابق اسکیمای اجباری تولید کن.' },
      ],
    });
  }

  const latency = Date.now() - t0;
  const raw = resp?.choices?.[0]?.message?.content || '{}';

  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    // Retry سبک اگر JSON خراب بود
    const resp2 = await openai.chat.completions.create({
      model: MODEL,
      response_format: { type: 'json_object' },
      temperature: TEMPERATURE,
      max_tokens: MAX_TOKENS,
      messages: [
        { role: 'system', content: 'فقط یک JSON فارسی معتبر با کلیدهای مشخص‌شده برگردان.' },
        { role: 'system', content: schemaGuide },
        { role: 'user', content: 'خروجی قبل نامعتبر بود؛ لطفاً JSON درست و کامل برگردان.' },
      ],
    });
    try { data = JSON.parse(resp2?.choices?.[0]?.message?.content || '{}'); }
    catch { data = {}; }
  }

  return {
    data,
    latency,
    tokensIn: resp?.usage?.prompt_tokens ?? 0,
    tokensOut: resp?.usage?.completion_tokens ?? 0,
  };
}

/* ---------------------------------------
   خط لوله آماده برای استفاده در Runner
   خروجی:
   {
     management, tech,
     telemetry: { latency, tokensIn, tokensOut },
     raw: { data }
   }
--------------------------------------- */
export async function runDualInsightsPipeline(input) {
  // 1) تولید خام از مدل
  const { data, latency, tokensIn, tokensOut } = await getDualInsights(input);

  // 2) اطمینان از فارسی‌بودن متن‌ها (با Mask کد)
  const faRaw = await ensurePersianJSON(data);

  // 3) اجبار شکل + پس‌پردازش فنی
  const faData = coerceDualShape(faRaw);
  faData.tech = postProcessTech(faData.tech || {});

  return {
    management: faData?.management || {},
    tech: faData?.tech || {},
    telemetry: { latency, tokensIn, tokensOut, model: MODEL },
    raw: { data: faData },
  };
}

/* ---------------------------------------
   ساخت Diagnostic از ورودی (اختیاری برای پیام واتساپ)
--------------------------------------- */
export function buildDiagnosticsFromInput(input) {
  const z = [];
  const zero = (x) => !x || (Array.isArray(x) && x.length === 0);
  if (zero(input?.finance)) z.push('مالی: داده‌ای دریافت نشد یا صفر بود.');
  if (zero(input?.whatsapp)) z.push('واتساپ: پیام/رویداد معتبر دریافت نشد.');
  if (zero(input?.instagram)) z.push('اینستاگرام: رویدادی ثبت نشد.');
  if (zero(input?.pdf_dispatch)) z.push('PDF: لاگ ارسالی وجود ندارد.');
  if (zero(input?.whatsapp_clicks)) z.push('کلیک واتساپ: گزارشی موجود نیست.');
  if (Array.isArray(input?.notes) && input.notes.length) {
    z.push('خطاها/یادداشت‌ها:');
    for (const n of input.notes) z.push(`- ${n.src || 'note'}: ${n.msg || n}`);
  }
  return z;
}

export default {
  buildPrompt,
  ensurePersianJSON,
  getDualInsights,
  postProcessTech,
  runDualInsightsPipeline,
  buildDiagnosticsFromInput,
};

