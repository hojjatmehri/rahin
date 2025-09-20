// src/ai/dualInsights.js
// تولید «تحلیل دوگانه» (مدیریتی + فنی) از داده‌های جمع‌آوری‌شده

import { openai } from '../config/Config.js';
import env from '../config/env.js';
import {
  applyQualityFilters,
  dedupeTechAcrossSections,
  injectMandatoryAutomationRule,
} from '../utils/normalizers.js';

const MODEL = env.RAHIN_MODEL || 'gpt-4o';

/* ---------------------------------------
   اسکیمای مورد انتظار (برای راهنمایی مدل)
--------------------------------------- */
const schemaGuide = `
خروجی فقط و فقط یک JSON معتبر باشد (بدون توضیح اضافی). کل خروجی فارسی باشد.

ساختار دقیق:
{
  "management": {
    "analysis": "string",
    "ideas": [
      { "title":"string","why":"string","how":"string","priority":0,"expected_impact":"string","eta_days":0,"risk":"string" }
    ],
    "risks": ["string"],
    "action_checklist_24h": ["string"],
    "missing_data": ["string"]
  },
  "tech": {
    "overview": "string",
    "connectors_improvements": [
      { "title":"string", "problem":"string", "reasons":"string", "solution":"string", "code_snippet":"string" }
    ],
    "data_model_changes": [
      { "title":"string", "problem":"string", "reasons":"string", "solution":"string", "code_snippet":"string" }
    ],
    "automation_rules": [
      { "title":"string", "problem":"string", "reasons":"string", "solution":"string", "code_snippet":"string" }
    ],
    "code_tasks": [
      {
        "title":"string",
        "rationale":"string",
        "acceptance_criteria":["string"],
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
   ساخت پرامپت ورودی مدل
--------------------------------------- */
export function buildPrompt(input) {
  return `
شما باید دو خروجی «management» و «tech» را دقیقاً در قالب JSON تولید کنید.
قوانین:
- زبانِ تمام متن‌ها فقط «فارسی» باشد.
- هیچ متن اضافه یا توضیح خارج از JSON ننویس.
- فقط یک شیء JSON معتبر با کلیدهای مشخص برگردان.
- منابع داده‌ی شما فقط این‌ها هستند: معاملات مالی از transactions، پیام‌های واتساپ از whatsapp_new_msg،
  رویدادهای اینستاگرام از atigh_instagram_dev، لاگ ارسال PDF از wa_pdf_dispatch_log، و آمار کلیک‌ها از click_logs.
- حتماً در بخش «tech.automation_rules» یک مورد درباره «ایجاد قوانین خودکار برای ارسال پیام‌های تبلیغاتی در شبکه‌های اجتماعی» پیشنهاد بده.

داده‌های امروز:
${JSON.stringify(input, null, 2)}
`;
}

/* ---------------------------------------
   کمک‌تابع: تشخیص آمیختگی انگلیسی
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
   فارسی‌سازی مقادیر متنی JSON (بدون تغییر ساختار)
--------------------------------------- */
export async function ensurePersianJSON(jsonObj) {
  if (!objectHasEnglish(jsonObj)) return jsonObj;

  const instr = `
این JSON را صرفاً به فارسی بازنویسی کن، اما ساختار و کلیدها دقیقاً همین بمانند.
هیچ کلید یا فیلدی حذف/اضافه نکن. فقط مقادیر متنی را فارسی کن. فقط همان JSON را برگردان.
`;
  const resp = await openai.chat.completions.create({
    model: MODEL,
    response_format: { type: 'json_object' },
    messages: [
      { role: 'system', content: 'مترجم ساختاری: فقط مقادیر متنی را فارسی کن، ساختار JSON را تغییر نده.' },
      { role: 'user', content: instr },
      { role: 'user', content: JSON.stringify(jsonObj) },
    ],
  });

  const raw = resp?.choices?.[0]?.message?.content || '{}';
  try { return JSON.parse(raw); } catch { return jsonObj; }
}

/* ---------------------------------------
   پس‌پردازش بخش فنی خروجی مدل
   - تزریق قانون اجباری اتوماسیون در صورت نبود
   - حذف تکرار بین‌بخشی (ruleهای تکراری با code_tasks)
   - فیلتر کیفیت آیتم‌های کم‌ارزش
--------------------------------------- */
export function postProcessTech(tech) {
  let out = tech || {};
  out = injectMandatoryAutomationRule(out);
  out = dedupeTechAcrossSections(out);
  out = applyQualityFilters(out);
  return out;
}

/* ---------------------------------------
   فراخوانی مدل و تحویل خروجی JSON معتبر
   خروجی: { data, latency, tokensIn, tokensOut }
--------------------------------------- */
export async function getDualInsights(input) {
  const prompt = buildPrompt(input);
  const t0 = Date.now();

  const resp = await openai.chat.completions.create({
    model: MODEL,
    response_format: { type: 'json_object' },
    messages: [
      { role: 'system', content: 'تو تحلیلگر و توسعه‌دهنده هستی. تمام خروجی‌ها باید فارسی باشند.' },
      { role: 'system', content: schemaGuide },
      { role: 'user', content: prompt },
    ],
  });

  const latency = Date.now() - t0;
  const raw = resp?.choices?.[0]?.message?.content || '{}';

  let data;
  try { data = JSON.parse(raw); } catch { data = {}; }

  return {
    data,
    latency,
    tokensIn: resp?.usage?.prompt_tokens ?? 0,
    tokensOut: resp?.usage?.completion_tokens ?? 0,
  };
}

/* ---------------------------------------
   خط لوله آماده برای استفاده در Runner
   ورودی: input (از collectors)
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

  // 2) اطمینان از فارسی‌بودن متن‌ها
  const faData = await ensurePersianJSON(data);

  // 3) پس‌پردازش فنی
  faData.tech = postProcessTech(faData.tech || {});

  return {
    management: faData?.management || {},
    tech: faData?.tech || {},
    telemetry: { latency, tokensIn, tokensOut, model: MODEL },
    raw: { data: faData },
  };
}

export default {
  buildPrompt,
  ensurePersianJSON,
  getDualInsights,
  postProcessTech,
  runDualInsightsPipeline,
};
