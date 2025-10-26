// src/ai/errorAnalyzer.js
// اسکن لاگ‌های اخیر، تحلیل خطا با OpenAI، ثبت در DB، و ارسال خلاصه (اختیاری)

import fs from 'fs';
import path from 'path';
import { openai, waService } from '../config/Config.js';
import env from '../config/env.js';
import { get as dbGet, run as dbRun } from '../db/db.js';
import { nowStamp, todayYMD, t0, took } from '../utils/time.js';
import { readLinesSafe, readCodeSnippet } from '../utils/files.js';
import { hashLine, normalizeMobile } from '../utils/normalizers.js';
import { sanitizeForWhatsApp, chunkText } from '../message/sanitize.js';
import logger from '../logging/logger.js';
// بالاى فایل: الگوهاى حذف بخش‌هاى متغیر
const RE_TS_PREFIX = /^\[\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,3})?(?:Z|[+\-]\d{2}:\d{2})?\]\s*/;
const RE_STEPID = /stepId\s*[:=]\s*scan-\d+#\d+/ig;
const RE_SCANID = /scan-\d+/ig;
const RE_UUIDHEX = /\b[0-9a-f]{8}\b/ig;          // تکه‌های هگز کوتاه مثل errorId
const RE_TIMENUM = /\b\d{10,}\b/g;               // تایم‌استمپ‌های عددى طولانى
const RE_SPACES = /\s+/g;

// تابع کلیدی:
function normalizeErrorKey(raw = '') {
  return String(raw)
    .replace(RE_TS_PREFIX, '')               // حذف تایم‌استمپ اول خط
    .replace(RE_STEPID, 'stepId=<X>')        // نرمال‌سازی stepId
    .replace(RE_SCANID, 'scan-<X>')          // نرمال‌سازی scanId
    .replace(RE_UUIDHEX, '<HEX>')            // نرمال‌سازی هگزهای کوتاه
    .replace(RE_TIMENUM, '<NUM>')            // نرمال‌سازی اعدادِ بلند
    .replace(/\s\|\sindex\.(?:[cm]?js|ts):\d+\s\|/i, ' | index.<file>:<line> |') // الگوی لوله‌ای
    .trim()
    .replace(RE_SPACES, ' ');
}

/* ---------------------------------------
   پارامترها
--------------------------------------- */
const LOGS_DIR = env.RAHIN_LOGS_DIR || './logs';
const LOG_FILE = env.RAHIN_LOG_FILE || ''; // اگر پر باشد فقط همین را می‌خوانیم
const MODEL = env.RAHIN_MODEL || 'gpt-4o';

/* ---------------------------------------
   ابزارهای کمکی مسیر
--------------------------------------- */
function resolvePathMaybeAbsolute(p) {
  if (!p) return '';
  return path.isAbsolute(p) ? p : path.resolve(process.cwd(), p);
}

function resolveLogTarget() {
  // اگر فایل مشخص است، فقط همان
  if (LOG_FILE) {
    return { mode: 'file', targets: [resolvePathMaybeAbsolute(LOG_FILE)] };
  }
  // در غیر اینصورت حالت دایرکتوری (روزانه)
  const dir = resolvePathMaybeAbsolute(LOGS_DIR);
  const today = todayYMD();
  const yesterday = new Date(Date.now() - 24 * 3600 * 1000).toISOString().slice(0, 10);
  return {
    mode: 'dir',
    targets: [path.join(dir, `${today}.log`), path.join(dir, `${yesterday}.log`)]
  };
}

/* ---------------------------------------
   زمان و پارس سطرهای لاگ
--------------------------------------- */
// انعطاف بیشتر در تشخیص تایم‌استمپ: [YYYY-MM-DD HH:mm:ss], [YYYY-MM-DDTHH:mm:ss.mmmZ], ...
function parseFileTimestamp(line = '') {
  const m = line.match(/^\[(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:[.,](\d{1,3}))?(?:Z|([+\-]\d{2}:\d{2}))?\]/);
  if (!m) return null;
  const d = m[1], h = m[2], ms = m[3] || '000', tz = m[4] || '';
  const iso = `${d}T${h}.${ms}${tz}`;
  const t = Date.parse(iso);
  return Number.isNaN(t) ? null : new Date(t);
}

function ymdOf(d) { return d.toISOString().slice(0, 10); }

/* ---------------------------------------
   فیلتر خطوط خارجی (سیستم دیگر)
--------------------------------------- */
function isForeignLine(s = '') {
  const t = s.toLowerCase();
  return t.includes('\\projects\\atighgashtai\\logs\\'.toLowerCase());
}

/* ---------------------------------------
   خواندن پنجرهٔ اخیر
--------------------------------------- */
export function scanRecentLogWindow(minutes = 70) {
  const now = new Date();
  const fromTs = new Date(now.getTime() - minutes * 60 * 1000);
  const { targets } = resolveLogTarget();

  const windowLines = [];
  for (const f of new Set(targets)) {
    if (!f || !fs.existsSync(f)) continue;
    const lines = readLinesSafe(f);
    for (const line of lines) {
      // حذف خطوط خارجی اگر به دایرکتوری دیگری اشاره دارند
      if (isForeignLine(line)) continue;

      const ts = parseFileTimestamp(line);
      if (!ts) continue;
      if (ts >= fromTs && ts <= now) {
        windowLines.push({ file: f, ts, line });
      }
    }
  }
  return windowLines.sort((a, b) => a.ts - b.ts);
}

export function splitErrorsWithContext(windowLines) {
  const errorsIdx = windowLines
    .map((x, i) => (/\[ERROR\]/.test(x.line) ? i : -1))
    .filter(i => i >= 0);

  return errorsIdx.map(i => {
    // 20 خط قبل + 10 خط بعد
    const ctx = windowLines.slice(Math.max(0, i - 20), Math.min(windowLines.length, i + 11));
    return { err: windowLines[i], context: ctx };
  });
}

/* ---------------------------------------
   استخراج مسیر فایل/شماره خط از لاگ
--------------------------------------- */
// الگوهای بدون نام‌گروه؛ پایدار در Node/Babel
const RE_AT_LINE =
  /\bat\s+(?:[^(]*\()?(?:file:\/\/\/)?([A-Za-z]:[\\\/][^:\s)]+|\/[^:\s)]+|\.\.?[\\\/][^:\s)]+)\.(js|mjs|cjs|ts|tsx|jsx):(\d+)(?::\d+)?\)?/;
const RE_ANYWHERE =
  /(?:file:\/\/\/)?([A-Za-z]:[\\\/][^:\s)]+|\/[^:\s)]+|\.\.?[\\\/][^:\s)]+)\.(js|mjs|cjs|ts|tsx|jsx):(\d+)/;
// الگوی « | index.js:744 | »
const RE_PIPE_INLINE = /\|\s*([^\|\s]+)\.(js|mjs|cjs|ts|tsx|jsx):(\d+)\s*\|/i;

function matchLocationInLine(line = '') {
  if (!line) return null;

  // حالت " | index.js:744 | "
  const p = line.match(RE_PIPE_INLINE);
  if (p) {
    const base = `${p[1]}.${p[2]}`;
    return { filePath: path.resolve(base), lineNo: Number(p[3]) };
  }

  // حالت رایج: "at ... file.ext:line:col"
  const m1 = line.match(RE_AT_LINE);
  if (m1) {
    const base = `${m1[1]}.${m1[2]}`;
    return { filePath: path.resolve(base), lineNo: Number(m1[3]) };
  }

  // فول‌بک: هرجای خط
  const m2 = line.match(RE_ANYWHERE);
  if (m2) {
    const base = `${m2[1]}.${m2[2]}`;
    return { filePath: path.resolve(base), lineNo: Number(m2[3]) };
  }

  return null;
}

export function extractFromErrorLine(line = '', context = []) {
  // اگر stack=... در خط هست، اول از آن بخوان
  const stackPart = line.match(/stack=(.*)$/);
  const stackText = stackPart ? stackPart[1] : '';

  let loc = matchLocationInLine(stackText) || matchLocationInLine(line);
  if (loc) return loc;

  // از کانتکست (۲۰ خط قبل/۱۰ بعد) هم جست‌وجو کن
  for (const c of context || []) {
    loc = matchLocationInLine(c.line);
    if (loc) return loc;
  }

  // نشد → nullها
  return { filePath: null, lineNo: null };
}

/* ---------------------------------------
   استخراج ورودی‌های احتمالی از متن خط
--------------------------------------- */
export function extractInputsFromLine(line = '') {
  const out = [];
  // context={...}
  const ctxMatch = line.match(/context=({[\s\S]*?})(?:\s*\||$)/);
  if (ctxMatch) {
    try { out.push({ kind: 'context', json: JSON.parse(ctxMatch[1]) }); } catch { /* ignore */ }
  }
  // args=...
  const argsMatch = line.match(/args=([\s\S]+?)(?:\s*\||$)/);
  if (argsMatch) {
    const raw = argsMatch[1];
    const jsonMatches = raw.match(/{[\s\S]*?}/g) || [];
    for (const jm of jsonMatches) {
      try { out.push({ kind: 'arg', json: JSON.parse(jm) }); } catch { /* ignore */ }
    }
  }
  return out;
}

/* ---------------------------------------
   تحلیل خطا با OpenAI (JSON فقط فارسی)
--------------------------------------- */
export async function analyzeErrorWithAI(err, context, filePath, lineNo) {
  // استخراج ورودی‌ها از خود خط خطا و خطوط کانتکست
  const inputs = extractInputsFromLine(err.line);
  for (const c of context) {
    const extra = extractInputsFromLine(c.line);
    if (extra.length) inputs.push(...extra);
  }

  const schemaHint = `
فقط JSON معتبر و فارسی:
{
  "short_summary": "خلاصهٔ یک‌خطی مدیریتی",
  "root_cause": "ریشهٔ فنی به‌اختصار",
  "fix_steps": ["گام ۱","گام ۲","..."],
  "code_patch": "در صورت نیاز، پچ/کد (JS/SQL). اگر لازم نیست خالی بگذار."
}`;

  const prompt = `
[خطای ثبت‌شده]
${err.line}

[فایل/لاین]
${filePath || '-'}${Number.isFinite(lineNo) ? ':' + lineNo : ''}

[ورودی‌های یافت‌شده]
${inputs.length ? JSON.stringify(inputs, null, 2) : 'یافت نشد'}

[کد اطراف]
${readCodeSnippet(filePath, lineNo)}

[کانتکست]
${context.map(x => x.line).join('\n')}

${schemaHint}
`;

  const resp = await openai.chat.completions.create({
    model: MODEL,
    response_format: { type: 'json_object' },
    messages: [
      { role: 'system', content: 'تحلیلگر خطای Node.js: فقط JSON فارسی برگردان.' },
      { role: 'user', content: prompt },
    ],
  });

  const raw = resp?.choices?.[0]?.message?.content || '{}';
  let data;
  try { data = JSON.parse(raw); } catch { data = {}; }
  return {
    data,
    tokensIn: resp?.usage?.prompt_tokens ?? 0,
    tokensOut: resp?.usage?.completion_tokens ?? 0,
  };
}

/* ---------------------------------------
   ارسال خلاصه خطا در واتساپ (اختیاری)
--------------------------------------- */
async function sendErrorSummaryWhatsApp({ toRaw, filePath, lineNo, ai, date }) {
  if (!toRaw || !waService) return false;
  const to = normalizeMobile(toRaw) || toRaw; // اگر گروه باشد هم کار کند

  const msg =
    `خلاصه خطا ${(filePath ? path.basename(filePath) : 'نامشخص')}${Number.isFinite(lineNo) ? ':' + lineNo : ''}\n` +
    `— خلاصه: ${ai?.data?.short_summary || 'نامشخص'}\n` +
    (ai?.data?.root_cause ? `— ریشه: ${ai.data.root_cause}\n` : '') +
    (Array.isArray(ai?.data?.fix_steps) && ai.data.fix_steps.length
      ? `— گام‌ها: ${ai.data.fix_steps.slice(0, 3).join(' ')}`
      : '');

  const parts = chunkText(sanitizeForWhatsApp(msg), 1200);
  for (const p of parts) {
    await waService.sendMessage(to, p);
    await new Promise(r => setTimeout(r, 600));
  }
  logger.info('ارسال خلاصه خطا به واتساپ انجام شد', { to, parts: parts.length, date });
  return true;
}

/* ---------------------------------------
   تابع اصلی پردازش لاگ‌های اخیر
--------------------------------------- */
const seenThisScan = new Set();     // <-- اضافه شود
const MAX_AI_PER_SCAN = 5;          // اختیاری: سقف تحلیل در هر اسکن
let aiCount = 0;                    // شمارنده تحلیل‌ها

export async function processRecentErrors(options = {}) {
  const {
    minutes = 70,
    sendWhatsApp = false,
    destMobile = env.WHATSAPP_DEST_MOBILE || '',   // اگر شماره مستقیم داری
    groupId = env.WHATSAPP_GROUP_ID || '',         // یا به گروه بفرست
  } = options;

  const traceId = `scan-${Date.now()}`;
  const startAll = t0();

  logger.info('شروع اسکن لاگ‌های اخیر', { traceId, minutes });

  // 1) خواندن پنجره زمانی
  const tRead = t0();
  const lines = scanRecentLogWindow(minutes);
  logger.info('خواندن لاگ‌ها', { found: lines.length, took: took(Date.now() - tRead) });
  if (!lines.length) {
    logger.info('هیچ خطی در بازه یافت نشد. پایان.', { traceId });
    return { scanned: 0, bundles: 0, handled: 0, sent: 0 };
  }

  // 2) باندل‌سازی خطاها
  const tBundle = t0();
  const bundles = splitErrorsWithContext(lines);
  logger.info('باندل‌سازی خطاها', { bundles: bundles.length, took: took(Date.now() - tBundle) });

  let handled = 0, sent = 0;

  // 3) پردازش هر خطا
  for (let idx = 0; idx < bundles.length; idx++) {
    const { err, context } = bundles[idx];
  
    // این دو خط کم بودند:
    const stepId = `${traceId}#${idx + 1}`;  // <-- اضافه شود
    const tErr = t0();                       // <-- اضافه شود
  
    // د-دیوپ در حافظه همین اسکن + کلید نرمال‌شده
    const key = normalizeErrorKey(err.line);
    if (seenThisScan.has(key)) {
      logger.debug('تکراری در همین اسکن - رد شد', { stepId, keySample: key.slice(0,80) });
      continue;
    }
    seenThisScan.add(key);
  
    const h = hashLine(key);
    try {
      await dbRun(`INSERT INTO rahin_error_sent(error_hash) VALUES (?)`, [h]);
      logger.info('ثبت ضدتکرار خطا', { stepId, errorId: h.slice(0, 8) });
    } catch {
      logger.debug('قبلاً پردازش شده (DB) - رد شد', { stepId, errorId: h.slice(0, 8) });
      continue;
    }
  
    // (اختیاری) محدودکنندهٔ نرخ تحلیل
    if (aiCount >= MAX_AI_PER_SCAN) {
      logger.info('سقف تحلیل AI در این اسکن پر شد؛ بقیه رد می‌شوند', { stepId });
      continue;
    }
    aiCount++;
  }
  logger.info('پایان اسکن خطاها', {
    traceId,
    scanned: lines.length,
    bundles: bundles.length,
    handled,
    sent,
    total: took(Date.now() - startAll),
  });

  return { scanned: lines.length, bundles: bundles.length, handled, sent };
}

/* ---------------------------------------
   API export
--------------------------------------- */
export default {
  processRecentErrors,
  scanRecentLogWindow,
  splitErrorsWithContext,
  extractFromErrorLine,
  extractInputsFromLine,
  analyzeErrorWithAI,
};
