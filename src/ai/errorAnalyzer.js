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

/* ---------------------------------------
   پارامترها
--------------------------------------- */
const LOGS_DIR = env.RAHIN_LOGS_DIR || './logs';
const MODEL = env.RAHIN_MODEL || 'gpt-4o';

/* ---------------------------------------
   ابزارهای کمکی
--------------------------------------- */

// مسیر فایل لاگ روز
function todayLogFile(ymd = todayYMD()) {
  return path.join(LOGS_DIR, `${ymd}.log`);
}

function ymdOf(d) { return d.toISOString().slice(0, 10); }

// [YYYY-MM-DD HH:mm:ss] [LEVEL] ...
function parseFileTimestamp(line = '') {
  const m = line.match(/^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]/);
  if (!m) return null;
  const t = m[1].replace(' ', 'T');
  const ms = Date.parse(t);
  return Number.isNaN(ms) ? null : new Date(ms);
}

export function scanRecentLogWindow(minutes = 70) {
  const now = new Date();
  const fromTs = new Date(now.getTime() - minutes * 60 * 1000);

  const files = new Set([
    todayLogFile(ymdOf(now)),
    todayLogFile(ymdOf(new Date(now.getTime() - 24 * 3600 * 1000))),
  ]);

  const windowLines = [];
  for (const f of files) {
    if (!fs.existsSync(f)) continue;
    const lines = readLinesSafe(f);
    for (const line of lines) {
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

/**
 * تلاش برای استخراج مسیر فایل/شماره خط از لاگ
 * - از stack=... در انتهای خط استفاده می‌کند اگر باشد
 * - در غیر اینصورت از قطعه‌های مانند file.js:123
 */
export function extractFromErrorLine(line = '') {
  // stack=...
  const stackPart = line.match(/stack=(.*)$/);
  const stackText = stackPart ? stackPart[1] : '';

  // الگو: at /path/file.js:123:45  یا at C:\path\file.js:123:45
  const stackMatch = stackText.match(/\bat\s+(?:[^(]*\()?(.*?\.js):(\d+):\d+\)?/);
  if (stackMatch) {
    return { filePath: path.resolve(stackMatch[1]), lineNo: Number(stackMatch[2]) };
  }

  // fallback: هر کجای متن که ...file.js:123 دیده شود
  const locMatch = line.match(/([A-Za-z]:[\\/][\w.\-\\/]+\.js|\.\.?[\\/][\w.\-\\/]+\.js|[\\/][\w.\-\\/]+\.js):(\d+)/);
  if (locMatch) {
    return { filePath: path.resolve(locMatch[1]), lineNo: Number(locMatch[2]) };
  }
  return { filePath: null, lineNo: null };
}

/**
 * استخراج ورودی‌های احتمالی از متن خط (context= {...} و args= ...)
 */
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
${filePath || '-'}${lineNo ? ':' + lineNo : ''}

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
    `🔎 خطای اخیر ${(filePath ? path.basename(filePath) : 'نامشخص')}${lineNo ? ':' + lineNo : ''}\n` +
    `— خلاصه: ${ai?.data?.short_summary || 'نامشخص'}\n` +
    (ai?.data?.root_cause ? `— ریشه: ${ai.data.root_cause}\n` : '') +
    (Array.isArray(ai?.data?.fix_steps) && ai.data.fix_steps.length
      ? `— گام‌ها: ${ai.data.fix_steps.slice(0, 3).join(' • ')}` : '');

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
    const b = bundles[idx];
    const { err, context } = b;
    const stepId = `${traceId}#${idx + 1}`;
    const tErr = t0();

    // 3-الف) ضدتکرار با hash
    const h = hashLine(err.line);
    try {
      await dbRun(`INSERT INTO rahin_error_sent(error_hash) VALUES (?)`, [h]);
      logger.info('ثبت ضدتکرار خطا', { stepId, errorId: h.slice(0, 8) });
    } catch {
      logger.debug('خطا قبلاً پردازش شده بود - رد شد', { stepId, errorId: h.slice(0, 8) });
      continue; // قبلاً دیده شده
    }

    // 3-ب) مکان فایل/لاین
    const tExtract = t0();
    const { filePath, lineNo } = extractFromErrorLine(err.line);
    logger.info('مکان خطا استخراج شد', { stepId, file: filePath || '-', line: lineNo || '-', took: took(Date.now() - tExtract) });

    // 3-پ) تحلیل با AI
    const tAi = t0();
    let ai;
    try {
      ai = await analyzeErrorWithAI(err, context, filePath, lineNo);
    } catch (e) {
      logger.error('خطا در analyzeErrorWithAI', { stepId, error: e?.message });
      ai = { data: { short_summary: 'تحلیل خودکار شکست خورد', root_cause: e?.message || 'نامشخص', fix_steps: [] }, tokensIn: 0, tokensOut: 0 };
    }
    logger.info('AI تحلیل شد', { stepId, tokens_in: ai.tokensIn || 0, tokens_out: ai.tokensOut || 0, took: took(Date.now() - tAi) });

    // 3-ت) ذخیره در جدول بینش خطاها
    const tDb = t0();
    try {
      await dbRun(`
        INSERT INTO rahin_error_insights
          (created_at, log_file, error_ts, error_line, file_path, line_no, short_summary, root_cause, fix_steps, code_patch, model, tokens_in, tokens_out)
        VALUES
          (datetime('now','localtime'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        err.file, err.ts.toISOString(), err.line,
        filePath, lineNo || null,
        ai?.data?.short_summary || '', ai?.data?.root_cause || '',
        Array.isArray(ai?.data?.fix_steps) ? ai.data.fix_steps.join(' | ') : '',
        ai?.data?.code_patch || '',
        MODEL,
        ai?.tokensIn || 0, ai?.tokensOut || 0,
      ]);
      logger.info('ذخیره rahin_error_insights انجام شد', { stepId, took: took(Date.now() - tDb) });
    } catch (e) {
      logger.error('خطا در ذخیره rahin_error_insights', { stepId, error: e?.message });
    }

    handled++;

    // 3-ث) ارسال خلاصه واتساپ (اختیاری)
    if (sendWhatsApp) {
      const to = destMobile || groupId || '';
      if (to) {
        try {
          await sendErrorSummaryWhatsApp({
            toRaw: to,
            filePath,
            lineNo,
            ai,
            date: todayYMD(),
          });
          sent++;
        } catch (e) {
          logger.error('خطا در ارسال واتساپ خلاصه خطا', { stepId, error: e?.message });
        }
      }
    }

    logger.info('پردازش خطا پایان یافت', { stepId, took: took(Date.now() - tErr) });
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
