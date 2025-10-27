// ========================================================
// File: logger.js (نسخهٔ محلی‌شده با WhatsApp داخلی)
// Author: Hojjat Mehri
// ========================================================

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import axios from 'axios';

// ===== WhatsApp Service (محلی) =====
class WhatsAppService {
  constructor(instanceId, token) {
    this.instanceId = instanceId;
    this.token = token;
    this.baseUrl = `https://api.ultramsg.com/${instanceId}/messages`;
    this.enabled = !!(instanceId && token);
  }

  async sendMessage(to, text) {
    if (!this.enabled) {
      console.log(`[logger:waService] ⚠️ sendMessage skipped (no credentials) -> ${to}: ${text.slice(0, 100)}...`);
      return;
    }
    try {
      const payload = {
        token: this.token,
        to,
        body: text,
      };
      const res = await axios.post(this.baseUrl, payload, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      });
      console.log(`[logger:waService] ✅ sent to ${to}: ${res.status}`);
    } catch (e) {
      console.warn(`[logger:waService] ❌ send failed to ${to}: ${e?.message || e}`);
    }
  }
}

// ===== ساخت waService از ENV =====
export const waService = new WhatsAppService(
  process.env.ULTRAMSG_INSTANCE_ID,
  process.env.ULTRAMSG_TOKEN
);

// ===== تنظیمات عمومی =====
const originalLog = console.log;
const originalWarn = console.warn;
const originalError = console.error;

const LOGGER_FILE_NAME = 'logger.js';
const ERROR_NOTIFICATION_PHONE =
  process.env.WHATSAPP_DEST_MOBILE || '09134052885';

// ===== Utilities =====
function pad2(n) {
  const s = String(Number(n) || 0);
  return s.length >= 2 ? s : '0' + s;
}
function pad3(n) {
  const s = String(Number(n) || 0);
  return s.length === 3 ? s : s.length === 2 ? '0' + s : '00' + s;
}
function getDateStr(d = new Date()) {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}
function getTimeStamp() {
  const d = new Date();
  return `${getDateStr(d)} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}.${pad3(d.getMilliseconds())}`;
}
function stringifySafe(obj) {
  try {
    if (typeof obj === 'string') return obj;
    if (obj instanceof Error)
      return `${obj.name}: ${obj.message}\n${obj.stack || ''}`;
    return JSON.stringify(obj, null, 2);
  } catch {
    return '[unserializable]';
  }
}

// ===== مسیر نسبی فراخوان =====
function getCallerInfoFallback() {
  try {
    const e = new Error();
    const lines = String(e.stack || '').split(/\r?\n/);
    for (const line of lines) {
      if (!line) continue;
      if (line.includes(LOGGER_FILE_NAME)) continue;
      if (line.includes('node:internal')) continue;
      const m = line.match(/\((.*?):(\d+):(\d+)\)/) || line.match(/at (.*?):(\d+):(\d+)/);
      if (m) {
        const full = path.resolve(m[1]);
        const cwd = process.cwd().replace(/\\/g, '/');
        let rel = full.replace(/\\/g, '/');
        if (rel.startsWith(cwd)) rel = rel.slice(cwd.length + 1);
        return `${rel}:${m[2]}:${m[3]}`;
      }
    }
  } catch {}
  return 'unknown:0:0';
}

// ===== فایل لاگ روزانه =====
const LOG_DIR = process.env.RAHIN_LOGS_DIR
  ? path.resolve(process.env.RAHIN_LOGS_DIR)
  : path.resolve('./logs');
try {
  fs.mkdirSync(LOG_DIR, { recursive: true });
} catch {}

let currentDateStr = null;
let stream = null;

function ensureStream() {
  const today = getDateStr();
  if (currentDateStr === today && stream) return;
  if (stream) try { stream.end('\n'); } catch {}
  currentDateStr = today;
  const filename = path.join(LOG_DIR, `${today}.log`);
  stream = fs.createWriteStream(filename, { flags: 'a', encoding: 'utf8' });
}
function writeLine(line) {
  try {
    ensureStream();
    if (stream) stream.write(line + '\n');
  } catch (e) {
    originalError('[logger] write failed:', e.message);
  }
}

// ===== Override‌ها =====
console.log = (...args) => {
  const ts = getTimeStamp();
  const caller = getCallerInfoFallback();
  const msg = args.map(a => stringifySafe(a)).join(' ');
  originalLog(...args, '|', ts, '|', caller);
  writeLine(`[${ts}] [INFO] ${msg} | ${caller}`);
};

console.warn = (...args) => {
  const ts = getTimeStamp();
  const caller = getCallerInfoFallback();
  const msg = args.map(a => stringifySafe(a)).join(' ');
  originalWarn(...args, '|', ts, '|', caller);
  writeLine(`[${ts}] [WARN] ${msg} | ${caller}`);
};

console.error = (...args) => {
  const ts = getTimeStamp();
  const caller = getCallerInfoFallback();
  const msg = args.map(a => stringifySafe(a)).join(' ');
  originalError(...args, '|', ts, '|', caller);
  writeLine(`[${ts}] [ERROR] ${msg} | ${caller}`);

  // ارسال واتساپ (اختیاری)
  try {
    const shortMsg = `❌ Error @ ${ts}\n${caller}\n${msg.slice(0, 1800)}`;
    waService.sendMessage(ERROR_NOTIFICATION_PHONE, shortMsg).catch(() => {});
  } catch {}
};

// ===== Exception handlers =====
process.on('uncaughtException', (err) => {
  console.error('UncaughtException', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('UnhandledRejection', reason);
});
