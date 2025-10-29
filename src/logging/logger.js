import 'file:///E:/Projects/rahin/logger.js';
// src/logging/logger.js
// لاگ با زمان تهران + نوشتن در فایل مرکزی و فایل روزانه

import fs from 'fs';
import path from 'path';
import { nowStamp, ymdOf } from '../utils/time.js';

const LEVELS = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };
let currentLevel =
  LEVELS[(process.env.LOG_LEVEL || 'INFO').toUpperCase()] ?? LEVELS.INFO;

const LOG_FILE = process.env.RAHIN_LOG_FILE
  ? path.resolve(process.env.RAHIN_LOG_FILE)
  : path.resolve(process.cwd(), 'rahin_ops.log');

const LOGS_DIR = process.env.RAHIN_LOGS_DIR
  ? path.resolve(process.env.RAHIN_LOGS_DIR)
  : path.resolve(process.cwd(), 'logs');

function ensureDir(dir) {
  try { fs.mkdirSync(dir, { recursive: true }); } catch {}
}

function safeStringify(o) {
  try { return JSON.stringify(o); } catch { return String(o); }
}

function buildLine(level, msg, ctx) {
  const base = `[${nowStamp()}] [${level}] ${msg}`;
  return (ctx && Object.keys(ctx).length)
    ? `${base} ${safeStringify(ctx)}`
    : base;
}

function writeToFiles(s) {
  // فایل مرکزی
  try { fs.appendFileSync(LOG_FILE, s + '\n', 'utf8'); } catch {}
  // فایل روزانه
  try {
    ensureDir(LOGS_DIR);
    const daily = path.join(LOGS_DIR, `${ymdOf()}.log`);
    fs.appendFileSync(daily, s + '\n', 'utf8');
  } catch {}
}

function emit(level, msg, ctx = null) {
  if (LEVELS[level] < currentLevel) return;
  const s = buildLine(level, msg, ctx);

  // خروجی کنسول مناسب هر سطح
  if (level === 'ERROR') console.error(s);
  else if (level === 'WARN') console.warn(s);
  else if (level === 'DEBUG') console.debug ? console.debug(s) : console.log(s);
  else console.log(s);

  // نوشتن در فایل‌ها
  writeToFiles(s);
}

// توابع سطحی
export function debug(msg, ctx) { emit('DEBUG', msg, ctx); }
export function info(msg, ctx)  { emit('INFO',  msg, ctx); }
export function warn(msg, ctx)  { emit('WARN',  msg, ctx); }
export function error(msg, ctx) { emit('ERROR', msg, ctx); }

// با کانتکست ثابت: هم به‌صورت wrapper، هم اجرای تابع پشتیبانی می‌شود
function makeWrapper(ctx = {}) {
  return {
    debug: (m, extra) => debug(m, { ...ctx, ...(extra || {}) }),
    info:  (m, extra) => info(m,  { ...ctx, ...(extra || {}) }),
    warn:  (m, extra) => warn(m,  { ...ctx, ...(extra || {}) }),
    error: (m, extra) => error(m, { ...ctx, ...(extra || {}) }),
  };
}

/**
 * استفاده:
 * 1) به‌صورت wrapper:
 *    const log = withLogContext({ module: 'finance' });
 *    log.info('start');
 *
 * 2) اجرای تابع:
 *    await withLogContext(async (log) => {
 *      log.info('start');
 *    }, { module: 'finance' });
 */
export function withLogContext(fnOrCtx, maybeCtx) {
  if (typeof fnOrCtx === 'function') {
    const fn = fnOrCtx;
    const ctx = maybeCtx || {};
    const logger = makeWrapper(ctx);
    return fn(logger);
  }
  return makeWrapper(fnOrCtx || {});
}

// کنترل سطح لاگ در زمان اجرا
export function setLevel(levelName = 'INFO') {
  const up = String(levelName || '').toUpperCase();
  if (LEVELS[up] !== undefined) currentLevel = LEVELS[up];
}
export function getLevel() {
  return Object.entries(LEVELS).find(([, v]) => v === currentLevel)?.[0] || 'INFO';
}

export default { debug, info, warn, error, withLogContext, setLevel, getLevel };

