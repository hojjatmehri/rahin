// src/clicks/enrichVisitorFromClicks.js
// استخراج موبایل از کلیک‌ها + همبست‌سازی واتساپ با اینباکس (±۲ دقیقه) و ذخیره در visitor_contacts

import { get as dbGet, all as dbAll, run as dbRun, exec as dbExec } from '../db/db.js';
import moment from 'moment-timezone';
import { pathToFileURL } from 'url';

/* ---------------------------------------
   Logger (Tehran time)
--------------------------------------- */
const LOG_NS = 'enrichVisitorFromClicks';
const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
const ENV_LEVEL = process.env.ENRICH_LOG_LEVEL || 'info';
const ENV_DEBUG = ['1','true','yes','on'].includes(String(process.env.ENRICH_DEBUG || '').toLowerCase());
let CURRENT_LEVEL = LOG_LEVELS[ENV_LEVEL] ?? LOG_LEVELS.info;

function tzNow() { return moment().tz('Asia/Tehran').format('YYYY-MM-DD HH:mm:ss'); }
function log(level, ...args) { if ((LOG_LEVELS[level] ?? 99) > CURRENT_LEVEL) return; console.log(`[${tzNow()}] [${LOG_NS}] [${level.toUpperCase()}]`, ...args); }
const logger = { error: (...a)=>log('error',...a), warn:(...a)=>log('warn',...a), info:(...a)=>log('info',...a), debug:(...a)=>log('debug',...a) };

logger.info('MODULE LOADED', {
  ENRICH_LOG_LEVEL: process.env.ENRICH_LOG_LEVEL || 'info',
  ENRICH_DEBUG: process.env.ENRICH_DEBUG || '',
  ENRICH_RUN_ON_STARTUP: process.env.ENRICH_RUN_ON_STARTUP || '',
  CWD: process.cwd(),
});

/* ---------------------------------------
   Helpers: URL sanitize & mobile parsing
--------------------------------------- */
function sanitizeUrl(u = '') {
  let s = String(u || '').trim();
  if (/^https?:\/\/https?:\/\//i.test(s)) {
    const orig = s;
    s = s.replace(/^http:\/\/https:\/\//i, 'https://').replace(/^https:\/\/http:\/\//i, 'http://');
    logger.debug('sanitizeUrl: fixed double-protocol', { before: orig, after: s });
  }
  if (/^https?:\/\/(https?:\/\/)/i.test(s)) {
    const orig = s;
    s = s.replace(/^https?:\/\/(https?:\/\/)/i, (_, inner) => inner);
    logger.debug('sanitizeUrl: collapsed duplicated scheme', { before: orig, after: s });
  }
  return s;
}

// 98xxxxxxxxxx
export function normalizeMobile(m) {
  const digits = String(m || '').replace(/[^\d]/g, '');
  if (!digits) return null;
  if (digits.startsWith('98')) return digits;
  if (digits.startsWith('0'))  return '98' + digits.slice(1);
  if (digits.startsWith('9'))  return '98' + digits;
  return digits;
}
function isValidIranMobile(msisdn = '') { return /^98(9\d{9})$/.test(msisdn); }

function extractAnyIranMobileFromString(s = '') {
  const m = String(s).match(/(?:\+?98|0098|0)?9\d{9}/);
  return m ? normalizeMobile(m[0]) : null;
}

export function extractMobileFromWhatsAppUrl(u = '') {
  try {
    const url = new URL(u);
    const host = url.hostname.toLowerCase();
    const isWa = /(^|\.)wa\.me$|(^|\.)whatsapp\.com$/.test(host);
    if (!isWa) return null;

    const pathNum = url.pathname.replace(/^\/+/, '').match(/^\d{9,15}$/);
    if (pathNum) return normalizeMobile(pathNum[0]);

    const p = url.searchParams.get('phone');
    if (p) return normalizeMobile(p);

    const t = url.searchParams.get('text') || '';
    const m = t.match(/(?:\+?98|0)?9\d{9}/);
    if (m) return normalizeMobile(m[0]);

    return null;
  } catch (e) {
    logger.debug('extractMobileFromWhatsAppUrl: invalid url', u, e?.message);
    return null;
  }
}

function extractMobileFromTelUrl(u = '') {
  if (!u) return null;
  let s = u.replace(/^tel:/i, '').replace(/[^\d+]/g, '');
  s = s.replace(/^(\+98|0098)/, '98').replace(/^0/, '98');
  return isValidIranMobile(s) ? s : null;
}

/* ---------------------------------------
   Constants
--------------------------------------- */
const AGENCY_NUMBERS = new Set([
  '989203136002', // واتساپ آژانس
]);
function isAgencyNumber(msisdn) { return AGENCY_NUMBERS.has(msisdn); }

/* ---------------------------------------
   DB: ensure & upsert
--------------------------------------- */
async function ensureVisitorContactsTable() {
  logger.debug('ensureVisitorContactsTable: start');
  await dbExec(`
    CREATE TABLE IF NOT EXISTS visitor_contacts (
      visitor_id TEXT NOT NULL,
      mobile     TEXT NOT NULL,
      source     TEXT,
      confidence INTEGER,
      last_seen  TEXT DEFAULT (datetime('now','localtime')),
      PRIMARY KEY (visitor_id, mobile)
    );
  `);
  await dbExec(`CREATE INDEX IF NOT EXISTS idx_visitor_contacts_mobile ON visitor_contacts(mobile);`);
  logger.debug('ensureVisitorContactsTable: done');
}

async function ensureClickInboxMatchTable() {
  await dbExec(`
    CREATE TABLE IF NOT EXISTS whatsapp_click_inbox_matches (
      click_id   INTEGER NOT NULL,
      inbox_id   INTEGER NOT NULL,
      matched_at TEXT    DEFAULT (datetime('now','localtime')),
      PRIMARY KEY (click_id, inbox_id)
    );
  `);
  await dbExec(`CREATE INDEX IF NOT EXISTS idx_wcim_inbox ON whatsapp_click_inbox_matches(inbox_id);`);
  await dbExec(`CREATE INDEX IF NOT EXISTS idx_wcim_click ON whatsapp_click_inbox_matches(click_id);`);
}

export async function upsertVisitorMobile(visitor_id, mobile, source = 'click', confidence = 90) {
  if (!visitor_id || !mobile) {
    logger.warn('upsertVisitorMobile: invalid params', { visitor_id, mobile });
    return;
  }
  await ensureVisitorContactsTable();
  logger.debug('upsertVisitorMobile: UPSERT', { visitor_id, mobile, source, confidence });
  await dbRun(`
    INSERT INTO visitor_contacts (visitor_id, mobile, source, confidence, last_seen)
    VALUES (?, ?, ?, ?, datetime('now','localtime'))
    ON CONFLICT(visitor_id, mobile) DO UPDATE SET
      last_seen  = datetime('now','localtime'),
      source     = CASE WHEN excluded.confidence >= visitor_contacts.confidence THEN excluded.source ELSE visitor_contacts.source END,
      confidence = MAX(visitor_contacts.confidence, excluded.confidence)
  `, [visitor_id, mobile, source, confidence]);
  logger.info('upsertVisitorMobile: upserted', { visitor_id, mobile, source, confidence });
}

/* ---------------------------------------
   Whatsapp click ↔ inbox correlation (±2m)
--------------------------------------- */
function tehranRangeAround(timeStr, windowSec) {
  // timeStr را به تهران تبدیل کن و ±windowSec بساز
  const base = moment.tz(timeStr, 'Asia/Tehran'); // فرض: clicked_at در تهران ذخیره شده
  if (!base.isValid()) return null;
  return {
    start: base.clone().subtract(windowSec, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
    end:   base.clone().add(windowSec, 'seconds').format('YYYY-MM-DD HH:mm:ss'),
  };
}

function stripCUs(msisdnWithSuffix = '') {
  // "98912.....@c.us" → "98912....."
  const only = String(msisdnWithSuffix || '').split('@')[0];
  return normalizeMobile(only);
}

export async function enrichFromWhatsappClicksAndInboxCorrelation({
  windowSec = 120,
  debug = ENV_DEBUG,
  limit = 5000,
  dryRun = false
} = {}) {
  if (debug) CURRENT_LEVEL = LOG_LEVELS['debug'];
  logger.info('==== WHATSAPP CORRELATION START ====', { windowSec, debug, dryRun });

  const metrics = {
    clicksScanned: 0,
    alreadyMatched: 0,
    inboxChecked: 0,
    matchedPairs: 0,
    upserted: 0,
    noInboxInWindow: 0,
    schemaErrors: 0,
    errors: 0,
  };

  try {
    const tblClick = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name='click_logs'`);
    const tblInbox = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name='whatsapp_new_msg'`);
    if (!tblClick || !tblInbox) {
      logger.warn('required tables missing', { click_logs: !!tblClick, whatsapp_new_msg: !!tblInbox });
      return { ok: false, reason: 'tables', metrics };
    }

    await ensureVisitorContactsTable();
    await ensureClickInboxMatchTable();

    // فقط کلیک‌های واتساپ
    const clicks = await dbAll(`
      SELECT c.id, c.visitor_id, c.click_type, c.target_url, c.clicked_at
      FROM click_logs c
      WHERE c.target_url IS NOT NULL
        AND (LOWER(c.click_type) LIKE '%whatsapp%' OR c.target_url LIKE '%wa.me%' OR c.target_url LIKE '%whatsapp.com%')
      ORDER BY c.clicked_at DESC
      LIMIT ?
    `, [limit]);

    metrics.clicksScanned = clicks?.length || 0;
    logger.info('whatsapp clicks fetched', { count: metrics.clicksScanned });

    for (const c of clicks || []) {
      try {
        if (!c.visitor_id) continue;

        const rawUrl = sanitizeUrl(String(c.target_url || ''));
        const range = tehranRangeAround(String(c.clicked_at || ''), windowSec);
        if (!range) {
          logger.debug('click skipped: invalid clicked_at', { id: c.id, clicked_at: c.clicked_at });
          continue;
        }

        // پیام‌های ورودی در بازهٔ زمانی (created_at محلی)
        const inboxRows = await dbAll(`
          SELECT id, ffrom, tto, fromMe, created_at, ttime
          FROM whatsapp_new_msg
          WHERE fromMe = 0
            AND created_at BETWEEN ? AND ?
        `, [range.start, range.end]);

        metrics.inboxChecked += inboxRows.length;

        if (!inboxRows.length) {
          metrics.noInboxInWindow++;
          logger.debug('no inbox in window', { click_id: c.id, range });
          continue;
        }

        for (const m of inboxRows) {
          const inboxId = m.id;
          const msisdn = stripCUs(m.ffrom); // شماره کاربر
          const toMsisdn = stripCUs(m.tto);  // باید شماره آژانس باشد

          if (!isValidIranMobile(msisdn)) {
            logger.debug('inbox row: invalid ffrom', { inboxId, ffrom: m.ffrom });
            continue;
          }
          if (isAgencyNumber(msisdn)) {
            logger.debug('inbox row: ffrom is agency (skip)', { inboxId, msisdn });
            continue;
          }
          // (اختیاری) اگر tto وجود دارد و آژانس نبود، می‌تونیم رد کنیم
          if (toMsisdn && !isAgencyNumber(toMsisdn)) {
            logger.debug('inbox row: tto is not agency (skip)', { inboxId, toMsisdn });
            continue;
          }

          // آیا قبلاً این جفت ذخیره شده؟
          const already = await dbGet(`
            SELECT 1 FROM whatsapp_click_inbox_matches WHERE click_id = ? AND inbox_id = ?
          `, [c.id, inboxId]);

          if (already) {
            metrics.alreadyMatched++;
            continue;
          }

          logger.info('MATCHED', {
            click_id: c.id,
            visitor_id: c.visitor_id,
            clicked_at: c.clicked_at,
            inbox_id: inboxId,
            inbox_created_at: m.created_at,
            user_msisdn: msisdn,
            url: rawUrl,
          });

          if (!dryRun) {
            await upsertVisitorMobile(c.visitor_id, msisdn, 'whatsapp_inbox_match', 99);
            await dbRun(`INSERT OR IGNORE INTO whatsapp_click_inbox_matches (click_id, inbox_id) VALUES (?, ?)`, [c.id, inboxId]);
            metrics.upserted++;
          }

          metrics.matchedPairs++;
          // اگر لازم نیست چندتا پیام را برای یک کلیک بخوانیم، می‌توانیم break کنیم
          // break;
        }
      } catch (rowErr) {
        metrics.errors++;
        logger.error('correlation row error', { click_id: c?.id, err: rowErr?.message || rowErr });
      }
    }

    logger.info('==== WHATSAPP CORRELATION END ====', { metrics });
    return { ok: true, metrics };

  } catch (e) {
    metrics.errors++;
    logger.error('FATAL in whatsapp correlation', e?.message || e);
    return { ok: false, reason: 'fatal', metrics };
  }
}

/* ---------------------------------------
   Direct extractor (fallback): tel/wa links
   — در عمل واتساپ مقصد آژانس است و اغلب بی‌اثر می‌ماند —
--------------------------------------- */
export async function enrichVisitorMobilesFromClicks({ debug = ENV_DEBUG, dryRun = false } = {}) {
  if (debug) CURRENT_LEVEL = LOG_LEVELS.debug;
  logger.info('==== ENRICH START ====', { debug, dryRun, LOG_LEVEL: Object.keys(LOG_LEVELS).find(k => LOG_LEVELS[k] === CURRENT_LEVEL) });

  try {
    const tbl = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name='click_logs'`);
    if (!tbl) {
      logger.warn('click_logs table NOT found. Exit.');
      return { inserted: 0, scanned: 0, metrics: { noTable: 1 } };
    }
    logger.info('click_logs table exists');

    const rows = await dbAll(`
      SELECT id, visitor_id, click_type, target_url, clicked_at
      FROM click_logs
      WHERE target_url IS NOT NULL
    `);
    const scanned = rows?.length || 0;
    logger.info('fetched click rows', { scanned });

    const metrics = {
      emptyUrlOrVisitor: 0,
      whatsappType: 0,
      telType: 0,
      unknownType: 0,
      agencySkipped: 0,
      upserted: 0,
      errors: 0
    };

    let inserted = 0;

    for (const r of rows || []) {
      try {
        const rawUrl = String(r.target_url || '').trim();
        if (!rawUrl || !r.visitor_id) {
          metrics.emptyUrlOrVisitor++;
          logger.debug('skip: empty url or visitor_id', { id: r.id, rawUrl, visitor_id: r.visitor_id });
          continue;
        }
        const url = sanitizeUrl(rawUrl);

        const type = String(r.click_type || '').toLowerCase();
        const isWhatsAppType = type.includes('whatsapp') || /wa\.me|whatsapp\.com/i.test(url);
        const isTelType      = type.includes('tel') || type.includes('call') || /^tel:/i.test(url);

        if (isTelType)        metrics.telType++;
        else if (isWhatsAppType) metrics.whatsappType++;
        else                  metrics.unknownType++;

        let mobile = null;
        if (isTelType) {
          mobile = extractMobileFromTelUrl(url) || extractMobileFromWhatsAppUrl(url) || extractAnyIranMobileFromString(url);
        } else if (isWhatsAppType) {
          mobile = extractMobileFromWhatsAppUrl(url) || extractMobileFromTelUrl(url) || extractAnyIranMobileFromString(url);
        } else {
          mobile = extractMobileFromTelUrl(url) || extractMobileFromWhatsAppUrl(url) || extractAnyIranMobileFromString(url);
        }

        if (!mobile) {
          continue; // الان هدف اصلی ما correlation است
        }

        if (isAgencyNumber(mobile)) {
          metrics.agencySkipped++;
          logger.debug('agency number skipped', { id: r.id, mobile });
          continue;
        }

        if (!dryRun) {
          const source = isTelType ? 'tel_click' : (isWhatsAppType ? 'whatsapp_click' : 'unknown_click');
          const confidence = isTelType ? 85 : (isWhatsAppType ? 95 : 80);
          await upsertVisitorMobile(r.visitor_id, mobile, source, confidence);
          inserted++;
          metrics.upserted++;
        }
      } catch (rowErr) {
        metrics.errors++;
        logger.error('row processing error', { id: r?.id, err: rowErr?.message || rowErr });
      }
    }

    logger.info('==== ENRICH END ====', { inserted, scanned, metrics });
    return { inserted, scanned, metrics };

  } catch (e) {
    logger.error('FATAL in enrichVisitorMobilesFromClicks', e?.message || e);
    return { inserted: 0, scanned: 0, metrics: { fatal: 1, message: e?.message || String(e) } };
  }
}

/* ---------------------------------------
   Auto-run logic (Windows-friendly)
--------------------------------------- */
const isDirect = !!(process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href);
const truthy = v => ['1','true','yes','on'].includes(String(v || '').toLowerCase());
const shouldAutorun = isDirect || truthy(process.env.ENRICH_RUN_ON_STARTUP);

if (shouldAutorun && !globalThis.__ENRICH_ALREADY_RAN__) {
  globalThis.__ENRICH_ALREADY_RAN__ = true;
  logger.info('AUTORUN TRIGGERED', {
    isDirect,
    ENRICH_RUN_ON_STARTUP: process.env.ENRICH_RUN_ON_STARTUP || '',
    ENRICH_DEBUG: process.env.ENRICH_DEBUG || '',
    ENRICH_DRYRUN: process.env.ENRICH_DRYRUN || ''
  });

  const debug = truthy(process.env.ENRICH_DEBUG);
  const dryRun = truthy(process.env.ENRICH_DRYRUN);

  // 1) همبست‌سازی واتساپ (اصلی)
  enrichFromWhatsappClicksAndInboxCorrelation({ debug, dryRun })
    .then(res => logger.info('AUTORUN RESULT (whatsapp correlation)', res))
    .catch(err => logger.error('AUTORUN ERROR (whatsapp correlation)', err?.message || err))
    .finally(() => {
      // 2) مسیر مستقیم (پشتیبان)
      enrichVisitorMobilesFromClicks({ debug, dryRun })
        .then(res => logger.info('AUTORUN RESULT (direct extractor)', res))
        .catch(err => logger.error('AUTORUN ERROR (direct extractor)', err?.message || err));
    });

} else {
  logger.info('NO AUTORUN', { isDirect, ENRICH_RUN_ON_STARTUP: process.env.ENRICH_RUN_ON_STARTUP || '' });
}
