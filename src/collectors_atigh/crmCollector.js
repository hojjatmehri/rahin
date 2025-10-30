// ============================================================
// File: src/collectors/crmCollector.js
// Purpose: جمع‌آوری شاخص‌های CRM برای rahin_daily_summary
// Author: Hojjat Mehri (Stable Version)
// ============================================================

import 'file:///E:/Projects/rahin/logger.js';
import { db } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';
import moment from 'moment-timezone';

const TZ = process.env.APP_TZ || 'Asia/Tehran';

// --- ENV
const CONTACTS_TBL    = process.env.CRM_CONTACTS_TABLE;
const CONTACTS_DATE   = process.env.CRM_CONTACTS_DATE_COL;
const CONTACTS_ID     = process.env.CRM_CONTACTS_ID_COL;
const CONTACTS_MOBILE = process.env.CRM_CONTACTS_MOBILE_COL;

const TX_TBL   = process.env.TX_TABLE || 'transactions';
const TX_DATE  = process.env.TX_DATE_COL;
const TX_CUST  = process.env.TX_CUSTOMER_COL;

const DEALS_TBL    = process.env.CRM_DEALS_TABLE;
const DEALS_DATE   = process.env.CRM_DEALS_DATE_COL;
const DEALS_STAGE  = process.env.CRM_DEALS_STAGE_COL;
const DEALS_VALUES = (process.env.CRM_DEALS_CONVERTED_VALUES || 'proposal,quoted,offer,price_sent')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

const log = (...a) => console.log('[crmCollector]', ...a);
const warn = (...a) => console.warn('[crmCollector][WARN]', ...a);

// ============================================================
// SCHEMA
// ============================================================
let __schemaEnsured = false;
function ensureDailySummaryColumns() {
  if (__schemaEnsured) return;
  __schemaEnsured = true;

  db.exec(`CREATE TABLE IF NOT EXISTS rahin_daily_summary (date TEXT PRIMARY KEY);`);
  const must = [
    'new_contacts INTEGER',
    'repeated_buyers INTEGER',
    'conversion_requests INTEGER'
  ];
  const existing = db.prepare(`PRAGMA table_info(rahin_daily_summary);`).all().map(c => c.name);
  for (const def of must) {
    const col = def.split(' ')[0];
    if (!existing.includes(col)) db.exec(`ALTER TABLE rahin_daily_summary ADD COLUMN ${def};`);
  }
}

// ============================================================
// HELPERS
// ============================================================
function startOfDayTehran(dateStr) {
  return moment.tz(dateStr, TZ).startOf('day').toDate();
}
function endOfDayTehran(dateStr) {
  return moment.tz(dateStr, TZ).endOf('day').toDate();
}
function parseDateFlexible(v) {
  if (!v) return null;
  const s = String(v).trim();
  if (/^\d{10,13}$/.test(s)) {
    const n = Number(s);
    return new Date(s.length === 13 ? n : n * 1000);
  }
  return new Date(s.replace(/\//g, '-'));
}
function normalizePersonId(v) {
  if (!v) return '';
  const digits = String(v).replace(/\D+/g, '');
  if (/^0\d{10}$/.test(digits)) return '98' + digits.slice(1);
  if (/^9\d{9}$/.test(digits)) return '98' + digits;
  if (/^98\d{10}$/.test(digits)) return digits;
  return v;
}
function pickCol(info, candidates) {
  const lower = info.map(c => c.name.toLowerCase());
  const names = info.map(c => c.name);
  for (const cand of candidates) {
    const idx = lower.indexOf(cand.toLowerCase());
    if (idx >= 0) return names[idx];
  }
  return null;
}
function hasTable(name) {
  return !!db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name);
}
function tableInfo(name) {
  try { return db.prepare(`PRAGMA table_info(${name});`).all(); }
  catch { return []; }
}

// ============================================================
// DISCOVERY
// ============================================================
function discoverContacts() {
  const candidates = CONTACTS_TBL ? [CONTACTS_TBL] : ['contacts','crm_contacts','didar_contacts'];
  for (const tbl of candidates) {
    if (!hasTable(tbl)) continue;
    const info = tableInfo(tbl);
    const date = CONTACTS_DATE || pickCol(info, ['created_at','createdAt','reg_date','inserted_at','ts']);
    const id   = CONTACTS_ID   || pickCol(info, ['id','contact_id','uid']);
    const mob  = CONTACTS_MOBILE || pickCol(info, ['mobile','phone','msisdn','whatsapp']);
    if (date) return { table: tbl, dateCol: date, idCol: id, mobileCol: mob };
  }
  return null;
}
function discoverTransactions() {
  if (!hasTable(TX_TBL)) return null;
  const info = tableInfo(TX_TBL);
  const date = TX_DATE || pickCol(info, ['created_at','regDate','timestamp','ts','payDate1']);
  const cust = TX_CUST || pickCol(info, ['customer_id','contact_id','mobile','phone']);
  if (date && cust) return { table: TX_TBL, dateCol: date, customerCol: cust };
  return null;
}
function discoverDeals() {
  const candidates = DEALS_TBL ? [DEALS_TBL] : ['deals','opportunities','crm_deals'];
  for (const tbl of candidates) {
    if (!hasTable(tbl)) continue;
    const info = tableInfo(tbl);
    const date  = DEALS_DATE  || pickCol(info, ['created_at','updated_at','stage_at']);
    const stage = DEALS_STAGE || pickCol(info, ['stage','status','pipeline_stage']);
    if (date && stage) return { table: tbl, dateCol: date, stageCol: stage };
  }
  return null;
}

// ============================================================
// CORE
// ============================================================
export function summarizeCrmForDate(dateStr) {
  ensureDailySummaryColumns();

  const from = startOfDayTehran(dateStr);
  const to   = endOfDayTehran(dateStr);

  // ---- new_contacts ----
  let new_contacts = 0;
  const cdesc = discoverContacts();
  if (cdesc) {
    const rows = db.prepare(`SELECT ${cdesc.dateCol} AS d FROM ${cdesc.table}`).all();
    for (const r of rows) {
      const dt = parseDateFlexible(r.d);
      if (!dt || isNaN(dt)) continue;
      const t = dt.getTime();
      if (t >= from.getTime() && t <= to.getTime()) new_contacts++;
    }
  } else warn('Contacts not found; new_contacts=0');

  // ---- repeated_buyers ----
  let repeated_buyers = 0;
  const tdesc = discoverTransactions();
  if (tdesc) {
    const rows = db.prepare(`SELECT ${tdesc.customerCol} AS c, ${tdesc.dateCol} AS d FROM ${tdesc.table}`).all();
    const seenBefore = new Set(), seenToday = new Set();
    for (const r of rows) {
      const cust = normalizePersonId(r.c);
      const dt = parseDateFlexible(r.d);
      if (!cust || !dt || isNaN(dt)) continue;
      const t = dt.getTime();
      if (t < from.getTime()) seenBefore.add(cust);
      else if (t <= to.getTime()) seenToday.add(cust);
    }
    for (const c of seenToday) if (seenBefore.has(c)) repeated_buyers++;
  } else warn('Transactions not found; repeated_buyers=0');

  // ---- conversion_requests ----
  let conversion_requests = 0;
  const ddesc = discoverDeals();
  if (ddesc) {
    const rows = db.prepare(`SELECT ${ddesc.dateCol} AS d, ${ddesc.stageCol} AS s FROM ${ddesc.table}`).all();
    for (const r of rows) {
      const stage = String(r.s || '').toLowerCase().trim();
      if (!DEALS_VALUES.includes(stage)) continue;
      const dt = parseDateFlexible(r.d);
      if (!dt || isNaN(dt)) continue;
      const t = dt.getTime();
      if (t >= from.getTime() && t <= to.getTime()) conversion_requests++;
    }
  } else log('Deals not found; conversion_requests=0');

  // ---- UPSERT ----
  db.prepare(`
    INSERT INTO rahin_daily_summary (date, new_contacts, repeated_buyers, conversion_requests)
    VALUES (@d, @nc, @rb, @cr)
    ON CONFLICT(date) DO UPDATE SET
      new_contacts = excluded.new_contacts,
      repeated_buyers = excluded.repeated_buyers,
      conversion_requests = excluded.conversion_requests;
  `).run({ d: dateStr, nc: new_contacts, rb: repeated_buyers, cr: conversion_requests });

  log(`✅ ${dateStr} → new=${new_contacts}, repeat=${repeated_buyers}, conv=${conversion_requests}`);
}

export function runCrmCollector(dateStr) {
  const target = dateStr || moment().tz(TZ).format('YYYY-MM-DD');
  summarizeCrmForDate(target);
}
