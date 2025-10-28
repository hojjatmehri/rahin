import '../../logger.js';
/**
 * crmCollector.js
 * تکمیل ستون‌های CRM در rahin_daily_summary:
 *   - new_contacts
 *   - repeated_buyers
 *   - conversion_requests
 *
 * ENV (اختیاری):
 *   APP_TZ=Asia/Tehran
 *   RAHIN_DB_MAIN=...                 # مسیر db_atigh.sqlite (اگر متفاوت است)
 *
 *   # Contacts
 *   CRM_CONTACTS_TABLE=contacts       # candidates: contacts, crm_contacts, didar_contacts
 *   CRM_CONTACTS_DATE_COL=created_at  # candidates: created_at, createdAt, reg_date, inserted_at, ts
 *   CRM_CONTACTS_ID_COL=id            # اختیاری (برای شمارش مطمئن)
 *   CRM_CONTACTS_MOBILE_COL=mobile    # اختیاری (برای اشتراک با تراکنش‌ها)
 *
 *   # Transactions (برای repeated_buyers)
 *   TX_TABLE=transactions
 *   TX_DATE_COL=created_at            # candidates: created_at, regDate, timestamp, ts, payDate1
 *   TX_CUSTOMER_COL=customer_id       # candidates: customer_id, contact_id, mobile, phone, national_id
 *
 *   # Deals / Opportunities (برای conversion_requests)
 *   CRM_DEALS_TABLE=deals             # candidates: deals, opportunities, crm_deals
 *   CRM_DEALS_DATE_COL=created_at     # candidates: created_at, updated_at, stage_at
 *   CRM_DEALS_STAGE_COL=stage         # candidates: stage, status, pipeline_stage
 *   CRM_DEALS_CONVERTED_VALUES=proposal,quoted,offer,price_sent  # جدا با کاما
 */

import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const PROJECT_ROOT     = process.env.RAHNEGAR_SOURCE_ROOT || path.resolve(process.cwd());
const DB_MAIN_PATH     = process.env.RAHIN_DB_MAIN || path.join(PROJECT_ROOT, "db_atigh.sqlite");
const RUN_TZ           = process.env.APP_TZ || "Asia/Tehran";

// Contacts ENV
const CONTACTS_TBL     = process.env.CRM_CONTACTS_TABLE || null;
const CONTACTS_DATE    = process.env.CRM_CONTACTS_DATE_COL || null;
const CONTACTS_ID      = process.env.CRM_CONTACTS_ID_COL || null;
const CONTACTS_MOBILE  = process.env.CRM_CONTACTS_MOBILE_COL || null;

// Transactions ENV
const TX_TBL           = process.env.TX_TABLE || "transactions";
const TX_DATE          = process.env.TX_DATE_COL || null;
const TX_CUST          = process.env.TX_CUSTOMER_COL || null;

// Deals ENV
const DEALS_TBL        = process.env.CRM_DEALS_TABLE || null;
const DEALS_DATE       = process.env.CRM_DEALS_DATE_COL || null;
const DEALS_STAGE      = process.env.CRM_DEALS_STAGE_COL || null;
const DEALS_VALUES     = (process.env.CRM_DEALS_CONVERTED_VALUES || "proposal,quoted,offer,price_sent")
                           .split(",").map(s => s.trim().toLowerCase()).filter(Boolean);

function log(...a){ console.log("[crmCollector]", ...a); }
function warn(...a){ console.warn("[crmCollector][WARN]", ...a); }

function openDB(){
  if (!fs.existsSync(DB_MAIN_PATH)) throw new Error(`DB not found: ${DB_MAIN_PATH}`);
 
 let db;
 
 try {
   db = new Database(DB_MAIN_PATH, {
     fileMustExist: false,
     timeout: 5000,
   });
 
   db.pragma("journal_mode = WAL");
   db.pragma("foreign_keys = ON");
   db.pragma("busy_timeout = 5000");
   db.pragma("synchronous = NORMAL");
   db.pragma("temp_store = MEMORY");
 
   console.log("[DB] sqlite ready (WAL + timeout)");
 } catch (err) {
   console.error("[DB] failed:", err.message);
   process.exit(1);
 }
  return db;
}

function tableInfo(db, name){ return db.prepare(`PRAGMA table_info(${name});`).all(); }
function hasTable(db, name){
  return !!db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name);
}
function hasColumn(db, table, col){
  return tableInfo(db, table).some(c => c.name === col);
}

function ensureDailySummaryColumns(db){
  db.exec(`
    CREATE TABLE IF NOT EXISTS rahin_daily_summary (date TEXT PRIMARY KEY);
  `);
  const must = [
    "new_contacts INTEGER",
    "repeated_buyers INTEGER",
    "conversion_requests INTEGER"
  ];
  for (const def of must){
    const col = def.split(" ")[0];
    if (!hasColumn(db, "rahin_daily_summary", col)){
      db.exec(`ALTER TABLE rahin_daily_summary ADD COLUMN ${def};`);
    }
  }
}

function toTehranDate(d){
  const s = new Date(d).toLocaleString("en-US", { timeZone: RUN_TZ });
  return new Date(s);
}
function startOfDayTehran(dStr){
  const td = toTehranDate(new Date(dStr + "T00:00:00"));
  td.setHours(0,0,0,0); return td;
}
function endOfDayTehran(dStr){
  const td = toTehranDate(new Date(dStr + "T23:59:59"));
  td.setHours(23,59,59,999); return td;
}
function parseDateFlexible(v){
  if (v == null) return null;
  const s = String(v).trim();
  if (/^\d{10,13}$/.test(s)) {
    const n = Number(s);
    return new Date(s.length === 13 ? n : n*1000);
  }
  return new Date(s.replace(/\//g,'-'));
}
function normalizePersonId(v){
  if (v == null) return "";
  let s = String(v).trim();
  // اگر شبیه موبایل باشد، به 98xxxx نرمال کن
  const onlyDigits = s.replace(/\D+/g,'');
  if (/^0\d{10}$/.test(onlyDigits)) return "98" + onlyDigits.slice(1);
  if (/^9\d{9}$/.test(onlyDigits))  return "98" + onlyDigits;
  if (/^98\d{10}$/.test(onlyDigits)) return onlyDigits;
  // وگرنه همان مقدار (برای customer_id/UUID و …)
  return s;
}

// ====== Auto-discovery helpers ======
function pickCol(info, candidates){
  const names = info.map(c => c.name);
  const lower = info.map(c => c.name.toLowerCase());
  for (const c of candidates){
    const i = lower.indexOf(c.toLowerCase());
    if (i >= 0) return names[i];
  }
  return null;
}
function discoverContacts(db){
  const candidates = CONTACTS_TBL
    ? [CONTACTS_TBL]
    : ["contacts","crm_contacts","didar_contacts","wp_user_identity"];
  for (const tbl of candidates){
    if (!hasTable(db, tbl)) continue;
    const info = tableInfo(db, tbl);
    const date = CONTACTS_DATE && hasColumn(db, tbl, CONTACTS_DATE)
                  ? CONTACTS_DATE
                  : pickCol(info, ["created_at","createdAt","reg_date","inserted_at","ts","timestamp","created"]);
    if (!date) continue;
    const id = CONTACTS_ID && hasColumn(db, tbl, CONTACTS_ID)
                ? CONTACTS_ID
                : pickCol(info, ["id","contact_id","uid","user_id"]);
    const mobile = CONTACTS_MOBILE && hasColumn(db, tbl, CONTACTS_MOBILE)
                    ? CONTACTS_MOBILE
                    : pickCol(info, ["mobile","phone","msisdn","whatsapp","cell"]);
    return { table: tbl, dateCol: date, idCol: id, mobileCol: mobile };
  }
  return null;
}
function discoverTransactions(db){
  if (!hasTable(db, TX_TBL)) return null;
  const info = tableInfo(db, TX_TBL);
  const date = TX_DATE && hasColumn(db, TX_TBL, TX_DATE)
                ? TX_DATE
                : pickCol(info, ["created_at","regDate","timestamp","ts","payDate1","date"]);
  const cust = TX_CUST && hasColumn(db, TX_TBL, TX_CUST)
                ? TX_CUST
                : pickCol(info, ["customer_id","contact_id","mobile","phone","national_id"]);
  return { table: TX_TBL, dateCol: date, customerCol: cust };
}
function discoverDeals(db){
  const candidates = DEALS_TBL ? [DEALS_TBL] : ["deals","opportunities","crm_deals"];
  for (const tbl of candidates){
    if (!hasTable(db, tbl)) continue;
    const info = tableInfo(db, tbl);
    const date = DEALS_DATE && hasColumn(db, tbl, DEALS_DATE)
                  ? DEALS_DATE
                  : pickCol(info, ["created_at","updated_at","stage_at","ts","timestamp","date"]);
    const stage = DEALS_STAGE && hasColumn(db, tbl, DEALS_STAGE)
                  ? DEALS_STAGE
                  : pickCol(info, ["stage","status","pipeline_stage"]);
    if (!date || !stage) continue;
    return { table: tbl, dateCol: date, stageCol: stage };
  }
  return null;
}

// ====== Core ======
export function summarizeCrmForDate(targetDateStr){
  const db = openDB();
  try{
    ensureDailySummaryColumns(db);

    const from = startOfDayTehran(targetDateStr);
    const to   = endOfDayTehran(targetDateStr);

    // ---- new_contacts ----
    let new_contacts = 0;
    const cdesc = discoverContacts(db);
    if (cdesc){
      const rows = db.prepare(`SELECT ${cdesc.dateCol} as _d FROM ${cdesc.table}`).all();
      for (const r of rows){
        const dt = parseDateFlexible(r._d);
        if (!(dt instanceof Date) || !isFinite(dt.getTime())) continue;
        const tdt = toTehranDate(dt);
        if (tdt >= from && tdt <= to) new_contacts++;
      }
    } else {
      warn("Contacts table not found or date column missing; new_contacts=0");
    }

    // ---- repeated_buyers ----
    let repeated_buyers = 0;
    const tdesc = discoverTransactions(db);
    if (tdesc && tdesc.dateCol && tdesc.customerCol){
      const rows = db.prepare(`SELECT ${tdesc.customerCol} as _c, ${tdesc.dateCol} as _d FROM ${tdesc.table} WHERE ${tdesc.customerCol} IS NOT NULL`).all();
      const seenBefore = new Set();   // customers with ANY txn before the day
      const seenToday  = new Set();   // customers with ANY txn in the day
      for (const r of rows){
        const cust = normalizePersonId(r._c);
        if (!cust) continue;
        const dt = parseDateFlexible(r._d);
        if (!(dt instanceof Date) || !isFinite(dt.getTime())) continue;
        const tdt = toTehranDate(dt);
        if (tdt < from) seenBefore.add(cust);
        else if (tdt >= from && tdt <= to) seenToday.add(cust);
      }
      // repeated buyers = intersection(seenBefore, seenToday)
      for (const c of seenToday) if (seenBefore.has(c)) repeated_buyers++;
    } else {
      warn("Transactions table or customer/date column missing; repeated_buyers=0");
    }

    // ---- conversion_requests ----
    let conversion_requests = 0;
    const ddesc = discoverDeals(db);
    if (ddesc){
      const rows = db.prepare(`SELECT ${ddesc.dateCol} as _d, ${ddesc.stageCol} as _s FROM ${ddesc.table}`).all();
      for (const r of rows){
        const stage = String(r._s ?? "").toLowerCase().trim();
        if (!stage) continue;
        if (!DEALS_VALUES.includes(stage)) continue;
        const dt = parseDateFlexible(r._d);
        if (!(dt instanceof Date) || !isFinite(dt.getTime())) continue;
        const tdt = toTehranDate(dt);
        if (tdt >= from && tdt <= to) conversion_requests++;
      }
    } else {
      // اگر جدول deals نداری، صفر می‌ماند
      log("Deals table not found; conversion_requests=0");
    }

    // ---- UPSERT ----
    db.prepare(`
      INSERT INTO rahin_daily_summary (date, new_contacts, repeated_buyers, conversion_requests)
      VALUES (@d, @nc, @rb, @cr)
      ON CONFLICT(date) DO UPDATE SET
        new_contacts = excluded.new_contacts,
        repeated_buyers = excluded.repeated_buyers,
        conversion_requests = excluded.conversion_requests
    `).run({ d: targetDateStr, nc: new_contacts, rb: repeated_buyers, cr: conversion_requests });

    log(`OK ${targetDateStr} → new_contacts=${new_contacts} repeated_buyers=${repeated_buyers} conversion_requests=${conversion_requests}`);
  } finally {
    try{ db.close(); }catch{}
  }
}

export function runCrmCollector(dateStr){
  const target = dateStr || new Date().toISOString().slice(0,10);
  summarizeCrmForDate(target);
}

