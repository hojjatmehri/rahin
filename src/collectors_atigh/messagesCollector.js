import 'file:///E:/Projects/rahin/logger.js';
/**
 * messagesCollector.js
 * تجمیع روزانه تعاملات (WhatsApp/Instagram) → rahin_daily_summary
 *
 * پشتیبانی‌ها:
 *  - جدول واتساپ inbound (مثل: whatsapp_new_msg)
 *  - جدول واتساپ outbound جدا (مثل: message_log)
 *  - نرمال‌سازی شناسهٔ چت: ffrom@c.us / mobile → 98xxxxxxxxxx
 *  - فیلتر کانال (channel='wa') برای جدول outbound
 *  - محاسبه میانگین زمان اولین پاسخ (FRT) با ترکیب in+out به وقت تهران
 *
 * ENV (اختیاری):
 *   MSG_DB_MAIN=...                 → مسیر DB (defaults to db_atigh.sqlite)
 *   APP_TZ=Asia/Tehran
 *
 *   # Inbound (واتساپ دریافتی)
 *   WA_TABLE=whatsapp_new_msg
 *   WA_TIME_COL=ttime               → ثانیه/میلی‌ثانیه/تاریخ متن
 *   WA_DIR_COL=fromMe               → 1=خروجی از جانب ما، 0=ورودی مشتری
 *   WA_CHAT_COL=ffrom               → شناسهٔ چت (ffrom/jid/phone/...)
 *
 *   # Outbound (واتساپ ارسالی در جدول جدا)
 *   WA_OUT_TABLE=message_log
 *   WA_OUT_TIME_COL=sent_at
 *   WA_OUT_CHAT_COL=mobile
 *   WA_OUT_FORCE_OUTBOUND=1         → همه ردیف‌های جدول outbound را outbound فرض کن
 *   WA_FILTER_CHANNEL=wa            → فقط کانال 'wa' را لحاظ کن (اگر ستون channel وجود دارد)
 *
 *   # Instagram (اختیاری)
 *   IG_TABLE=instagram_messages
 *   (ستون‌ها auto-discover می‌شوند)
 */

import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const PROJECT_ROOT = process.env.RAHNEGAR_SOURCE_ROOT || path.resolve(process.cwd());
const DB_MAIN_PATH = process.env.MSG_DB_MAIN || path.join(PROJECT_ROOT, "db_atigh.sqlite");
const RUN_TZ = process.env.APP_TZ || "Asia/Tehran";

// === Explicit ENV overrides ===
const WA_TABLE            = process.env.WA_TABLE || "whatsapp_new_msg";   // inbound
const WA_TIME_COL         = process.env.WA_TIME_COL || null;
const WA_DIR_COL          = process.env.WA_DIR_COL || null;               // e.g., fromMe
const WA_CHAT_COL         = process.env.WA_CHAT_COL || null;              // e.g., ffrom

const WA_OUT_TABLE        = process.env.WA_OUT_TABLE || null;             // outbound table (optional)
const WA_OUT_TIME_COL     = process.env.WA_OUT_TIME_COL || null;          // e.g., sent_at
const WA_OUT_CHAT_COL     = process.env.WA_OUT_CHAT_COL || null;          // e.g., mobile
const WA_OUT_FORCE_OUT    = (process.env.WA_OUT_FORCE_OUTBOUND || "1") === "1";
const WA_FILTER_CHANNEL   = process.env.WA_FILTER_CHANNEL || null;        // e.g., "wa"

const IG_TABLE = process.env.IG_TABLE || null; // اگر نداری، null بگذار

function log(...a){ console.log("[messagesCollector]", ...a); }
function warn(...a){ console.warn("[messagesCollector][WARN]", ...a); }

// ======================== Time Helpers (Tehran) ========================
function toTehranDate(d){
  const s = new Date(d).toLocaleString("en-US", { timeZone: RUN_TZ });
  return new Date(s);
}
function ymdTehran(d){
  const td = toTehranDate(d);
  const yy = td.getFullYear();
  const mm = String(td.getMonth()+1).padStart(2,'0');
  const dd = String(td.getDate()).padStart(2,'0');
  return `${yy}-${mm}-${dd}`;
}
function startOfDayTehran(dStr){
  const td = toTehranDate(new Date(dStr + "T00:00:00"));
  td.setHours(0,0,0,0);
  return td;
}
function endOfDayTehran(dStr){
  const td = toTehranDate(new Date(dStr + "T23:59:59"));
  td.setHours(23,59,59,999);
  return td;
}

// ======================== DB & Schema ========================
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
function hasTable(db, name){
  const r = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name);
  return !!r;
}
function tableInfo(db, name){ return db.prepare(`PRAGMA table_info(${name});`).all(); }
function hasColumn(db, table, col){
  return tableInfo(db, table).some(c => c.name === col);
}

function ensureDailySummaryColumns(db){
  db.exec(`
    CREATE TABLE IF NOT EXISTS rahin_daily_summary (
      date TEXT PRIMARY KEY
    );
  `);
  const must = [
    "messages_in INTEGER",
    "messages_out INTEGER",
    "avg_first_response_time_sec REAL",
    "active_whatsapp_chats INTEGER"
  ];
  for (const def of must){
    const col = def.split(" ")[0];
    if (!hasColumn(db, "rahin_daily_summary", col)){
      db.exec(`ALTER TABLE rahin_daily_summary ADD COLUMN ${def};`);
    }
  }
}

// ======================== Normalizers & Discovery ========================
function normalizeChatId(raw){
  if (raw == null) return "";
  let s = String(raw).trim();

  // حذف بخش دامنه واتساپ (…@c.us)
  const at = s.indexOf("@");
  if (at > 0) s = s.slice(0, at);

  // فقط رقم
  s = s.replace(/\D+/g, "");

  // 09xxxxxxxxx → 98xxxxxxxxxx
  if (/^0\d{10}$/.test(s)) s = "98" + s.slice(1);
  // 9xxxxxxxxx  → 98xxxxxxxxxx
  if (/^9\d{9}$/.test(s)) s = "98" + s;

  return s;
}

function discoverCols(db, table){
  const info = tableInfo(db, table);
  const names = info.map(c => c.name);
  const lower = info.map(c => c.name.toLowerCase());
  const pick = (cands) => { for (const c of cands){ const i = lower.indexOf(c.toLowerCase()); if (i >= 0) return names[i]; } return null; };

  const isOutbound = (WA_OUT_TABLE && table === WA_OUT_TABLE);

  const timeEnv = isOutbound ? WA_OUT_TIME_COL : WA_TIME_COL;
  const chatEnv = isOutbound ? WA_OUT_CHAT_COL : WA_CHAT_COL;
  const dirEnv  = isOutbound ? null : WA_DIR_COL; // جهت فقط برای inbound مهم است

  const timeCol = (timeEnv && names.includes(timeEnv)) ? timeEnv : pick(["ts","timestamp","created_at","time","date","ttime","sent_at"]);
  const fromMeCol = (dirEnv && names.includes(dirEnv)) ? dirEnv : pick(["from_me","fromMe","is_outbound","outbound","direction","is_outgoing"]);
  const chatCol = (chatEnv && names.includes(chatEnv)) ? chatEnv : pick(["chat_id","from","ffrom","sender","jid","phone","conversation_id","wa_number","number","to","mobile","tto"]);

  return { timeCol, fromMeCol, chatCol };
}

// ======================== Fetch Day Messages ========================
function parseDateFlexible(v){
  if (v == null) return null;
  const s = String(v).trim();
  if (/^\d{10,13}$/.test(s)) {
    const n = Number(s);
    return new Date(s.length === 13 ? n : n*1000);
  }
  // تلاش: 'YYYY-MM-DD HH:MM:SS' یا مشابه
  return new Date(s.replace(/\//g,'-'));
}

function fetchDayMessages(db, table, cols, targetDateStr){
  const from = startOfDayTehran(targetDateStr);
  const to   = endOfDayTehran(targetDateStr);

  // بارگیری خام و فیلتر در JS (برای سازگاری با انواع تاریخ)
  const rows = db.prepare(`SELECT * FROM ${table}`).all();

  const out = [];
  for (const r of rows){
    // فیلتر کانال برای جدول‌هایی که ستون channel دارند
    if (WA_FILTER_CHANNEL && Object.prototype.hasOwnProperty.call(r, "channel")) {
      if (String(r.channel ?? "").toLowerCase() !== String(WA_FILTER_CHANNEL).toLowerCase()) continue;
    }

    if (!cols.timeCol) continue;
    const dt = parseDateFlexible(r[cols.timeCol]);
    if (!(dt instanceof Date) || !isFinite(dt.getTime())) continue;

    const tdt = toTehranDate(dt);
    if (tdt < from || tdt > to) continue;

    // outbound؟
    let outbound = null;
    if (cols.fromMeCol != null) {
      const v = r[cols.fromMeCol];
      if (typeof v === "number") outbound = v ? true : false;
      else if (typeof v === "boolean") outbound = !!v;
      else if (typeof v === "string") {
        const L = v.toLowerCase().trim();
        if (["out","outbound","true","1"].includes(L)) outbound = true;
        else if (["in","inbound","false","0"].includes(L)) outbound = false;
      }
    }
    // اگر جدول outbound است و جهت معلوم نشد، به‌اجبار outbound
    if (outbound == null && WA_OUT_TABLE && table === WA_OUT_TABLE && WA_OUT_FORCE_OUT) outbound = true;
    if (outbound == null) outbound = false;

    const chatRaw = cols.chatCol ? r[cols.chatCol] : null;
    const chat = normalizeChatId(chatRaw);

    out.push({ ts: tdt.getTime(), outbound, chat });
  }

  out.sort((a,b)=>a.ts-b.ts);
  return out;
}

// ======================== Core Collector ========================
export function summarizeMessagesForDate(targetDateStr){
  const db = openDB();
  try{
    ensureDailySummaryColumns(db);

    let messages_in = 0;
    let messages_out = 0;
    let active_whatsapp_chats = 0;
    let avg_first_response_time_sec = null;

    // --- WhatsApp inbound ---
    let waInbound = [];
    if (WA_TABLE && hasTable(db, WA_TABLE)){
      const waCols = discoverCols(db, WA_TABLE);
      if (!waCols.timeCol){
        warn(`No time column in ${WA_TABLE}. Skipping inbound.`);
      } else {
        waInbound = fetchDayMessages(db, WA_TABLE, waCols, targetDateStr);
      }
    } else {
      warn(`WhatsApp inbound table not found: ${WA_TABLE}`);
    }

    // --- WhatsApp outbound (separate table) ---
    let waOutbound = [];
    if (WA_OUT_TABLE && hasTable(db, WA_OUT_TABLE)) {
      const outCols = discoverCols(db, WA_OUT_TABLE);
      if (!outCols.timeCol) {
        warn(`No time column in ${WA_OUT_TABLE}. Skipping outbound.`);
      } else {
        waOutbound = fetchDayMessages(db, WA_OUT_TABLE, outCols, targetDateStr)
          .map(m => ({...m, outbound: true}));
      }
    }

    // شمارش پیام‌ها
    for (const m of waInbound){ if (m.outbound) messages_out++; else messages_in++; }
    for (const m of waOutbound){ messages_out++; }

    // چت‌های فعال (distinct در هر دو)
    const activeSet = new Set();
    for (const m of waInbound)  if (m.chat) activeSet.add(m.chat);
    for (const m of waOutbound) if (m.chat) activeSet.add(m.chat);
    active_whatsapp_chats = activeSet.size;

    // محاسبه FRT از ترکیب in+out
    const allForFrt = waInbound.concat(waOutbound).sort((a,b)=>a.ts-b.ts);
    const byChat = new Map();
    for (const m of allForFrt){
      if (!m.chat) continue;
      if (!byChat.has(m.chat)) byChat.set(m.chat, []);
      byChat.get(m.chat).push(m);
    }
    const deltas = [];
    for (const arr of byChat.values()){
      const firstIn  = arr.find(x => !x.outbound);
      const firstOut = arr.find(x => x.outbound && x.ts > (firstIn?.ts ?? Infinity));
      if (firstIn && firstOut) deltas.push(Math.max(0, Math.round((firstOut.ts - firstIn.ts)/1000)));
    }
    if (deltas.length){
      const sum = deltas.reduce((a,b)=>a+b,0);
      avg_first_response_time_sec = sum / deltas.length;
    }

    // --- Instagram (اختیاری؛ فقط به شمارش in/out افزوده می‌شود) ---
    if (IG_TABLE && hasTable(db, IG_TABLE)){
      const igCols = discoverCols(db, IG_TABLE);
      if (!igCols.timeCol){
        warn(`No time column in ${IG_TABLE}. Skipping Instagram.`);
      } else {
        const ig = fetchDayMessages(db, IG_TABLE, igCols, targetDateStr);
        for (const m of ig){
          if (m.outbound) messages_out++;
          else messages_in++;
        }
        // توجه: active_whatsapp_chats تعریفاً فقط واتساپ است
      }
    }

    // UPSERT به summary
    db.prepare(`
      INSERT INTO rahin_daily_summary
        (date, messages_in, messages_out, avg_first_response_time_sec, active_whatsapp_chats)
      VALUES
        (@d, @mi, @mo, @frt, @active)
      ON CONFLICT(date) DO UPDATE SET
        messages_in = excluded.messages_in,
        messages_out = excluded.messages_out,
        avg_first_response_time_sec = excluded.avg_first_response_time_sec,
        active_whatsapp_chats = excluded.active_whatsapp_chats
    `).run({
      d: targetDateStr,
      mi: messages_in,
      mo: messages_out,
      frt: avg_first_response_time_sec,
      active: active_whatsapp_chats
    });

    log(`OK ${targetDateStr} → in=${messages_in} out=${messages_out} active_wa=${active_whatsapp_chats} frt=${avg_first_response_time_sec ?? "-"}`);
  } finally {
    try{ db.close(); }catch{}
  }
}

// API ساده برای استفادهٔ بیرونی/جاب
export function runMessagesCollector(dateStr){
  const target = dateStr || ymdTehran(new Date());
  summarizeMessagesForDate(target);
}

