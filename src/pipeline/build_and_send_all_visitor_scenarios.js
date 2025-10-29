import 'file:///E:/Projects/rahin/logger.js';

/**
 * Build + store + send scenarios for ALL rows in visitor_contacts
 * - Reads latest weekly table journey_events_w... from ARCHIVE_DB_PATH
 * - Aggregates per visitor_id → mobile
 * - Enriches with whatsapp_new_msg, wa_pdf_dispatch_log, atigh_instagram_dev, DidarCRM
 * - Generates scenario via OpenAI and stores it in person_unified_profile
 * - Sends scenario to WhatsApp operator: waService.sendMessage(to, part)  (supports CONFIG.waService too)
 */

// --- Force-load .env from AtighgashtAI project (absolute path) ---

import dotenv from 'dotenv';
import path from 'path';
dotenv.config({
  path: path.resolve('E:/Projects/AtighgashtAI/.env')
});
console.log('[VisitorScenario] Loaded .env from AtighgashtAI');


// === Cross-Silencer integration (linked to AtighgashtAI) ===
import { shouldSilence } from "../collectors_atigh/crossSilencer.js";



import Database from "better-sqlite3";
import moment from "moment-timezone";
import OpenAI from "openai";
import pLimit from "p-limit";
import {
  findContact, searchContact, checkDealExists, getDealDetailById
} from "./DidarCRMService.js";
// (very top)
import CONFIG, { waService } from '../config/Config.js';

// make services globally visible for sendWhatsAppText()
globalThis.CONFIG = CONFIG;
globalThis.waService = waService;

console.log('🔌 WA service wired:', !!globalThis.waService);


// ====== ENV ======
const MAIN_DB = process.env.MAIN_DB_PATH || "E:\\Projects\\AtighgashtAI\\db_atigh.sqlite";
const ARCH_DB = process.env.ARCHIVE_DB_PATH || "E:\\Projects\\AtighgashtAI\\db_archive.sqlite";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const WHATSAPP_OPERATOR = normalizeMsisdn(process.env.WHATSAPP_OPERATOR || "09134052885");
const TZ = "Asia/Tehran";
const CONCURRENCY = 4;// Number(process.env.SCENARIO_CONCURRENCY || 3);
const DRY_RUN = process.env.DRY_RUN === "1";

console.log("▶️ build_and_send_all_visitor_scenarios: boot", {
  MAIN_DB,
  ARCH_DB,
  WHATSAPP_OPERATOR,
  CONCURRENCY,
  DRY_RUN,
  OPENAI_CONFIGURED: !!OPENAI_API_KEY,
});
function safeParseJson(v, fallback = []) {
  if (Array.isArray(v)) return v;      // اگر خودش آرایه است، دست‌نزن
  if (!v) return fallback;             // null/undefined/"" → برگرد fallback
  if (typeof v === "string") {
    try { return JSON.parse(v); }
    catch (e) {
      console.warn("[safeParseJson] failed:", e.message, "value=", v);
      return fallback;
    }
  }
  return fallback;                     // بقیه‌ی انواع
}


// مبدّل فیلدهای دلخواه از Didar → آبجکت سبک
function mapDealLite(d) {
  const f = d?.Fields || {};
  return {
    id: d.Id,
    code: d.Code,
    title: d.Title,
    status: d.Status,                         // Pending / Won / Lost
    stage_id: d.PipelineStageId,
    pipeline_id: d.PipelineId,
    register_time: d.RegisterTime,
    price: d.Price,
    owner_name: d?.Owner?.DisplayName || null,

    // ====== فیلدهای سفارشی شما ======
    destination: f["Field_8783_0_1473"] || null,      // مقصد
    pax_count: f["Field_8783_12_25"] ?? null,       // تعداد نفرات
    hotel_class: (f["Field_8783_9_13"] || []).join("، "), // کلاس/سطح
    transport: f["Field_8783_0_1475"] || null,      // وسیله (مثلاً قطار)
    trip_type: f["Field_8783_4_32"] || null,        // نوع سفر (سیاحتی/…)
    group_type: f["Field_8783_4_33"] || null,        // گروه (خانواده/…)

    // از خود لیست دیل‌ها
    next_followup_at: d.NextActivityDate || null,          // زمان پیگیری بعدی
    last_activity_at: d.LastActivityUpdateTime || null,
  };
}

// فقط برای نمایش تاریخ به «تهران»
function fmtTeh(iso) {
  if (!iso) return null;
  try {
    return new Date(iso).toLocaleString("fa-IR", { timeZone: "Asia/Tehran", hour12: false });
  } catch { return iso; }
}


function extractOpenAIText(resp) {
  if (!resp) return '';
  if (resp.output_text) return String(resp.output_text).trim();
  if (Array.isArray(resp.output)) {
    const parts = [];
    for (const b of resp.output) {
      if (Array.isArray(b.content)) {
        for (const c of b.content) if (typeof c.text === 'string') parts.push(c.text);
      }
    }
    return parts.join('\n').trim();
  }
  if (resp.choices?.[0]?.message?.content)
    return String(resp.choices[0].message.content).trim();
  return '';
}
function buildFallbackScenario(p) {
  const mins = Math.round((p.total_dwell_sec || 0) / 60);
  const pages = (p.sample_pages || []).slice(-10).map((x, i) =>
    `${i + 1}. ${x.url}${x.dwell_sec ? ` (${x.dwell_sec}s)` : ''}`
  ).join('\n') || '-';

  return [
    `خلاصه لید: ${p.contact_name || 'نام نامشخص'} (${p.mobile.replace(/^98/, '0')})`,
    `حضور در سایت: از ${p.first_seen || '-'} تا ${p.last_seen || '-'}، حدود ${mins} دقیقه.`,
    `صفحات اخیر:\n${pages}`,
    `تعاملات: واتساپ ${p.whatsapp_inbound_count || 0} پیام${p.last_whatsapp_at ? `، آخرین در ${p.last_whatsapp_at}` : ''}.`,
    `کاتالوگ: ${p.pdf_sent_count || 0} مورد${p.last_pdf_title ? `، آخرین: «${p.last_pdf_title}»` : ''}.`,
    `دیدار: ${p.didar_contact_id ? `ContactId=${p.didar_contact_id}${p.contact_name ? `، نام: ${p.contact_name}` : ''}` : 'یافت نشد'}.`,
    `اقدام بعدی: تماس کوتاه + پرسش تاریخ/بودجه/مقصد، سپس ۲ پیشنهاد نزدیک به علایق اخیر.`
  ].join('\n');
}

function phoneCandidates(m) {
  const num = String(m).replace(/[^\d]/g, "");
  const no0 = num.startsWith("0") ? num.slice(1) : num;
  const no98 = no0.startsWith("98") ? no0.slice(2) : no0;
  return [
    "0" + no98,         // 09…
    "98" + no98,        // 98…
    no98                // 9…
  ];
}

function pickLast(arr = []) {
  return Array.isArray(arr) && arr.length ? arr[0] : null;
}


function extractDestinationFromFields(fieldsObj = {}) {
  // فیلد مقصد شما در ساخت Deal همین بود:
  // "Field_8783_0_1473": destination
  return fieldsObj?.["Field_8783_0_1473"] || null;
}

function parseActivities(acts = []) {
  // آخرین نوت
  const notes = acts
    .filter(a => (a?.Note || a?.ResultNote))
    .sort((a, b) => new Date(b.DoneDate || b.CreateDate || 0) - new Date(a.DoneDate || a.CreateDate || 0));
  const lastNote = notes.length ? (notes[0].Note || notes[0].ResultNote) : null;

  // اولین فعالیت باز به عنوان پیگیری بعدی
  const nextFollow = acts.find(a => a.IsDone === false && a.DueDate);

  return {
    last_note: lastNote || null,
    next_followup_at: nextFollow?.DueDate || null,
    last_activity_at: notes.length ? (notes[0].DoneDate || notes[0].CreateDate) : null,
  };
}

function stripUrl(u) {
  try { const x = new URL(u); return (x.origin + x.pathname).replace(/\/$/, ""); }
  catch { return String(u).split("?")[0]; }
}
function normalizeUrl(u) {
  try { return String(u).split("?")[0]; } catch { return u; }
}

export function compactPages(pages = [], limit = 8) {
  return pages
    .filter(p => (p?.dwell_sec ?? 0) >= 5)           // فقط engagement
    .map(p => ({ ts: p.ts, url: normalizeUrl(p.url), dwell_sec: p.dwell_sec }))
    .slice(0, limit);
}

function humanDuration(a, b) {
  const s = Math.max(0, Math.round((new Date(b) - new Date(a)) / 1000));
  return s < 60 ? `${s} ثانیه` : `${Math.round(s / 60)} دقیقه`;
}

function fmt(ts) {
  const d = new Date(ts);
  if (isNaN(+d)) return ts || "";
  const pad = n => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function humanizeRange(startISO, endISO) {
  if (!startISO || !endISO) return "—";
  const s = new Date(startISO), e = new Date(endISO);
  const secs = Math.max(0, Math.floor((e - s) / 1000));
  if (secs < 60) return `${secs} ثانیه`;
  return `${Math.round(secs / 60)} دقیقه`;
}
export function classifyCRMState(contactExists, deals = []) {
  // 1) کانتکت نیست ⇒ no_crm
  if (!contactExists) return { guardKey: 'no_crm', reason: 'no_contact' };

  // 2) کانتکت هست ولی هیچ Deal ندارد ⇒ no_crm
  if (!deals || deals.length === 0) {
    return { guardKey: 'no_crm', reason: 'no_deal' };
  }

  // 3) اگر حتی یکی از Dealها باز باشد ⇒ free
  const hasActive = deals.some(d => {
    const st = String(d?.Status || '').trim();
    return !['Lost', 'Won'].includes(st) && !d?.IsDeleted;
  });
  if (hasActive) return { guardKey: 'free', reason: 'active_deal' };

  // 4) در غیر این صورت فقط دیل بسته دارد ⇒ lost
  return { guardKey: 'lost', reason: 'only_closed_deals' };
}




// ===== غنی‌سازی از دیدار با فانکشن‌های موجود =====
export async function enrichFromDidarByMobile(mobile) {
  let didar_contact_id = null;
  let contact_name = null;

  // 1) contactId
  for (const ph of phoneCandidates(mobile)) {
    didar_contact_id = await findContact(ph);
    if (didar_contact_id) {
      // نام مخاطب
      const res = await searchContact({ MobilePhone: ph });
      contact_name = res?.List?.[0]?.DisplayName || res?.List?.[0]?.FirstName || null;
      break;
    }
  }

  if (!didar_contact_id) {
        const crm_state = classifyCRMState(false, []);
        return {
      didar_contact_id: null,
      contact_name: null,
      deals_json: "[]",
      destination: null,
      next_followup_at: null,
      last_note: null,
      latest_stage: null,
      latest_status: null,
     crm_state,
      guardKey: crm_state.guardKey,
    };
  }

  // 2) همه معاملات
  const deals = (await checkDealExists(didar_contact_id)) || [];

  // مرتب بر اساس RegisterTime (جدیدترین اول)
  const sorted = deals
    .slice()
    .sort((a, b) => new Date(b.RegisterTime || 0) - new Date(a.RegisterTime || 0));

  // برای سرعت، فقط 3 مورد آخر را باز می‌کنیم
  const top = sorted.slice(0, 3);

  // 3) جزئیات هر معامله
  const details = (await Promise.all(top.map(d => getDealDetailById(d.Id)))).filter(Boolean);

  const deals_slim = details.map(d => {
    const f = d.Fields || {};
    const { last_note, next_followup_at, last_activity_at } = parseActivities(d.Activities || []);
    return {
      id: d.Id,
      title: d.Title,
      status: d.Status || null,
      stage: d.PipelineStageTitle || d.StageTitle || d?.PipelineStage?.Title || null,
      price: d.Price ?? null,

      // ←← فیلدهای سفارشی
      destination: f["Field_8783_0_1473"] || null,
      pax_count: f["Field_8783_12_25"] ?? null,
      hotel_class: (f["Field_8783_9_13"] || []).join("، "),
      transport: f["Field_8783_0_1475"] || null,
      trip_type: f["Field_8783_4_32"] || null,
      group_type: f["Field_8783_4_33"] || null,
      owner_name: d?.Owner?.DisplayName || null,

      // اکتیویتی‌ها
      last_note,
      next_followup_at,
      last_activity_at,
      register_time: d.RegisterTime || null,
    };
  });

// طبقه‌بندی وضعیت CRM (کانتکت هست، لیست دیل‌های بازخوانی شده داریم)
  const crm_state = classifyCRMState(true, details);
  const latest = pickLast(deals_slim); // جدیدترین

  return {
    didar_contact_id,
    contact_name,
    deals_json: JSON.stringify(deals_slim),
    destination: latest?.destination || null,
    next_followup_at: latest?.next_followup_at || null,
    last_note: latest?.last_note || null,
    latest_stage: latest?.stage || null,
    latest_status: latest?.status || null,
    crm_state,
    guardKey: crm_state.guardKey,
  };
}

// ===== سناریو نویسی قطعی (بدون AI) مطابق خواسته‌ها =====
function faDealStatus(status) {
  const key = String(status || '').trim().toLowerCase();
  const map = {
    pending: 'درجریان',
    lost: 'ناموفق',
    won: 'موفق',
  };
  return map[key] || status || null;
}

export function composeScenario({ profile, didar, engagedPages }) {
  const lines = [];
  const name = profile.contact_name || "نامشخص";

  // lines.push(`سناریوی لید «${name}» (${profile.mobile})`);
  // lines.push(`————————————————`);
  lines.push(`خلاصه لید: ${name} (${profile.mobile})`);
  // lines.push(`حضور در سایت: از ${profile.first_seen} تا ${profile.last_seen}، حدود ${humanDuration(profile.first_seen, profile.last_seen)}.`);
  lines.push(`حضور در سایت: حدود ${humanDuration(profile.first_seen, profile.last_seen)}.`);

  if (engagedPages?.length) {
    lines.push(`صفحات درگیر:`);
    engagedPages.forEach((p, i) => lines.push(`${i + 1}. ${p.url}`));
  }

  if (didar?.didar_contact_id) {
    const ds = Array.isArray(didar.deals_slim)
      ? didar.deals_slim
      : safeParseJson(didar.deals_json, []);
    const d0 = ds[0] || {};

    const statusFa = faDealStatus(d0.status);
    if (statusFa) lines.push(`وضعیت:   ${statusFa}`);
    if (d0.destination) lines.push(`مقصد:   ${d0.destination}`);
    if (d0.transport) lines.push(`حمل‌ونقل:   ${d0.transport}`);
    if (d0.pax_count != null) lines.push(`نفرات:   ${d0.pax_count}`);
    if (d0.trip_type) lines.push(`نوع سفر:   ${d0.trip_type}`);
    if (d0.group_type) lines.push(`گروه:   ${d0.group_type}`);
    if (d0.hotel_class) lines.push(`کلاس هتل:   ${d0.hotel_class}`);
    if (d0.owner_name) lines.push(`مسؤول:   ${d0.owner_name}`);
    if (didar.next_followup_at) lines.push(`پیگیری بعدی:   ${didar.next_followup_at}`);
    if (didar.last_note) lines.push(`آخرین یادداشت:   ${didar.last_note}`);
  }


  return lines.join("\n");
}



// ====== Helpers ======
function uaDeviceType(ua=""){
  const s = String(ua).toLowerCase();
  if (!s) return null;
  if (s.includes("tablet") || s.includes("ipad")) return "tablet";
  if (s.includes("mobi")) return "mobile";
  if (s.includes("bot") || s.includes("crawl") || s.includes("spider")) return "bot";
  return "desktop";
}
function uaBrowser(ua=""){
  const s = String(ua).toLowerCase();
  if (!s) return null;
  if (s.includes("edg/") || s.includes("edge")) return "edge";
  if (s.includes("chrome") && !s.includes("chromium") && !s.includes("edg/")) return "chrome";
  if (s.includes("safari") && !s.includes("chrome")) return "safari";
  if (s.includes("firefox")) return "firefox";
  if (s.includes("opr/") || s.includes("opera")) return "opera";
  if (s.includes("chromium")) return "chromium";
  return null;
}
function uaOS(ua=""){
  const s = String(ua).toLowerCase();
  if (!s) return null;
  if (s.includes("windows nt 10")) return "Windows 10/11";
  if (s.includes("windows nt")) return "Windows";
  if (s.includes("android")) return "Android";
  if (s.includes("iphone") || s.includes("ipad") || s.includes("ios")) return "iOS";
  if (s.includes("mac os x") || s.includes("macintosh")) return "macOS";
  if (s.includes("linux")) return "Linux";
  return null;
}
function pickMode(values=[]) {
  const map = new Map();
  for (const v of values) {
    const t = (v===undefined || v===null) ? null : String(v).trim();
    if (!t) continue;
    map.set(t, (map.get(t)||0)+1);
  }
  let best=null, cnt=0;
  for (const [k,c] of map) if (c>cnt) {best=k; cnt=c;}
  return best || null;
}


function normalizeMsisdn(raw = "") {
  let s = String(raw).replace(/[^\d]/g, "");
  if (!s) return "";
  if (s.startsWith("0098")) s = s.slice(2);     // 0098XXXXXXXXXX -> 98XXXXXXXXXX
  if (s.startsWith("09")) s = "98" + s.slice(1);// 09XXXXXXXXX     -> 98XXXXXXXXXX
  if (s.startsWith("0")) s = "98" + s.slice(1); // 0XXXXXXXXXX     -> 98XXXXXXXXXX
  if (!s.startsWith("98") && /^\d{10,12}$/.test(s)) s = "98" + s; // fallback
  return s;
}
function isIranMobile(m = "") { return /^98(9\d{9})$/.test(m); }
function nowTeh() { return moment().tz(TZ).format("YYYY-MM-DD HH:mm:ss"); }
function escapeSqlitePath(p) { return String(p).replace(/'/g, "''"); }

function toIsoSmart(x) {
  if (x === null || x === undefined) return null;
  const s = String(x).trim();
  if (!s) return null;
  if (/^\d{10}$/.test(s)) return moment.unix(Number(s)).toISOString(); // seconds
  if (/^\d{13}$/.test(s)) return moment(Number(s)).toISOString();      // millis
  const m = moment(s);
  return m.isValid() ? m.toISOString() : null;
}
function toTehDateTime(x) {
  const iso = toIsoSmart(x);
  return iso ? moment(iso).tz(TZ).format("YYYY-MM-DD HH:mm:ss") : null;
}


// WhatsApp: send long text as chunks
async function sendWhatsAppText(toMsisdn, text) {
  const chunks = chunkText(text, 3500);
  const svc = globalThis.CONFIG?.waService || globalThis.waService;

  if (!svc?.sendMessage) {
    console.warn('⚠️ WA send skipped: no service available.', { to: toMsisdn, chunks: chunks.length });
    return { sent: false, chunks: chunks.length };
  }

  console.log(`📦 WhatsApp chunking: ${chunks.length} part(s) for ${toMsisdn}`);
  for (let i = 0; i < chunks.length; i++) {
    console.log(`📤 WhatsApp send part ${i + 1}/${chunks.length} → ${toMsisdn} (len=${chunks[i].length})`);
    const res = await svc.sendMessage(toMsisdn, chunks[i]);
    console.log('✅ UltraMSG response:', res);
  }
  return { sent: true, chunks: chunks.length };
}


function chunkText(s, max = 3500) {
  const out = [];
  let i = 0;
  while (i < s.length) {
    let j = Math.min(i + max, s.length);
    if (j < s.length) {
      const lastNl = s.lastIndexOf("\n", j);
      if (lastNl > i + 500) j = lastNl; // break at a line boundary if possible
    }
    out.push(s.slice(i, j));
    i = j;
  }
  return out;
}


// ====== Send-guard helpers ======
function normStatus(s) {
  const t = String(s || '').trim().toLowerCase();
  if (t.includes('lost') || t.includes('ناموفق') || t.includes('لغو')) return 'lost';
  if (t.includes('won') || t.includes('موفق')) return 'won';
  if (t.includes('pending') || t.includes('درجریان') || t.includes('در جریان') || t.includes('open')) return 'pending';
  return null;
}





// ====== DB bootstrap ======
console.time("⏱ DB init");

 let db;
 
 try {
   db = new Database(MAIN_DB, {
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
try {
  db.exec(`ATTACH DATABASE '${escapeSqlitePath(ARCH_DB)}' AS arch;`);
  console.log("🧩 archive DB attached as 'arch'.");
} catch (e) {
  console.error("❌ Failed to ATTACH archive DB:", e?.message || e);
  throw e;
}
console.timeEnd("⏱ DB init");

// ====== Schema (output table) ======
console.log("🛠 Ensuring output table person_unified_profile exists…");

db.exec(`
  CREATE TABLE IF NOT EXISTS person_unified_profile (
    mobile TEXT PRIMARY KEY,
    last_visitor_id TEXT,
    contact_name TEXT,
    didar_contact_id TEXT,
    instagram_id TEXT,
    first_seen TEXT,
    last_seen TEXT,
    sessions INTEGER,
    pages_viewed INTEGER,
    total_dwell_sec INTEGER,
    whatsapp_inbound_count INTEGER,
    last_whatsapp_text TEXT,
    last_whatsapp_at TEXT,
    pdf_sent_count INTEGER,
    last_pdf_title TEXT,
    last_pdf_at TEXT,
    deals_json TEXT,
    sample_pages_json TEXT,
    scenario_text TEXT,
    scenario_model TEXT,
    scenario_sent_at TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
  );
  `);
// ——— ensure new columns exist ———
(function ensureProfileExtraCols(){
  const cols = pragmaTableInfo("main.person_unified_profile").map(c => c.name);
  const needed = [
    ["device_type","TEXT"],
    ["browser","TEXT"],
    ["os","TEXT"],
    ["country","TEXT"],
    ["region","TEXT"],
    ["city","TEXT"]
  ];
  for (const [name, type] of needed) {
    if (!cols.includes(name)) {
      try {
        db.exec(`ALTER TABLE person_unified_profile ADD COLUMN ${name} ${type};`);
        console.log(`➕ Added column person_unified_profile.${name}`);
      } catch (e) {
        console.warn(`⚠️ add column ${name} failed (maybe exists):`, e?.message || e);
      }
    }
  }
})();

// ——— بعد از CREATE TABLE person_unified_profile ... ———
db.exec(`
  CREATE TABLE IF NOT EXISTS scenario_send_guard (
    mobile        TEXT NOT NULL,
    status        TEXT NOT NULL,     -- 'won' | 'lost' | 'no_crm'
    first_sent_at TEXT DEFAULT (datetime('now','+03:30')),
    last_sent_at  TEXT DEFAULT (datetime('now','+03:30')),
    send_count    INTEGER DEFAULT 1,
    PRIMARY KEY (mobile, status)
  );
  CREATE INDEX IF NOT EXISTS idx_ssg_status ON scenario_send_guard(status);
`);
console.log("🛡️ Guard table ready.");

const guardCheckStmt = db.prepare(`
  SELECT 1 FROM scenario_send_guard WHERE mobile = ? AND status = ?
`);
const guardUpsertStmt = db.prepare(`
  INSERT INTO scenario_send_guard (mobile, status)
  VALUES (?, ?)
  ON CONFLICT(mobile, status) DO UPDATE SET
    last_sent_at = datetime('now','+03:30'),
    send_count   = scenario_send_guard.send_count + 1
`);


const upsertStmt = db.prepare(`
INSERT INTO person_unified_profile (
  mobile, last_visitor_id, contact_name, didar_contact_id, instagram_id,
  first_seen, last_seen, sessions, pages_viewed, total_dwell_sec,
  whatsapp_inbound_count, last_whatsapp_text, last_whatsapp_at,
  pdf_sent_count, last_pdf_title, last_pdf_at,
  deals_json, sample_pages_json, scenario_text, scenario_model, scenario_sent_at,
  device_type, browser, os, country, region, city,
  updated_at
) VALUES (
  @mobile, @last_visitor_id, @contact_name, @didar_contact_id, @instagram_id,
  @first_seen, @last_seen, @sessions, @pages_viewed, @total_dwell_sec,
  @whatsapp_inbound_count, @last_whatsapp_text, @last_whatsapp_at,
  @pdf_sent_count, @last_pdf_title, @last_pdf_at,
  @deals_json, @sample_pages_json, @scenario_text, @scenario_model, @scenario_sent_at,
  @device_type, @browser, @os, @country, @region, @city,
  datetime('now')
)
ON CONFLICT(mobile) DO UPDATE SET
  last_visitor_id = COALESCE(excluded.last_visitor_id, person_unified_profile.last_visitor_id),
  contact_name = COALESCE(excluded.contact_name, person_unified_profile.contact_name),
  didar_contact_id = COALESCE(excluded.didar_contact_id, person_unified_profile.didar_contact_id),
  instagram_id = COALESCE(excluded.instagram_id, person_unified_profile.instagram_id),
  first_seen = COALESCE(person_unified_profile.first_seen, excluded.first_seen),
  last_seen = COALESCE(excluded.last_seen, person_unified_profile.last_seen),
  sessions = COALESCE(excluded.sessions, person_unified_profile.sessions),
  pages_viewed = COALESCE(excluded.pages_viewed, person_unified_profile.pages_viewed),
  total_dwell_sec = COALESCE(excluded.total_dwell_sec, person_unified_profile.total_dwell_sec),
  whatsapp_inbound_count = COALESCE(excluded.whatsapp_inbound_count, person_unified_profile.whatsapp_inbound_count),
  last_whatsapp_text = COALESCE(excluded.last_whatsapp_text, person_unified_profile.last_whatsapp_text),
  last_whatsapp_at = COALESCE(excluded.last_whatsapp_at, person_unified_profile.last_whatsapp_at),
  pdf_sent_count = COALESCE(excluded.pdf_sent_count, person_unified_profile.pdf_sent_count),
  last_pdf_title = COALESCE(excluded.last_pdf_title, person_unified_profile.last_pdf_title),
  last_pdf_at = COALESCE(excluded.last_pdf_at, person_unified_profile.last_pdf_at),
  deals_json = COALESCE(excluded.deals_json, person_unified_profile.deals_json),
  sample_pages_json = COALESCE(excluded.sample_pages_json, person_unified_profile.sample_pages_json),
  scenario_text = COALESCE(excluded.scenario_text, person_unified_profile.scenario_text),
  scenario_model = COALESCE(excluded.scenario_model, person_unified_profile.scenario_model),
  scenario_sent_at = COALESCE(excluded.scenario_sent_at, person_unified_profile.scenario_sent_at),

  device_type = COALESCE(excluded.device_type, person_unified_profile.device_type),
  browser = COALESCE(excluded.browser, person_unified_profile.browser),
  os = COALESCE(excluded.os, person_unified_profile.os),
  country = COALESCE(excluded.country, person_unified_profile.country),
  region = COALESCE(excluded.region, person_unified_profile.region),
  city = COALESCE(excluded.city, person_unified_profile.city),

  updated_at = datetime('now');

`);
console.log("🧷 UPSERT statement prepared.");

// ====== dynamic columns helpers ======
function pragmaTableInfo(dbDotTable) {
  const [dbName, tbl] = dbDotTable.split(".");
  return db.prepare(`PRAGMA ${dbName}.table_info(${tbl});`).all();
}
function pick(cols, candidates, fallback = null) {
  for (const c of candidates) if (cols.includes(c)) return c;
  return fallback; // اگر fallback=null بود یعنی پیدا نکردیم
}


// ====== latest weekly table in arch ======
function latestWeeklyTable() {
  const row = db.prepare(`
    SELECT name FROM arch.sqlite_master
    WHERE type='table' AND name LIKE 'journey_events_w%'
    ORDER BY name DESC LIMIT 1
  `).get();
  if (!row?.name) {
    console.error("❌ No weekly table found in archive DB (journey_events_w%).");
    throw new Error("journey_events weekly table not found in archive");
  }
  return row.name;
}
// ——— Helpers: robust, null-safe ———
function extractMsisdnFromJid(jid = '') {
  // نمونه‌ها: "989144097260@c.us" یا "false_989144097260@c.us_3A0366..."
  // اولین عدد متوالی قبل از "@c.us" را می‌گیرد
  const s = String(jid || '');
  const m = s.match(/(\d{9,15})@c\.us/i);
  return m ? m[1] : null;
}

function normalizeMobile(mobile = '', ffrom = '', tto = '') {
  // 1) اگر mobile موجود بود، تمیز و نرمال کن
  let m = String(mobile || '').replace(/[^\d]/g, '');
  if (!m) {
    // 2) از ffrom یا tto استخراج کن
    m = extractMsisdnFromJid(ffrom) || extractMsisdnFromJid(tto) || '';
  }
  if (!m) return null;

  // 3) نرمال به فرمت 98xxxxxxxxxx
  if (m.startsWith('0098')) m = m.slice(2);
  if (m.startsWith('0')) m = '98' + m.slice(1);
  else if (!m.startsWith('98')) {
    // اگر 10 رقمی و با 9 شروع شد، ایران فرض کن
    if (m.length === 10 && m.startsWith('9')) m = '98' + m;
    // در غیر اینصورت همانی که هست (برای کشورهای دیگر)
  }

  // حداقل ولیدیشن
  if (!/^\d{10,15}$/.test(m)) return null;
  return m;
}

// ——— بک‌فیل موبایل‌ها در whatsapp_new_msg ———
function fixMobilesInWhatsapp() {
  console.log('🔧 Normalizing whatsapp_new_msg.mobile ...');

  const rows = db.prepare(`
    SELECT msg_id, mobile, ffrom, tto
    FROM whatsapp_new_msg
  `).all();
  const update = db.prepare(`UPDATE whatsapp_new_msg SET mobile = ? WHERE msg_id = ?`);

  let updated = 0, skipped = 0;
  for (const r of rows) {
    const norm = normalizeMobile(r.mobile, r.ffrom, r.tto);
    if (!norm) { skipped++; continue; }
    if (norm !== r.mobile) {
      update.run(norm, r.msg_id);
      updated++;
    }
  }
  console.log(`✅ Mobile normalization done: updated=${updated}, skipped=${skipped}, total=${rows.length}`);
}


// ====== collect maps (whatsapp, pdf, instagram) keyed by mobile ======
function collectWhatsappInbound() {
  console.time("⏱ collectWhatsappInbound");
  const cols = pragmaTableInfo("main.whatsapp_new_msg").map(c => c.name);
  console.log("ℹ️ whatsapp_new_msg columns:", cols);

  const colMobile = pick(cols, ["mobile"], null);
  const colFrom = pick(cols, ["ffrom", "from", "sender"], null);
  const colBody = pick(cols, ["body", "message", "text"], null);
  const colFromMe = pick(cols, ["fromMe", "from_me", "is_outbound"], "fromMe");
  // created_at را به ttime ترجیح بده اگر بود
  const colTime = pick(cols, ["created_at", "ttime", "timestamp", "time", "ts"], null);

  if (!colFromMe || !(colMobile || colFrom) || !colTime) {
    console.warn("⚠️ Missing critical WA columns. Skipping inbound map.", { colMobile, colFrom, colTime, colFromMe });
    console.timeEnd("⏱ collectWhatsappInbound");
    return new Map();
  }

  const selectMsisdn = colMobile ? colMobile : colFrom;
  const selectBody = colBody ? `, ${colBody} AS body` : `, NULL AS body`;

  const rows = db.prepare(`
    SELECT ${selectMsisdn} AS msisdn, ${colTime} AS ttime ${selectBody}
    FROM main.whatsapp_new_msg
    WHERE ${colFromMe} = 0 AND ${selectMsisdn} IS NOT NULL
  `).all();

  const map = new Map();
  let invalid = 0;
  for (const r of rows) {
    const mob = normalizeMsisdn(r.msisdn);
    if (!isIranMobile(mob)) { invalid++; continue; }
    const slot = map.get(mob) || { count: 0, last_text: null, last_at: null };
    slot.count += 1;

    const atIso = toIsoSmart(r.ttime); // 10/13-digit unix or ISO
    if (!slot.last_at || (atIso && atIso > slot.last_at)) {
      slot.last_at = atIso;
      slot.last_text = r.body ? String(r.body).slice(0, 1000) : null;
    }
    map.set(mob, slot);
  }
  console.timeEnd("⏱ collectWhatsappInbound");
  console.log("📥 WhatsApp inbound collected:", {
    raw_rows: rows.length,
    unique_mobiles: map.size,
    invalid_mobiles: invalid
  });
  return map;
}

function collectPdfDispatch() {
  console.time("⏱ collectPdfDispatch");
  const logCols = (tbl) => { const c = pragmaTableInfo(tbl).map(x => x.name); console.log(`ℹ️ ${tbl} columns:`, c); return c; };

  const pdfLogCols = logCols("main.wa_pdf_dispatch_log");
  if (!pdfLogCols.length) {
    console.warn("⚠️ Table wa_pdf_dispatch_log not found. Skipping PDF map.");
    console.timeEnd("⏱ collectPdfDispatch");
    return new Map();
  }
  const hasContactId = pdfLogCols.includes("contact_id");
  const hasCreatedAt = pdfLogCols.includes("created_at");
  const hasPdfId = pdfLogCols.includes("pdf_id");

  const contactsCols = logCols("main.contacts");
  const hasPhoneE164 = contactsCols.includes("phone_e164");

  const pdfTCols = logCols("main.pdf_templates");
  const hasPdfTitle = pdfTCols.includes("title");

  if (!hasContactId || !hasPhoneE164) {
    console.warn("⚠️ Missing contact linkage (contact_id or phone_e164). Skipping PDF map.");
    console.timeEnd("⏱ collectPdfDispatch");
    return new Map();
  }

  // title از pdf_templates اگر بود
  const selectTitle = (hasPdfId && hasPdfTitle) ? `, p.title AS title` : `, NULL AS title`;
  const leftJoinPdf = (hasPdfId && hasPdfTitle) ? `LEFT JOIN main.pdf_templates p ON p.id = l.pdf_id` : ``;
  const selectAt = hasCreatedAt ? `l.created_at` : `NULL`;

  // شماره تماس را بدون + می‌گیریم تا با نرمالایز خودمان سازگار شود
  const rows = db.prepare(`
    SELECT REPLACE(c.phone_e164, '+','') AS mobile, ${selectAt} AS at ${selectTitle}
    FROM main.wa_pdf_dispatch_log l
    JOIN main.contacts c ON c.id = l.contact_id
    ${leftJoinPdf}
    WHERE c.phone_e164 IS NOT NULL
  `).all();

  const map = new Map();
  let invalid = 0;
  for (const r of rows) {
    const mob = normalizeMsisdn(r.mobile);
    if (!isIranMobile(mob)) { invalid++; continue; }
    const slot = map.get(mob) || { count: 0, last_title: null, last_at: null };
    slot.count += 1;

    const atIso = toIsoSmart(r.at);
    if (!slot.last_at || (atIso && atIso > slot.last_at)) {
      slot.last_at = atIso;
      slot.last_title = r.title ? String(r.title) : null;
    }
    map.set(mob, slot);
  }
  console.timeEnd("⏱ collectPdfDispatch");
  console.log("📚 PDF dispatch collected:", {
    raw_rows: rows.length,
    unique_mobiles: map.size,
    invalid_mobiles: invalid
  });
  return map;
}

function collectInstagram() {
  console.time("⏱ collectInstagram");
  const cols = pragmaTableInfo("main.atigh_instagram_dev").map(c => c.name);
  console.log("ℹ️ atigh_instagram_dev columns:", cols);

  const colMobile = pick(cols, ["mobile"], null);
  const colInsta = pick(cols, ["instagram_id", "ig_id", "insta_id"], null);

  if (!colMobile || !colInsta) {
    console.warn("⚠️ Instagram columns missing. Skipping IG map.", { colMobile, colInsta });
    console.timeEnd("⏱ collectInstagram");
    return new Map();
  }

  const rows = db.prepare(`
    SELECT ${colMobile} AS mobile, ${colInsta} AS instagram_id
    FROM main.atigh_instagram_dev
    WHERE ${colMobile} IS NOT NULL AND ${colInsta} IS NOT NULL
  `).all();

  const map = new Map();
  let invalid = 0;
  for (const r of rows) {
    const mob = normalizeMsisdn(r.mobile);
    if (!isIranMobile(mob)) { invalid++; continue; }
    map.set(mob, String(r.instagram_id));
  }
  console.timeEnd("⏱ collectInstagram");
  console.log("📷 Instagram map collected:", {
    raw_rows: rows.length,
    unique_mobiles: map.size,
    invalid_mobiles: invalid
  });
  return map;
}



// ====== journey aggregation per visitor_id from weekly table ======
function aggregateJourneyForVisitor(weeklyTbl, visitorId) {
  const fq = `arch.${weeklyTbl}`;
  const cols = pragmaTableInfo(fq).map(c => c.name);

  const colVisitor = pick(cols, ["visitor_id", "visitor", "vid"], "visitor_id");
  const colTime = pick(cols, ["event_time", "created_at", "timestamp", "ts", "time", "start_time"], "created_at");
  const colUrl = pick(cols, ["url", "page_url", "path", "location", "page"], null);
  const colDwell = pick(cols, ["dwell_sec", "duration_sec", "duration", "stay_seconds", "engagement_time_sec"], null);
  const colSession = pick(cols, ["session_id", "visit_id", "session"], null);

  // candidates for device/UA/geo
  const colUA = pick(cols, ["user_agent","ua","useragent"], null);
  const colDevice = pick(cols, ["device_type","device","deviceCategory"], null);
  const colBrowser = pick(cols, ["browser","ua_browser"], null);
  const colOS = pick(cols, ["os","os_name","ua_os"], null);

  const colCountry = pick(cols, ["country","geo_country","country_name","country_code"], null);
  const colRegion  = pick(cols, ["region","state","subdivision","geo_region"], null);
  const colCity    = pick(cols, ["city","geo_city","locality"], null);

  const selectFields =
    `j.${colTime} AS ts` +
    (colUrl ? `, j.${colUrl} AS url` : ``) +
    (colDwell ? `, COALESCE(j.${colDwell},0) AS dwell` : `, 0 AS dwell`) +
    (colSession ? `, j.${colSession} AS session_id` : `, NULL AS session_id`) +
    (colUA ? `, j.${colUA} AS ua` : `, NULL AS ua`) +
    (colDevice ? `, j.${colDevice} AS device_type` : `, NULL AS device_type`) +
    (colBrowser ? `, j.${colBrowser} AS browser` : `, NULL AS browser`) +
    (colOS ? `, j.${colOS} AS os` : `, NULL AS os`) +
    (colCountry ? `, j.${colCountry} AS country` : `, NULL AS country`) +
    (colRegion ? `, j.${colRegion} AS region` : `, NULL AS region`) +
    (colCity ? `, j.${colCity} AS city` : `, NULL AS city`);

  const rows = db.prepare(`
    SELECT ${selectFields}
    FROM ${fq} j
    WHERE j.${colVisitor} = ?
  `).all(visitorId);

  if (!rows.length) {
    return {
      first_seen: null, last_seen: null,
      pages_viewed: 0, total_dwell_sec: 0,
      sessions: 0, sample_pages: [],
      device_type: null, browser: null, os: null,
      country: null, region: null, city: null
    };
  }

  let first_seen = null, last_seen = null, total = 0;
  const sessions = new Set();
  const sample_pages = [];

  // buckets for mode
  const deviceList = [], browserList = [], osList = [];
  const countryList = [], regionList = [], cityList = [];
  const uaList = [];

  for (const r of rows) {
    const tsTeh = toTehDateTime(r.ts);
    if (tsTeh) {
      if (!first_seen || tsTeh < first_seen) first_seen = tsTeh;
      if (!last_seen || tsTeh > last_seen) last_seen = tsTeh;
    }
    total += Number(r.dwell || 0);
    if (r.session_id) sessions.add(String(r.session_id));
    if (colUrl && r.url && Number(r.dwell || 0) > 0 && sample_pages.length < 50) {
      sample_pages.push({ ts: tsTeh || r.ts, url: stripUrl(String(r.url)), dwell_sec: Number(r.dwell || 0) });
    }

    if (r.device_type) deviceList.push(r.device_type);
    if (r.browser) browserList.push(r.browser);
    if (r.os) osList.push(r.os);
    if (r.country) countryList.push(r.country);
    if (r.region) regionList.push(r.region);
    if (r.city) cityList.push(r.city);
    if (r.ua) uaList.push(r.ua);
  }

  // derive from UA if missing
  let device_mode = pickMode(deviceList);
  let browser_mode = pickMode(browserList);
  let os_mode = pickMode(osList);

  if (!device_mode || !browser_mode || !os_mode) {
    const ua = pickMode(uaList);
    if (ua) {
      if (!device_mode) device_mode = uaDeviceType(ua);
      if (!browser_mode) browser_mode = uaBrowser(ua);
      if (!os_mode) os_mode = uaOS(ua);
    }
  }

  return {
    first_seen, last_seen,
    pages_viewed: rows.length,
    total_dwell_sec: total,
    sessions: sessions.size,
    sample_pages,
    device_type: device_mode || null,
    browser: browser_mode || null,
    os: os_mode || null,
    country: pickMode(countryList) || null,
    region: pickMode(regionList) || null,
    city: pickMode(cityList) || null
  };
}



// —— enrichFromDidar: موبایل → کانتکت → لیست دیل‌ها → اسلیم
export async function enrichFromDidar(mobile) {
  // نرمال‌سازی ساده شماره (اگر قبلاً داری، از همون استفاده کن)
  const candidates = [
    mobile.replace(/[^\d]/g, ""),
    ("0" + mobile.replace(/[^\d]/g, "").replace(/^0|^98/, "")),
    ("98" + mobile.replace(/[^\d]/g, "").replace(/^0|^98/, "")),
  ];

  let contactId = null, contact_name = null;
  for (const ph of candidates) {
    contactId = await findContact(ph);
    if (contactId) {
      const res = await searchContact({ MobilePhone: ph });
      contact_name = res?.List?.[0]?.DisplayName || res?.List?.[0]?.FirstName || null;
      break;
    }
  }

  if (!contactId) {
    const crm_state = classifyCRMState(false, []);
    return {
      didar_contact_id: null,
      contact_name: null,
      deals_json: "[]",
      last_note: null,
      next_followup_at: null,
      destination: null,
      crm_state,
      guardKey: crm_state.guardKey,
    };
  }

  const rawDeals = await checkDealExists(contactId) || [];
  // جدیدترین اول
  rawDeals.sort((a, b) => new Date(b.RegisterTime || 0) - new Date(a.RegisterTime || 0));

  const deals_slim = rawDeals.map(mapDealLite);

  // اگر آخرین نوت هم لازم داری:
  let last_note = null;
  if (deals_slim[0]) {
    const det = await getDealDetailById(deals_slim[0].id);
    const acts = det?.Activities || [];
    const notes = acts
      .filter(a => a?.Note || a?.ResultNote)
      .sort((a, b) => new Date(b.DoneDate || b.CreateDate || 0) - new Date(a.DoneDate || a.CreateDate || 0));
    last_note = notes.length ? (notes[0].Note || notes[0].ResultNote) : null;
  }

  // مقادیر خلاصه از جدیدترین دیل
  const latest = deals_slim[0] || {};

  
  // طبقه‌بندی وضعیت CRM:
  // توجه: برای تصمیم «no_crm وقتی دیلی نیست» کافی‌ست همین rawDeals را بررسی کنیم.
  // اگر Status داخل rawDeals وجود داشت از همان استفاده می‌شود؛ نبود هم مهم نیست،
  // چون صرفِ خالی بودن آرایه باعث no_crm می‌شود.
  const crm_state = classifyCRMState(true, rawDeals);

  return {
    didar_contact_id: contactId || null,
    contact_name,
    deals_slim,                                   // همیشه آرایه
    deals_json: JSON.stringify(deals_slim || []), // همیشه رشته معتبر JSON
    destination: latest.destination || null,
    next_followup_at: latest.next_followup_at ? fmtTeh(latest.next_followup_at) : null,
    last_note,
    crm_state,
    guardKey: crm_state.guardKey,
  };

}


// ====== OpenAI scenario ======
const openai = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;




// ====== MAIN ======
export async function runAllVisitorScenarios() {
  console.log("🚀 Starting full pipeline…");
  
  fixMobilesInWhatsapp();
  const weekly = latestWeeklyTable();
  console.log(`🧭 Weekly table selected: arch.${weekly}`);

  // preload side-maps (keyed by mobile)
  const waMap = collectWhatsappInbound();
  const pdfMap = collectPdfDispatch();
  const igMap = collectInstagram();

  // iterate ALL visitor_contacts
  const vcCols = pragmaTableInfo("main.visitor_contacts").map(c => c.name);
  console.log("ℹ️ visitor_contacts columns:", vcCols);
  const colVid = pick(vcCols, ["visitor_id", "vid", "visitor"], "visitor_id");
  const colMob = pick(vcCols, ["mobile", "msisdn", "phone"], null);

  if (!colMob) {
    throw new Error(`visitor_contacts has no mobile column. Available columns: ${vcCols.join(", ")}`);
  }

  console.time("⏱ fetch visitor_contacts");
  const contacts = db.prepare(`
    SELECT ${colVid} AS visitor_id, ${colMob} AS mobile
    FROM main.visitor_contacts
    WHERE ${colMob} IS NOT NULL
  `).all();
  console.timeEnd("⏱ fetch visitor_contacts");

  console.log(`👥 visitor_contacts with mobile: ${contacts.length}`);

  if (!isIranMobile(WHATSAPP_OPERATOR)) {
    console.warn(`⚠️ WHATSAPP_OPERATOR (${WHATSAPP_OPERATOR}) does not look like an Iran mobile in 98xxxxxxxxxx format.`);
  }

  const limit = pLimit(CONCURRENCY);
  let processed = 0, sentOk = 0, sentSkip = 0, errors = 0;

  const tasks = contacts.map(row => limit(async () => {
    const rawMob = row.mobile;
    const mobile = normalizeMsisdn(rawMob);
    if (!isIranMobile(mobile)) {
      console.warn(`⚠️ Skipping invalid contact mobile:`, rawMob);
      return;
    }

    try {
      console.log(`\n— — — — —\n🧩 Process visitor: visitor_id=${row.visitor_id} mobile=${mobile}`);

      // journey aggregation for THIS visitor_id
      const j = aggregateJourneyForVisitor(weekly, row.visitor_id);
      console.log("📊 Journey summary:", {
        pages_viewed: j.pages_viewed,
        total_dwell_sec: j.total_dwell_sec,
        sessions: j.sessions,
        first_seen: j.first_seen,
        last_seen: j.last_seen,
        sample_pages_preview: (j.sample_pages || []).slice(0, 2)
      });

      // merges from side maps (by mobile)
      const w = waMap.get(mobile) || { count: 0, last_text: null, last_at: null };
      const p = pdfMap.get(mobile) || { count: 0, last_title: null, last_at: null };
      const instagram_id = igMap.get(mobile) || null;

      console.log("📨 Interactions:", {
        whatsapp_inbound_count: w.count,
        last_whatsapp_at: w.last_at,
        pdf_sent_count: p.count,
        last_pdf_at: p.last_at,
        instagram_id
      });

      // ——— DIDAR ———
      const didar = await enrichFromDidarByMobile(mobile);

      // پروفایل سبک برای سناریو
      const profile = {
        mobile,
        contact_name: didar?.contact_name || null,
        first_seen: j.first_seen,
        last_seen: j.last_seen,
        sessions: j.sessions,
        pages_viewed: j.pages_viewed,
        total_dwell_sec: j.total_dwell_sec,
      };

      // صفحات درگیر (بدون UTM و فقط صفحاتی که dwell>=5s)
      const engagedPages = compactPages(j.sample_pages || [], 8);

      // آرایه‌ی دیل‌ها را ایمن استخراج کن
      const dealsArr = Array.isArray(didar?.deals_slim)
        ? didar.deals_slim
        : safeParseJson(didar?.deals_json, []);

      // *** محاسبه‌ی guardKey مطابق منطق جدید ***
      const hasDidar = !!didar?.didar_contact_id;
      const hasDeals = dealsArr.length > 0;
      const hasActive = dealsArr.some(d => {
        const st = String(d?.status || '').trim();
        return st && !['Lost', 'Won'].includes(st);
      });

      // آخرین وضعیت برای موارد بدون اکتیو
      const latestRawStatus = didar?.latest_status || dealsArr?.[0]?.status || null;
      const latestStatus = normStatus(latestRawStatus); // 'lost' | 'won' | 'pending' | null

      let guardKey = null;
      if (!hasDidar) {
        guardKey = 'no_crm';
      } else if (!hasDeals) {                 // ←← کانتکت هست ولی دیل ندارد
        guardKey = 'no_crm';
      } else if (!hasActive) {
        // همه دیل‌ها بسته‌اند → guard بر اساس آخرین وضعیت
        if (latestStatus === 'lost' || latestStatus === 'won') {
          guardKey = latestStatus;
        } else {
          // اگر به هر دلیل نرمالایز نشد، به عنوان بسته در نظر بگیر
          guardKey = 'lost';
        }
      }
      // اگر hasActive=true ⇒ guardKey=null یعنی حالت «free» و محدودیتی برای ارسال نداریم

      // سناریوی متنی
      const scenario_text = composeScenario({
        profile,
        didar: {
          didar_contact_id: didar?.didar_contact_id || null,
          deals_slim: dealsArr,
          next_followup_at: didar?.next_followup_at || null,
          last_note: didar?.last_note || null,
          destination: didar?.destination || null,
          latest_stage: didar?.latest_stage || null,
          latest_status: didar?.latest_status || null,
        },
        engagedPages
      });

      const scenario_model = "deterministic-v1";
      const scenario_sent_at = nowTeh();
      console.log("🌐 Device/Geo:", {
        device_type: j.device_type, browser: j.browser, os: j.os,
        country: j.country, region: j.region, city: j.city
      });
      
      // Store (UPSERT) — scenario MUST be stored even in DRY_RUN
      const record = {
        mobile,
        last_visitor_id: row.visitor_id,
        contact_name: didar?.contact_name || null,
        didar_contact_id: didar?.didar_contact_id || null,
        instagram_id,
        first_seen: j.first_seen,
        last_seen: j.last_seen,
        sessions: j.sessions,
        pages_viewed: j.pages_viewed,
        total_dwell_sec: j.total_dwell_sec,
        whatsapp_inbound_count: w.count,
        last_whatsapp_text: w.last_text,
        last_whatsapp_at: w.last_at,
        pdf_sent_count: p.count,
        last_pdf_title: p.last_title,
        last_pdf_at: p.last_at,
        deals_json: didar?.deals_json || JSON.stringify(dealsArr || []),
        sample_pages_json: JSON.stringify(j.sample_pages || []),
        scenario_text,
        scenario_model,
        scenario_sent_at,
      
        // NEW:
        device_type: j.device_type || null,
        browser: j.browser || null,
        os: j.os || null,
        country: j.country || null,
        region: j.region || null,
        city: j.city || null,
      };
      
// ===== Cross-Silencer Filter =====
let silence = null;
try {
  silence = await shouldSilence(mobile);
  if (silence?.active) {
    console.log(`🤫 [CrossSilencer] ${mobile} muted until ${silence.until} (${silence.reason || 'no reason'})`);
  } else {
    console.log(`✅ [CrossSilencer] ${mobile} not muted, safe to send.`);
  }
} catch (e) {
  console.warn(`⚠️ [CrossSilencer] failed for ${mobile}:`, e?.message || e);
}

      // قفل ارسال بر اساس guardKey
      let allowSend = true;

      if (silence?.active) {
        allowSend = false;
        console.log(`🛑 [Silence] Skip send for ${mobile}, active until ${silence.until}`);
      } else if (guardKey) {
        const already = guardCheckStmt.get(mobile, guardKey);
        if (already) {
          allowSend = false;
          console.log(`⏭️ [Guard] Skip send for ${mobile}: guardKey=${guardKey} (already sent once).`);
        }
      }
      

      try {
        upsertStmt.run(record);
        console.log(`📝 Scenario saved for ${mobile} (len=${scenario_text.length})`);
      } catch (e) {
        errors++;
        console.error(`❌ Failed to UPSERT scenario for ${mobile}:`, e?.message || e);
      }

      // ارسال به اپراتور واتساپ
      if (!allowSend) {
        sentSkip++;
      } else if (!DRY_RUN) {
        try {
          if (silence?.active) {
            console.log(`🛑 [Silence] Skipped WhatsApp send for ${mobile}, still under silence window.`);
            sentSkip++;
            return;
          }
          
          await sendWhatsAppText(WHATSAPP_OPERATOR, scenario_text);
          console.log(`📨 Sent to operator ${WHATSAPP_OPERATOR} for ${mobile} (guardKey=${guardKey || 'free'})`);
          if (guardKey) {
            try { guardUpsertStmt.run(mobile, guardKey); }
            catch (e) { console.warn('⚠️ guard upsert failed:', e?.message || e); }
          }
          sentOk++;
        } catch (e) {
          errors++;
          console.error(`❌ WhatsApp send failed for ${mobile}:`, e?.message || e);
        }
      } else {
        console.log(`[DRY_RUN] Would send to operator ${WHATSAPP_OPERATOR} for ${mobile} (guardKey=${guardKey || 'free'})`);
        sentSkip++;
      }

      // *** شمارنده‌ی پردازش ***
      processed++;

    } catch (e) {
      errors++;
      console.error(`❌ Pipeline error for ${mobile}:`, e?.message || e);
    }
  }));

  console.time("⏱ run all visitors");
  await Promise.all(tasks);
  console.timeEnd("⏱ run all visitors");

  console.log("✅ DONE: all visitor_contacts processed.", {
    processed,
    sentOk,
    sentSkip,
    errors
  });
}

// ========================================================
// Self Scheduler (runs every day 08:00 Asia/Tehran)
// ========================================================
import cron from "node-cron";

function logNow(msg) {
  const now = moment().tz("Asia/Tehran").format("YYYY-MM-DD HH:mm:ss");
  console.log(`[Scheduler ${now}] ${msg}`);
}

// تابع اجراگر امن
async function runDaily() {
  try {
    logNow("🚀 Triggering build_and_send_all_visitor_scenarios() ...");
    await runAllVisitorScenarios();
    logNow("✅ Finished daily visitor scenarios run.");
  } catch (err) {
    logNow(`❌ Error: ${err?.message || err}`);
  }
}

// اجرای اولیه در صورت FORCE_RUN
if (process.env.FORCE_RUN === "1") {
  runDaily();
}

// تنظیم کران: هر روز ساعت ۸ صبح به وقت تهران
cron.schedule("0 8 * * *", runDaily, {
  timezone: "Asia/Tehran"
});

logNow("🕗 Daily cron scheduled for 08:00 Asia/Tehran.");


// Robust direct-run detection for ESM
// if (typeof process !== "undefined" && process.argv?.[1]) {
//   try {
//     const isDirect = (() => {
//       const thisPath = new URL(import.meta.url).pathname.replace(/\\/g, "/");
//       const argvPath = process.argv[1].replace(/\\/g, "/");
//       return thisPath.endsWith(argvPath);
//     })();
//     if (isDirect) {
//       runAllVisitorScenarios().catch(err => {
//         console.error("FATAL:", err?.response?.data || err?.message || err);
//         process.exit(1);
//       });
//     }
//   } catch {
//     // CommonJS fallback (if bundled differently)
//     // eslint-disable-next-line no-undef
//     if (typeof require !== "undefined" && require?.main === module) {
//       runAllVisitorScenarios().catch(err => {
//         console.error("FATAL:", err?.response?.data || err?.message || err);
//         process.exit(1);
//       });
//     }
//   }
// }

