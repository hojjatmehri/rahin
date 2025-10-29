import 'file:///E:/Projects/rahin/logger.js';
/**
 * wordpress_visitors.js
 * شمارش visitor_id یکتا برای یک تاریخ مشخص با منطق آرشیو:
 * - اگر date < امروزِ تهران → خواندن از db_archive (و در صورت وجود، union با db اصلی)
 * - اگر date == امروز → فقط db اصلی
 */

import Database from "better-sqlite3";

const DEBUG = true;
const TABLE_CANDIDATES = ["wp_user_identity","user_identity_log","rahin_visitors"];
const DATE_CANDIDATES  = ["created_at","createdAt","reg_date","regDate","date","ts","timestamp"];
const VID_CANDIDATES   = ["visitor_id","visitorId","visitor","vid"];

function logd(...a){ if (DEBUG) console.log("[visitors]", ...a); }

/** YYYY-MM-DD در Asia/Tehran */
export function todayTehranStr() {
  try {
    const s = new Date().toLocaleString("sv-SE", { timeZone: "Asia/Tehran", hour12: false });
    return s.slice(0, 10); // "YYYY-MM-DD"
  } catch {
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth()+1).padStart(2,"0");
    const day = String(d.getDate()).padStart(2,"0");
    return `${y}-${m}-${day}`;
  }
}

export function yesterdayTehranStr() {
  try {
    const now = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Tehran" }));
    const yst = new Date(+now - 24*60*60*1000);
    const y = yst.getFullYear();
    const m = String(yst.getMonth()+1).padStart(2,"0");
    const d = String(yst.getDate()).padStart(2,"0");
    return `${y}-${m}-${d}`;
  } catch {
    const d = new Date(Date.now() - 24*60*60*1000);
    const y = d.getFullYear();
    const m = String(d.getMonth()+1).padStart(2,"0");
    const day = String(d.getDate()).padStart(2,"0");
    return `${y}-${m}-${day}`;
  }
}

function tableExists(db, name) {
  const row = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name);
  return !!(row && row.name);
}

function getColumns(db, table) {
  return db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name);
}

function detectColumns(columns) {
  const dateCol = DATE_CANDIDATES.find(c => columns.includes(c)) || null;
  const vidCol  = VID_CANDIDATES.find(c => columns.includes(c))  || null;
  return { dateCol, vidCol };
}

function findFirstExistingTable(db) {
  for (const t of TABLE_CANDIDATES) {
    if (tableExists(db, t)) return t;
  }
  return null;
}

/** شمارش یکتای visitor_id در یک DB برای تاریخ داده‌شده (LIKE 'YYYY-MM-DD%') */
function countUniqueInSingleDB(db, dateStr) {
  logd("scanning single DB for", dateStr);
  const table = findFirstExistingTable(db);
  logd("chosen table =", table);
  if (!table) return { count: 0, vids: new Set() };

  const cols = getColumns(db, table);
  logd("columns =", cols);
  const { dateCol, vidCol } = detectColumns(cols);
  logd("detected dateCol =", dateCol, "vidCol =", vidCol);
  if (!dateCol || !vidCol) return { count: 0, vids: new Set() };

  const rows = db.prepare(`SELECT ${vidCol} AS vid FROM ${table} WHERE ${dateCol} LIKE ?`).all(`${dateStr}%`);
  const set = new Set();
  for (const r of rows) if (r?.vid != null) set.add(String(r.vid));
  logd("unique count =", set.size);
  return { count: set.size, vids: set };
}

/** منطق هوشمند آرشیو/اصلی */
export function countUniqueVisitorsSmart(dateStr, mainPath = "./db_atigh.sqlite", archivePath = "./db_archive.sqlite") {
  const today = todayTehranStr();
  logd("mode =", (dateStr < today ? "archive_mode" : "today_mode"), "| date =", dateStr, "| today =", today);

  const mainDB = new Database(mainPath);
  try {
    if (dateStr < today) {
      let unionSet = new Set();
      let archiveDone = false;
      try {
        const archiveDB = new Database(archivePath);
        try {
          const ar = countUniqueInSingleDB(archiveDB, dateStr);
          archiveDone = true;
          unionSet = new Set([...ar.vids]);
        } finally {
          archiveDB.close();
        }
      } catch (e) {
        logd("archive open failed or not present:", e?.message || e);
      }
      const mainRes = countUniqueInSingleDB(mainDB, dateStr);
      for (const v of mainRes.vids) unionSet.add(v);
      return { source: (archiveDone ? "archive+main" : "main_only"), count: unionSet.size };
    } else {
      const mainRes = countUniqueInSingleDB(mainDB, dateStr);
      return { source: "main_only", count: mainRes.count };
    }
  } finally {
    mainDB.close();
  }
}

/** summary */
export function ensureSummaryTableAndColumn(dbMain) {
  dbMain.exec(`
    CREATE TABLE IF NOT EXISTS rahin_daily_summary (
      date TEXT PRIMARY KEY,
      total_visitors INTEGER,
      messages_in INTEGER,
      messages_out INTEGER,
      transactions_count INTEGER,
      sales_amount INTEGER,
      blocked_users INTEGER,
      top_destination TEXT
    );
  `);
  const cols = dbMain.prepare("PRAGMA table_info(rahin_daily_summary)").all().map(c => c.name);
  if (!cols.includes("total_visitors")) {
    dbMain.exec("ALTER TABLE rahin_daily_summary ADD COLUMN total_visitors INTEGER;");
  }
}

export function upsertDailyVisitors(dbMain, dateStr, total) {
  dbMain.prepare(`
    INSERT INTO rahin_daily_summary (date, total_visitors)
    VALUES (?, ?)
    ON CONFLICT(date) DO UPDATE SET total_visitors=excluded.total_visitors;
  `).run(dateStr, total);
}

export function collectVisitorsDailySmart(dateStr, mainPath = "./db_atigh.sqlite", archivePath = "./db_archive.sqlite") {
  const dbMain = new Database(mainPath);
  try {
    ensureSummaryTableAndColumn(dbMain);
    const { source, count } = countUniqueVisitorsSmart(dateStr, mainPath, archivePath);
    upsertDailyVisitors(dbMain, dateStr, count);
    logd("FINAL → date =", dateStr, "source =", source, "total =", count);
    return { date: dateStr, total: count, source };
  } finally {
    dbMain.close();
  }
}

