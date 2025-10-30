import 'file:///E:/Projects/rahin/logger.js';
/**
 * visitors_from_archive_partitions.js
 * آرشیو: جداول weekly با نام journey_events_wYYYY_MM_DD (پایان هفته = شنبه)
 * برای یک تاریخ، جدول شنبه‌ی همان هفته را پیدا می‌کنیم و:
 *   total = COUNT(DISTINCT visitor_id) WHERE created_at LIKE 'YYYY-MM-DD%'
 * سپس در db_atigh.sqlite -> rahin_daily_summary.total_visitors آپسرت می‌کنیم.
 */

import Database from "better-sqlite3";

// --- تاریخ‌ها (Asia/Tehran) ---
function todayTehranStr(){
  try{
    const s = new Date().toLocaleString("sv-SE",{timeZone:"Asia/Tehran",hour12:false});
    return s.slice(0,10);
  }catch{
    const d=new Date();const y=d.getFullYear();const m=String(d.getMonth()+1).padStart(2,"0");const dd=String(d.getDate()).padStart(2,"0");
    return `${y}-${m}-${dd}`;
  }
}
export function yesterdayTehranStr(){
  try{
    const now = new Date(new Date().toLocaleString("en-US",{timeZone:"Asia/Tehran"}));
    const yst = new Date(+now - 24*60*60*1000);
    const y=yst.getFullYear();const m=String(yst.getMonth()+1).padStart(2,"0");const d=String(yst.getDate()).padStart(2,"0");
    return `${y}-${m}-${d}`;
  }catch{
    const d=new Date(Date.now()-24*60*60*1000);const y=d.getFullYear();const m=String(d.getMonth()+1).padStart(2,"0");const dd=String(d.getDate()).padStart(2,"0");
    return `${y}-${m}-${dd}`;
  }
}

/** شنبه‌ی همان هفته (week-end) */
function weekEndSaturday(dateStr /* YYYY-MM-DD */){
  const [Y,M,D] = dateStr.split("-").map(Number);
  const dt = new Date(Date.UTC(Y, M-1, D));      // UTC برای پایداری روز هفته
  const dow = dt.getUTCDay();                     // 0..6
  const deltaToSat = (6 - dow + 7) % 7;          // فاصله تا شنبه
  const satUTC = new Date(dt.getTime() + deltaToSat*24*60*60*1000);
  const y = satUTC.getUTCFullYear();
  const m = String(satUTC.getUTCMonth()+1).padStart(2,"0");
  const d = String(satUTC.getUTCDate()).padStart(2,"0");
  return `${y}-${m}-${d}`;
}

/** نام جدول پارتیشن */
function tableForDate(dateStr){
  const weekEnd = weekEndSaturday(dateStr);
  const t = `journey_events_w${weekEnd.replaceAll("-","_")}`;
  return { table: t, week_end: weekEnd };
}

// --- DB helpers ---
function ensureSummaryTable(dbMain){
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
  const cols = dbMain.prepare("PRAGMA table_info(rahin_daily_summary)").all().map(c=>c.name);
  if (!cols.includes("total_visitors")){
    dbMain.exec("ALTER TABLE rahin_daily_summary ADD COLUMN total_visitors INTEGER;");
  }
}
function upsertDailyVisitors(dbMain, dateStr, total){
  dbMain.prepare(`
    INSERT INTO rahin_daily_summary (date, total_visitors)
    VALUES (?, ?)
    ON CONFLICT(date) DO UPDATE SET total_visitors=excluded.total_visitors;
  `).run(dateStr, total);
}

/** خواندن از آرشیو و برگرداندن عدد یکتا */
export function countDailyUniqueFromArchive(dateStr, archivePath="./db_archive.sqlite"){
  const { table, week_end } = tableForDate(dateStr);
  
   let db;
   
   try {
     db = new Database(archivePath, {
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
  try{
    const exists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(table);
    if (!exists?.name){
      return { ok:false, reason:`archive partition table not found: ${table}`, table, week_end, date:dateStr };
    }
    // created_at متن ساده است (YYYY-MM-DD HH:mm:ss)
    const sql = `SELECT COUNT(DISTINCT visitor_id) AS c FROM ${table} WHERE created_at LIKE ?`;
    const row = db.prepare(sql).get(`${dateStr}%`);
    const total = Number(row?.c || 0);
    return { ok:true, date:dateStr, week_end, table, total };
  } finally {

  }
}

/**
 * مسیر کامل: گرفتن عدد از آرشیو و نوشتن در summary (DB اصلی)
 * NOTE: اگر total==0 و FORCE_ZERO != '1' باشد، چیزی نمی‌نویسد (skip).
 */
export function writeDailyVisitorsFromArchive(dateStr, mainPath="./db_atigh.sqlite", archivePath="./db_archive.sqlite"){
  const res = countDailyUniqueFromArchive(dateStr, archivePath);
  if (!res.ok) return res;

  const forceZero = process.env.FORCE_ZERO === '1';
  if (!forceZero && Number(res.total) === 0) {
    return { ...res, ok:false, reason:'total==0 (skipped, use --force-zero to write zero)' };
  }

  const main = new Database(mainPath);
  try{
    ensureSummaryTable(main);
    upsertDailyVisitors(main, dateStr, res.total);
  } finally {
    main.close();
  }
  return { ...res };
}

