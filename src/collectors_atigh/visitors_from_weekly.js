import '../../logger.js';
/**
 * visitors_from_weekly.js
 * منبع: db_archive.sqlite → جدول rahin_visitors_weekly (week_start = YYYY-MM-DD)
 * هدف: برای یک تاریخ (پیش‌فرض دیروز تهران)، مقدار تقریبی total_visitors روزانه را
 * از unique_visitors هفتگی استخراج (تقسیم بر 7) و در db_atigh.sqlite → rahin_daily_summary ثبت کند.
 */

import Database from "better-sqlite3";

// ---- ابزارهای تاریخ (Asia/Tehran) ----
function todayTehranStr(){
  try{
    const s = new Date().toLocaleString("sv-SE",{timeZone:"Asia/Tehran",hour12:false});
    return s.slice(0,10);
  }catch{
    const d=new Date();const y=d.getFullYear();const m=String(d.getMonth()+1).padStart(2,'0');const day=String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${day}`;
  }
}
export function yesterdayTehranStr(){
  try{
    const now = new Date(new Date().toLocaleString("en-US",{timeZone:"Asia/Tehran"}));
    const yst = new Date(+now - 24*60*60*1000);
    const y=yst.getFullYear();const m=String(yst.getMonth()+1).padStart(2,'0');const d=String(yst.getDate()).padStart(2,'0');
    return `${y}-${m}-${d}`;
  }catch{
    const d=new Date(Date.now()-24*60*60*1000);const y=d.getFullYear();const m=String(d.getMonth()+1).padStart(2,'0');const day=String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${day}`;
  }
}

/**
 * بر اساس خروجی‌های آرشیو، به نظر هفته از دوشنبه (Monday) شروع می‌شود.
 * week_start را برای هر تاریخ تولید می‌کنیم → تاریخ دوشنبه همان هفته در Asia/Tehran.
 */
function weekStartMondayTehran(dateStr /* YYYY-MM-DD */){
  const [Y,M,D] = dateStr.split("-").map(Number);
  // تاریخ را به زمان تهران نگاشت نمی‌کنیم؛ برای محاسبه‌ی روز هفته کافی است همان تقویم گرگوریان را بگیریم.
  const dt = new Date(Date.UTC(Y, M-1, D)); // UTC برای پایداری روز هفته
  // getUTCDay(): یکشنبه=0...شنبه=6 → Monday=1
  const day = dt.getUTCDay(); 
  const deltaToMonday = (day + 6) % 7; // اگر دوشنبه=1 → فاصله تا دوشنبه
  const mondayUTC = new Date(dt.getTime() - deltaToMonday*24*60*60*1000);
  const y = mondayUTC.getUTCFullYear();
  const m = String(mondayUTC.getUTCMonth()+1).padStart(2,'0');
  const d = String(mondayUTC.getUTCDate()).padStart(2,'0');
  return `${y}-${m}-${d}`;
}

// ---- DB helpers ----
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

// ---- هسته: خواندن هفتگی و برآورد روزانه ----
export function estimateDailyVisitorsFromWeekly(dateStr, mainPath="./db_atigh.sqlite", archivePath="./db_archive.sqlite"){
  // اگر تاریخ امروز بود، آرشیو شاملش نیست. ولی این فایل طبق نیاز فقط برای "دیروز و قبل" استفاده می‌شود.
  const weekStart = weekStartMondayTehran(dateStr);
  const archive = new Database(archivePath);
  try{
    // جدول: rahin_visitors_weekly | ستون‌ها: week_start, unique_visitors, ...
    const row = archive.prepare(`
      SELECT week_start, unique_visitors
      FROM rahin_visitors_weekly
      WHERE week_start = ?
    `).get(weekStart);

    if (!row){
      return { ok:false, reason:`No weekly row in archive for week_start=${weekStart}`, date:dateStr, week_start:weekStart };
    }

    const weeklyUnique = Number(row.unique_visitors || 0);
    // ساده و صریح: تقسیم بر 7، گرد کردن به نزدیک‌ترین عدد صحیح
    const dailyEstimate = Math.round(weeklyUnique / 7);

    // ثبت در DB اصلی
    const main = new Database(mainPath);
    try{
      ensureSummaryTable(main);
      upsertDailyVisitors(main, dateStr, dailyEstimate);
    } finally {
      main.close();
    }

    return { ok:true, date:dateStr, week_start:weekStart, weekly_unique:weeklyUnique, daily_estimate:dailyEstimate };
  } finally {
    archive.close();
  }
}

