import '../../logger.js';
/**
 * siteAnalyticsCollector.js
 * خروجی‌ها برای یک تاریخ (دیروز یا تاریخ ورودی):
 *  - rahin_site_daily(date, pageviews, unique_visitors, peak_hour, peak_hour_count)
 *  - rahin_site_hourly(date, hour, views)
 *  - rahin_site_top_pages(date, rank, page, views, unique_visitors, avg_duration_sec, avg_scroll_depth, clicks)
 *
 * منبع: db_archive.sqlite → journey_events_wYYYY_MM_DD  (شنبه همان هفته)
 * کشف خودکار ستون‌ها: path/url/route ، duration ، scroll ، clicks
 */

import Database from "better-sqlite3";

// === تاریخ‌ها (Asia/Tehran) ===
export function yesterdayTehranStr(){
  try{
    const now = new Date(new Date().toLocaleString("en-US",{timeZone:"Asia/Tehran"}));
    const yst = new Date(+now - 24*60*60*1000);
    const y=yst.getFullYear(), m=String(yst.getMonth()+1).padStart(2,'0'), d=String(yst.getDate()).padStart(2,'0');
    return `${y}-${m}-${d}`;
  }catch{
    const d=new Date(Date.now()-24*60*60*1000);
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  }
}
function weekEndSaturday(dateStr){
  const [Y,M,D] = dateStr.split("-").map(Number);
  const dt = new Date(Date.UTC(Y, M-1, D)); const dow = dt.getUTCDay();
  const deltaToSat = (6 - dow + 7) % 7;
  const sat = new Date(dt.getTime() + deltaToSat*24*60*60*1000);
  const y=sat.getUTCFullYear(), m=String(sat.getUTCMonth()+1).padStart(2,'0'), d=String(sat.getUTCDate()).padStart(2,'0');
  return `${y}-${m}-${d}`;
}
function tableForDate(dateStr){
  const wEnd = weekEndSaturday(dateStr);
  return `journey_events_w${wEnd.replaceAll('-','_')}`;
}

// === کشف ستون‌ها ===
const PAGE_HINTS = ["path","page_path","url","page_url","pathname","route","uri"];
const DURATION_HINTS = ["duration_sec","engagement_time_sec","time_spent","time_on_page_sec","session_duration_sec"];
const SCROLL_HINTS = ["scroll_depth","max_scroll","scroll_percent","scroll_pct"];
const CLICKS_HINTS = ["clicks","clicks_count","click_count"];
function listCols(db, t){ return db.prepare(`PRAGMA table_info(${t})`).all().map(c=>c.name); }
function pickCol(cols, hints){
  const hitExact = hints.find(h=>cols.includes(h));
  if (hitExact) return hitExact;
  const lower = cols.map(c=>c.toLowerCase());
  for (const h of hints){
    const idx = lower.findIndex(c=>c.includes(h.toLowerCase()));
    if (idx>=0) return cols[idx];
  }
  return null;
}

// === ساخت جداول مقصد در DB اصلی ===
function ensureDestTables(dbMain){
  dbMain.exec(`
    CREATE TABLE IF NOT EXISTS rahin_site_daily (
      date TEXT PRIMARY KEY,
      pageviews INTEGER,
      unique_visitors INTEGER,
      peak_hour INTEGER,
      peak_hour_count INTEGER,
      created_at TEXT DEFAULT (datetime('now','localtime')),
      updated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS rahin_site_hourly (
      date TEXT,
      hour INTEGER,           -- 0..23
      views INTEGER,
      PRIMARY KEY(date, hour)
    );
    CREATE TABLE IF NOT EXISTS rahin_site_top_pages (
      date TEXT,
      rank INTEGER,           -- 1..5
      page TEXT,
      views INTEGER,
      unique_visitors INTEGER,
      avg_duration_sec REAL,
      avg_scroll_depth REAL,
      clicks INTEGER,
      PRIMARY KEY(date, rank)
    );
    CREATE TRIGGER IF NOT EXISTS trg_site_daily_updated AFTER UPDATE ON rahin_site_daily
    BEGIN
      UPDATE rahin_site_daily SET updated_at = datetime('now','localtime') WHERE date=NEW.date;
    END;
  `);
}

function upsertSiteDaily(dbMain, date, patch){
  const keys = Object.keys(patch);
  const placeholders = keys.map(_=>"?").join(",");
  const updates = keys.map(k=>`${k}=excluded.${k}`).join(", ");
  const values = keys.map(k=>patch[k]);
  dbMain.prepare(`
    INSERT INTO rahin_site_daily (date, ${keys.join(",")})
    VALUES (?, ${placeholders})
    ON CONFLICT(date) DO UPDATE SET ${updates};
  `).run(date, ...values);
}
function upsertHourly(dbMain, date, rows){
  const stmt = dbMain.prepare(`
    INSERT INTO rahin_site_hourly(date,hour,views) VALUES(?,?,?)
    ON CONFLICT(date,hour) DO UPDATE SET views=excluded.views;
  `);
  const tx = dbMain.transaction((arr)=>{ for(const r of arr) stmt.run(date, r.hour, r.views); });
  tx(rows);
}
function replaceTopPages(dbMain, date, rows){
  // پاک و جایگزین برای همان روز
  dbMain.prepare(`DELETE FROM rahin_site_top_pages WHERE date=?`).run(date);
  const stmt = dbMain.prepare(`
    INSERT INTO rahin_site_top_pages(date,rank,page,views,unique_visitors,avg_duration_sec,avg_scroll_depth,clicks)
    VALUES (?,?,?,?,?,?,?,?)
  `);
  const tx = dbMain.transaction((arr)=>{ for(const r of arr) stmt.run(date,r.rank,r.page,r.views,r.unique_visitors,r.avg_duration_sec,r.avg_scroll_depth,r.clicks); });
  tx(rows);
}

// === هسته: خواندن از آرشیو و ساخت آنالیتیکس ===
export function buildSiteAnalyticsDaily(dateStr, mainPath="./db_atigh.sqlite", archivePath="./db_archive.sqlite"){
  const table = tableForDate(dateStr);
  const dbA = new Database(archivePath);
  try{
    const exists = dbA.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(table);
    if(!exists?.name) return {ok:false, reason:`archive table not found: ${table}`};

    const cols = listCols(dbA, table);
    // ستون‌های پایه
    if (!cols.includes("created_at") || !cols.includes("visitor_id")){
      return {ok:false, reason:`required columns missing in ${table} (need created_at & visitor_id)`};
    }
    const pageCol = pickCol(cols, PAGE_HINTS) || "path";
    const durCol  = pickCol(cols, DURATION_HINTS); // ممکن است نباشد
    const scrCol  = pickCol(cols, SCROLL_HINTS);
    const clkCol  = pickCol(cols, CLICKS_HINTS);

    // فیلتر همان روز
    const like = `${dateStr}%`;

    // 1) آمار ساعتی
    const hourly = dbA.prepare(`
      SELECT CAST(substr(created_at,12,2) AS INTEGER) AS hour, COUNT(*) AS views
      FROM ${table}
      WHERE created_at LIKE ? AND ${pageCol} IS NOT NULL AND TRIM(${pageCol})!=''
      GROUP BY hour
      ORDER BY hour
    `).all(like);

    // 2) صفحات (views, unique, averages…)
    const baseSelect = `
      SELECT ${pageCol} AS page,
             COUNT(*) AS views,
             COUNT(DISTINCT visitor_id) AS unique_visitors
             ${durCol ? `, AVG(NULLIF(${durCol},0)) AS avg_duration_sec` : `, NULL AS avg_duration_sec`}
             ${scrCol ? `, AVG(NULLIF(${scrCol},0)) AS avg_scroll_depth` : `, NULL AS avg_scroll_depth`}
             ${clkCol ? `, SUM(${clkCol}) AS clicks` : `, NULL AS clicks`}
      FROM ${table}
      WHERE created_at LIKE ? AND ${pageCol} IS NOT NULL AND TRIM(${pageCol})!=''
      GROUP BY ${pageCol}
      ORDER BY views DESC
    `;
    const pages = dbA.prepare(baseSelect).all(like);

    // 3) Totals + Peak
    const totalViews = pages.reduce((a,b)=>a + Number(b.views||0), 0);
    const uniqueVisitors = dbA.prepare(`
      SELECT COUNT(DISTINCT visitor_id) AS u
      FROM ${table}
      WHERE created_at LIKE ? AND ${pageCol} IS NOT NULL AND TRIM(${pageCol})!=''
    `).get(like)?.u || 0;

    let peakHour = null, peakHourCount = 0;
    for (const h of hourly){ if (h.views > peakHourCount){ peakHour=h.hour; peakHourCount=h.views; } }

    // 4) Top 5
    const top5 = pages.slice(0,5).map((r,idx)=>({
      rank: idx+1,
      page: String(r.page || ""),
      views: Number(r.views||0),
      unique_visitors: Number(r.unique_visitors||0),
      avg_duration_sec: r.avg_duration_sec!=null ? Number(r.avg_duration_sec) : null,
      avg_scroll_depth: r.avg_scroll_depth!=null ? Number(r.avg_scroll_depth) : null,
      clicks: r.clicks!=null ? Number(r.clicks) : null
    }));

    // 5) نوشتن در DB اصلی
    const dbM = new Database(mainPath);
    try{
      ensureDestTables(dbM);
      upsertSiteDaily(dbM, dateStr, {
        pageviews: Number(totalViews||0),
        unique_visitors: Number(uniqueVisitors||0),
        peak_hour: peakHour ?? 0,
        peak_hour_count: Number(peakHourCount||0)
      });
      upsertHourly(dbM, dateStr, hourly.map(h=>({hour:h.hour, views:Number(h.views||0)})));
      replaceTopPages(dbM, dateStr, top5);
    } finally { dbM.close(); }

    return {ok:true, table, pageCol, durCol, scrCol, clkCol, totalViews, uniqueVisitors, peakHour, peakHourCount, top5};
  } finally {
    dbA.close();
  }
}

