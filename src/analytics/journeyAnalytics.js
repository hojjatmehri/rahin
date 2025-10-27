import '../../logger.js';
// src/analytics/journeyAnalytics.js
import moment from "moment-timezone";
import env from "../config/env.js";

const TZ = "Asia/Tehran";

/** اتصال DB آرشیو روی همین کانکشن (فقط یک‌بار) */
function ensureArchiveAttached(db) {
  // اگر قبلاً attach شده، کاری نکن
  const dbs = db.prepare("PRAGMA database_list;").all();
  const hasArch = dbs.some(x => String(x.name).toLowerCase() === "arch");
  if (hasArch) return;

  // مسیر از ENV یا پیش‌فرض شما
  const archPath = (env.ARCHIVE_DB_PATH || "C:\\Users\\Administrator\\Desktop\\Projects\\AtighgashtAI\\db_archive.sqlite")
    .replace(/'/g, "''");
  db.exec(`ATTACH DATABASE '${archPath}' AS arch;`);
}

/** سازندهٔ بیان SQL برای تبدیل TEXT → unix seconds (ستون دلخواه، پیش‌فرض created_at) */
function sqlEpochFrom(col = "created_at") {
  // الگوی تاریخ‌های ISO یا ساده، بدون میلی‌ثانیه و Z
  return `
CAST(
  strftime('%s',
    CASE
      WHEN instr(${col},'T')>0
        THEN REPLACE(REPLACE(
               substr(${col},1,
                 CASE WHEN instr(${col},'.')>0
                      THEN instr(${col},'.')-1
                      ELSE length(${col}) END
               )
             ,'T',' ')
           ,'Z','')
      ELSE REPLACE(${col},'/','-')
    END
  ) AS INTEGER
)`.trim();
}

// اگر جای دیگری لازم داشتی، همچنان ثابت export می‌کنیم (با ستون created_at)
export const SQL_EPOCH_FROM_TEXT = sqlEpochFrom("created_at");

/** لیست جدول‌های هفتگی آرشیو (journey_events_wYYYY_MM_DD...) */
export function listWeeklyTablesCovering(db /*, startIso, endIso */) {
  ensureArchiveAttached(db);
  const rows = db
    .prepare(`
      SELECT name
      FROM arch.sqlite_master
      WHERE type='table' AND name LIKE 'journey_events_w%'
      ORDER BY name DESC
    `)
    .all();

  // ساده: همه را برگردان (حجم‌تان کم است)
  return rows.map(r => r.name);
}

/** تجمیع KPIهای جرنی از جدول‌های هفتگی در بازه [startIso, endIso) */
export function aggregateJourney(db, startIso, endIso) {
  ensureArchiveAttached(db);

  const tables = listWeeklyTablesCovering(db, startIso, endIso);
  if (!tables.length) return null;

  const startS = Math.floor(moment.tz(startIso, TZ).valueOf() / 1000);
  const endS   = Math.floor(moment.tz(endIso,   TZ).valueOf() / 1000);

  let totalEvents = 0;
  const uniqVisitors = new Set();
  let engagedEvents = 0;   // dwell>0
  let dwellSum = 0;

  const epochExpr = sqlEpochFrom("created_at");

  for (const t of tables) {
    // ستون‌ها مطابق اسکیما آرشیو شما
    const rows = db
      .prepare(`
        SELECT
          visitor_id,
          page_url,
          COALESCE(time_spent_seconds, 0) AS dwell,
          ${epochExpr} AS ts
        FROM arch.${t}
        WHERE created_at IS NOT NULL
          AND ${epochExpr} >= ? AND ${epochExpr} < ?
      `)
      .all(startS, endS);

    totalEvents += rows.length;

    for (const r of rows) {
      if (r.visitor_id) uniqVisitors.add(String(r.visitor_id));
      const d = Number(r.dwell) || 0;
      if (d > 0) {
        engagedEvents += 1;
        dwellSum += d;
      }
    }
  }

  const visitors = uniqVisitors.size;
  const avgDwellPerEvent = totalEvents ? Math.round(dwellSum / totalEvents) : 0;
  const engagementRate = totalEvents ? +(engagedEvents / totalEvents).toFixed(3) : 0;

  return {
    start: startIso,
    end: endIso,
    events: totalEvents,
    unique_visitors: visitors,
    engaged_events: engagedEvents,
    engagement_rate: engagementRate,         // سهم رویدادهای دارای dwell>0
    dwell_sum_sec: dwellSum,
    avg_dwell_per_event_sec: avgDwellPerEvent
  };
}

export default {
  SQL_EPOCH_FROM_TEXT,
  listWeeklyTablesCovering,
  aggregateJourney,
};

