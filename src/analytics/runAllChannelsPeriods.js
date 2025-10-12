// src/analytics/runAllChannelsPeriods.js
import moment from "moment-timezone";
import { CONFIG } from "../config/Config.js";

const TZ = "Asia/Tehran";
import util from "node:util";

const SQL_DEBUG = String(process.env.SQL_DEBUG || "0") === "1";

/** چاپِ عمیق و بدون [Object] */
function printDeep(label, obj) {
    if (!SQL_DEBUG) return;
    try {
        // BigInt/undefined/توابع را هندل کنیم
        const seen = new WeakSet();
        const json = JSON.stringify(
            obj,
            (k, v) => {
                if (typeof v === "bigint") return v.toString();
                if (typeof v === "function") return `[Function ${v.name || "anonymous"}]`;
                if (typeof v === "object" && v !== null) {
                    if (seen.has(v)) return "[Circular]";
                    seen.add(v);
                }
                return v;
            },
            2
        );
        console.log("[SQL_DEBUG]", label, "\n" + json);
    } catch (e) {
        console.log(
            "[SQL_DEBUG]",
            label,
            "\n" +
            util.inspect(obj, {
                depth: null,
                colors: false,
                compact: false,
                breakLength: 120,
                maxArrayLength: Infinity,
            })
        );
    }
}

/** چاپ ساده برای پیام‌های کوتاه */
function logDbg(label, payload) {
    if (!SQL_DEBUG) return;
    if (payload && typeof payload === "object") {
        printDeep(label, payload);
    } else {
        console.log("[SQL_DEBUG]", label, payload ?? "");
    }
}

/* =========================
 *  Debug helpers
 * ========================= */

function qGet(db, sql, params = []) {
    if (SQL_DEBUG) {
        console.log("──────────────── SQL GET ────────────────");
        console.log(sql.trim());
        console.log("params:", params);
    }
    const t0 = Date.now();
    const row = db.prepare(sql).get(...params);
    if (SQL_DEBUG) {
        console.log("row:", row);
        console.log("took_ms:", Date.now() - t0);
    }
    return row;
}
function qAll(db, sql, params = []) {
    if (SQL_DEBUG) {
        console.log("──────────────── SQL ALL ────────────────");
        console.log(sql.trim());
        console.log("params:", params);
    }
    const t0 = Date.now();
    const rows = db.prepare(sql).all(...params);
    if (SQL_DEBUG) {
        console.log("rows:", rows?.length ?? 0, rows?.slice?.(0, 5));
        console.log("took_ms:", Date.now() - t0);
    }
    return rows;
}

/* =========================
 *  Epoch helpers (self-contained)
 * ========================= */
// ستون تاریخ → اپوک ثانیه (SQLite)
// ==== ثابت مشترک: فقط از created_at استفاده کن
const DATE_COL = "created_at";

function epochExpr(col) {
    // بدون تغییر، فقط همیشه با created_at صدا می‌زنیم
    return `
    strftime(
      '%s',
      replace(
        replace(
          replace(substr(${col}, 1, 19), '/', '-'),
          'T', ' '
        ),
        'Z', ''
      )
    )
  `;
}


// بازهٔ عددی (start/end به اپوک تبدیل می‌شوند)
function toEpochBounds(startIso, endIso) {
    const s = Math.floor(moment.tz(startIso, TZ).valueOf() / 1000);
    const e = Math.floor(moment.tz(endIso, TZ).valueOf() / 1000);
    logDbg("toEpochBounds:", { startIso, endIso, s, e });
    return [s, e];
}
// شرط بین دو عدد بدون strftime روی پارامترها
function betweenEpochNumeric(col) {
    return `${epochExpr(col)} >= ? AND ${epochExpr(col)} < ?`;
}

/* =========================
 *  Time ranges (7d, MTD-greg, prev-to-date-greg)
 * ========================= */
function jalaliMonthRanges(now = new Date()) {
    const nowTeh = moment.tz(now, TZ);
    const nowIso = nowTeh.toDate().toISOString();

    const k7dStart = nowTeh.clone().subtract(7, "days").toDate().toISOString();

    // fallback: گریگوریان (برای هم‌خوانی با داده‌های ISO)
    const mtdStart = nowTeh.clone().startOf("month").toDate().toISOString();

    const prevM = nowTeh.clone().subtract(1, "month");
    const prevStart = prevM.clone().startOf("month").toDate().toISOString();
    const daysPassed = nowTeh.diff(nowTeh.clone().startOf("month"), "days");
    const prevEnd = prevM.clone().startOf("month").add(daysPassed, "days").toDate().toISOString();

    const r = {
        k7d: { start: k7dStart, end: nowIso, title: "۷ روز اخیر" },
        mtd_g: { start: mtdStart, end: nowIso, title: "ماه جاری (گریگوری)" },
        prev_to_date_g: { start: prevStart, end: prevEnd, title: "ماه قبل تا امروز (گریگوری)" },
    };
    logDbg("ranges:", r);
    return r;
}

/* =========================
 *  DB utils
 * ========================= */
function tableExists(db, name) {
    try {
        const row = qGet(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", [name]);
        const ok = !!row;
        logDbg(`tableExists(${name})=`, ok);
        return ok;
    } catch (e) {
        logDbg(`tableExists(${name}) ERROR:`, e?.message || e);
        return false;
    }
}
function hasColumn(db, table, col) {
    try {
        const rows = qAll(db, `PRAGMA table_info(${table})`);
        const ok = rows?.some((x) => String(x.name).toLowerCase() === col.toLowerCase());
        logDbg(`hasColumn(${table}, ${col})=`, ok);
        return ok;
    } catch (e) {
        logDbg(`hasColumn(${table}, ${col}) ERROR:`, e?.message || e);
        return false;
    }
}

/* =========================
 *  Aggregators
 * ========================= */
/** Finance: transactions (exclude isCanceled=1) */
function aggFinance(db, startIso, endIso) {
    const t = "transactions";
    logDbg("aggFinance.start", { startIso, endIso, table: t });
    if (!tableExists(db, t)) return { count: 0, revenue: 0, profit: 0 };

    const [s, e] = toEpochBounds(startIso, endIso);

    // فرض: همه چیز created_at دارد؛ isCanceled اگر نبود هم مشکلی نیست (IFNULL)
    const sql = `
      SELECT
        COUNT(*) AS cnt,
        COALESCE(SUM(sellAmount),0) AS revenue,
        COALESCE(SUM(profit),0)     AS profit
      FROM ${t}
      WHERE ${betweenEpochNumeric(DATE_COL)}
        AND IFNULL(isCanceled,0)=0
    `;
    const r = qGet(db, sql, [s, e]) || {};
    const out = { count: r.cnt || 0, revenue: r.revenue || 0, profit: r.profit || 0 };
    logDbg("aggFinance.out", out);
    return out;
}


/** WhatsApp: whatsapp_new_msg + wa_pdf_dispatch_log
 *  - تاریخ اصلی: ttime (UTC/ISO)
 *  - inbound: fromMe=0 , outbound: fromMe=1
 */
function aggWhatsApp(db, startIso, endIso) {
    const t = "whatsapp_new_msg";
    logDbg("aggWhatsApp.start", { startIso, endIso, table: t });
    let inbound = 0, outbound = 0, pdf_sent_ok = 0, pdf_sent_fail = 0;
    const [s, e] = toEpochBounds(startIso, endIso);

    if (tableExists(db, t)) {
        // فقط created_at
        const sqlIn = `
        SELECT COUNT(*) c FROM ${t}
        WHERE IFNULL(fromMe,0)=0 AND ${betweenEpochNumeric(DATE_COL)}
      `;
        inbound = qGet(db, sqlIn, [s, e])?.c || 0;

        const sqlOut = `
        SELECT COUNT(*) c FROM ${t}
        WHERE IFNULL(fromMe,0)=1 AND ${betweenEpochNumeric(DATE_COL)}
      `;
        outbound = qGet(db, sqlOut, [s, e])?.c || 0;
    }

    if (tableExists(db, "wa_pdf_dispatch_log")) {
        const okIn = `('ok','success','sent','delivered','read')`;
        const failIn = `('fail','failed','error')`;

        const sqlOk = `
        SELECT COUNT(*) c FROM wa_pdf_dispatch_log
        WHERE LOWER(IFNULL(wa_status,'')) IN ${okIn}
          AND ${betweenEpochNumeric(DATE_COL)}
      `;
        pdf_sent_ok = qGet(db, sqlOk, [s, e])?.c || 0;

        const sqlFail = `
        SELECT COUNT(*) c FROM wa_pdf_dispatch_log
        WHERE (
          LOWER(IFNULL(wa_status,'')) IN ${failIn}
          OR (error_message IS NOT NULL AND TRIM(error_message) <> '')
        )
        AND ${betweenEpochNumeric(DATE_COL)}
      `;
        pdf_sent_fail = qGet(db, sqlFail, [s, e])?.c || 0;
    }

    const out = { inbound, outbound, pdf_sent_ok, pdf_sent_fail };
    logDbg("aggWhatsApp.out", out);
    return out;
}


/** Instagram: سه منبع
 *  - atigh_instagram_dev: created_at (fallback: created_date) → events
 *  - comment: created_date → comments
 *  - reply  : created_date → replies
 */
function aggInstagram(db, startIso, endIso) {
    logDbg("aggInstagram.start", { startIso, endIso });
    const [s, e] = toEpochBounds(startIso, endIso);

    let events = 0, comments = 0, replies = 0;

    if (tableExists(db, "atigh_instagram_dev")) {
        const sql = `
        SELECT COUNT(*) c FROM atigh_instagram_dev
        WHERE ${betweenEpochNumeric('created_date')}
      `;
        events = qGet(db, sql, [s, e])?.c || 0;
    }

    if (tableExists(db, "comment")) {
        const sql = `
        SELECT COUNT(*) c FROM comment
        WHERE ${betweenEpochNumeric('created_date')}
      `;
        comments = qGet(db, sql, [s, e])?.c || 0;
    }

    if (tableExists(db, "reply")) {
        const sql = `
        SELECT COUNT(*) c FROM reply
        WHERE ${betweenEpochNumeric('created_date')}
      `;
        replies = qGet(db, sql, [s, e])?.c || 0;
    }

    const out = { events, comments, replies, total_engagements: events + comments + replies };
    logDbg("aggInstagram.out", out);
    return out;
}


/** Clicks: click_logs
 *  - تاریخ: clicked_at (fallback: created_at)
 *  - تفکیک: whatsapp / tel|phone|call
 */
function aggClicks(db, startIso, endIso) {
    const t = "click_logs";
    logDbg("aggClicks.start", { startIso, endIso, table: t });
    if (!tableExists(db, t)) return { total: 0, whatsapp_clicks: 0, tel_clicks: 0 };
  
    const [s, e] = toEpochBounds(startIso, endIso);
  
    const sql = `
      WITH norm AS (
        SELECT
          id,
          LOWER(IFNULL(click_type,'')) AS ct,
          CASE
            WHEN created_at GLOB '[0-9]*' AND LENGTH(created_at) >= 13 THEN CAST(created_at AS INTEGER)/1000
            WHEN created_at GLOB '[0-9]*' AND LENGTH(created_at) = 10  THEN CAST(created_at AS INTEGER)
            ELSE strftime(
                   '%s',
                   CASE
                     WHEN instr(created_at,'T')>0
                       THEN REPLACE(
                              substr(created_at,1,CASE WHEN instr(created_at,'.')>0 THEN instr(created_at,'.')-1 ELSE length(created_at) END),
                              'T',' '
                            )
                     ELSE REPLACE(
                            substr(created_at,1,CASE WHEN instr(created_at,'.')>0 THEN instr(created_at,'.')-1 ELSE length(created_at) END),
                            '/','-'
                          )
                   END,
                   'utc'
                 )
          END AS ts
        FROM ${t}
      )
      SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN ct='whatsapp' THEN 1 ELSE 0 END) AS whatsapp_clicks,
        SUM(CASE WHEN ct IN ('tel','phone','call') THEN 1 ELSE 0 END) AS tel_clicks
      FROM norm
      WHERE ts >= ? AND ts < ?;
    `;
  
    const r = qGet(db, sql, [s, e]) || {};
    const out = {
      total: r.total || 0,
      whatsapp_clicks: r.whatsapp_clicks || 0,
      tel_clicks: r.tel_clicks || 0,
    };
    logDbg("aggClicks.out", out);
    return out;
  }
  


/* =========================
 *  Build period + compare
 * ========================= */
function buildPeriod(db, startIso, endIso, title) {
    logDbg("buildPeriod:", { title, startIso, endIso });
    const out = {
        start: startIso,
        end: endIso,
        title,
        finance: aggFinance(db, startIso, endIso),
        whatsapp: aggWhatsApp(db, startIso, endIso),
        instagram: aggInstagram(db, startIso, endIso),
        clicks: aggClicks(db, startIso, endIso),
    };
    printDeep(`buildPeriod.out ${title}`, out);
    return out;
}

function pct(curr, prev) {
    if (!isFinite(prev) || prev === 0) return null;
    return ((curr - prev) / prev) * 100;
}
function diffObj(curr, prev) {
    const out = {};
    for (const k of Object.keys(curr)) {
        const a = Number(curr[k] || 0);
        const b = Number((prev || {})[k] || 0);
        out[k] = { curr: a, prev: b, delta_pct: pct(a, b) };
    }
    return out;
}

/* =========================
 *  API
 * ========================= */
// @ts-check

/**
 * @typedef {import('better-sqlite3').Database} BetterDB
 */

/**
 * رِنج‌ها و آگرگیشن را اجرا می‌کند.
 * @param {BetterDB} db  // ⬅️ فقط better-sqlite3
 */
export async function runAllChannelsPeriods(db) {
    logDbg("runAllChannelsPeriods.start");
    const r = jalaliMonthRanges(new Date());

    const k7d = buildPeriod(db, r.k7d.start, r.k7d.end, r.k7d.title);
    const mtd = buildPeriod(db, r.mtd_g.start, r.mtd_g.end, "ماه جاری شمسی (تا اکنون)");
    const prev = buildPeriod(db, r.prev_to_date_g.start, r.prev_to_date_g.end, "ماه قبل تا امروز");

    const cmp = {
        runKey: `all_channels_compare:${r.mtd_g.start}→${r.mtd_g.end}:${Date.now()}`,
        current: mtd,
        prev_to_date: prev,
        diff: {
            finance: diffObj(mtd.finance, prev.finance),
            whatsapp: diffObj(mtd.whatsapp, prev.whatsapp),
            instagram: diffObj(mtd.instagram, prev.instagram),
            clicks: diffObj(mtd.clicks, prev.clicks),
        },
        range_title: "مقایسهٔ ماه جاری تا امروز با ماه قبل تا همین روز",
    };

    const result = { k7d, mtd, cmp };
    printDeep("runAllChannelsPeriods.out", result);
    // برای حذف هرگونه [Object] در نماهای داخلی:
    printDeep("cmp.current", result.cmp.current);
    printDeep("cmp.prev_to_date", result.cmp.prev_to_date);
    printDeep("cmp.diff", result.cmp.diff);
    return result;
}

export default { runAllChannelsPeriods };
