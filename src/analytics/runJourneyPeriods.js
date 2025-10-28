import '../../logger.js';
// src/analytics/runJourneyPeriods.js
import Database from "better-sqlite3";
import moment from "moment-timezone";
import { aggregateJourney } from "./journeyAnalytics.js";
import { startOfCurrentJMonthISO, startOfPrevJMonthISO, nowTehISO, prevMonthToNowISO } from "../utils/jalali.js";
import { nowStamp } from "../utils/time.js";
import { CONFIG } from "../config/Config.js";

const TZ = "Asia/Tehran";

function upsertPeriodAggregate(db, kind, startIso, endIso, payload) {
  const runKey = `${kind}:${startIso}→${endIso}:${Date.now()}`;
  db.prepare(`
    INSERT INTO rahin_period_aggregates (created_at, run_key, period_kind, start_ymd, end_ymd, payload_json)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(
    nowStamp(),
    runKey,
    kind,
    startIso.slice(0,10),
    endIso.slice(0,10),
    JSON.stringify(payload)
  );
  return runKey;
}

export function formatMgmtSummary(o) {
  const fmt = (n) => new Intl.NumberFormat('fa-IR').format(n||0);
  const p = (x)=> (x*100).toFixed(1).replace('.0','');
  return [
    `جرنی سایت — خلاصه`,
    `— بازه: ${o.range_title}`,
    `بازدیدکننده یکتا: ${fmt(o.unique_visitors)}`,
    `تعداد رویدادها: ${fmt(o.events)}`,
    `رویدادهای درگیر (dwell>0): ${fmt(o.engaged_events)} (${p(o.engagement_rate)}٪)`,
    `میانگین dwell/رویداد: ${fmt(o.avg_dwell)} ثانیه`,
  ].join('\n');
}

export function diffBlocks(curr, prev) {
  // prev و curr هر دو خروجی aggregateJourney
  const safe = (x)=> (x || 0);
  function pct(a,b){ return b ? ((a-b)/b)*100 : (a? +Infinity : 0); }
  return {
    events:       { curr: safe(curr.events), prev: safe(prev.events), delta_pct: pct(curr.events, prev.events) },
    visitors:     { curr: safe(curr.unique_visitors), prev: safe(prev.unique_visitors), delta_pct: pct(curr.unique_visitors, prev.unique_visitors) },
    engaged:      { curr: safe(curr.engaged_events), prev: safe(prev.engaged_events), delta_pct: pct(curr.engaged_events, prev.engaged_events) },
    avg_dwell:    { curr: safe(curr.avg_dwell_per_event_sec), prev: safe(prev.avg_dwell_per_event_sec), delta_pct: pct(curr.avg_dwell_per_event_sec, prev.avg_dwell_per_event_sec) },
    engagement_r: { curr: safe(curr.engagement_rate), prev: safe(prev.engagement_rate), delta_pct: pct(curr.engagement_rate, prev.engagement_rate) },
  };
}

export async function runJourneyPeriods() {
  const db = CONFIG.db.raw || new Database(CONFIG.DB_PATH_MAIN || "E:\\Projects\\AtighgashtAI\\db_atigh.sqlite");
  try {
    // 1) ۷ روز اخیر
    const endIso = nowTehISO();
    const start7d = moment.tz(endIso, TZ).subtract(7, "days").toISOString();
    const k7d = aggregateJourney(db, start7d, endIso);
    const runKey7d = upsertPeriodAggregate(db, "journey_7d", start7d, endIso, k7d);

    // 2) MTD ماه جلالی جاری
    const startMonth = startOfCurrentJMonthISO();
    const kmtd = aggregateJourney(db, startMonth, endIso);
    const runKeyMTD = upsertPeriodAggregate(db, "journey_mtd_jalali", startMonth, endIso, kmtd);

    // 3) PrevMonth-to-Date (همتا با امروز)
    const { startPrev, endPrevEqNow } = prevMonthToNowISO();
    const kprev = aggregateJourney(db, startPrev, endPrevEqNow);
    const kcmp = { current: kmtd, prev_to_date: kprev, diff: diffBlocks(kmtd, kprev) };
    const runKeyCMP = upsertPeriodAggregate(db, "journey_prev_mtd_jalali_compare", startMonth, endIso, kcmp);

    return {
      k7d: { runKey: runKey7d, ...k7d, range_title: "۷ روز اخیر" },
      mtd: { runKey: runKeyMTD, ...kmtd, range_title: "ماه جاری شمسی (تا اکنون)" },
      cmp: { runKey: runKeyCMP, ...kcmp, range_title: "مقایسه ماه قبل تا امروز با ماه جاری تا امروز" }
    };
  } finally {
    if (!CONFIG.db.raw) db.close();
  }
}

