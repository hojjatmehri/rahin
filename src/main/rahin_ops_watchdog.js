// import 'file:///E:/Projects/rahin/logger.js';
/**
 * Rahin Ops Watchdog
 * - اجرای زمان‌بندی
 * - جمع‌آوری داده‌های مالی، واتساپ، اینستاگرام، PDF
 * - تولید بینش مدیریتی و فنی با OpenAI
 * - ذخیره در DB + ارسال خلاصه در واتساپ
 * - (جدید) اجرای تحلیل چندکاناله داخل خود واچ‌داگ برای تست/ارسال
 */

import 'file:///E:/Projects/rahin/logger.js';
import dotenv from 'dotenv';
dotenv.config({ path: 'E:/Projects/AtighgashtAI/.env' });
import '../../AtighgashtAI/lib/health/channelEventListener.js';

import path from "path";
import { CONFIG } from "../config/Config.js";
import { nowStamp, todayYMD } from "../utils/time.js";
import { info as log } from "../logging/logger.js";
import { ensureMinimalSchema } from "../db/schemaGuard.js";
import { shouldProceed, readCurrentSignature, readLastSignature, saveSignature } from "../guards/proceedGuard.js";

import { collectFinance } from "../collectors/financeCollector.js";
import { collectWhatsApp, whatsappClickInsightsShort } from "../collectors/whatsappCollector.js";
import { collectInstagram } from "../collectors/instagramCollector.js";
import { collectPDF } from "../collectors/pdfCollector.js";

import { getDualInsights, ensurePersianJSON, postProcessTech } from "../ai/dualInsights.js";
import { buildManagementMessage, buildTechSummaryMessage } from "../message/messageBuilders.js";
import { sanitizeForWhatsApp, chunkText, forcePersianText } from "../message/sanitize.js";
import { processRecentErrors } from "../ai/errorAnalyzer.js";
import { enrichVisitorMobilesFromClicks } from "../clicks/enrichVisitorFromClicks.js";
import env from "../config/env.js";
import { openai } from "../config/Config.js";

import util from 'node:util';

// ⬅️ (جدید) تحلیل چندکاناله‌ی آماده‌مان
import { generateAllChannelsAnalysisText } from "../analytics/runAllChannelsAndAnalyze.mjs";
// ⬅️ (جدید) تزریق کانکشن better-sqlite3 به لایهٔ db
import { useBetterSqlite3 } from "../db/db.js";

export const SQL_DEBUG = String(process.env.SQL_DEBUG || '0') === '1';
const PRINT_PERIODIC_ANALYTICS = process.env.WATCHDOG_PRINT_ANALYTICS === '1';   // فقط چاپ در کنسول
const SEND_PERIODIC_ANALYTICS = process.env.WATCHDOG_SEND_ANALYTICS === '1';   // ارسال در واتساپ

/** چاپ آبجکت‌های بزرگ وقتی SQL_DEBUG=1 */
export function dump(label, obj) {
  if (!SQL_DEBUG) return;
  console.log(label);
  console.log(util.inspect(obj, { depth: null, colors: false, maxArrayLength: Infinity, compact: false, breakLength: 120 }));
}

export function j(obj) {
  const seen = new WeakSet();
  return JSON.stringify(obj, (k, v) => {
    if (typeof v === 'bigint') return v.toString();
    if (typeof v === 'object' && v !== null) { if (seen.has(v)) return '[Circular]'; seen.add(v); }
    return v;
  }, 2);
}

// اتصال آرشیو به کانکشن اصلی DB (برای جداول arch.*)
function ensureArchiveAttached(db) {
  const archPath = (env.ARCHIVE_DB_PATH || "E:\\Projects\\AtighgashtAI\\db_archive.sqlite")
    .replace(/'/g, "''");

  // better-sqlite3؟
  if (typeof db?.pragma === 'function' && typeof db?.exec === 'function') {
    try {
      const list = db.pragma('database_list', { simple: false }); // [{seq,name,file},...]
      const hasArch = Array.isArray(list) && list.some(x => String(x.name).toLowerCase() === 'arch');
      if (!hasArch) {
        db.exec(`ATTACH DATABASE '${archPath}' AS arch;`);
        console.log(`[arch] attached (better-sqlite3): ${archPath}`);
      }
      return;
    } catch (e) {
      console.error("[arch] better-sqlite3 attach failed:", e?.message || e);
    }
  }

  // sqlite3-promisified؟
  if (typeof db?.all === 'function' && typeof db?.run === 'function') {
    (async () => {
      try {
        const rows = await db.all("PRAGMA database_list;");
        const hasArch = Array.isArray(rows) && rows.some(r => String(r.name).toLowerCase() === 'arch');
        if (!hasArch) {
          await db.run(`ATTACH DATABASE '${archPath}' AS arch;`);
          console.log(`[arch] attached (sqlite3): ${archPath}`);
        }
      } catch (e) {
        console.error("[arch] sqlite3 attach failed:", e?.message || e);
      }
    })();
    return;
  }

  console.warn("[arch] Unknown DB driver; skipping attach.");
}


// ================== تنظیمات ==================
const MODEL = process.env.RAHIN_MODEL || "gpt-4o";
const DEST_MOBILE_RAW = process.env.WHATSAPP_DEST_MOBILE || "09134052885";
const FORCE_RUN = process.argv.includes('--force') || process.env.RAHIN_FORCE === '1';
const ONCE_ONLY = process.argv.includes('--once') || process.env.RAHIN_ONCE === '1';

// ================== زمان‌بندی دقیق در تهران (05:00, 11:00, 17:00, 23:50) ==================
const TEH_TZ = "Asia/Tehran";
const CUSTOM_ANCHORS = [[5, 0], [11, 0], [17, 0], [23, 50]];

function tzOffsetMillis(tz = TEH_TZ) {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: tz, timeZoneName: "shortOffset", hour: "2-digit", minute: "2-digit" }).formatToParts(new Date());
  const tzn = parts.find((p) => p.type === "timeZoneName")?.value || "GMT+0";
  const m = tzn.match(/GMT([+-])(\d{1,2})(?::(\d{2}))?/i);
  if (!m) return 0;
  const sign = m[1] === "-" ? -1 : 1;
  const hh = parseInt(m[2] || "0", 10);
  const mm = parseInt(m[3] || "0", 10);
  return sign * ((hh * 60 + mm) * 60 * 1000);
}
function msUntilNextAnchor(tz = TEH_TZ, anchors = CUSTOM_ANCHORS) {
  const nowUtc = Date.now();
  const off = tzOffsetMillis(tz);
  const tzNow = new Date(nowUtc + off);
  const Y = tzNow.getUTCFullYear(), M = tzNow.getUTCMonth(), D = tzNow.getUTCDate();
  const h = tzNow.getUTCHours(), m = tzNow.getUTCMinutes(), s = tzNow.getUTCSeconds();
  if (anchors.some(([H, Mm]) => H === h && Mm === m) && s < 3) return 1000;
  let next = anchors.find(([H, Mm]) => (h < H) || (h === H && m < Mm));
  let targetY = Y, targetM = M, targetD = D;
  if (!next) {
    next = anchors[0];
    const nextTzMidnightUtcMs = Date.UTC(Y, M, D + 1, 0, 0, 0) - off;
    const nextTzDate = new Date(nextTzMidnightUtcMs + off);
    targetY = nextTzDate.getUTCFullYear(); targetM = nextTzDate.getUTCMonth(); targetD = nextTzDate.getUTCDate();
  }
  const [targetH, targetMins] = next;
  const targetUtcMs = Date.UTC(targetY, targetM, targetD, targetH, targetMins, 0) - off;
  return Math.max(1000, targetUtcMs - nowUtc);
}
function scheduleAtAnchors(runFn, tz = TEH_TZ, anchors = CUSTOM_ANCHORS) {
  const planNext = () => {
    const delay = msUntilNextAnchor(tz, anchors);
    const nextUtc = Date.now() + delay;
    const nextTz = new Date(nextUtc + tzOffsetMillis(tz));
    const hh = String(nextTz.getUTCHours()).padStart(2, "0");
    const mm = String(nextTz.getUTCMinutes()).padStart(2, "0");
    console.log(`⏰ اجرای بعدی در تهران: ${nextTz.toLocaleDateString("fa-IR")} ${hh}:${mm}`);
    setTimeout(async () => { try { await runFn(); } catch (e) { console.error("Scheduled run error:", e?.message || e); } finally { planNext(); } }, delay);
  };
  planNext();
}

// ==== helpers ====
function normalizeMobile(m) {
  const digits = String(m || "").replace(/[^\d]/g, "");
  if (!digits) return null;
  if (digits.startsWith("98")) return digits;
  if (digits.startsWith("0")) return "98" + digits.slice(1);
  if (digits.startsWith("9")) return "98" + digits;
  return digits;
}

// ================== اجرای یک نوبت ==================
async function runOnce() {
  // ⬅️ مهم: اتصال واچ‌داگ را به لایهٔ db تزریق کن تا همهٔ کوئری‌ها روی همین کانکشن باشند
  // useBetterSqlite3(CONFIG.db);

  await processRecentErrors();
  await enrichVisitorMobilesFromClicks();

  const currSig = await readCurrentSignature();
  const lastSig = await readLastSignature();
  const decision = shouldProceed(currSig, lastSig, 0.10);

  if (!FORCE_RUN && !decision.pass) {
    log(`[${nowStamp()}] skipped: no significant change`);
    // حتی اگر اسکپ شد، اگر خواستی تحلیل را صرفاً برای تست چاپ کنی:
    if (PRINT_PERIODIC_ANALYTICS) {
      try {
        const text = await generateAllChannelsAnalysisText(null);
        console.log('\n================= تحلیل چندکاناله (واچ‌داگ) =================\n');
        console.log(text);
        console.log('\n================================================================\n');
      } catch (e) { console.error('[watchdog] analytics print error:', e?.message || e); }
    }
    return;
  }

  const period = { kind: "today", date: todayYMD() };
  const [finance, whatsapp, instagram, pdf_dispatch, whatsapp_clicks] = await Promise.all([
    collectFinance().catch(e => (log(`collectFinance ERR=${e?.message || e}`), {})),
    collectWhatsApp().catch(e => (log(`collectWhatsApp ERR=${e?.message || e}`), {})),
    collectInstagram().catch(e => (log(`collectInstagram ERR=${e?.message || e}`), {})),
    collectPDF().catch(e => (log(`collectPDF ERR=${e?.message || e}`), {})),
    whatsappClickInsightsShort().catch(e => (log(`whatsappClickInsightsShort ERR=${e?.message || e}`), {})),
  ]);

  // ⬅️ (جدید) تحلیل چندکاناله را همین‌جا بگیر (با همان DB)
  let periodicText = '';
  try {
    periodicText = await generateAllChannelsAnalysisText(null); // خودش better-sqlite3 باز می‌کنه
    if (PRINT_PERIODIC_ANALYTICS && periodicText) {
      console.log('\n================= تحلیل چندکاناله (واچ‌داگ) =================\n');
      console.log(periodicText);
      console.log('\n================================================================\n');
    }
  } catch (e) {
    console.error('[watchdog] analytics generate error:', e?.message || e);
  }

  const input = { period, finance, whatsapp, instagram, pdf_dispatch, whatsapp_clicks, notes: [] };
  const runKey = `${input.period.date}:${Date.now()}`;

  try {
    const { data, latency, tokensIn, tokensOut } = await getDualInsights(input);
    let faData = await ensurePersianJSON(data);
    faData.tech = postProcessTech(faData.tech || {});

    await CONFIG.db.run(
      `INSERT INTO rahin_dual_insights
        (created_at, run_key, period_date, metrics_json, mgmt_json, tech_json, model, tokens_in, tokens_out, latency_ms, error)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
      [
        nowStamp(),
        runKey,
        input.period.date,
        JSON.stringify(input),
        JSON.stringify(faData?.management || {}),
        JSON.stringify(faData?.tech || {}),
        MODEL,
        tokensIn || 0,
        tokensOut || 0,
        latency || 0,
      ]
    );

    // --- ارسال تفکیکی هر کانال ---
    try {
      // 1) از خروجی کالکتورها payload بساز
      const channelsPayload = {
        Finance: {
          today: {
            total_sales_today: finance?.sales?.total_sales_today ?? 0,
            total_buy_today: finance?.sales?.total_buy_today ?? 0,
            profit_today: finance?.sales?.profit_today ?? 0,
            orders_today: finance?.sales?.orders_today ?? 0,
            avg_order_value: finance?.sales?.avg_order_value ?? 0,
            income_rate_pct: finance?.sales?.income_rate_pct ?? 0,
            paid_today: finance?.finance?.paid_today ?? 0,
            customer_debt_today: finance?.finance?.customer_debt_today ?? 0,
          },
          k7d: {
            fin_activity_7d: finance?.finance?.fin_activity_7d ?? null,
            top_services: finance?.top_services ?? [],
            payment_methods_today: finance?.payment_methods_today ?? [],
          }
          // mtd / cmp اگر بعداً اضافه شد، اینجا هم بگذار
        },
        WhatsApp: {
          today: {
            inbound: whatsapp?.inbound_today ?? 0,
            unique_contacts: whatsapp?.unique_contacts_today ?? 0,
          },
          k7d: {
            inbound: whatsapp?.inbound_7d ?? 0,
            mapped_visitors: whatsapp?.mapped_visitors_7d ?? 0,
          },
        },
        Instagram: {
          today: { events: instagram?.dev_events_today ?? 0 },
          k7d: { events: instagram?.dev_events_7d ?? 0 },
          by_type_today: instagram?.by_type ?? [],
        },
        Clicks: {
          k7d: {
            wa_click_rate: whatsapp_clicks?.wa_click_rate ?? 0,
            top_sources: whatsapp_clicks?.top_sources ?? [],
            top_pages: whatsapp_clicks?.top_pages ?? [],
          },
        },
      };

      // 2) فراخوانی جدید: دیگر DB نمی‌دهیم؛ همان payload را بده
      const { analyzePerChannel } = await import("../analytics/perChannelAnalyze.js");
      const perChannel = await analyzePerChannel(channelsPayload, {
        model: MODEL,
        apiKey: process.env.OPENAI_API_KEY,
      });

      const to = normalizeMobile(DEST_MOBILE_RAW);
      const order = ["Finance", "WhatsApp", "Instagram", "Clicks"];

      if (to) {
        for (const name of order) {
          const header = (
            name === "Finance" ? "💰 «مالی»" :
              name === "WhatsApp" ? "💬 «واتساپ»" :
                name === "Instagram" ? "📷 «اینستاگرام»" :
                  "🖱️ «کلیک‌ها»"
          );
          let body = perChannel[name] ?? "فعلاً در دسترس نیست.";
          body = await forcePersianText(body);
          body = body.replace(/\n?فعلاً در دسترس نیست\.?\s*فعلاً در دسترس نیست\.?/g, "فعلاً در دسترس نیست");
// قالب‌بندی برای خوانایی: هر جمله در خط جدا
// قالب‌بندی برای خوانایی: هر جمله یا بخش توضیحی در خط جدا
body = body
  .replace(/([.:!؟])\s+/g, "$1\n")     // بعد از نقطه، دو نقطه، علامت سؤال و تعجب خط جدید بگذار
  .replace(/\n{3,}/g, "\n\n")          // حداکثر دو خط خالی متوالی
  .trim();


          const msg = `${header}\n${body}`;

          for (const part of chunkText(sanitizeForWhatsApp(msg), 1200)) {
            try {
              await CONFIG.waService.sendMessage(to, part);
              await new Promise(r => setTimeout(r, 600));
            } catch (e) {
              log(`[${nowStamp()}] WA send (${name}) ERR=${e?.message || e}`);
            }
          }
        }

        // ⬅️ (اختیاری) اگر خواستی همان «تحلیل چندکاناله» هم در واتساپ برود
        if (SEND_PERIODIC_ANALYTICS && periodicText) {
          const to2 = normalizeMobile(DEST_MOBILE_RAW);
          const msg2 = `📊 «خلاصهٔ چندکاناله»\n${periodicText}`;
          for (const part of chunkText(sanitizeForWhatsApp(msg2), 1200)) {
            await CONFIG.waService.sendMessage(to2, part);
            await new Promise(r => setTimeout(r, 600));
          }
        }

        log(`[${nowStamp()}] ✓ per-channel analyses sent.`);
      } else {
        log(`[${nowStamp()}] WARN: destination mobile invalid; skip per-channel send.`);
      }
    } catch (e) {
      log(`[${nowStamp()}] per-channel analyses ERROR=${e?.message || e}`);
    }

    await saveSignature(currSig);
    log(`[${nowStamp()}] ✓ run=${runKey} sent`);
  } catch (e) {
    log(`[${nowStamp()}] run=${runKey} ERROR=${e?.message || e}`);
  }
}

// ================== main ==================
function ensureArchiveAttachedIfNeeded() {
  ensureArchiveAttached(CONFIG.db);
}

async function main() {
  await ensureMinimalSchema();
  ensureArchiveAttachedIfNeeded(); // قبل از runOnce

  console.log("Rahin Ops Watchdog شروع شد…");
  console.log("Log:", path.resolve(process.env.RAHIN_LOG_FILE || "./rahin_ops.log"));

  // اجرای فوری (یک‌بار)
  await runOnce();

  // اگر --once نبود، برو روی زمان‌بندی
  const ONCE_ONLY = process.argv.includes('--once') || process.env.RAHIN_ONCE === '1';
  if (!ONCE_ONLY) {
    scheduleAtAnchors(runOnce, TEH_TZ, CUSTOM_ANCHORS);
  }
}

main().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});

