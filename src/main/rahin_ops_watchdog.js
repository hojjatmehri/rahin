// src/main/rahin_ops_watchdog.js

/**
 * Rahin Ops Watchdog
 * - اجرای زمان‌بندی
 * - جمع‌آوری داده‌های مالی، واتساپ، اینستاگرام، PDF
 * - تولید بینش مدیریتی و فنی با OpenAI
 * - ذخیره در DB + ارسال خلاصه در واتساپ
 */

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
import { sanitizeForWhatsApp, chunkText } from "../message/sanitize.js";
import { processRecentErrors } from "../ai/errorAnalyzer.js";
import { enrichVisitorMobilesFromClicks } from "../clicks/enrichVisitorFromClicks.js";

// ================== تنظیمات ==================
const MODEL = process.env.RAHIN_MODEL || "gpt-4o";
const DEST_MOBILE_RAW = process.env.WHATSAPP_DEST_MOBILE || "09134052885";

// ================== زمان‌بندی ۶ ساعته هم‌تراز با تهران ==================
const TEH_TZ = "Asia/Tehran";

/** محاسبهٔ آفست میلی‌ثانیهٔ «تهران» نسبت به UTC با Intl */
function tzOffsetMillis(tz = TEH_TZ) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    timeZoneName: "shortOffset",
    hour: "2-digit",
    minute: "2-digit",
  }).formatToParts(new Date());
  const tzn = parts.find((p) => p.type === "timeZoneName")?.value || "GMT+0";
  const m = tzn.match(/GMT([+-])(\d{1,2})(?::(\d{2}))?/i);
  if (!m) return 0;
  const sign = m[1] === "-" ? -1 : 1;
  const hh = parseInt(m[2] || "0", 10);
  const mm = parseInt(m[3] || "0", 10);
  return sign * ((hh * 60 + mm) * 60 * 1000);
}

/** فاصلهٔ زمانی تا مرز بعدی ۶ ساعته (۰۰، ۰۶، ۱۲، ۱۸ به وقت تهران) */
function msUntilNext6hBoundary(tz = TEH_TZ) {
  const now = Date.now();
  const off = tzOffsetMillis(tz);
  const tzNow = new Date(now + off); // اکنونِ تهران

  const Y = tzNow.getUTCFullYear();
  const M = tzNow.getUTCMonth();
  const D = tzNow.getUTCDate();
  const h = tzNow.getUTCHours();

  // ساعت فعلی «تهران» را به بلاک ۶ساعته بعدی گرد می‌کنیم
  const blockStarts = [0, 6, 12, 18];
  let nextHour = blockStarts.find((H) => H > h) ?? 0;
  let targetUtcMs = Date.UTC(Y, M, D, nextHour, 0, 0) - off;

  // اگر از ۱۸ گذشته‌ایم و nextHour صفر شد، فردای تهران ساعت ۰۰
  if (nextHour === 0 && h >= 18) {
    targetUtcMs = Date.UTC(Y, M, D + 1, 0, 0, 0) - off;
  }

  return Math.max(1000, targetUtcMs - now);
}

/** زمان‌بند: اجرای تابع در مرزهای ۶ ساعتهٔ تهران، با اجرای خودکار پس از هر بار اتمام */
function scheduleEvery6h(runFn) {
  const planNext = () => {
    const delay = msUntilNext6hBoundary(TEH_TZ);
    console.log(`⏰ اجرای بعدی تا ${Math.round(delay / 60000)} دقیقه دیگر (مرز ۶ساعتهٔ تهران).`);
    setTimeout(async () => {
      try {
        await runFn();
      } catch (e) {
        console.error("Scheduled run error:", e?.message || e);
      } finally {
        planNext(); // باززمان‌بندی برای مرز بعدی
      }
    }, delay);
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
  await processRecentErrors(); // آنالیز خطاها
  await enrichVisitorMobilesFromClicks(); // استخراج شماره موبایل از کلیک‌ها

  const currSig = await readCurrentSignature();
  const lastSig = await readLastSignature();
  const decision = shouldProceed(currSig, lastSig, 0.10);

  if (!decision.pass) {
    log(`[${nowStamp()}] skipped: no significant change`);
    return;
  }

  const input = {
    period: { kind: "today", date: todayYMD() },
    finance: await collectFinance(),
    whatsapp: await collectWhatsApp(),
    instagram: await collectInstagram(),
    pdf_dispatch: await collectPDF(),
    whatsapp_clicks: await whatsappClickInsightsShort(),
    notes: [],
  };

  const runKey = `${input.period.date}:${Date.now()}`;

  try {
    const { data, latency, tokensIn, tokensOut } = await getDualInsights(input);
    let faData = await ensurePersianJSON(data);
    faData.tech = postProcessTech(faData.tech || {});

    // ذخیره تحلیل در DB
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
        tokensIn,
        tokensOut,
        latency,
      ]
    );

    // واتساپ: ارسال گزارش
    const to = normalizeMobile(DEST_MOBILE_RAW);
    const mgmtMsg = buildManagementMessage(runKey, input.period.date, faData.management);
    const techSummary = buildTechSummaryMessage(runKey, input.period.date, faData.tech);

    for (const msg of [mgmtMsg, techSummary]) {
      for (const part of chunkText(sanitizeForWhatsApp(msg), 1200)) {
        await CONFIG.waService.sendMessage(to, part);
        await new Promise((r) => setTimeout(r, 600));
      }
    }

    // ===== اجرای درونی سناریوهای visitor بعد از ارسال گزارش =====
    try {
      const mod = await import("../scenarios/visitorScenarios.js"); // مسیر را در صورت نیاز تغییر بده
      if (typeof mod.runAllVisitorScenarios === "function") {
        log(`[${nowStamp()}] شروع اجرای سناریوهای visitor...`);
        await mod.runAllVisitorScenarios();
        log(`[${nowStamp()}] سناریوهای visitor با موفقیت اجرا شد.`);
      } else {
        log(`[${nowStamp()}] ماژول سناریو پیدا شد ولی تابع runAllVisitorScenarios وجود ندارد.`);
      }
    } catch (e) {
      log(`[${nowStamp()}] اجرای سناریوهای visitor ناموفق بود: ${e?.message || e}`);
    }

    await saveSignature(currSig);
    log(`[${nowStamp()}] ✓ run=${runKey} sent`);
  } catch (e) {
    log(`[${nowStamp()}] run=${runKey} ERROR=${e.message}`);
  }
}

// ================== main: اجرای فوری + زمان‌بندی ۶ ساعته ==================
async function main() {
  await ensureMinimalSchema();
  console.log("Rahin Ops Watchdog شروع شد…");
  console.log("Log:", path.resolve(process.env.RAHIN_LOG_FILE || "./rahin_ops.log"));

  // اجرای فوری (یک‌بار)
  await runOnce();

  // سپس اجرای هر ۶ ساعت بر اساس Asia/Tehran
  scheduleEvery6h(runOnce);
}

main().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});
