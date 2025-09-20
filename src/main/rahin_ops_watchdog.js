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
import { nowIso, todayYMD } from "../utils/time.js";
import { log } from "../logging/logger.js";
import { ensureMinimalSchema } from "../db/schemaGuard.js";
import { shouldProceed, readCurrentSignature, readLastSignature, saveSignature } from "../guards/proceedGuard.js";

import { collectFinance } from "../collectors/financeCollector.js";
import { collectWhatsApp, whatsappClickInsightsShort } from "../collectors/whatsappCollector.js";
import { collectInstagram } from "../collectors/instagramCollector.js";
import { collectPDF } from "../collectors/pdfCollector.js";

import { getDualInsights, ensurePersianJSON } from "../ai/dualInsights.js";
import { injectMandatoryAutomationRule } from "../ai/dualInsights.js";
import { dedupeTechAcrossSections, applyQualityFilters } from "../ai/dualInsights.js";

import { buildManagementMessage, buildTechSummaryMessage, buildTechItemMessages } from "../message/messageBuilders.js";
import { sanitizeForWhatsApp, chunkText } from "../message/sanitize.js";
import { processRecentErrors } from "../ai/errorAnalyzer.js";
import { enrichVisitorMobilesFromClicks } from "../clicks/enrichVisitorFromClicks.js";

// ================== تنظیمات ==================
const INTERVAL = Math.max(5, Number(process.env.RAHIN_INTERVAL_MIN || 60)); // دقیقه
const MODEL = process.env.RAHIN_MODEL || "gpt-4o";
const DEST_MOBILE_RAW = process.env.WHATSAPP_DEST_MOBILE || "09134052885";

// ================== اجرای یک نوبت ==================
async function runOnce() {
  await processRecentErrors(); // آنالیز خطاها
  await enrichVisitorMobilesFromClicks(); // استخراج شماره موبایل از کلیک‌ها

  const currSig = await readCurrentSignature();
  const lastSig = await readLastSignature();
  const decision = shouldProceed(currSig, lastSig, 0.10);

  if (!decision.pass) {
    log(`[${nowIso()}] skipped: no significant change`);
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
    faData.tech = injectMandatoryAutomationRule(faData.tech || {});
    faData.tech = dedupeTechAcrossSections(faData.tech || {});
    faData.tech = applyQualityFilters(faData.tech || {});

    // ذخیره تحلیل در DB
    await CONFIG.db.run(
      `INSERT INTO rahin_dual_insights
        (created_at, run_key, period_date, metrics_json, mgmt_json, tech_json, model, tokens_in, tokens_out, latency_ms, error)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
      [
        nowIso(),
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

    await saveSignature(currSig);
    log(`[${nowIso()}] ✓ run=${runKey} sent`);

  } catch (e) {
    log(`[${nowIso()}] run=${runKey} ERROR=${e.message}`);
  }
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

// ================== زمان‌بندی ==================
async function main() {
  await ensureMinimalSchema();
  const ms = INTERVAL * 60 * 1000;
  console.log(`پایش دوره‌ای فعال شد: هر ${INTERVAL} دقیقه`);
  await runOnce(); // اجرای فوری
  setInterval(runOnce, ms); // اجراهای بعدی
}

console.log("Rahin Ops Watchdog شروع شد…");
console.log("Log:", path.resolve(process.env.RAHIN_LOG_FILE || "./rahin_ops.log"));
main().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});
