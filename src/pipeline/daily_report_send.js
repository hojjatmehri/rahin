// ========================================================
// File: src/pipeline/daily_report_send.js
// Purpose: Generate & send daily summary of visitor scenarios via WhatsApp
// Author: Hojjat Mehri
// ========================================================

import '../../logger.js';
import 'dotenv/config';
import Database from 'better-sqlite3';
import moment from 'moment-timezone';
import { waService } from '../config/Config.js';

const TZ = 'Asia/Tehran';
const DB_PATH = process.env.MAIN_DB_PATH || 'E:/Projects/AtighgashtAI/db_atigh.sqlite';
const MANAGER_MOBILE = process.env.WHATSAPP_OPERATOR || '989134052885';

// اتصال به DB
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// === ابزار کمکی
function fmt(ts) {
  return ts ? moment(ts).tz(TZ).format('YYYY-MM-DD HH:mm') : '-';
}

// === مرحله 1: جمع‌آوری آمار Guard
function getGuardStats() {
  return db.prepare(`
    SELECT status, COUNT(*) AS cnt, MAX(last_sent_at) AS last_time
    FROM scenario_send_guard
    GROUP BY status
    ORDER BY cnt DESC
  `).all();
}

// === مرحله 2: آمار کل پروفایل‌ها
function getProfileStats() {
  return db.prepare(`
    SELECT
      COUNT(*) AS total_profiles,
      SUM(CASE WHEN scenario_text IS NOT NULL THEN 1 ELSE 0 END) AS with_scenario,
      SUM(CASE WHEN didar_contact_id IS NULL THEN 1 ELSE 0 END) AS no_crm
    FROM person_unified_profile
  `).get();
}

// === مرحله 3: ساخت متن خلاصه فارسی
function buildReportText() {
  const now = moment().tz(TZ).format('YYYY-MM-DD HH:mm');
  const guards = getGuardStats();
  const prof = getProfileStats();

  let txt = `📊 گزارش روزانه راهین (${now})\n\n`;
  txt += `👤 مجموع پروفایل‌ها: ${prof.total_profiles}\n`;
  txt += `🧩 دارای سناریو: ${prof.with_scenario}\n`;
  txt += `❌ بدون CRM در دیدار: ${prof.no_crm}\n\n`;

  txt += `🛡️ وضعیت ارسال‌ها:\n`;
  for (const g of guards) {
    txt += `- ${g.status}: ${g.cnt} مورد (آخرین در ${fmt(g.last_time)})\n`;
  }

  return txt.trim();
}

// === مرحله 4: ارسال واتساپ
async function sendDailyReport() {
  const text = buildReportText();
  console.log('\n====== Daily Report ======\n' + text + '\n==========================\n');

  if (!waService?.sendMessage) {
    console.error('❌ WhatsApp service not available.');
    return;
  }

  try {
    await waService.sendMessage(MANAGER_MOBILE, text);
    console.log(`✅ WhatsApp report sent to ${MANAGER_MOBILE}`);
  } catch (err) {
    console.error('❌ Failed to send WhatsApp report:', err.message || err);
  }
}

// === مرحله 5: اجرای مستقیم یا با فلگ FORCE_RUN
if (process.env.FORCE_RUN === '1') {
  sendDailyReport();
} else {
  // اگر توسط PM2 با cron_restart اجرا شده، فقط همین تابع اجرا شود.
  console.log(`[DailyReport] Triggered by PM2 cron (${moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss')})`);
  sendDailyReport();
}
