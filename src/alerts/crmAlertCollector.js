// ========================================================
// File: src/alerts/crmAlertCollector.js
// Author: Hojjat Mehri
// Role: Collect CRM inactivity alerts, log results, record sync_health, and send summary via WhatsApp
// ========================================================

import sqlite3 from "sqlite3";
import { open } from "sqlite";
import moment from "moment-timezone";
import dotenv from "dotenv";
import { shouldSuppressCrossChannel } from "./crmAlertSuppressor.js";
import { sendWhatsapp } from "../lib/whatsapp/sendWhatsapp.js";

dotenv.config();

const MOD = "[CrmAlertCollector]";
const TZ = "Asia/Tehran";
const DB_PATH = process.env.SQLITE_DB_PATH || "E:/Projects/AtighgashtAI/db_atigh.sqlite";
const DEV_MOBILE = process.env.DEV_ALERT_MOBILE || "";
const SEND_REAL = String(process.env.SEND_REAL_ALERTS || "0") === "1";
const ALERT_GRACE_DAYS = Number(process.env.ALERT_GRACE_DAYS || 14);

const log = (...a) => console.log(MOD, ...a);
const err = (...a) => console.error(MOD, ...a);

export async function collectCrmAlerts() {
  const start = Date.now();
  const now = moment().tz(TZ);
  const db = await open({ filename: DB_PATH, driver: sqlite3.Database });
  log(`${now.format("YYYY-MM-DD HH:mm:ss")} 🚀 Collector started for CRM alerts`);

  let created = 0, extended = 0, suppressed = 0, skipped = 0;

  try {
    // --- Step 1: واکشی داده‌ها از ویوی خلاصه ارتباط ---
    const rows = await db.all(`
      SELECT contact_mobile AS contact_id, name, last_activity_at, in_count_30d, out_count_30d
      FROM v_contact_comm_summary
      WHERE contact_mobile IS NOT NULL
    `);
    

    const cutoff = now.clone().subtract(ALERT_GRACE_DAYS, "days");
    const candidates = rows.filter(r => {
      if (!r.last_activity_at) return true;
      const last = moment.tz(r.last_activity_at, TZ);
      return last.isBefore(cutoff) && (r.in_count_30d == 0 && r.out_count_30d == 0);
    });

    log(`Found ${candidates.length} potential inactive contacts`);

    // --- Step 2: بررسی هر مخاطب ---
    for (const c of candidates) {
      try {
        const cross = await shouldSuppressCrossChannel(c.contact_id);
        if (cross.suppressed) {
          suppressed++;
          continue;
        }

        const existing = await db.get(`
          SELECT id, suppress_until FROM didar_crm_alerts
          WHERE contact_id = ? AND alert_type = 'inactive_contact'
          ORDER BY created_at DESC LIMIT 1
        `, [c.contact_id]);

        if (existing) {
          const until = moment(existing.suppress_until).tz(TZ);
          if (until.isAfter(now)) {
            skipped++;
            continue;
          } else {
            const newUntil = now.clone().add(7, "days").format("YYYY-MM-DD HH:mm:ss");
            await db.run(`
              UPDATE didar_crm_alerts 
              SET suppress_until = ?, extended_count = COALESCE(extended_count,0)+1 
              WHERE id = ?
            `, [newUntil, existing.id]);
            extended++;
            continue;
          }
        }

        await db.run(`
          INSERT INTO didar_crm_alerts 
          (contact_id, alert_type, message, created_at, suppress_until, meta_json)
          VALUES (?, 'inactive_contact', ?, ?, ?, ?)
        `, [
          c.contact_id,
          `مخاطب ${c.name || "بدون‌نام"} در ${ALERT_GRACE_DAYS} روز اخیر هیچ فعالیتی نداشته است.`,
          now.format("YYYY-MM-DD HH:mm:ss"),
          now.clone().add(7, "days").format("YYYY-MM-DD HH:mm:ss"),
          JSON.stringify({ last_activity_at: c.last_activity_at })
        ]);
        created++;
      } catch (inner) {
        err(`⚠️ Skipped contact ${c.contact_id}: ${inner.message}`);
        continue;
      }
    }

    // --- Step 3: ثبت در sync_health ---
    const duration = Date.now() - start;
    await db.run(`
      INSERT INTO sync_health (
        module_name, executed_at, created_count, extended_count, suppressed_count, skipped_count, duration_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `, [
      "crm_alerts_daily",
      now.format("YYYY-MM-DD HH:mm:ss"),
      created, extended, suppressed, skipped, duration
    ]);

    // --- Step 4: ساخت متن خلاصه و ارسال واتساپ ---
    const msg = [
      `📊 گزارش اجرای جمع‌آوری هشدار CRM`,
      `⏰ ${now.format("YYYY-MM-DD HH:mm")}`,
      `🧩 ماژول: crm_alerts_daily`,
      `ساخته‌شده: ${created}`,
      `تمدیدشده: ${extended}`,
      `ساکت‌شده: ${suppressed}`,
      `ردشده: ${skipped}`,
      `⏱ مدت‌زمان اجرا: ${duration} ms`
    ].join("\n");

    if (!SEND_REAL && DEV_MOBILE) {
      await sendWhatsapp(DEV_MOBILE, msg);
      log(`📨 Sent summary to DEV (${DEV_MOBILE})`);
    } else if (SEND_REAL) {
      log(`📨 Real alerts enabled — ready for production.`);
    } else {
      log(`⚠️ No DEV_ALERT_MOBILE set. Message skipped.`);
    }

    log(`✅ Summary — Created: ${created}, Extended: ${extended}, Suppressed: ${suppressed}, Skipped: ${skipped}`);
  } catch (e) {
    err(`❌ Error in collectCrmAlerts: ${e.stack || e.message}`);
  } finally {
    try { await db.close(); } catch {}
    log("🧱 Database closed.");
  }
}
