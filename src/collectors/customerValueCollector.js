// ============================================================
// File: src/collectors/customerValueCollector.js
// Purpose: محاسبه امتیاز ارزش مشتری بر اساس CRM + WhatsApp + Transactions
// Author: Hojjat Mehri (v10 – Final Stable)
// ============================================================

import 'file:///E:/Projects/rahin/logger.js';
import { db } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';
import moment from 'moment-timezone';

const MOD = '[CustomerValueCollector]';
const dbPath = 'E:/Projects/AtighgashtAI/db_atigh.sqlite';
const TZ = 'Asia/Tehran';
const log = (...a) => console.log(MOD, ...a);

// ========== Helpers ==========
function normalizeMobile(num = '') {
  const digits = String(num).replace(/[^\d]/g, '');
  if (digits.length < 10) return null;
  const last10 = digits.slice(-10);
  return '98' + (last10.startsWith('9') ? last10 : last10.slice(1));
}

function calcCRMStageScore(stage = '') {
  const s = (stage || '').toLowerCase();
  if (s.includes('won') || s.includes('موفق')) return 40;
  if (s.includes('pending') || s.includes('در حال')) return 20;
  if (s.includes('new') || s.includes('جدید')) return 10;
  return 0;
}

function calcWhatsAppScore(countLast30d = 0) {
  return Math.min(100, countLast30d * 5);
}

function calcFinancialScore(amount = 0, profit = 0, payments = 0, types = []) {
  if (amount <= 0) return 0;
  let base = Math.min(100, Math.log10(amount + 1) * 25);
  const hasCash = types.some(t => t.includes('نقد'));
  const hasCredit = types.some(t => t.includes('چک') || t.includes('اقساط') || t.includes('اعتباری'));
  if (hasCash) base *= 1.1;
  else if (hasCredit) base *= 0.9;
  base += Math.min(20, ((profit / Math.max(amount, 1)) * 100) / 5); // سود بیشتر → امتیاز بیشتر
  base += Math.min(30, payments * 5); // تعداد پرداخت‌ها
  return Math.min(100, base);
}

function rankLabel(score) {
  if (score >= 85) return 'پلاتینیوم';
  if (score >= 65) return 'طلایی';
  if (score >= 40) return 'نقره‌ای';
  return 'برنزی';
}

// ========== Main ==========
export function collectCustomerValue() {

  // جدول نهایی
  db.exec(`
    CREATE TABLE IF NOT EXISTS customer_value (
      mobile TEXT PRIMARY KEY,
      value_score REAL,
      whatsapp_score REAL,
      crm_stage_score REAL,
      financial_score REAL,
      total_interactions INTEGER,
      total_amount REAL,
      payments_count INTEGER,
      last_payment_at TEXT,
      rank_label TEXT,
      updated_at TEXT
    );
  `);

  const now = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
  log('DB connected:', dbPath, '|', now);

  // موبایل‌های یکتا از CRM و Transactions
  const allMobiles = db.prepare(`
    SELECT DISTINCT m FROM (
      SELECT TRIM(mobile_phone) AS m FROM didar_contacts WHERE mobile_phone IS NOT NULL
      UNION
      SELECT TRIM(mobile) FROM transactions WHERE mobile IS NOT NULL
    ) WHERE m IS NOT NULL AND LENGTH(m) >= 10;
  `).all();
  log('Mobiles fetched:', allMobiles.length);

  // استیتمنت‌های آماده
  const waStmt = db.prepare(`
    SELECT COUNT(*) AS cnt
    FROM wa_messages_valid
    WHERE ts_millis IS NOT NULL
      AND LENGTH(phone_norm) >= 10
      AND substr(phone_norm, -10) = ?
      AND datetime(ts_millis / 1000, 'unixepoch') >= datetime('now', '-30 day')
  `);

  const contactIdStmt = db.prepare(`
    SELECT id FROM didar_contacts
    WHERE mobile_phone IS NOT NULL AND substr(mobile_phone, -10) = ?
    ORDER BY updated_at DESC LIMIT 1;
  `);

  const crmStageByContactIdStmt = db.prepare(`
    SELECT COALESCE(p1.title, p2.title, 'نامشخص') AS stage_title
    FROM didar_deals d
    LEFT JOIN didar_pipelines p1 ON d.pipeline_stage_id = p1.id
    LEFT JOIN didar_pipelines p2 ON d.pipeline_id = p2.id
    WHERE d.contact_id = ?
    ORDER BY d.updated_at DESC
    LIMIT 1;
  `);

  const txnStmt = db.prepare(`
    SELECT
      COALESCE(SUM(CASE WHEN isCanceled=0 THEN sellAmount ELSE 0 END), 0) AS total_amount,
      COALESCE(SUM(CASE WHEN isCanceled=0 THEN profit ELSE 0 END), 0) AS total_profit,
      COUNT(CASE WHEN isCanceled=0 THEN 1 END) AS purchases,
      GROUP_CONCAT(DISTINCT TRIM(payType1 || ',' || COALESCE(payType2,''))) AS all_types,
      COALESCE(MAX(created_at), MAX(regDateGregorian), MAX(regDate)) AS last_payment
    FROM transactions
    WHERE LENGTH(mobile) >= 10 AND substr(mobile, -10) = ?;
  `);

  const upsert = db.prepare(`
    INSERT INTO customer_value (
      mobile, value_score, whatsapp_score, crm_stage_score, financial_score,
      total_interactions, total_amount, payments_count, last_payment_at, rank_label, updated_at
    )
    VALUES (
      @mobile, @value_score, @whatsapp_score, @crm_stage_score, @financial_score,
      @total_interactions, @total_amount, @payments_count, @last_payment_at, @rank_label, datetime('now')
    )
    ON CONFLICT(mobile) DO UPDATE SET
      value_score = excluded.value_score,
      whatsapp_score = excluded.whatsapp_score,
      crm_stage_score = excluded.crm_stage_score,
      financial_score = excluded.financial_score,
      total_interactions = excluded.total_interactions,
      total_amount = excluded.total_amount,
      payments_count = excluded.payments_count,
      last_payment_at = excluded.last_payment_at,
      rank_label = excluded.rank_label,
      updated_at = datetime('now');
  `);

  let updated = 0, skipped = 0;

  const trx = db.transaction(() => {
    for (const row of allMobiles) {
      const norm = normalizeMobile(row.m);
      if (!norm) { skipped++; continue; }
      const last10 = norm.slice(-10);

      // WhatsApp
      const waCount = waStmt.get(last10)?.cnt ?? 0;
      const whatsappScore = calcWhatsAppScore(waCount);

      // CRM
      const contact = contactIdStmt.get(last10);
      const crmStage = contact ? (crmStageByContactIdStmt.get(contact.id)?.stage_title ?? '') : '';
      const crmStageScore = calcCRMStageScore(crmStage);

      // مالی
      const t = txnStmt.get(last10) ?? {};
      const totalAmount = Number(t.total_amount) || 0;
      const totalProfit = Number(t.total_profit) || 0;
      const paymentsCount = Number(t.purchases) || 0;
      const lastPaymentAt = t.last_payment || null;
      const typesRaw = t.all_types || '';
      const types = typesRaw.split(',').filter(x => !!x.trim());

      const financialScore = calcFinancialScore(totalAmount, totalProfit, paymentsCount, types);

      // شرط برای جلوگیری از over-score در مشتری‌های تک‌خریدی
      let valueScore =
        (crmStageScore ?? 0) * 0.3 +
        (whatsappScore ?? 0) * 0.3 +
        (financialScore ?? 0) * 0.4;

      if (paymentsCount < 2 || totalAmount < 10_000_000)
        valueScore = Math.min(valueScore, 30);

      const label = rankLabel(valueScore);

      upsert.run({
        mobile: norm,
        value_score: valueScore.toFixed(2),
        whatsapp_score: whatsappScore,
        crm_stage_score: crmStageScore,
        financial_score: financialScore,
        total_interactions: waCount,
        total_amount: totalAmount,
        payments_count: paymentsCount,
        last_payment_at: lastPaymentAt,
        rank_label: label
      });

      updated++;
    }
  });

  trx();

  log(`✅ Updated ${updated} records (skipped ${skipped})`);

  // ایجاد View
  db.exec(`
    DROP VIEW IF EXISTS v_customer_value_ranked;
    CREATE VIEW v_customer_value_ranked AS
    SELECT
      mobile,
      ROUND(value_score, 2) AS value_score,
      whatsapp_score,
      crm_stage_score,
      financial_score,
      total_interactions,
      total_amount,
      payments_count,
      last_payment_at,
      updated_at,
      rank_label
    FROM customer_value
    ORDER BY value_score DESC;
  `);


  log('✅ View v_customer_value_ranked refreshed.');
}

// CLI Run
if (process.argv[1].endsWith('customerValueCollector.js')) {
  collectCustomerValue();
}
