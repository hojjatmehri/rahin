// ============================================================
// File: src/collectors/financialScoreDynamic.js
// Purpose: Dynamic Financial Score model based on transaction distribution
// Author: Hojjat Mehri (v2 - Percentile-based Scoring)
// ============================================================

import 'file:///E:/Projects/rahin/logger.js';
import Database from 'better-sqlite3';
import moment from 'moment-timezone';
import WhatsAppService from '../WhatsAppService.js';

const TZ = 'Asia/Tehran';
const DB_PATH = 'E:/Projects/AtighgashtAI/db_atigh.sqlite';
const MANAGER_MOBILE = process.env.DEV_ALERT_MOBILE || '09134052885';

// Helper
function percentile(arr, p) {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a,b)=>a-b);
  const idx = (p/100)*(sorted.length-1);
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + (sorted[upper]-sorted[lower])*(idx-lower);
}

// =============================== MAIN ===============================
export async function computeFinancialScoreDynamic() {
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  const now = moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss');
  console.log(`[FinancialScoreDynamic] 🚀 Started at ${now}`);

  // 1. دریافت مبالغ فروش معتبر
  const txns = db.prepare(`
    SELECT mobile, sellAmount AS amount, profit, payType1, payType2, isCanceled
    FROM transactions
    WHERE isCanceled=0 AND sellAmount>0
  `).all();

  if (txns.length === 0) {
    console.warn('[FinancialScoreDynamic] ⚠️ No valid transactions found.');
    db.close();
    return;
  }

  const amounts = txns.map(t => Number(t.amount) || 0).filter(v=>v>0);
  const p25 = percentile(amounts, 25);
  const p50 = percentile(amounts, 50);
  const p75 = percentile(amounts, 75);
  const p90 = percentile(amounts, 90);
  const max = Math.max(...amounts);

  console.log(`[FinancialScoreDynamic] Distribution → p25=${p25.toFixed(0)} p50=${p50.toFixed(0)} p75=${p75.toFixed(0)} p90=${p90.toFixed(0)} max=${max.toFixed(0)}`);

  // 2. گروه‌بندی مشتری‌ها
  const byMobile = new Map();
  for (const t of txns) {
    const key = String(t.mobile || '').replace(/[^\d]/g,'');
    if (key.length < 10) continue;
    const mob = '98' + key.slice(-10);
    if (!byMobile.has(mob)) byMobile.set(mob, []);
    byMobile.get(mob).push(t);
  }

  // 3. محاسبه امتیاز مالی پویا
  const upsert = db.prepare(`
    INSERT INTO customer_value (
      mobile, financial_score, total_amount, payments_count, last_payment_at, updated_at
    )
    VALUES (@mobile, @financial_score, @total_amount, @payments_count, datetime('now'), datetime('now'))
    ON CONFLICT(mobile) DO UPDATE SET
      financial_score = excluded.financial_score,
      total_amount = excluded.total_amount,
      payments_count = excluded.payments_count,
      last_payment_at = excluded.last_payment_at,
      updated_at = datetime('now');
  `);

  let updated = 0;
  for (const [mobile, arr] of byMobile.entries()) {
    const totalAmount = arr.reduce((a,b)=>a+Number(b.amount||0),0);
    const profit = arr.reduce((a,b)=>a+Number(b.profit||0),0);
    const payments = arr.length;
    const types = arr.map(x => `${x.payType1||''},${x.payType2||''}`).join(',');
    const hasCash = /نقد/.test(types);
    const hasCredit = /(چک|اقساط|اعتباری)/.test(types);

    let baseScore = 0;
    if (totalAmount >= p90) baseScore = 100;
    else if (totalAmount >= p75) baseScore = 85;
    else if (totalAmount >= p50) baseScore = 65;
    else if (totalAmount >= p25) baseScore = 40;
    else baseScore = 20;

    if (hasCash) baseScore *= 1.1;
    else if (hasCredit) baseScore *= 0.9;

    const profitBonus = Math.min(20, (profit / Math.max(totalAmount,1)) * 100 / 5);
    const loyaltyBonus = Math.min(20, payments * 5);

    const financialScore = Math.min(100, baseScore + profitBonus + loyaltyBonus);

    upsert.run({
      mobile,
      financial_score: financialScore,
      total_amount: totalAmount,
      payments_count: payments,
      last_payment_at: moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss')
    });

    updated++;
  }

  db.close();
  console.log(`[FinancialScoreDynamic] ✅ Updated ${updated} records.`);

  // 4. خلاصه آماری و گزارش واتساپ
  const db2 = new Database(DB_PATH);
  const summary = db2.prepare(`
    SELECT
      CASE
        WHEN financial_score >= 85 THEN 'پلاتینیوم'
        WHEN financial_score >= 65 THEN 'طلایی'
        WHEN financial_score >= 40 THEN 'نقره‌ای'
        ELSE 'برنزی'
      END AS tier,
      COUNT(*) AS cnt,
      ROUND(AVG(financial_score),1) AS avg_score,
      ROUND(AVG(total_amount),0) AS avg_amount
    FROM customer_value
    GROUP BY tier
    ORDER BY avg_score DESC;
  `).all();
  db2.close();

  console.table(summary);

  let text = `📊 گزارش امتیاز مالی پویا (${moment().tz(TZ).format('YYYY-MM-DD HH:mm')})\n\n`;
  for (const s of summary) {
    text += `${s.tier}: ${s.cnt} مشتری، میانگین امتیاز ${s.avg_score}، میانگین خرید ${s.avg_amount.toLocaleString()} تومان\n`;
  }

  await WhatsAppService.sendMessage('98' + MANAGER_MOBILE.replace(/^0/, ''), text)
  .then(() => console.log('[FinancialScoreDynamic] ✅ WhatsApp summary sent.'))
  .catch(e => console.error('[FinancialScoreDynamic] ⚠️ WhatsApp send failed:', e.message));

}

// CLI trigger
if (process.argv[1].endsWith('financialScoreDynamic.js')) {
  computeFinancialScoreDynamic();
}
