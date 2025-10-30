// @ts-check
// node src/analytics/runAllChannelsAndAnalyze.mjs

import 'dotenv/config.js';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
// @ts-ignore
import { db } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';
import util from 'node:util';

// --------------------------- اتصال اولیه مطمئن ---------------------------
try {
  const test = db.prepare("SELECT datetime('now','localtime') AS now").get();
  console.log(`[DB] ✅ Shared connection verified @ ${test.now}`);
} catch (e) {
  console.error(`[DB] ❌ Connection failed before collectors: ${e.message}`);
  process.exit(1);
}

// --------------------------- کالکتورها ---------------------------
import { collectWhatsApp, whatsappClickInsightsShort } from '../collectors/whatsappCollector.js';
import { collectPDF } from '../collectors/pdfCollector.js';
import { collectInstagram } from '../collectors/instagramCollector.js';
import { collectFinance } from '../collectors/financeCollector.js';

/* ----------------- ENV & FLAGS ----------------- */
const TZ = process.env.TZ || 'Asia/Tehran';
const SQL_DEBUG = String(process.env.SQL_DEBUG || '0') === '1';

/* ----------------- HELPERS ----------------- */
function log(...a) { console.log(...a); }
function warn(...a) { console.warn(...a); }

function isDirectRun(importMetaUrl) {
  const thisPath = fileURLToPath(importMetaUrl);
  const argv1 = process.argv[1] ? path.resolve(process.argv[1]) : '';
  return path.resolve(thisPath) === argv1;
}

// ---------------- Debug utils ----------------
export function dump(label, obj) {
  if (!SQL_DEBUG) return;
  console.log(label);
  console.log(util.inspect(obj, {
    depth: null,
    colors: false,
    maxArrayLength: Infinity,
    compact: false,
    breakLength: 120,
  }));
}

export function j(obj) {
  const seen = new WeakSet();
  return JSON.stringify(
    obj,
    (k, v) => {
      if (typeof v === 'bigint') return v.toString();
      if (typeof v === 'object' && v !== null) {
        if (seen.has(v)) return '[Circular]';
        seen.add(v);
      }
      return v;
    },
    2
  );
}

/* -------------- متن‌ساز کالکتورها -------------- */
const faDigits = '۰۱۲۳۴۵۶۷۸۹';
const fmtNumFa = (n) => {
  const x = Number.isFinite(+n) ? Math.round(+n).toString() : String(n ?? 0);
  return x.replace(/\d/g, d => faDigits[d]);
};
const fmtPctFa = (p) => `${Number(p || 0).toFixed(1)}`.replace(/\d/g, d => faDigits[d]) + '٪';

function buildCollectorsText({ wa, wai, pdf, ig, fin }) {
  const lines = [];
  lines.push('خلاصهٔ کالکتورها:');

  if (wa) {
    lines.push(`- واتساپ:
  امروز: پیام‌های ورودی ${fmtNumFa(wa.inbound_today)} | مخاطبان یکتا ${fmtNumFa(wa.unique_contacts_today)}
  ۷ روز اخیر: ورودی ${fmtNumFa(wa.inbound_7d)} | بازدیدکنندگان مپ‌شده ${fmtNumFa(wa.mapped_visitors_7d)}`);
  }

  if (wai) {
    const srcs = (wai.top_sources || []).map(s => `${s.source}: ${fmtNumFa(s.cnt)}`).join(' ، ');
    const pages = (wai.top_pages || []).map(p => `${p.page}: ${fmtNumFa(p.cnt)}`).join(' ، ');
    lines.push(`- بینش کلیک واتساپ:
  نرخ کلیک واتساپ: ${fmtNumFa(wai.wa_click_rate)}٪
  برترین سورس‌ها: ${srcs || '—'}
  برترین صفحات: ${pages || '—'}`);
  }

  if (pdf) {
    const today = (pdf.today_by_status || []).map(s => `${s.status}: ${fmtNumFa(s.cnt)}`).join(' ، ');
    const s7 = (pdf.last_7d_by_status || []).map(s => `${s.status}: ${fmtNumFa(s.cnt)}`).join(' ، ');
    lines.push(`- PDF:
  توزیع وضعیت‌ها امروز: ${today || '—'}
  توزیع وضعیت‌ها ۷روز: ${s7 || '—'}`);
  }

  if (ig) {
    const byTypeStr = (ig.by_type || []).map(r => `${r.event_type}: ${fmtNumFa(r.cnt)}`).join(' ، ');
    lines.push(`- اینستاگرام:
  رویدادهای امروز: ${fmtNumFa(ig.dev_events_today)}
  ۷ روز اخیر: ${fmtNumFa(ig.dev_events_7d)}
  امروز به تفکیک نوع: ${byTypeStr || '—'}`);
  }

  if (fin) {
    const s = fin.sales || {};
    const pay = fin.payment_methods_today || [];
    const payStr = pay.map(p => `${p.method}: ${fmtNumFa(p.cnt)}`).join(' ، ');
    lines.push(`- مالی:
  فروش امروز: مبلغ فروش ${fmtNumFa(s.total_sales_today)} | سود ${fmtNumFa(s.profit_today)} | تعداد سفارش ${fmtNumFa(s.orders_today)} | میانگین سبد ${fmtNumFa(s.avg_order_value)} | نرخ سود ${fmtPctFa(s.income_rate_pct)}
  پرداخت امروز: ${fmtNumFa(fin.finance?.paid_today || 0)}
  بدهی مشتری امروز: ${fmtNumFa(fin.finance?.customer_debt_today || 0)}
  وضعیت ۷روز: ${fin.finance?.fin_activity_7d || '—'}
  روش‌های پرداخت امروز: ${payStr || '—'}`);
  }

  return lines.join('\n');
}

/** هستهٔ اجرا – فقط کالکتورها */
export async function generateAllChannelsAnalysisText() {
  try {
    let wa = {}, wai = {}, pdf = {}, ig = {}, fin = {};
    try { wa = await collectWhatsApp(); } catch (e) { warn('[collector] WhatsApp:', e?.message || e); }
    try { wai = await whatsappClickInsightsShort(); } catch (e) { warn('[collector] WA Insights:', e?.message || e); }
    try { pdf = await collectPDF(); } catch (e) { warn('[collector] PDF:', e?.message || e); }
    try { ig = await collectInstagram(); } catch (e) { warn('[collector] Instagram:', e?.message || e); }
    try { fin = await collectFinance(); } catch (e) { warn('[collector] Finance:', e?.message || e); }

    return buildCollectorsText({ wa, wai, pdf, ig, fin });
  } finally {
    // اتصال singleton را نمی‌بندیم
  }
}

/* ------------- CLI ------------- */
if (isDirectRun(import.meta.url)) {
  (async () => {
    if (SQL_DEBUG) log(`[env] SQL_DEBUG=1 (TZ=${TZ})`);
    const txt = await generateAllChannelsAnalysisText();
    console.log('\n================= تحلیل تولیدشده =================\n');
    console.log(txt);
    console.log('\n==================================================\n');
  })().catch(err => {
    console.error('ERROR:', err?.message || err);
    process.exit(1);
  });
}
