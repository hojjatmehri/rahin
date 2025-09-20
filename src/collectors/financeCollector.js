// src/collectors/financeCollector.js
// جمع‌آوری داده‌های مالی (امروز، پرداخت‌ها، بدهی مشتری، فعالیت ۷روز، سرویس‌های برتر، روش‌های پرداخت)

import { get as dbGet, all as dbAll } from '../db/db.js';
// اگر این دو فایل را داری فعال بمانند؛ اگر نه فعلاً کامنت کن تا بدون enrich هم کار کند.
import { rebuildDestinationDictionary } from '../destination/destinationModel.js';
import { inferDestination } from '../destination/destinationInfer.js';

async function tableExists(name) {
  const row = await dbGet(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, [name]);
  return !!row;
}

export async function collectFinance() {
  const out = { sales: {}, finance: {}, top_services: [], payment_methods_today: [] };

  // بازسازی واژه‌نامه مقصد (ایمن است اگر سنگین نباشد)
  try {
    if (typeof rebuildDestinationDictionary === 'function') {
      await rebuildDestinationDictionary({ days: 90 });
    }
  } catch (e) {
    out._note_fin0 = e.message;
  }

  // فروش/سود/تعداد سفارش امروز
  try {
    const s = await dbGet(`
      SELECT
        IFNULL(SUM(sellAmount),0) AS total_sell_today,
        IFNULL(SUM(buyAmount ),0) AS total_buy_today,
        IFNULL(SUM(profit    ),0) AS total_profit_today,
        COUNT(*)                  AS orders_today,
        AVG(sellAmount)           AS avg_order_value
      FROM transactions
      WHERE date(regDate)=date('now','localtime')
    `);
    out.sales = {
      total_sales_today: Number(s?.total_sell_today || 0),
      total_buy_today: Number(s?.total_buy_today || 0),
      profit_today: Number(s?.total_profit_today || 0),
      orders_today: Number(s?.orders_today || 0),
      avg_order_value: Number(s?.avg_order_value || 0),
      income_rate_pct:
        Number(s?.total_sell_today || 0) > 0
          ? Number(((s.total_profit_today / s.total_sell_today) * 100).toFixed(2))
          : 0,
    };
  } catch (e) {
    out._note_fin1 = e.message;
  }

  // پرداخت‌های امروز
  try {
    const p = await dbGet(`
      SELECT
        IFNULL(SUM(CASE WHEN date(payDate1)=date('now','localtime') THEN paidAmount1 ELSE 0 END),0) +
        IFNULL(SUM(CASE WHEN date(payDate2)=date('now','localtime') THEN paidAmount2 ELSE 0 END),0) AS paid_today
      FROM transactions
      WHERE (payDate1 IS NOT NULL AND payDate1<>'') OR (payDate2 IS NOT NULL AND payDate2<>'')
    `);
    out.finance.paid_today = Number(p?.paid_today || 0);
  } catch (e) {
    out._note_fin2 = e.message;
  }

  // بدهی مشتریان امروز
  try {
    const d = await dbGet(`
      SELECT IFNULL(SUM(customerDebt),0) AS customer_debt_today
      FROM transactions
      WHERE date(regDate)=date('now','localtime')
    `);
    out.finance.customer_debt_today = Number(d?.customer_debt_today || 0);
  } catch (e) {
    out._note_fin3 = e.message;
  }

  // فعالیت مالی ۷روز گذشته
  try {
    const a = await dbGet(`
      SELECT CASE WHEN EXISTS(
        SELECT 1 FROM transactions WHERE datetime(regDate) >= datetime('now','-7 days','localtime')
      ) THEN 'ACTIVE_7D' ELSE 'STALE' END AS fin_7d
    `);
    out.finance.fin_activity_7d = a?.fin_7d || 'STALE';
  } catch (e) {
    out._note_fin4 = e.message;
  }

  // سرویس‌های برتر امروز (با enrich مقصد در صورت موجود بودن ماژول‌ها)
  try {
    const rows = await dbAll(`
      SELECT serviceTitle, COUNT(*) AS cnt, SUM(sellAmount) AS sum_sell
      FROM transactions
      WHERE date(regDate)=date('now','localtime')
      GROUP BY serviceTitle
      ORDER BY sum_sell DESC
      LIMIT 10
    `);

    const enriched = [];
    for (const r of rows || []) {
      try {
        let dest = { destination_code: null, confidence: null, source: 'na' };
        if (typeof inferDestination === 'function') {
          dest = await inferDestination(r.serviceTitle);
        }
        enriched.push({
          serviceTitle: r.serviceTitle,
          destination_code: dest.destination_code,
          dest_confidence: dest.confidence,
          cnt: Number(r.cnt || 0),
          sum_sell: Number(r.sum_sell || 0),
          source: dest.source,
        });
      } catch {
        enriched.push({
          serviceTitle: r.serviceTitle,
          destination_code: null,
          dest_confidence: null,
          cnt: Number(r.cnt || 0),
          sum_sell: Number(r.sum_sell || 0),
          source: 'na',
        });
      }
    }
    out.top_services = enriched;
  } catch (e) {
    out._note_fin5 = e.message;
  }

  // توزیع روش‌های پرداخت امروز
  try {
    const dist = await dbAll(`
      SELECT payType AS method, COUNT(*) AS cnt FROM (
        SELECT payType1 AS payType, payDate1 AS payDate FROM transactions
        UNION ALL
        SELECT payType2 AS payType, payDate2 AS payDate FROM transactions
      )
      WHERE payType IS NOT NULL AND payType<>'' AND date(payDate)=date('now','localtime')
      GROUP BY payType
      ORDER BY cnt DESC
    `);
    out.payment_methods_today = (dist || []).map(x => ({
      method: x.method,
      cnt: Number(x.cnt || 0),
    }));
  } catch (e) {
    out._note_fin6 = e.message;
  }

  return out;
}
