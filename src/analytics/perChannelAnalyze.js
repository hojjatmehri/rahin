import '../../logger.js';
// ========================================================
// File: src/analytics/perChannelAnalyze.js
// Author: Hojjat Mehri
// Role: تولید تحلیل متنی برای هر کانال از روی payload کالکتورها
// ========================================================

import OpenAI from "openai";

/**
 * شکل expected payload:
 * {
 *   Finance:   { today: {...}, k7d: {...}, mtd?: {...}, cmp?: {...} },
 *   WhatsApp:  {...},
 *   Instagram: {...},
 *   Clicks:    {...}
 * }
 */

function toFa(n) {
  try { return Number(n ?? 0).toLocaleString("fa-IR"); }
  catch { return String(n ?? 0); }
}

// ساخت پرومپت برای GPT (برای کانال‌هایی غیر از مالی)
function buildStrictChannelPrompt(channel, payload) {
  const haveMtd = !!payload?.mtd;
  const haveCmp = !!(payload?.cmp && Object.keys(payload.cmp).length);

  return `
تو یک تحلیلگر کسب‌وکار هستی و فقط و فقط دربارهٔ کانال «${channel}» تحلیل بنویس.
- ساختار خروجی:
  پاراگراف ۱ «۷ روز اخیر» (اگر داده دارد) + ۲ پیشنهاد اجرایی مشخص.
  پاراگراف ۲ «ماه جاری تا امروز» (اگر داده در دسترس نیست، صریح بنویس «فعلاً در دسترس نیست»).
  خط سوم: مقایسه با ماه قبل (اگر نداریم بنویس فعلاً در دسترس نیست).
- خروجی کوتاه، فارسی و واتساپ‌پسند باشد.
- داده‌های عددی را فارسی بنویس.
  
داده‌های ${channel}:
${JSON.stringify(payload ?? {}, null, 2)}
`.trim();
}

/**
 * تحلیل تفکیکی برای چند کانال
 */
export async function analyzePerChannel(channelsPayload, { model = "gpt-4o", apiKey = process.env.OPENAI_API_KEY } = {}) {
  const openai = new OpenAI({ apiKey });
  const channels = ["Finance", "WhatsApp", "Instagram", "Clicks"];
  const out = {};

  for (const ch of channels) {
    const payload = channelsPayload?.[ch] || {};

    // ========================================================
    // 🔹 بخش مخصوص کانال مالی (بدون GPT)
    // ========================================================
    if (ch === "Finance") {
      const f = payload || payload.Finance || {};
      console.log("[FinanceAnalyzer] payload keys:", Object.keys(f));
      const sales = f.today || f.sales || {};
      const finance = f.finance || f.k7d || {};
      

      const todaySales = sales.total_sales_today || 0;
      const profit = sales.profit_today || 0;
      const orders = sales.orders_today || 0;
      const debt = finance.customer_debt_today || 0;
      const margin = sales.income_rate_pct || 0;
      const active7d = finance.fin_activity_7d || "STALE";

      // اگر اصلاً داده مالی ثبت نشده باشد
      if (todaySales === 0 && profit === 0 && orders === 0) {
        out[ch] = "ماه جاری تا امروز: فعلاً در دسترس نیست.";
        continue;
      }

      const weekLine =
        active7d === "ACTIVE_7D"
          ? `در ۷ روز اخیر، کانال مالی فعال بوده است.`
          : `در ۷ روز اخیر، فعالیت مالی خاصی ثبت نشده است.`;

      const suggestions =
        active7d === "ACTIVE_7D"
          ? "پیشنهاد: کمپین‌های تبلیغاتی و تحلیل مشتریان برای افزایش نرخ سود اجرا شود."
          : "پیشنهاد: پیگیری مجدد مشتریان غیرفعال و به‌روزرسانی فاکتورها توصیه می‌شود.";

      out[ch] = `
${weekLine} ${suggestions}

ماه جاری تا امروز:
فروش کل ${toFa(todaySales)} تومان
سود ${toFa(profit)} تومان (${toFa(margin)}٪)
تعداد سفارش ${toFa(orders)} فقره
بدهی مشتریان ${toFa(debt)} تومان
`.trim();

      continue;
    }

    // ========================================================
    // 🔹 سایر کانال‌ها (با GPT)
    // ========================================================
    const prompt = buildStrictChannelPrompt(ch, payload);
    try {
      const resp = await openai.chat.completions.create({
        model,
        temperature: 0.2,
        messages: [
          { role: "system", content: "دستیار تحلیلگر فارسی برای گزارش واتساپ؛ فقط درباره همان کانال بنویس." },
          { role: "user", content: prompt },
        ],
      });

      let text = (resp?.choices?.[0]?.message?.content || "").trim();
      // پاکسازی تکرار احتمالی "فعلاً در دسترس نیست"
      text = text.replace(/\n?فعلاً در دسترس نیست\.?\s*فعلاً در دسترس نیست\.?/g, "فعلاً در دسترس نیست");
      out[ch] = text;
    } catch (e) {
      out[ch] = "فعلاً در دسترس نیست.";
    }
  }

  return out;
}

export default { analyzePerChannel };

