// src/analytics/perChannelAnalyze.js
import OpenAI from "openai";

/**
 * این ماژول هیچ دیتایی کوئری نمی‌کند.
 * فقط از «payload» که خودت بهش می‌دهی استفاده می‌کند (خروجی کالکتورها).
 *
 * شکلِ expected payload:
 * {
 *   Finance:   { k7d: {...}, today: {...}, mtd: {...}|undefined, cmp: {...}|undefined },
 *   WhatsApp:  { k7d: {...}, today: {...}, mtd: {...}|undefined, cmp: {...}|undefined },
 *   Instagram: { k7d: {...}, today: {...}, mtd: {...}|undefined, cmp: {...}|undefined },
 *   Clicks:    { k7d: {...}, today: {...}, mtd: {...}|undefined, cmp: {...}|undefined },
 * }
 *
 * نکته: اگر mtd/cmp نداری، اصلاً پاس نده یا null بده؛ پرومپت طوری نوشته شده که
 * در صورت نبودن بخش‌ها، آن قسمت را با «فعلاً در دسترس نیست» جمع‌بندی کند.
 */

function toFa(n) {
  try { return Number(n ?? 0).toLocaleString("fa-IR"); }
  catch { return String(n ?? 0); }
}

// ـــ پرومپت خیلی مقاوم نسبت به نبودن بعضی فیلدها ـــ
function buildStrictChannelPrompt(channel, payload) {
  // payload شامل today, k7d, mtd?, cmp?
  const haveMtd = !!payload?.mtd;
  const haveCmp = !!(payload?.cmp && Object.keys(payload.cmp).length);

  return `
تو یک تحلیلگر کسب‌وکار هستی و فقط و فقط دربارهٔ کانال «${channel}» تحلیل بنویس.
- ساختار خروجی:
  • پاراگراف ۱ «۷ روز اخیر» (اگر داده دارد) + ۲ پیشنهاد اجرایی خیلی مشخص.
  • پاراگراف ۲ «ماه جاری تا امروز» (اگر داده در دسترس نیست، صریح بنویس «فعلاً در دسترس نیست»).
  • یک خط «مقایسه با ماه قبل تا امروز» فقط اگر دادهٔ مقایسه‌ای داریم؛ وگرنه همان جملهٔ «فعلاً در دسترس نیست».
- داده‌های عددی را فارسی و با جداکننده هزار بنویس. خروجی فقط متنِ فارسی واتساپ‌پسند باشد.

دادهٔ ${channel} (today/k7d${haveMtd ? '/mtd' : ''}${haveCmp ? '/cmp' : ''}):
${JSON.stringify(payload ?? {}, null, 2)}
`.trim();
}

/**
 * تحلیل تفکیکی برای چند کانال، بر اساس payload آمادهٔ کالکتورها
 * @param {Record<string, any>} channelsPayload  payloadِ آماده (Finance/WhatsApp/Instagram/Clicks)
 * @param {{model?: string, apiKey?: string}} opts
 * @returns {Promise<Record<string, string>>}
 */
export async function analyzePerChannel(channelsPayload, { model = "gpt-4o", apiKey = process.env.OPENAI_API_KEY } = {}) {
  const openai = new OpenAI({ apiKey });
  const channels = ["Finance", "WhatsApp", "Instagram", "Clicks"];
  const out = {};

  for (const ch of channels) {
    const payload = channelsPayload?.[ch] || {};
    const prompt = buildStrictChannelPrompt(ch, payload);

    const resp = await openai.chat.completions.create({
      model,
      temperature: 0.2,
      messages: [
        { role: "system", content: "دستیار تحلیلگر کسب‌وکار فارسی؛ خروجی کوتاه، واتساپ‌پسند و فقط دربارهٔ همان کانال." },
        { role: "user", content: prompt }
      ],
    });

    out[ch] = (resp?.choices?.[0]?.message?.content || "").trim();
  }
  return out;
}

export default { analyzePerChannel };
