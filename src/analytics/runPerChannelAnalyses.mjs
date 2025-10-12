// src/analytics/runPerChannelAnalyses.mjs
// node src/analytics/runPerChannelAnalyses.mjs
import 'dotenv/config.js';
import OpenAI from 'openai';
import { CONFIG } from '../config/Config.js';
import env from '../config/env.js';

import { collectFinance } from '../collectors/financeCollector.js';
import { collectWhatsApp } from '../collectors/whatsappCollector.js';
import { collectInstagram } from '../collectors/instagramCollector.js';
import { whatsappClickInsightsShort } from '../collectors/whatsappCollector.js';

import { analyzePerChannel } from './perChannelAnalyze.js';

// اگر لازم شد از arch بخوانیم (برای بعضی کالکتورها)
function ensureArchiveAttached(db) {
  try {
    const list = db.pragma('database_list', { simple: false });
    const hasArch = Array.isArray(list) && list.some(x => String(x.name).toLowerCase() === 'arch');
    if (!hasArch) {
      const archPath = (env.ARCHIVE_DB_PATH || 'C:\\Users\\Administrator\\Desktop\\Projects\\AtighgashtAI\\db_archive.sqlite')
        .replace(/'/g, "''");
      db.exec(`ATTACH DATABASE '${archPath}' AS arch;`);
      console.log(`[arch] attached: ${archPath}`);
    }
  } catch (e) {
    console.warn('[arch] attach failed:', e?.message || e);
  }
}

function toFa(n) {
  try { return Number(n ?? 0).toLocaleString('fa-IR'); }
  catch { return String(n ?? 0); }
}

/**
 * از خروجی کالکتورها یک payload برای هر کانال می‌سازیم.
 * توجه: هرچه کالکتورها بیشتر عدد بدهند (مثلاً mtd)، این payload هم غنی‌تر می‌شود.
 */
async function buildChannelsPayload() {
  const [fin, wa, ig, wai] = await Promise.all([
    collectFinance().catch(() => ({})),
    collectWhatsApp().catch(() => ({})),
    collectInstagram().catch(() => ({})),
    whatsappClickInsightsShort().catch(() => ({}))
  ]);

  // Finance
  const financePayload = {
    today: {
      orders: fin?.sales?.orders_today ?? 0,
      revenue: fin?.sales?.total_sales_today ?? 0,
      profit: fin?.sales?.profit_today ?? 0,
      avg_order: fin?.sales?.avg_order_value ?? 0,
      income_rate_pct: fin?.sales?.income_rate_pct ?? 0,
    },
    k7d: {
      activity: fin?.finance?.fin_activity_7d || null, // مثلا ACTIVE_7D
      // اگر اعداد 7روز داری اینجا بگذار
    }
    // mtd: ...   // اگر کالکتور اضافه کرد، اینجا اضافه می‌شود
    // cmp: ...   // همین‌طور
  };

  // WhatsApp
  const waPayload = {
    today: {
      inbound: wa?.inbound_today ?? 0,
      unique_contacts: wa?.unique_contacts_today ?? 0,
    },
    k7d: {
      inbound: wa?.inbound_7d ?? 0,
      mapped_visitors: wa?.mapped_visitors_7d ?? 0,
    }
    // mtd / cmp اگر داشتی اضافه کن
  };

  // Instagram (dev table metrics)
  const igPayload = {
    today: {
      events: ig?.dev_events_today ?? 0,
    },
    k7d: {
      events: ig?.dev_events_7d ?? 0,
    },
    by_type_today: ig?.by_type ?? []
  };

  // Clicks (از whatsappClickInsightsShort برای high-level بینش)
  const clicksPayload = {
    today: {
      // اگر چیزی از today داری اضافه کن
    },
    k7d: {
      wa_click_rate: wai?.wa_click_rate ?? 0,
      top_sources: wai?.top_sources ?? [],
      top_pages: wai?.top_pages ?? [],
    }
  };

  return {
    Finance: financePayload,
    WhatsApp: waPayload,
    Instagram: igPayload,
    Clicks: clicksPayload,
  };
}

async function main() {
  ensureArchiveAttached(CONFIG.db);

  const channelsPayload = await buildChannelsPayload();
  const analyses = await analyzePerChannel(channelsPayload, {
    model: process.env.RAHIN_MODEL || 'gpt-4o',
    apiKey: process.env.OPENAI_API_KEY
  });

  console.log('\n================= تحلیل تفکیکی کانال‌ها (بر پایهٔ کالکتورها) =================\n');
  for (const k of ["Finance", "WhatsApp", "Instagram", "Clicks"]) {
    console.log(`【${k}】\n${analyses[k] || '—'}\n`);
  }
  console.log('\n===============================================================================\n');
}

main().catch(err => {
  console.error('ERROR:', err?.message || err);
  process.exit(1);
});
