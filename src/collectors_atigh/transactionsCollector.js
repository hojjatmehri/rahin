import '../../logger.js';
/**
 * transactionsCollector.js
 * خلاصه‌سازی روزانهٔ تراکنش‌ها (تعداد، جمع فروش، جمع سود، مقصد برتر)
 *
 * ورودی: db_atigh.sqlite → جدول transactions
 * خروجی: db_atigh.sqlite → جدول rahin_daily_summary (upsert)
 *
 * ستون‌های قابل کشف در transactions:
 * - تاریخ/زمان: created_at | regDate | updated_at | payDate1 | ts | timestamp
 * - فروش: sellAmount | sell_amount | amount_sell
 * - خرید: buyAmount | buy_amount | amount_buy | cost
 * - مقصد: destination | dest | city | top_destination | route
 *
 * TZ: Asia/Tehran
 */

import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const PROJECT_ROOT = process.env.RAHNEGAR_SOURCE_ROOT || path.resolve(process.cwd());
const DB_MAIN_PATH = process.env.RAHIN_DB_MAIN || path.join(PROJECT_ROOT, "db_atigh.sqlite");
const RUN_TZ = process.env.APP_TZ || "Asia/Tehran";

// ---------- utils ----------
function log(...a){ console.log("[transactionsCollector]", ...a); }
function warn(...a){ console.warn("[transactionsCollector][WARN]", ...a); }
function errl(...a){ console.error("[transactionsCollector][ERROR]", ...a); }

function toTehranDate(d){
  const s = new Date(d).toLocaleString("en-US", { timeZone: RUN_TZ });
  return new Date(s);
}
function ymdTehran(d){
  const td = toTehranDate(d);
  const yy = td.getFullYear();
  const mm = String(td.getMonth()+1).padStart(2,'0');
  const dd = String(td.getDate()).padStart(2,'0');
  return `${yy}-${mm}-${dd}`;
}
function startOfDayTehran(d){
  const td = toTehranDate(d);
  td.setHours(0,0,0,0);
  return td;
}
function endOfDayTehran(d){
  const td = toTehranDate(d);
  td.setHours(23,59,59,999);
  return td;
}

function safeNumber(x){
  const n = Number(String(x).replace(/[^\d.-]/g,''));
  return Number.isFinite(n) ? n : 0;
}

function openDB(){
  if (!fs.existsSync(DB_MAIN_PATH)) throw new Error(`Main DB not found: ${DB_MAIN_PATH}`);
  
   let db;
   
   try {
     db = new Database(DB_MAIN_PATH, {
       fileMustExist: false,
       timeout: 5000,
     });
   
     db.pragma("journal_mode = WAL");
     db.pragma("foreign_keys = ON");
     db.pragma("busy_timeout = 5000");
     db.pragma("synchronous = NORMAL");
     db.pragma("temp_store = MEMORY");
   
     console.log("[DB] sqlite ready (WAL + timeout)");
   } catch (err) {
     console.error("[DB] failed:", err.message);
     process.exit(1);
   }
  return db;
}

// ---------- schema guards ----------
function tableInfo(db, name){
  return db.prepare(`PRAGMA table_info(${name});`).all();
}
function hasTable(db, name){
  const r = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name);
  return !!r;
}
function hasColumn(db, table, col){
  const info = tableInfo(db, table);
  return info.some(c => c.name === col);
}
function ensureDailySummaryColumns(db){
  db.exec(`
    CREATE TABLE IF NOT EXISTS rahin_daily_summary (
      date TEXT PRIMARY KEY
      -- سایر ستون‌ها بعداً با ALTER اضافه می‌شوند
    );
  `);

  const mustHave = [
    "transactions_count INTEGER",
    "sales_amount INTEGER",
    "profit_amount INTEGER",
    "top_destination TEXT"
  ];
  for (const def of mustHave){
    const col = def.split(" ")[0];
    if (!hasColumn(db, "rahin_daily_summary", col)){
      db.exec(`ALTER TABLE rahin_daily_summary ADD COLUMN ${def};`);
    }
  }
}

// ---------- column discovery on transactions ----------
function discoverColumns(db){
  if (!hasTable(db, "transactions")) {
    throw new Error("Table 'transactions' not found.");
  }
  const cols = tableInfo(db, "transactions").map(c => c.name.toLowerCase());

  function pick(cands){
    for (const c of cands){
      const i = cols.indexOf(c.toLowerCase());
      if (i >= 0) return tableInfo(db, "transactions")[i].name;
    }
    return null;
  }

  const dateCol = pick(["created_at","regDate","updated_at","payDate1","ts","timestamp"]);
  const sellCol = pick(["sellAmount","sell_amount","amount_sell"]);
  const buyCol  = pick(["buyAmount","buy_amount","amount_buy","cost"]);
  const destCol = pick(["destination","dest","city","top_destination","route"]);

  if (!dateCol) warn("No obvious date/time column found. Will parse nothing → no aggregation.");
  if (!sellCol && !buyCol) warn("No sell/buy columns found. Totals will be zero.");

  return { dateCol, sellCol, buyCol, destCol };
}

// ---------- core ----------
export function summarizeTransactionsForDate(targetDateStr){
  const db = openDB();
  try{
    ensureDailySummaryColumns(db);

    const { dateCol, sellCol, buyCol, destCol } = discoverColumns(db);
    if (!dateCol){
      // تاریخ نداریم → صفرنویسی
      zeroWrite(db, targetDateStr);
      log(`No date column in 'transactions'. Wrote zeros for ${targetDateStr}.`);
      return;
    }

    // محدوده روز به وقت تهران
    const from = startOfDayTehran(targetDateStr);
    const to   = endOfDayTehran(targetDateStr);

    // تلاش برای کشیدن همه ردیف‌های روز
    const stmt = db.prepare(`
      SELECT * FROM transactions
      WHERE ${dateCol} IS NOT NULL
    `);
    const rows = stmt.all();

    // فیلتر در JS چون نوع فیلد تاریخ ممکن است TEXT/INT و متنوع باشد
    const dayRows = [];
    for (const r of rows){
      const raw = r[dateCol];
      let dt = null;
      if (raw == null) continue;

      // تلاش هوشمند: اگر عددِ میلی‌ثانیه/ثانیه باشد؛ اگر متنِ تاریخ باشد
      const s = String(raw).trim();
      if (/^\d{10,13}$/.test(s)) {
        const n = Number(s);
        dt = new Date(s.length === 13 ? n : n * 1000);
      } else {
        // اجازه بده تاریخ‌های yyyy-mm-dd یا 'yyyy/mm/dd hh:mm' هم بیاید
        dt = new Date(s.replace(/\//g,'-'));
      }

      if (!isFinite(dt?.getTime?.())) continue;

      const tdt = toTehranDate(dt);
      if (tdt >= from && tdt <= to) {
        dayRows.push(r);
      }
    }

    if (dayRows.length === 0){
      zeroWrite(db, targetDateStr);
      log(`No transactions in range for ${targetDateStr}. Wrote zeros.`);
      return;
    }

    // محاسبات
    let transactions_count = 0;
    let sales_amount = 0;
    let profit_amount = 0;

    const destCount = new Map();

    for (const r of dayRows){
      transactions_count += 1;

      const sell = sellCol ? safeNumber(r[sellCol]) : 0;
      const buy  = buyCol  ? safeNumber(r[buyCol])  : 0;
      sales_amount += sell;
      profit_amount += (sell - buy);

      if (destCol){
        const d = (r[destCol] ?? "").toString().trim();
        if (d){
          destCount.set(d, (destCount.get(d) || 0) + 1);
        }
      }
    }

    // مقصد برتر
    let top_destination = null;
    if (destCount.size > 0){
      top_destination = Array.from(destCount.entries())
        .sort((a,b) => b[1] - a[1])[0][0];
    }

    // نوشتن در rahin_daily_summary (UPSERT)
    db.prepare(`
      INSERT INTO rahin_daily_summary (date, transactions_count, sales_amount, profit_amount, top_destination)
      VALUES (@date, @tc, @sa, @pa, @td)
      ON CONFLICT(date) DO UPDATE SET
        transactions_count = excluded.transactions_count,
        sales_amount       = excluded.sales_amount,
        profit_amount      = excluded.profit_amount,
        top_destination    = excluded.top_destination
    `).run({
      date: targetDateStr,
      tc: transactions_count,
      sa: sales_amount,
      pa: profit_amount,
      td: top_destination
    });

    log(`Done ${targetDateStr} → tx=${transactions_count} sales=${sales_amount} profit=${profit_amount} top=${top_destination || '-'}`);
  } finally {
    try { db.close(); } catch {}
  }
}

function zeroWrite(db, dateStr){
  db.prepare(`
    INSERT INTO rahin_daily_summary (date, transactions_count, sales_amount, profit_amount, top_destination)
    VALUES (@d, 0, 0, 0, NULL)
    ON CONFLICT(date) DO UPDATE SET
      transactions_count=0, sales_amount=0, profit_amount=0, top_destination=NULL
  `).run({ d: dateStr });
}

// API ساده برای استفادهٔ بیرونی
export function runTransactionsCollector(dateStr){
  const target = dateStr || ymdTehran(new Date());
  // اگر بدون تاریخ صدا زده شد، روزِ جاریِ تهران را خلاصه می‌کند
  summarizeTransactionsForDate(target);
}

