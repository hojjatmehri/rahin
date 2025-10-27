import '../../logger.js';
// بک‌فیل سه جدول اینستاگرام به interactions با تشخیص هوشمند ستون زمان
import Database from "better-sqlite3";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/* ------------------- DB path (فقط از ENV) ------------------- */
function resolveDbPath() {
  const envPath = process.env.SQLITE_DB_PATH || process.env.DB_PATH;
  if (!envPath) {
    throw new Error(
      "SQLITE_DB_PATH تعریف نشده. مثال:\n" +
      "Windows PowerShell:\n$env:SQLITE_DB_PATH=\"C:\\Users\\Administrator\\Desktop\\Projects\\AtighgashtAI\\db_atigh.sqlite\""
    );
  }
  const p = path.isAbsolute(envPath) ? envPath : path.resolve(process.cwd(), envPath);
  if (!fs.existsSync(p)) throw new Error(`فایل DB یافت نشد: ${p} (ENV=${envPath})`);
  return p;
}

const dbPath = resolveDbPath();
const db = new Database(dbPath);
db.pragma("journal_mode = WAL");
console.log("Using DB:", dbPath);

/* ------------------- ابزار DB ------------------- */
function listTables() {
  return db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all().map(r => r.name);
}
function hasTable(name) {
  const r = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name);
  return !!r;
}
function getColumns(table) {
  return db.prepare(`PRAGMA table_info("${table}")`).all(); // [{cid,name,type,...}]
}
function hasColumn(table, col) {
  return getColumns(table).some(c => c.name === col);
}

/* ------------------- اطمینان از اسکیما interactions ------------------- */
function ensureInteractionsSchema() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS interactions (
      id TEXT PRIMARY KEY,
      occurred_at TEXT NOT NULL,
      channel TEXT NOT NULL,
      event_type TEXT NOT NULL,
      user_ref TEXT,
      contact TEXT,
      session_id TEXT,
      source TEXT,
      medium TEXT,
      campaign TEXT,
      page TEXT,
      content_id TEXT,
      metadata_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_interactions_time ON interactions(occurred_at);
    CREATE INDEX IF NOT EXISTS idx_interactions_chan_evt ON interactions(channel, event_type);
    CREATE INDEX IF NOT EXISTS idx_interactions_src ON interactions(source, medium, campaign);
    CREATE INDEX IF NOT EXISTS idx_interactions_contact ON interactions(contact);
  `);
}
ensureInteractionsSchema();

/* ------------------- تشخیص ستون زمان ------------------- */
// ENV override: اگر لازم شد، نام ستون را از ENV بده
// IG_DEV_TIME_COL, IG_REPLY_TIME_COL, IG_COMMENT_TIME_COL
function pickTimeColumn(table) {
  const override = {
    atigh_instagram_dev: process.env.IG_DEV_TIME_COL,
    reply: process.env.IG_REPLY_TIME_COL,
    comment: process.env.IG_COMMENT_TIME_COL,
  }[table];
  if (override) {
    if (!hasColumn(table, override)) {
      throw new Error(`ستون زمان ${override} در جدول ${table} وجود ندارد.`);
    }
    return override;
  }

  const candidates = ["created_date", "created_at", "createdAt", "timestamp", "occurred_at", "date"];
  for (const c of candidates) if (hasColumn(table, c)) return c;

  // Heuristic
  const cols = getColumns(table);
  const hit = cols.find(c => /(created|date|time|occurred|timestamp)/i.test(c.name));
  if (hit) return hit.name;

  const msg =
    `no datetime column found for ${table}. ` +
    `tables=${JSON.stringify(listTables())} cols=${JSON.stringify(cols.map(c => c.name))}`;
  throw new Error(msg);
}

/* ------------------- نرمال‌سازی datetime به localtime ------------------- */
function normalizedDatetimeExpr(col) {
  const c = `"${col}"`;
  return `
    CASE
      WHEN typeof(${c})='integer' THEN datetime(${c}, 'unixepoch', 'localtime')
      WHEN CAST(${c} AS INTEGER) > 10000000000 THEN datetime(CAST(${c} AS INTEGER)/1000, 'unixepoch', 'localtime')
      WHEN CAST(${c} AS INTEGER) BETWEEN 1000000000 AND 5000000000 THEN datetime(CAST(${c} AS INTEGER), 'unixepoch', 'localtime')
      ELSE datetime(${c})
    END
  `;
}

/* ------------------- بک‌فیل یک جدول ------------------- */
function backfillTable({ table, eventType, idPrefix, metaCols, includeContact = false }) {
  if (!hasTable(table)) {
    console.warn(`Skip: table "${table}" not found.`);
    return { table, inserted: 0, timeCol: null, skipped: true };
  }

  const timeCol = pickTimeColumn(table);
  const dtExpr = normalizedDatetimeExpr(timeCol);

  const existingMetaCols = metaCols.filter(c => hasColumn(table, c));
  const metaJson = existingMetaCols.length
    ? `json_object(${existingMetaCols.map(c => `'${c}', ${c}`).join(", ")})`
    : "NULL";

  const contactExpr = includeContact && hasColumn(table, "mobile") ? "mobile" : "NULL";

  const sql = `
    INSERT OR IGNORE INTO interactions
      (id, occurred_at, channel, event_type, user_ref, contact, session_id, source, medium, campaign, page, content_id, metadata_json)
    SELECT
      '${idPrefix}:' || id,
      ${dtExpr},
      'instagram',
      '${eventType}',
      CAST(social_user_id AS TEXT),
      ${contactExpr},
      NULL, NULL, NULL, NULL, NULL, NULL,
      ${metaJson}
    FROM "${table}"
    WHERE ${timeCol} IS NOT NULL
  `;

  const info = db.prepare(sql).run();
  console.log(`[${table}] time_col=${timeCol} inserted=${info.changes}`);
  return { table, inserted: info.changes, timeCol };
}

/* ------------------- اجرا ------------------- */
try {
  console.log("Tables:", listTables());

  const r1 = backfillTable({
    table: "atigh_instagram_dev",
    eventType: "ig_contact",
    idPrefix: "instagram:ig_contact",
    metaCols: ["name","destination","personnel_id","personnel_name","novin_id","instagram_id"],
    includeContact: true,
  });

  const r2 = backfillTable({
    table: "reply",
    eventType: "ig_reply",
    idPrefix: "instagram:ig_reply",
    metaCols: ["message_id","novin_id","message","response","username"],
  });

  const r3 = backfillTable({
    table: "comment",
    eventType: "ig_comment",
    idPrefix: "instagram:ig_comment",
    metaCols: ["comment_id","novin_id","message","response"],
  });

  console.log("Backfill done:", r1, r2, r3);

  const sum = db.prepare(`
    SELECT channel, event_type, COUNT(*) cnt
    FROM interactions
    WHERE channel='instagram'
    GROUP BY 1,2 ORDER BY 3 DESC
  `).all();
  console.log("Interactions summary:", sum);
} catch (e) {
  console.error("Backfill error:", e.message);
  process.exit(1);
}

