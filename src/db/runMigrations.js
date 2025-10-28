import '../../logger.js';
import fs from "fs";
import path from "path";
import Database from "better-sqlite3";

// مسیر دیتابیس (طبق چیزی که گفتی)
const dbFile = "E:/Projects/AtighgashtAI/db_atigh.sqlite";

// مسیر پوشه‌ی migrations
const migrationsDir = path.resolve("./src/db/migrations");

// اتصال به دیتابیس

 let db;
 
 try {
   db = new Database(dbFile , {
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
console.log("✅ Connected to database:", dbFile);

// گرفتن لیست فایل‌های migration و مرتب‌سازی
const migrationFiles = fs
  .readdirSync(migrationsDir)
  .filter((f) => f.endsWith(".sql"))
  .sort();

console.log("📂 Migration files to apply:", migrationFiles);

// اجرای همه migrationها
for (const file of migrationFiles) {
  const filePath = path.join(migrationsDir, file);
  const sql = fs.readFileSync(filePath, "utf-8");

  // جدا کردن دستورات
  const statements = sql
    .split(/;\s*[\r\n]+/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  console.log(`🚀 Running migration: ${file}`);

  db.transaction(() => {
    for (const stmt of statements) {
      try {
        db.prepare(stmt).run();
      } catch (err) {
        console.error(`❌ Error in ${file}:`, err.message);
      }
    }
  })();

  console.log(`✔ Finished: ${file}`);
}

console.log("🎉 All migrations applied successfully!");
db.close();

