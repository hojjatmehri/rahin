import '../logger.js';
import Database from "better-sqlite3";

import { db } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';

try {
  db.exec(`
    ALTER TABLE atigh_instagram_dev ADD COLUMN event_type TEXT;
  `);

  db.exec(`
    ALTER TABLE atigh_instagram_dev ADD COLUMN payload TEXT;
  `);

  console.log("✅ ستون‌های جدید (event_type, payload) اضافه شدند.");
} catch (err) {
  console.error("❌ خطا در اجرای ALTER TABLE:", err.message);
}

