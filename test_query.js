// File: E:\Projects\rahin\test_create_v_customer_segments.js
import Database from "better-sqlite3";

import { db } from 'file:///E:/Projects/rahin/src/lib/db/dbSingleton.js';
db.exec(`
DROP VIEW IF EXISTS v_customer_segments;
CREATE VIEW v_customer_segments AS
SELECT
  rank_label,
  COUNT(*) AS total_customers,
  ROUND(AVG(value_score), 2) AS avg_value_score,
  ROUND(AVG(total_amount), 0) AS avg_total_amount,
  ROUND(AVG(payments_count), 1) AS avg_payments_count
FROM customer_value
WHERE value_score IS NOT NULL
GROUP BY rank_label
ORDER BY
  CASE rank_label
    WHEN 'پلاتینیوم' THEN 1
    WHEN 'طلایی' THEN 2
    WHEN 'نقره‌ای' THEN 3
    WHEN 'برنزی' THEN 4
    ELSE 5
  END;
`);
console.log("✅ View v_customer_segments created successfully.");

// تست سریع
const rows = db.prepare(`SELECT * FROM v_customer_segments;`).all();
console.table(rows);

