import Database from 'better-sqlite3';

const db = new Database('E:/Projects/AtighgashtAI/db_atigh.sqlite');
db.exec(`
CREATE VIEW IF NOT EXISTS v_customer_value_ranked AS
SELECT
  cv.mobile,
  p.contact_name,
  ROUND(cv.value_score, 2) AS value_score,
  ROUND(cv.whatsapp_score, 2) AS whatsapp_score,
  ROUND(cv.crm_stage_score, 2) AS crm_stage_score,
  ROUND(cv.visit_score, 2) AS visit_score,
  cv.recency_days,
  cv.total_interactions,
  cv.last_active_at,
  cv.updated_at
FROM customer_value cv
LEFT JOIN person_unified_profile p ON p.mobile = cv.mobile
ORDER BY cv.value_score DESC;
`);
console.log('✅ v_customer_value_ranked view created.');
db.close();
