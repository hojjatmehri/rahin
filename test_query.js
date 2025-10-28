import Database from "better-sqlite3";
const db = new Database('E:/Projects/AtighgashtAI/db_atigh.sqlite');

db.exec(`
  DROP VIEW IF EXISTS v_customer_value_ranked;

  CREATE VIEW v_customer_value_ranked AS
  SELECT
    cv.mobile,
    COALESCE(pup.contact_name,'درج نشده') AS contact_name,
    cv.value_score,
    cv.whatsapp_score,
    cv.crm_stage_score,
    cv.financial_score,
    cv.total_interactions,
    cv.total_amount,
    cv.payments_count,
    cv.last_payment_at,
    cv.updated_at,
    cv.rank_label,
    CAST((julianday('now') - julianday(cv.last_payment_at)) AS INT) AS recency_days
  FROM customer_value cv
  LEFT JOIN person_unified_profile pup
    ON pup.mobile = cv.mobile
  ORDER BY cv.value_score DESC;
`);

console.log('✅ View rebuilt with contact_name default.');
db.close();
