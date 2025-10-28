-- ============================================================
-- File: src/db/views/v_customer_value_ranked.sql
-- Purpose: ایجاد رده‌بندی مشتریان بر اساس امتیاز ارزش (0–100)
-- ============================================================

CREATE VIEW IF NOT EXISTS v_customer_value_ranked AS
SELECT
  mobile,
  ROUND(value_score, 2) AS value_score,
  whatsapp_score,
  crm_stage_score,
  financial_score,
  total_interactions,
  total_amount,
  payments_count,
  last_payment_at,
  updated_at,
  CASE
    WHEN value_score >= 85 THEN 'پلاتینیوم'
    WHEN value_score >= 65 THEN 'طلایی'
    WHEN value_score >= 40 THEN 'نقره‌ای'
    ELSE 'برنزی'
  END AS rank_label
FROM customer_value
ORDER BY value_score DESC;
