-- ========================================================
-- Migration: 050_customer_value.sql
-- Purpose : ایجاد جدول امتیاز ارزش مشتری (Customer Value)
-- Author  : Hojjat Mehri
-- ========================================================

CREATE TABLE IF NOT EXISTS customer_value (
  mobile TEXT PRIMARY KEY,
  value_score REAL,              -- امتیاز کلی (0 تا 100)
  whatsapp_score REAL,           -- امتیاز تعامل واتساپ
  crm_stage_score REAL,          -- امتیاز مرحله CRM
  visit_score REAL,              -- امتیاز فعالیت وب‌سایت
  recency_days INTEGER,          -- تعداد روز از آخرین تعامل
  total_interactions INTEGER,    -- مجموع تعاملات (واتساپ + بازدید + PDF)
  last_active_at TEXT,           -- آخرین زمان تعامل
  updated_at TEXT DEFAULT (datetime('now')) -- زمان آخرین به‌روزرسانی
);
