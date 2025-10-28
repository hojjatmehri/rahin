// ============================================================
// File: ecosystem.config.cjs
// Purpose: تنظیمات اجرای خودکار همه Jobهای راهین با PM2
// Author: Hojjat Mehri
// ============================================================

module.exports = {
  apps: [
    // ========================================================
    // ۱. Watchdog مرکزی راهین (پایش یکپارچه)
    // ========================================================
    {
      name: "rahin_ops_watchdog",
      script: "src/main/rahin_ops_watchdog.js",
      cwd: "E:/Projects/rahin",
      node_args: [],
      watch: false,
      autorestart: true,
      max_restarts: 10,
      time: true,
      out_file: "E:/Projects/rahin/logs/rahin_ops_watchdog.out.log",
      error_file: "E:/Projects/rahin/logs/rahin_ops_watchdog.err.log",
      merge_logs: false,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran"
      }
    },

    // ========================================================
    // ۲. سناریوهای بازدیدکنندگان (هر روز ساعت ۸ صبح)
    // ========================================================
    {
      name: "rahin-daily-visitor-scenarios",
      script: "src/pipeline/build_and_send_all_visitor_scenarios.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 8 * * *", // هر روز ساعت ۸ صبح تهران
      autorestart: false,
      time: true,
      out_file: "E:/Projects/rahin/logs/rahin-daily-visitor-scenarios.out.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-visitor-scenarios.err.log",
      merge_logs: false,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        ARCHIVE_DB_PATH: "E:/Projects/AtighgashtAI/db_archive.sqlite",
        WHATSAPP_OPERATOR: "09134052885",
        DRY_RUN: "0"
      }
    },

    // ========================================================
    // ۳. هشدارهای CRM دیدار (هر شب ساعت ۲ بامداد)
    // ========================================================
    {
      name: "rahin-daily-crm-alerts",
      script: "src/jobs/job_crm_alerts_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 2 * * *", // ساعت ۰۲:۰۰ تهران
      autorestart: false,
      time: true,
      out_file: "E:/Projects/rahin/logs/rahin-daily-crm-alerts.out.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-crm-alerts.err.log",
      merge_logs: false,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        SQLITE_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        DEV_ALERT_MOBILE: "09134052885",
        SEND_REAL_ALERTS: "0"
      }
    },

    // ========================================================
    // ۴. ارزش مشتری (Customer Value Collector)
    // ========================================================
    {
      name: "rahin-daily-customer-value",
      script: "src/jobs/job_customer_value_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 9 * * *", // هر روز ساعت ۹ صبح
      autorestart: false,
      time: true,
      out_file: "E:/Projects/rahin/logs/rahin-daily-customer-value.out.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-customer-value.err.log",
      merge_logs: false,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        DEV_ALERT_MOBILE: "09134052885",
        DRY_RUN: "0",
        ULTRAMSG_INSTANCE_ID: process.env.ULTRAMSG_INSTANCE_ID,
        ULTRAMSG_TOKEN: process.env.ULTRAMSG_TOKEN
      }
    },

    // ========================================================
    // ۵. گزارش عصرانه (خروجی روزانه)
    // ========================================================
    {
      name: "rahin-daily-report",
      script: "src/pipeline/daily_report_send.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 18 * * *", // هر روز ساعت ۱۸
      autorestart: false,
      time: true,
      out_file: "E:/Projects/rahin/logs/rahin-daily-report.out.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-report.err.log",
      merge_logs: false,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        FORCE_RUN: "0"
      }
    }
  ]
};
