// ============================================================
// File: ecosystem.config.cjs
// Purpose: زمان‌بندی دقیق اجرای خودکار Jobهای راهین با PM2
// Author: Hojjat Mehri
// ============================================================

module.exports = {
  apps: [
    // ========================================================
    // ۱. Watchdog مرکزی راهین
    // ========================================================
    {
      name: "rahin_ops_watchdog",
      script: "src/main/rahin_ops_watchdog.js",
      cwd: "E:/Projects/rahin",
      watch: false,
      autorestart: true,
      exec_mode: "fork",
      max_restarts: 10,
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin_ops_watchdog.log",
      error_file: "E:/Projects/rahin/logs/rahin_ops_watchdog.err.log",
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran"
      }
    },

    // ========================================================
    // ۲. همگام‌سازی پروفایل‌ها (Didar + Transactions)
    // ========================================================
    {
      name: "rahin-daily-unified-profiles",
      script: "src/jobs/job_sync_unified_profiles.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "55 7 * * *", // ساعت ۰۷:۵۵ صبح تهران
      autorestart: false,
      exec_mode: "fork",
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-unified-profiles.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-unified-profiles.err.log",
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        DEV_ALERT_MOBILE: "09134052885",
        DRY_RUN: "0",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        ULTRAMSG_INSTANCE_ID: process.env.ULTRAMSG_INSTANCE_ID,
        ULTRAMSG_TOKEN: process.env.ULTRAMSG_TOKEN
      }
    },

    // ========================================================
    // ۳. سناریوهای بازدیدکنندگان (راس ۸:۰۰ صبح)
    // ========================================================
    {
      name: "rahin-daily-visitor-scenarios",
      script: "src/pipeline/build_and_send_all_visitor_scenarios.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 8 * * *",
      autorestart: false,
      exec_mode: "fork",
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-visitor-scenarios.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-visitor-scenarios.err.log",
      merge_logs: true,
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
    // ۴. ارزش مشتری (Customer Value Collector)
    // ========================================================
    {
      name: "rahin-daily-customer-value",
      script: "src/jobs/job_customer_value_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 9 * * *", // ساعت ۰۹:۰۰ صبح
      autorestart: false,
      exec_mode: "fork",
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-customer-value.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-customer-value.err.log",
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        DEV_ALERT_MOBILE: "09134052885",
        DRY_RUN: "0",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        ULTRAMSG_INSTANCE_ID: process.env.ULTRAMSG_INSTANCE_ID,
        ULTRAMSG_TOKEN: process.env.ULTRAMSG_TOKEN
      }
    },

    // ========================================================
    // ۵. هشدارهای CRM دیدار (راس ۰۲:۰۰ بامداد)
    // ========================================================
    {
      name: "rahin-daily-crm-alerts",
      script: "src/jobs/job_crm_alerts_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 2 * * *",
      autorestart: false,
      exec_mode: "fork",
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-crm-alerts.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-crm-alerts.err.log",
      merge_logs: true,
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
    // ۶. گزارش عصرانه (راس ۱۸:۰۰)
    // ========================================================
    {
      name: "rahin-daily-report",
      script: "src/pipeline/daily_report_send.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 18 * * *",
      autorestart: false,
      exec_mode: "fork",
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-report.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-report.err.log",
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        FORCE_RUN: "0"
      }
    },

    // ========================================================
    // ۷. محاسبه امتیاز مالی روزانه (Financial Score)
    // ========================================================
    {
      name: "rahin-daily-financial-score",
      script: "src/jobs/job_financial_score_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "10 18 * * *",
      autorestart: false,
      exec_mode: "fork",
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-financial-score.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-financial-score.err.log",
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        DRY_RUN: "0",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite"
      }
    },

    {
      name: "atigh-channel-health-monitor",
      script: "jobs/channel_health_monitor.js",
      cwd: "E:/Projects/AtighgashtAI",
      autorestart: true,
      watch: false,
      exec_mode: "fork",
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/AtighgashtAI/logs/channel-health-monitor.log",
      error_file: "E:/Projects/AtighgashtAI/logs/channel-health-monitor.err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "produclearction",
        APP_TZ: "Asia/Tehran",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        DEV_ALERT_MOBILE: "09134052885",
        ULTRAMSG_INSTANCE_ID: process.env.ULTRAMSG_INSTANCE_ID,
        ULTRAMSG_TOKEN: process.env.ULTRAMSG_TOKEN
      }
    }
    
    
  ]
};
