// ============================================================
// File: ecosystem.config.cjs
// Purpose: PM2 Master Orchestration (Run/Stop/Restart Control)
// Author:  Hojjat Mehri
// ============================================================

const enabled = (key, def = true) => {
  const val = process.env[key];
  if (val === undefined) return def;
  return String(val).toLowerCase() === "true" || val === "1";
};

module.exports = {
  apps: [
    // ========================================================
    // Watchdog مرکزی Rahin
    // ========================================================
    enabled("ENABLED_OPS_WATCHDOG") && {
      name: "rahin_ops_watchdog",
      script: "src/main/rahin_ops_watchdog.js",
      cwd: "E:/Projects/rahin",
      autorestart: true,
      watch: false,
      exec_mode: "fork",
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin_ops_watchdog.log",
      error_file: "E:/Projects/rahin/logs/rahin_ops_watchdog.err.log",
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
      },
    },

    // ========================================================
    // همگام‌سازی پروفایل‌ها
    // ========================================================
    enabled("ENABLED_UNIFIED_PROFILES") && {
      name: "rahin-daily-unified-profiles",
      script: "src/jobs/job_sync_unified_profiles.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "55 7 * * *",
      autorestart: false,
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-unified-profiles.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-unified-profiles.err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        DEV_ALERT_MOBILE: "09134052885",
      },
    },

    // ========================================================
    // سناریوهای بازدیدکنندگان
    // ========================================================
    enabled("ENABLED_VISITOR_SCENARIOS") && {
      name: "rahin-daily-visitor-scenarios",
      script: "src/pipeline/build_and_send_all_visitor_scenarios.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 8 * * *",
      autorestart: false,
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-visitor-scenarios.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-visitor-scenarios.err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        ARCHIVE_DB_PATH: "E:/Projects/AtighgashtAI/db_archive.sqlite",
        WHATSAPP_OPERATOR: "09134052885",
      },
    },

    // ========================================================
    // Collector ارزش مشتری
    // ========================================================
    enabled("ENABLED_CUSTOMER_VALUE") && {
      name: "rahin-daily-customer-value",
      script: "src/jobs/job_customer_value_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 9 * * *",
      autorestart: false,
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-customer-value.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-customer-value.err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        DEV_ALERT_MOBILE: "09134052885",
      },
    },

    // ========================================================
    // هشدارهای CRM دیدار
    // ========================================================
    enabled("ENABLED_CRM_ALERTS") && {
      name: "rahin-daily-crm-alerts",
      script: "src/jobs/job_crm_alerts_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 2 * * *",
      autorestart: false,
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-crm-alerts.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-crm-alerts.err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        SQLITE_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        DEV_ALERT_MOBILE: "09134052885",
        SEND_REAL_ALERTS: "0",
      },
    },

    // ========================================================
    // گزارش عصرانه
    // ========================================================
    enabled("ENABLED_DAILY_REPORT") && {
      name: "rahin-daily-report",
      script: "src/pipeline/daily_report_send.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 18 * * *",
      autorestart: false,
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-report.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-report.err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
      },
    },

    // ========================================================
    // امتیاز مالی روزانه
    // ========================================================
    enabled("ENABLED_FINANCIAL_SCORE") && {
      name: "rahin-daily-financial-score",
      script: "src/jobs/job_financial_score_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "10 18 * * *",
      autorestart: false,
      time: true,
      timezone: "Asia/Tehran",
      out_file: "E:/Projects/rahin/logs/rahin-daily-financial-score.log",
      error_file: "E:/Projects/rahin/logs/rahin-daily-financial-score.err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        APP_TZ: "Asia/Tehran",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
      },
    },

    // ========================================================
    // Channel Health Monitor
    // ========================================================
    enabled("ENABLED_CHANNEL_HEALTH") && {
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
        NODE_ENV: "production",
        MAIN_DB_PATH: "E:/Projects/AtighgashtAI/db_atigh.sqlite",
        DEV_ALERT_MOBILE: "09134052885",
        ULTRAMSG_INSTANCE_ID: process.env.ULTRAMSG_INSTANCE_ID,
        ULTRAMSG_TOKEN: process.env.ULTRAMSG_TOKEN,
      },
    },

    // ========================================================
    // سایر سرویس‌های Atigh/Rahnegar
    // ========================================================
    enabled("ENABLED_RAHNEGAR_LOOP") && {
      name: "rahnegar_loop",
      script: "E:/Projects/RahnegarM/loop/rahnegar_loop.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_RAHNEGAR_NIGHTLY") && {
      name: "rahnegar_nightly",
      script: "E:/Projects/RahnegarM/level1/rahnegar_nightly.js",
      autorestart: false,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_DISPATCH_MINUTE") && {
      name: "dispatch_every_minute",
      script: "E:/Projects/AtighgashtAI/jobs/dispatch_every_minute.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },
        // ========================================================
    // سایر سرویس‌های AtighgashtAI
    // ========================================================
    enabled("ENABLED_ARCHIVE_JOURNEY") && {
      name: "archive_journey_events",
      script: "E:/Projects/AtighgashtAI/archive_journey_events.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_WORDPRESS_IMPORT") && {
      name: "dailyWordPressImport",
      script: "E:/Projects/AtighgashtAI/dailyWordPressImport.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_DIDAR") && {
      name: "didar",
      script: "E:/Projects/AtighgashtAI/didar.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_DIDAR_HOURLY") && {
      name: "didar_hourly_sync",
      script: "E:/Projects/AtighgashtAI/jobs/didar_hourly_sync.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_INSTAGRAM_MESSAGE") && {
      name: "instagram_message",
      script: "E:/Projects/AtighgashtAI/instagram_message.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_TRIP_GUIDE") && {
      name: "tripGuideScheduler",
      script: "E:/Projects/AtighgashtAI/tripGuideScheduler.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_RAHINE_SERVER") && {
      name: "rahine_smoke_server",
      script: "E:/Projects/AtighgashtAI/server.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

    enabled("ENABLED_RAHIN_DUAL_WATCHDOG") && {
      name: "rahin_dual_watchdog",
      script: "E:/Projects/AtighgashtAI/rahin_dual_watchdog.js",
      autorestart: true,
      time: true,
      timezone: "Asia/Tehran",
    },

  ].filter(Boolean),
};
