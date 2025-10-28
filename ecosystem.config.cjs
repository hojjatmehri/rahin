module.exports = {
  apps: [
    // --- Job اصلی Watchdog ---
    {
      name: 'rahin_ops_watchdog',
      script: 'src/main/rahin_ops_watchdog.js',
      cwd: 'E:/Projects/rahin',
      node_args: [],
      env: {
        NODE_ENV: 'production'
      },
      watch: false,
      autorestart: true,
      max_restarts: 10
    },

    // --- Job سناریوهای بازدید ---
    {
      name: "rahin-daily-visitor-scenarios",
      script: "src/pipeline/build_and_send_all_visitor_scenarios.js",
      cwd: 'E:/Projects/rahin',
      cron_restart: "0 8 * * *", // هر روز ساعت ۸ صبح
      autorestart: false,
      time: true,
      env: {
        MAIN_DB_PATH: "E:\\Projects\\AtighgashtAI\\db_atigh.sqlite",
        ARCHIVE_DB_PATH: "E:\\Projects\\AtighgashtAI\\db_archive.sqlite",
        WHATSAPP_OPERATOR: "09134052885",
        DRY_RUN: "0",
        NODE_ENV: "production"
      }
    },

    // --- Job جدید: CRM Alerts Daily ---
    {
      name: "rahin-daily-crm-alerts",
      script: "src/jobs/job_crm_alerts_daily.js",
      cwd: "E:/Projects/rahin",
      cron_restart: "0 2 * * *", // هر شب ساعت ۰۲:۰۰
      autorestart: false,
      time: true,
      env: {
        NODE_ENV: "production",
        SQLITE_DB_PATH: "E:\\Projects\\AtighgashtAI\\db_atigh.sqlite",
        DEV_ALERT_MOBILE: "09134052885",
        SEND_REAL_ALERTS: "0"
      }
    }
  ]
};
