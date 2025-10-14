// module.exports = {
//     apps: [
//       {
//         name: 'rahin_ops_watchdog',
//         script: 'src/main/rahin_ops_watchdog.js',
//         cwd: 'C:/Users/Administrator/Desktop/Projects/rahin', // ریشه پروژه که .env همان‌جاست
//         node_args: [],
//         env: {
//           NODE_ENV: 'production'
//         },
//         watch: false,
//         autorestart: true,
//         max_restarts: 10
//       }
//     ]
//   };
  

module.exports = {
  apps: [{
    name: "rahin-daily-visitor-scenarios",
    script: "src/pipeline/build_and_send_all_visitor_scenarios.js",
    // اجرای روزانه ساعت ۸ صبح
    cron_restart: "0 8 * * *",
    autorestart: false,
    time: true,
    env: {
      MAIN_DB_PATH: "C:\\Users\\Administrator\\Desktop\\Projects\\AtighgashtAI\\db_atigh.sqlite",
      ARCHIVE_DB_PATH: "C:\\Users\\Administrator\\Desktop\\Projects\\AtighgashtAI\\db_archive.sqlite",
      WHATSAPP_OPERATOR: "09134052885",
      DRY_RUN: "0",
      NODE_ENV: "production"
    }
  }]
};
