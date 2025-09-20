module.exports = {
    apps: [
      {
        name: 'rahin_ops_watchdog',
        script: 'src/main/rahin_ops_watchdog.js',
        cwd: 'C:/Users/Administrator/Desktop/Projects/rahin', // ریشه پروژه که .env همان‌جاست
        node_args: [],
        env: {
          NODE_ENV: 'production'
        },
        watch: false,
        autorestart: true,
        max_restarts: 10
      }
    ]
  };
  