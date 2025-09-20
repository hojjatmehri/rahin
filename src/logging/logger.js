// src/logging/logger.js
// لاگ ساده با سطح‌بندی و کانتکست

const LEVELS = {
    DEBUG: 0,
    INFO:  1,
    WARN:  2,
    ERROR: 3,
  };
  
  // سطح پیش‌فرض (قابل تغییر با process.env.LOG_LEVEL)
  let currentLevel = LEVELS.INFO;
  
  if (process.env.LOG_LEVEL && LEVELS[process.env.LOG_LEVEL.toUpperCase()] !== undefined) {
    currentLevel = LEVELS[process.env.LOG_LEVEL.toUpperCase()];
  }
  
  /**
   * قالب‌بندی زمان فعلی به ISO کوتاه
   */
  function ts() {
    return new Date().toISOString();
  }
  
  /**
   * چاپ پیام لاگ
   * @param {string} level - DEBUG | INFO | WARN | ERROR
   * @param {string} msg
   * @param {object} ctx - context key/val
   */
  function log(level, msg, ctx = null) {
    if (LEVELS[level] < currentLevel) return;
  
    const base = `[${ts()}] [${level}] ${msg}`;
    if (ctx && Object.keys(ctx).length) {
      console.log(base, JSON.stringify(ctx));
    } else {
      console.log(base);
    }
  }
  
  // توابع سطحی
  export function debug(msg, ctx) { log('DEBUG', msg, ctx); }
  export function info(msg, ctx)  { log('INFO',  msg, ctx); }
  export function warn(msg, ctx)  { log('WARN',  msg, ctx); }
  export function error(msg, ctx) { log('ERROR', msg, ctx); }
  
  /**
   * اجرای یک تابع async با context ثابت در لاگ‌ها
   * مثال:
   *   await withLogContext(async log => {
   *     log.info("شروع شد");
   *     ...
   *   }, {module: "finance"});
   */
  export async function withLogContext(fn, ctx = {}) {
    const wrapper = {
      debug: (m, extra) => debug(m, { ...ctx, ...extra }),
      info:  (m, extra) => info(m,  { ...ctx, ...extra }),
      warn:  (m, extra) => warn(m,  { ...ctx, ...extra }),
      error: (m, extra) => error(m, { ...ctx, ...extra }),
    };
    return await fn(wrapper);
  }
  
  // اکسپورت عمومی
  export default { debug, info, warn, error, withLogContext };
  