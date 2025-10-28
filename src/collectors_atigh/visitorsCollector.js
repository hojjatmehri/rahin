import '../../logger.js';
/**
 * visitorsCollector.js
 * اجرای جمع‌آوری بازدیدکننده‌ها برای یک تاریخ.
 * پیش‌فرض: دیروز تهران
 * استفاده:
 *   node src/collectors/visitorsCollector.js --date 2025-10-18
 *   node src/collectors/visitorsCollector.js --yesterday
 */
import { collectVisitorsDailySmart, yesterdayTehranStr } from "./wordpress_visitors.js";

function parseArg(flag, fallback=null) {
  const ix = process.argv.indexOf(flag);
  if (ix >= 0 && process.argv[ix+1]) return process.argv[ix+1];
  return fallback;
}

async function main() {
  const forceYesterday = process.argv.includes('--yesterday');
  const date = forceYesterday ? yesterdayTehranStr() : (parseArg('--date') || yesterdayTehranStr());
  const mainPath = parseArg('--db', './db_atigh.sqlite');
  const archivePath = parseArg('--db-archive', './db_archive.sqlite');

  try {
    const res = collectVisitorsDailySmart(date, mainPath, archivePath);
    console.log(JSON.stringify({ ok: true, ...res }));
  } catch (e) {
    console.error('collectVisitorsDailySmart failed:', e?.message || e);
    process.exit(1);
  }
}

main();

