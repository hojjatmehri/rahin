# ============================================================
# PowerShell Test Script for Rahin financeCollector.js
# ============================================================

Write-Host "=== تست کالکتور مالی Rahin ==="
$ErrorActionPreference = "Stop"

# مسیر ریشه پروژه
$root = "C:\Users\Administrator\Desktop\Projects\rahin"

# بررسی Node
Write-Host "Node version:"
node -v

# اجرای مستقیم تابع collectFinance در Node.js
Write-Host "`nدر حال اجرای collectFinance()..."

$script = @"
import { collectFinance } from '../src/collectors/financeCollector.js';
import { CONFIG } from '../src/config/Config.js';
import { ensureMinimalSchema } from '../src/db/schemaGuard.js';
import path from 'path';

console.log('[Test] اتصال به دیتابیس...');
await ensureMinimalSchema();

try {
  console.time('collectFinance');
  const res = await collectFinance();
  console.timeEnd('collectFinance');
  console.log('\\n=== خروجی collectFinance() ===');
  console.log(JSON.stringify(res, null, 2));

  // اگر جدول transactions وجود دارد، تعداد رکوردها را هم چاپ کن
  try {
    const db = CONFIG?.db;
    if (db) {
      const row = await db.get("SELECT COUNT(*) AS cnt FROM transactions");
      console.log('\\nتعداد رکوردهای transactions:', row?.cnt ?? 'نامشخص');
    } else {
      console.warn('⚠️ اتصال DB در CONFIG یافت نشد.');
    }
  } catch (e) {
    console.error('⚠️ خطا در شمارش رکوردها:', e?.message || e);
  }

} catch (err) {
  console.error('\\n❌ خطا در collectFinance:', err?.message || err);
}
"@

# اجرای inline با پشتیبانی از import/export
node --input-type=module -e $script
