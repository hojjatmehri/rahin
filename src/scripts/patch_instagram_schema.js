// src/scripts/patch_instagram_schema.js
import { all, exec, close } from '../db/db.js';

async function ensureColumn(table, col, def = 'TEXT') {
  const cols = await all(`PRAGMA table_info('${table}')`);
  const has = new Set(cols.map(c => c.name));
  if (!has.has(col)) {
    await exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${def};`);
    console.log(`+ added ${table}.${col}`);
  } else {
    console.log(`= exists ${table}.${col}`);
  }
}

(async () => {
  try {
    await ensureColumn('atigh_instagram_dev', 'event_type', 'TEXT');
    await ensureColumn('atigh_instagram_dev', 'payload', 'TEXT');
    console.log('✅ patch done');
  } catch (e) {
    console.error('❌ patch failed:', e.message);
    process.exit(1);
  } finally {
    try { await close(); } catch {}
  }
})();
