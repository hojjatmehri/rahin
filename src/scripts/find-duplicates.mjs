import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const ROOT = path.resolve(process.cwd(), 'src');
const IGNORE = new Set(['node_modules', '.git', '.pm2logs']);
const exts = new Set(['.js', '.mjs', '.cjs', '.ts', '.jsx', '.tsx', '.json']);

function* walk(dir) {
  for (const name of fs.readdirSync(dir)) {
    if (IGNORE.has(name)) continue;
    const p = path.join(dir, name);
    const st = fs.statSync(p);
    if (st.isDirectory()) yield* walk(p);
    else yield p;
  }
}

const byHash = new Map();
const byBase = new Map();

for (const file of walk(ROOT)) {
  const ext = path.extname(file);
  if (!exts.has(ext)) continue;
  const data = fs.readFileSync(file);
  const hash = crypto.createHash('md5').update(data).digest('hex');
  const base = path.basename(file).toLowerCase();

  if (!byHash.has(hash)) byHash.set(hash, []);
  byHash.get(hash).push(file);

  if (!byBase.has(base)) byBase.set(base, []);
  byBase.get(base).push(file);
}

console.log('=== Exact duplicates (same content) ===');
for (const [h, files] of byHash.entries()) {
  if (files.length > 1) {
    console.log(`\n[${h}]`);
    files.forEach(f => console.log(' -', f));
  }
}

console.log('\n=== Same name (possible conceptual duplicates) ===');
for (const [b, files] of byBase.entries()) {
  if (files.length > 1) {
    console.log(`\n[${b}]`);
    files.forEach(f => console.log(' -', f));
  }
}
