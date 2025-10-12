// src/scripts/dedupe_project.mjs
// هدف: 1) حذف داپلیکیت‌ها  2) بازنویسی ایمپورت‌ها به مسیر مرجع  3) rename corn → cron
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const ROOT = path.resolve(process.cwd());
const SRC  = path.join(ROOT, 'src');
const DRY  = process.argv.includes('--dry');

const exts = new Set(['.js','.mjs','.cjs','.ts','.tsx','.jsx','.json']);
const IGNORE_DIRS = new Set(['node_modules','.git','.pm2logs','.vscode','.idea']);
const log = (...a)=>console.log(...a);

function* walk(dir) {
  for (const name of fs.readdirSync(dir)) {
    if (IGNORE_DIRS.has(name)) continue;
    const p = path.join(dir, name);
    const st = fs.statSync(p);
    if (st.isDirectory()) yield* walk(p); else yield p;
  }
}

// 1) سیاست‌ها: فایل مرجع (Single Source of Truth)
const KEEP_EXACT = {
  // periods.js → فقط این یکی بماند
  'periods.js': path.join(SRC, 'lib','time','periods.js'),
};
const RENAME_CONCEPTUAL = [
  // summaryBuilder.js → تفکیک نقش‌ها
  {
    sources: [
      path.join(SRC,'analytics','summaryBuilder.js'),
      path.join(SRC,'reporters','summaryBuilder.js'),
    ],
    plan: [
      { from: path.join(SRC,'analytics','summaryBuilder.js'),
        to:   path.join(SRC,'analytics','summaryModel.js') },
      { from: path.join(SRC,'reporters','summaryBuilder.js'),
        to:   path.join(SRC,'reporters','summaryPrinter.js') },
    ],
    rewrite: [
      // ایمپورت‌های قدیمی را به نام‌های جدید ببر
      { pattern: /(['"])..\/analytics\/summaryBuilder\.js\1/g, replace: `'../analytics/summaryModel.js'` },
      { pattern: /(['"])..\/reporters\/summaryBuilder\.js\1/g, replace: `'../reporters/summaryPrinter.js'` },
      { pattern: /(['"])@\/analytics\/summaryBuilder\1/g,    replace: `'@/analytics/summaryModel'` },
      { pattern: /(['"])@\/reporters\/summaryBuilder\1/g,    replace: `'@/reporters/summaryPrinter'` },
    ]
  }
];

// 2) بازنویسی ایمپورت‌ها برای periods.js
const IMPORT_REWRITES = [
  // analytics/periods.js → lib/time/periods.js
  { pattern: /(['"])(\.{1,2}\/)+analytics\/periods\.js\1/g, replace: `'../lib/time/periods.js'` },
  { pattern: /(['"])@\/analytics\/periods\1/g,               replace: `'@/lib/time/periods'` },
];

// 3) rename پوشه corn → cron
function renameCornToCron() {
  const corn = path.join(SRC,'corn');
  const cron = path.join(SRC,'cron');
  if (fs.existsSync(corn)) {
    log(`rename dir: ${corn} -> ${cron}`);
    if (!DRY) {
      if (fs.existsSync(cron)) {
        // اگر از قبل هست، محتویات corn را به cron ببریم و بعد corn را حذف کنیم
        for (const f of fs.readdirSync(corn)) {
          const from = path.join(corn,f), to = path.join(cron,f);
          if (fs.existsSync(to)) fs.rmSync(to, { force:true, recursive:true });
          fs.renameSync(from, to);
        }
        fs.rmdirSync(corn);
      } else {
        fs.renameSync(corn, cron);
      }
    }
    // بازنویسی ایمپورت‌ها
    rewriteInTree([
      { pattern: /(['"])(\.{0,2}\/)?corn\//g, replace: `'$1cron/` }, // relative
      { pattern: /@\/corn\//g, replace: '@/cron/' }                  // alias
    ]);
  }
}

// ابزار بازنویسی همه فایل‌های کد
function rewriteInTree(rules) {
  for (const file of walk(SRC)) {
    const ext = path.extname(file);
    if (!exts.has(ext)) continue;
    let txt = fs.readFileSync(file, 'utf8');
    let changed = false;
    for (const r of rules) {
      const before = txt;
      txt = txt.replace(r.pattern, r.replace);
      if (txt !== before) changed = true;
    }
    if (changed) {
      log(`rewrite imports: ${file}`);
      if (!DRY) fs.writeFileSync(file, txt, 'utf8');
    }
  }
}

// 4) حذف داپلیکیت‌های دقیق (hash برابر) و نگه‌داشت مرجع
function dedupeExact() {
  const byHash = new Map();
  for (const file of walk(SRC)) {
    const ext = path.extname(file);
    if (!exts.has(ext)) continue;
    const buf = fs.readFileSync(file);
    const h = crypto.createHash('md5').update(buf).digest('hex');
    if (!byHash.has(h)) byHash.set(h, []);
    byHash.get(h).push(file);
  }
  for (const [h, files] of byHash) {
    if (files.length < 2) continue;
    // اگر basename یکی از KEEP_EXACT است، مرجع را نگه می‌داریم
    const bases = files.map(f=>path.basename(f));
    const uniqBases = Array.from(new Set(bases));
    for (const b of uniqBases) {
      if (KEEP_EXACT[b]) {
        const keep = KEEP_EXACT[b];
        for (const f of files.filter(x=>path.basename(x)===b && x !== keep)) {
          log(`delete exact dup (keep ${path.relative(ROOT,keep)}): ${path.relative(ROOT,f)}`);
          if (!DRY) fs.rmSync(f, { force:true });
        }
      }
    }
  }
}

// 5) برنامه‌ی مخصوص summaryBuilder → جابجایی + بازنویسی + حذف نسخه‌های قدیمی
function applyConceptualPlans() {
  for (const plan of RENAME_CONCEPTUAL) {
    // move files
    for (const step of plan.plan) {
      if (fs.existsSync(step.from)) {
        log(`rename file: ${path.relative(ROOT, step.from)} -> ${path.relative(ROOT, step.to)}`);
        if (!DRY) {
          // اگر مقصد هست، حذفش کن
          if (fs.existsSync(step.to)) fs.rmSync(step.to, { force:true });
          fs.renameSync(step.from, step.to);
        }
      }
    }
    // rewrite imports
    if (plan.rewrite?.length) rewriteInTree(plan.rewrite);
    // حذف هر فایل باقی‌مانده با نام قدیم (اگر کپی دیگری باقی‌ست)
    for (const src of plan.sources) {
      if (fs.existsSync(src)) {
        log(`delete old conceptual duplicate: ${path.relative(ROOT, src)}`);
        if (!DRY) fs.rmSync(src, { force:true });
      }
    }
  }
}

// 6) بازنویسی ایمپورت‌های عمومی (periods.js و …)
function rewriteGeneralImports() {
  rewriteInTree(IMPORT_REWRITES);
}

// اجرا
log(`=== DEDUPE START (dry=${DRY}) ===`);
renameCornToCron();
applyConceptualPlans();
rewriteGeneralImports();
dedupeExact();
log('=== DEDUPE DONE ===');
