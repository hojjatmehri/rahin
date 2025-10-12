// print-tree.mjs
// Usage:
//   node print-tree.mjs [startPath] --ignore "node_modules,.git,dist" [--max-depth 5]
// Notes:
//   - ESM only (.mjs). No external packages.
//   - --ignore: لیست جداشده با کاما؛ از الگوهای ساده * و ? پشتیبانی می‌کند.
//   - مثال: node print-tree.mjs --ignore "node_modules,.git"

import fs from 'fs';
import path from 'path';
import os from 'os';
import url from 'url';

const argv = process.argv.slice(2);

// ---- parse args
function parseArgs(args) {
  const out = { start: '.', ignore: [], maxDepth: Infinity };
  let i = 0;
  while (i < args.length) {
    const a = args[i];
    if (a === '--ignore') {
      const v = args[i + 1] || '';
      out.ignore = v
        .split(',')
        .map(s => s.trim())
        .filter(Boolean);
      i += 2;
    } else if (a === '--max-depth') {
      const v = Number(args[i + 1]);
      out.maxDepth = Number.isFinite(v) && v >= 0 ? v : out.maxDepth;
      i += 2;
    } else if (a.startsWith('--')) {
      // unknown flag → skip
      i += 1;
    } else {
      out.start = a;
      i += 1;
    }
  }
  return out;
}

const { start, ignore, maxDepth } = parseArgs(argv);
const startPath = path.resolve(start);

// ---- glob → RegExp (supports * and ?)
function escapeRegex(s) {
  return s.replace(/[|\\{}()[\]^$+./]/g, '\\$&');
}
function globToRegExp(glob) {
  // Treat leading/trailing spaces as literal after trim already done
  let re = '';
  for (let i = 0; i < glob.length; i++) {
    const ch = glob[i];
    if (ch === '*') re += '.*';
    else if (ch === '?') re += '.';
    else re += escapeRegex(ch);
  }
  // match segment or entire relpath; we’ll test both
  return new RegExp(`^${re}$`, 'i');
}

const ignoreRegexes = ignore.map(globToRegExp);

// ---- ignore logic
function shouldIgnore(entryName, relPath) {
  if (ignoreRegexes.length === 0) return false;
  const segs = relPath.split(path.sep);
  return ignoreRegexes.some(rx => {
    if (rx.test(entryName)) return true;
    if (rx.test(relPath)) return true;
    return segs.some(s => rx.test(s));
  });
}

// ---- FS helpers
function safeReadDir(p) {
  try {
    return fs.readdirSync(p, { withFileTypes: true });
  } catch (e) {
    return Object.assign(new Error('EACCESS'), { code: 'EACCESS', message: e.message });
  }
}

function isSymlinkToDir(fullPath, dirent) {
  if (!dirent.isSymbolicLink()) return false;
  try {
    const real = fs.realpathSync(fullPath);
    return fs.existsSync(real) && fs.statSync(real).isDirectory();
  } catch {
    return false;
  }
}

// ---- tree printing
const BR = os.EOL;
const PIPE = '│  ';
const TEE = '├──';
const ELB = '└──';
const IND = '   ';

function printLine(prefix, connector, name, note = '') {
  process.stdout.write(`${prefix}${connector} ${name}${note ? ' ' + note : ''}${BR}`);
}

function listDir(currPath, relPath = '', prefix = '', depth = 0) {
  const dirents = safeReadDir(currPath);
  if (dirents instanceof Error) {
    printLine(prefix, ELB, `[ERROR: ${dirents.message}]`);
    return;
  }

  // filter + sort: dirs first, then files; alphabetical
  const entries = dirents
    .filter(d => !shouldIgnore(d.name, path.join(relPath, d.name)))
    .sort((a, b) => {
      if (a.isDirectory() !== b.isDirectory()) return a.isDirectory() ? -1 : 1;
      return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' });
    });

  entries.forEach((dirent, idx) => {
    const isLast = idx === entries.length - 1;
    const connector = isLast ? ELB : TEE;
    const childPrefix = prefix + (isLast ? IND : PIPE);
    const full = path.join(currPath, dirent.name);
    const rel = path.join(relPath, dirent.name);

    const symlink = dirent.isSymbolicLink();
    let note = '';
    if (symlink) {
      try {
        const real = fs.realpathSync(full);
        note = `-> ${real}`;
      } catch {
        note = '-> [broken]';
      }
    }

    printLine(prefix, connector, dirent.name, note);

    const canDescend =
      (dirent.isDirectory() || isSymlinkToDir(full, dirent)) &&
      depth < maxDepth;

    if (canDescend) {
      listDir(full, rel, childPrefix, depth + 1);
    }
  });
}

// ---- main
(function main() {
  // header
  console.log(startPath);
  // if start itself is file, just print it
  let stat;
  try {
    stat = fs.lstatSync(startPath);
  } catch (e) {
    console.error(`Path not found: ${startPath}`);
    process.exit(1);
  }

  if (stat.isFile()) {
    // single file output
    printLine('', ELB, path.basename(startPath));
    process.exit(0);
  }

  listDir(startPath, '', '', 0);
})();
