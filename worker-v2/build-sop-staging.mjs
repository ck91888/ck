import { mkdir, copyFile, readFile, writeFile, rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '..');
const out = resolve(here, '.sop-staging-assets');
// Publish only this explicit asset list, never repository files or secrets.
await rm(out, { recursive: true, force: true });
await mkdir(resolve(out, 'sop'), { recursive: true });
await mkdir(resolve(out, 'shared'), { recursive: true });
for (const file of ['index.html', 'style.css', 'app.js']) {
  await copyFile(resolve(root, 'sop', file), resolve(out, 'sop', file));
}
for (const file of ['html5-qrcode.min.js', 'xlsx.full.min.js']) {
  await copyFile(resolve(root, 'shared', file), resolve(out, 'shared', file));
}
await writeFile(resolve(out, 'sop/config.js'), "window.SOP_API = location.origin + '/api';\n");
const page = resolve(out, 'sop/index.html');
const html = await readFile(page, 'utf8');
await writeFile(page, html.replace('<main>', '<main><p role="note" style="padding:12px;background:#fff0c2;color:#563b00;font-weight:bold">独立测试环境 · 仅使用虚拟订单和测试人员 / 테스트 전용</p>'));
await writeFile(resolve(out, '_redirects'), '/ /sop/ 302\n');
await writeFile(resolve(out, '_headers'), '/sop/*\n  Cache-Control: no-store\n');
console.log('Prepared isolated SOP test assets; API uses same-origin /api.');
