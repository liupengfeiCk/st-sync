'use strict';

// Summarizes st-sync access log traffic for one client: node log-summary.js <ip> [sinceIso] [logFile]
const fs = require('fs');

const [ip, since = '', file = '/var/log/st-sync/access.log'] = process.argv.slice(2);
const rows = fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l))
    .filter((r) => r.ip === ip && r.t >= since);
const mb = (n) => (n / 1048576).toFixed(2);

const groups = new Map();
for (const r of rows) {
    const key = r.u.startsWith('/__sync/chunks') ? 'chunks' : r.mode;
    const g = groups.get(key) || { n: 0, wireDown: 0, wireUp: 0, origDown: 0, origUp: 0 };
    g.n++;
    g.wireDown += r.out;
    g.wireUp += r.in;
    if (r.mode.includes('down')) g.origDown += r.orig;
    if (r.mode.startsWith('up')) g.origUp += r.orig;
    groups.set(key, g);
}
console.log(`${rows.length} requests from ${ip} ${rows.length ? `${rows[0].t} .. ${rows[rows.length - 1].t}` : ''}`);
console.log('group            n   down MB   up MB   (sync originals: down MB / up MB)');
let down = 0;
let up = 0;
for (const [key, g] of [...groups].sort((a, b) => b[1].wireDown + b[1].wireUp - a[1].wireDown - a[1].wireUp)) {
    down += g.wireDown;
    up += g.wireUp;
    console.log(`${key.padEnd(14)} ${String(g.n).padStart(4)} ${mb(g.wireDown).padStart(9)} ${mb(g.wireUp).padStart(7)}   ${g.origDown || g.origUp ? `${mb(g.origDown)} / ${mb(g.origUp)}` : ''}`);
}
console.log(`total wire: down ${mb(down)}MB, up ${mb(up)}MB (HTTP bodies only)`);
console.log('largest non-sync transfers:');
rows.filter((r) => !r.u.startsWith('/__sync') && !r.mode.includes('down') && !r.mode.startsWith('up'))
    .sort((a, b) => b.out + b.in - a.out - a.in).slice(0, 8)
    .forEach((r) => console.log(`  ${mb(r.out + r.in).padStart(7)}MB ${r.m} ${r.u.slice(0, 70)} (${r.mode}, ${r.ms}ms)`));
