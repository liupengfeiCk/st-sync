'use strict';

// CPU cost of chunking vs the gzip the plain path already pays: node bench.js <file>
const fs = require('fs');
const zlib = require('zlib');
const Cdc = require('../cdc.js');

const data = fs.readFileSync(process.argv[2]);
const mb = data.length / 1048576;

function time(name, fn, runs = 3) {
    let best = Infinity;
    let result;
    for (let i = 0; i < runs; i++) {
        const t0 = process.hrtime.bigint();
        result = fn();
        best = Math.min(best, Number(process.hrtime.bigint() - t0) / 1e6);
    }
    console.log(`${name.padEnd(34)} ${best.toFixed(0).padStart(6)}ms  ${(mb / (best / 1000)).toFixed(0).padStart(5)}MB/s`);
    return result;
}

console.log(`input ${mb.toFixed(1)}MB`);
const parts = time('cdc split + hash (batch)', () => Cdc.split(data));
time('cdc streaming chunker + hash', () => {
    const chunker = Cdc.createChunker();
    let n = 0;
    for (let o = 0; o < data.length; o += 65536) {
        for (const c of chunker.push(data.subarray(o, o + 65536))) { Cdc.hash(c, 0, c.length); n++; }
    }
    for (const c of chunker.end()) { Cdc.hash(c, 0, c.length); n++; }
    return n;
});
time('encode upload envelope (no inline)', () => Cdc.encodeUpload(data, parts, []));
const gz = time('gzip level 6 (plain path)', () => zlib.gzipSync(data), 1);
time('gunzip', () => zlib.gunzipSync(gz));
console.log(`${parts.hashes.length} chunks, gzip ratio ${(gz.length / data.length * 100).toFixed(1)}%`);
