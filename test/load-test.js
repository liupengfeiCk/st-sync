'use strict';

// Resource check: node load-test.js <proxyPort> <proxyPid>
// Runs concurrent large sync downloads, plain passthrough downloads and chunked uploads,
// sampling the proxy's RSS throughout. Fails if RSS exceeds LIMIT_MB or any transfer is wrong.
const http = require('http');
const crypto = require('crypto');
const { execFileSync } = require('child_process');
const Cdc = require('../cdc.js');

const port = Number(process.argv[2]);
const pid = Number(process.argv[3]);
const limitMB = Number(process.env.LIMIT_MB || 160);
const MB = 1024 * 1024;
let failures = 0;
let peak = 0;

function rssMB() {
    try {
        return Number(execFileSync('ps', ['-o', 'rss=', '-p', String(pid)]).toString().trim()) / 1024;
    } catch {
        return -1;
    }
}

const sampler = setInterval(() => { peak = Math.max(peak, rssMB()); }, 200);
setTimeout(() => {
    console.log('FAIL timed out: a transfer is stuck');
    process.exit(2);
}, Number(process.env.TIMEOUT_MS || 120000)).unref();

function check(name, ok, detail = '') {
    if (!ok) failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'} ${name}${detail ? ' — ' + detail : ''}`);
}

/** Streams the response, hashing it instead of keeping it. */
function fetchDigest(method, path, headers = {}, body = null) {
    return new Promise((resolve, reject) => {
        const req = http.request({ host: '127.0.0.1', port, method, path, headers }, (res) => {
            const hash = crypto.createHash('sha1');
            const kept = [];
            let length = 0;
            res.on('data', (c) => {
                length += c.length;
                hash.update(c);
                if (path === '/__sync/chunks' || res.headers['x-sync-encoding'] || res.statusCode !== 200 || length < 4 * MB) kept.push(c);
            });
            res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, length, sha1: hash.digest('hex'), body: Buffer.concat(kept) }));
        });
        req.on('error', reject);
        req.end(body);
    });
}

async function syncDownload(path) {
    const manifest = await fetchDigest('GET', path, { 'x-sync-accept': 'cdc1' });
    if (manifest.headers['x-sync-encoding'] !== 'cdc1') return { passthrough: true, ...manifest };
    const { hashes, lengths } = Cdc.decodeManifest(new Uint8Array(manifest.body));
    const hash = crypto.createHash('sha1');
    let length = 0;
    for (let i = 0; i < hashes.length; i += 400) {
        const batch = hashes.slice(i, i + 400);
        const res = await fetchDigest('POST', '/__sync/chunks', { 'content-type': 'application/octet-stream' }, Buffer.from(Cdc.encodeHashList(batch)));
        if (res.status !== 200) throw new Error(`chunks ${res.status}`);
        const expected = lengths.slice(i, i + 400).reduce((a, b) => a + b, 0);
        if (res.body.length !== expected) throw new Error(`chunk batch length ${res.body.length} != ${expected}`);
        hash.update(res.body);
        length += res.body.length;
    }
    return { passthrough: false, length, sha1: hash.digest('hex'), manifestBytes: manifest.length };
}

async function upload(sizeMB, seed) {
    const data = crypto.createHash('sha256').update(seed).digest();
    const body = Buffer.alloc(sizeMB * MB);
    for (let o = 0; o < body.length; o += 32) crypto.createHash('sha256').update(data).update(String(o)).digest().copy(body, o);
    const parts = Cdc.split(body);
    const all = parts.hashes.map((_, i) => i);
    const envelope = Buffer.from(Cdc.encodeUpload(body, parts, all));
    const res = await fetchDigest('POST', '/api/sink', { 'content-type': 'application/octet-stream', 'x-sync-encoding': 'cdc1' }, envelope);
    const expected = crypto.createHash('sha1').update(body).digest('hex');
    return { ok: res.status === 200 && JSON.parse(res.body).sha1 === expected, status: res.status, error: res.headers['x-sync-error'] };
}

(async () => {
    const base = rssMB();
    console.log(`proxy rss at start: ${base.toFixed(0)}MB, limit ${limitMB}MB`);
    const scenario = process.env.SCENARIO;
    if (scenario) {
        const jobs = {
            plain: () => Promise.all(['a', 'b', 'c'].map((s) => fetchDigest('GET', `/api/huge?mb=${process.env.MB || 100}&seed=${s}`))),
            sync: () => Promise.all(['b', 'c', 'd'].map((s) => syncDownload(`/api/huge?mb=100&seed=${s}`))),
            upload: () => Promise.all(['u1', 'u2', 'u3'].map((s) => upload(33, s))),
        };
        const result = await jobs[scenario]();
        clearInterval(sampler);
        console.log(scenario, JSON.stringify(result.map((r) => r.ok ?? r.passthrough ?? r.status)), `peak ${Math.max(peak, rssMB()).toFixed(0)}MB, end ${rssMB().toFixed(0)}MB`);
        process.exit(0);
    }

    const plain = fetchDigest('GET', '/api/huge?mb=100&seed=a');
    const downloads = ['b', 'c', 'd', 'e'].map((seed) => syncDownload(`/api/huge?mb=100&seed=${seed}`));
    await new Promise((r) => setTimeout(r, 300));
    const uploads = ['u1', 'u2', 'u3'].map((seed) => upload(33, seed));
    const [plainRes, ...rest] = await Promise.all([plain, ...downloads, ...uploads]);
    const downloadResults = rest.slice(0, 4);
    const uploadResults = rest.slice(4);

    check('plain 100MB passthrough streamed', plainRes.status === 200 && plainRes.length >= 100 * MB, `${(plainRes.length / MB).toFixed(1)}MB`);
    const synced = downloadResults.filter((r) => !r.passthrough);
    check('concurrent sync downloads complete', downloadResults.every((r) => r.length >= 100 * MB), downloadResults.map((r) => `${r.passthrough ? 'passthrough' : 'sync'} ${(r.length / MB).toFixed(1)}MB`).join(', '));
    check('excess downloads queue for a slot before degrading', synced.length >= 3, `${synced.length} of 4 downloads synced`);
    const verify = await Promise.all(downloadResults.map((r, i) => fetchDigest('GET', `/api/huge?mb=100&seed=${'bcde'[i]}`)));
    check('sync downloads match plain bytes', downloadResults.every((r, i) => r.sha1 === verify[i].sha1));
    check('chunked uploads never refused', uploadResults.every((r) => r.ok), uploadResults.map((r) => r.ok ? 'ok' : `${r.status} ${r.error}`).join(', '));

    clearInterval(sampler);
    peak = Math.max(peak, rssMB());
    check(`proxy peak rss under ${limitMB}MB`, peak > 0 && peak < limitMB, `peak ${peak.toFixed(0)}MB`);
    console.log(failures ? `${failures} FAILED` : 'ALL PASSED');
    process.exit(failures ? 1 : 0);
})().catch((err) => {
    console.error(err);
    process.exit(1);
});
