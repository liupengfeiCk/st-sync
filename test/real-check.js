'use strict';

// Read-only end-to-end check against the real SillyTavern through st-sync, run on the server:
//   node test/real-check.js [stDir] [port]
// Both default to the values install.sh wrote to /etc/st-sync/env.
// Credentials are read from SillyTavern's config.yaml and never printed. Nothing is written to user data.
const fs = require('fs');
const http = require('http');
const zlib = require('zlib');
const crypto = require('crypto');
const Cdc = require('../cdc.js');

const installed = {};
try {
    for (const line of fs.readFileSync('/etc/st-sync/env', 'utf8').split('\n')) {
        const match = /^(\w+)=(.*)$/.exec(line);
        if (match) installed[match[1]] = match[2];
    }
} catch { /* not installed with install.sh */ }
const ST_DIR = process.argv[2] || installed.ST_SYNC_ST_DIR;
const port = Number(process.argv[3] || installed.ST_SYNC_LISTEN_PORT || 8000);
if (!ST_DIR) {
    console.error('usage: node test/real-check.js <SillyTavern dir> [port]');
    process.exit(2);
}
const yaml = require(`${ST_DIR}/node_modules/yaml`);
const config = yaml.parse(fs.readFileSync(`${ST_DIR}/config.yaml`, 'utf8'));
const auth = config.basicAuthMode
    ? 'Basic ' + Buffer.from(`${config.basicAuthUser.username}:${config.basicAuthUser.password}`).toString('base64')
    : null;
const cookies = new Map();
let failures = 0;

function check(name, ok, detail = '') {
    if (!ok) failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'} ${name}${detail ? ' — ' + detail : ''}`);
}

function request(method, path, headers = {}, body = null) {
    return new Promise((resolve, reject) => {
        const all = { ...headers };
        if (auth) all.authorization = auth;
        if (cookies.size) all.cookie = [...cookies].map(([k, v]) => `${k}=${v}`).join('; ');
        const req = http.request({ host: '127.0.0.1', port, method, path, headers: all }, (res) => {
            for (const line of res.headers['set-cookie'] || []) {
                const [pair] = line.split(';');
                const i = pair.indexOf('=');
                cookies.set(pair.slice(0, i), pair.slice(i + 1));
            }
            const parts = [];
            let wire = 0;
            res.on('data', (c) => { parts.push(c); wire += c.length; });
            res.on('end', () => {
                let data = Buffer.concat(parts);
                if (res.headers['content-encoding'] === 'gzip') data = zlib.gunzipSync(data);
                resolve({ status: res.statusCode, headers: res.headers, body: data, wire });
            });
        });
        req.on('error', reject);
        req.end(body);
    });
}

const sha1 = (b) => crypto.createHash('sha1').update(b).digest('hex');
const mb = (n) => `${(n / 1048576).toFixed(2)}MB`;

async function syncDownload(path, headers, body) {
    const manifest = await request('POST', path, { ...headers, 'x-sync-accept': 'cdc1' }, body);
    if (manifest.headers['x-sync-encoding'] !== 'cdc1') return { manifest, data: manifest.body, wire: manifest.wire };
    const { hashes } = Cdc.decodeManifest(new Uint8Array(manifest.body));
    const pieces = [];
    let wire = manifest.wire;
    for (let i = 0; i < hashes.length; i += 2000) {
        const res = await request('POST', '/__sync/chunks', { 'content-type': 'application/octet-stream', 'accept-encoding': 'gzip', ...headers }, Buffer.from(Cdc.encodeHashList(hashes.slice(i, i + 2000))));
        if (res.status !== 200) throw new Error(`chunks ${res.status}`);
        pieces.push(res.body);
        wire += res.wire;
    }
    return { manifest, data: Buffer.concat(pieces), wire, chunks: hashes.length };
}

(async () => {
    const page = await request('GET', '/', { 'accept-encoding': 'gzip' });
    check('index served with shim', page.status === 200 && /<script src="\/__sync\/shim\.js\?v=/.test(page.body.toString()), `status ${page.status}, proxy ${page.headers['x-sync-proxy']}`);

    const token = JSON.parse((await request('GET', '/csrf-token')).body).token;
    const headers = { 'content-type': 'application/json', 'x-csrf-token': token };

    for (const path of ['/api/settings/get', '/api/characters/all']) {
        const plain = await request('POST', path, { ...headers, 'accept-encoding': 'gzip' }, '{}');
        const first = await syncDownload(path, headers, '{}');
        const second = await syncDownload(path, headers, '{}');
        check(`${path} sync download identical`, plain.status === 200 && sha1(first.data) === sha1(plain.body) && sha1(second.data) === sha1(plain.body),
            `${mb(plain.body.length)} raw, plain gzip wire ${mb(plain.wire)}, manifest ${mb(second.manifest.wire)} for ${first.chunks} chunks`);
    }

    const messages = Array.from({ length: 400 }, (_, i) => ({ role: i % 2 ? 'assistant' : 'user', content: `turn ${i}: ` + 'The quick brown fox jumps over the lazy dog. '.repeat(20) }));
    const body = Buffer.from(JSON.stringify(messages));
    const plainCount = JSON.parse((await request('POST', '/api/tokenizers/openai/count?model=gpt-4o', headers, body)).body).token_count;
    const parts = Cdc.split(body);
    let envelope = Cdc.encodeUpload(body, parts, parts.hashes.map((_, i) => i));
    const up1 = await request('POST', '/api/tokenizers/openai/count?model=gpt-4o', { ...headers, 'x-sync-encoding': 'cdc1' }, Buffer.from(envelope));
    envelope = Cdc.encodeUpload(body, parts, []);
    const up2 = await request('POST', '/api/tokenizers/openai/count?model=gpt-4o', { ...headers, 'x-sync-encoding': 'cdc1' }, Buffer.from(envelope));
    const count1 = up1.status === 200 ? JSON.parse(up1.body).token_count : up1.status;
    const count2 = up2.status === 200 ? JSON.parse(up2.body).token_count : up2.status;
    check('chunked upload parsed by SillyTavern', count1 === plainCount && count2 === plainCount,
        `${body.length} bytes, tokens plain ${plainCount} / inline ${count1} / hashes-only (${envelope.length} bytes) ${count2}`);

    console.log(failures ? `${failures} FAILED` : 'ALL PASSED');
    process.exit(failures ? 1 : 0);
})().catch((err) => {
    console.error('ERROR', err.message);
    process.exit(1);
});
