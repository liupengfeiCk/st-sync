'use strict';

// Protocol-level checks against a running proxy + fake upstream: node protocol-test.js <proxyPort>
const http = require('http');
const zlib = require('zlib');
const Cdc = require('../cdc.js');

const port = Number(process.argv[2] || 18345);
const auth = process.env.AUTH || '';
let failures = 0;

function request(method, path, headers = {}, body = null) {
    return new Promise((resolve, reject) => {
        const req = http.request({ host: '127.0.0.1', port, method, path, headers: { ...(auth ? { authorization: auth } : {}), ...headers } }, (res) => {
            const parts = [];
            res.on('data', (c) => parts.push(c));
            res.on('end', () => {
                let data = Buffer.concat(parts);
                if (res.headers['content-encoding'] === 'gzip') data = zlib.gunzipSync(data);
                resolve({ status: res.statusCode, headers: res.headers, body: data });
            });
        });
        req.on('error', reject);
        req.end(body);
    });
}

function check(name, ok, detail = '') {
    if (!ok) failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'} ${name}${detail ? ' — ' + detail : ''}`);
}

(async () => {
    const html = await request('GET', '/', { 'accept-encoding': 'gzip' });
    const text = html.body.toString();
    check('html injected first in head', /<head>\s*<script src="\/__sync\/shim\.js\?v=[0-9a-f]{12}"><\/script>/.test(text));
    const again = await request('GET', '/', { 'if-none-match': html.headers.etag });
    check('html 304 with injected etag', again.status === 304, `etag ${html.headers.etag}`);

    const shimUrl = text.match(/\/__sync\/shim\.js\?v=[0-9a-f]{12}/)[0];
    const shim = await request('GET', shimUrl);
    check('shim served', shim.status === 200 && shim.body.includes('StSyncCdc') && /immutable/.test(shim.headers['cache-control']));

    const data = Buffer.from(JSON.stringify(Array.from({ length: 20000 }, (_, i) => ({ i, s: 'protocol-' + i }))));
    const parts = Cdc.split(data);
    const noInline = Cdc.encodeUpload(data, parts, []);
    const missing = await request('POST', '/api/save', { 'content-type': 'application/json', 'x-sync-encoding': 'cdc1' }, Buffer.from(noInline));
    const missingList = missing.status === 409 ? JSON.parse(missing.body).missing : [];
    check('unknown chunks -> 409 with missing list', missing.status === 409 && missingList.length > 0, `${missingList.length} of ${parts.hashes.length}`);

    const full = Cdc.encodeUpload(data, parts, missingList);
    const ok = await request('POST', '/api/save', { 'content-type': 'application/json', 'x-sync-encoding': 'cdc1' }, Buffer.from(full));
    const saved = JSON.parse(ok.body);
    check('upload reconstructed', ok.status === 200 && saved.length === data.length && saved.type === 'application/json');

    const second = await request('POST', '/api/save', { 'content-type': 'application/json', 'x-sync-encoding': 'cdc1' }, Buffer.from(noInline));
    check('known chunks accepted without inline data', second.status === 200, `envelope ${noInline.length} bytes for ${data.length}`);

    const corrupted = Buffer.from(Cdc.encodeUpload(data, parts, [0]));
    corrupted[corrupted.length - 1] ^= 0xff;
    const bad = await request('POST', '/api/save', { 'content-type': 'application/json', 'x-sync-encoding': 'cdc1' }, corrupted);
    check('corrupted inline chunk rejected before upstream', bad.status === 400 && bad.headers['x-sync-error'] === 'hash-mismatch');

    const garbage = await request('POST', '/api/save', { 'x-sync-encoding': 'cdc1' }, Buffer.from('not an envelope'));
    check('bad envelope rejected with x-sync-error', garbage.status === 400 && garbage.headers['x-sync-error'] === 'bad-envelope');

    const manifest = await request('POST', '/api/big', { 'x-sync-accept': 'cdc1', 'content-type': 'application/json' }, '{}');
    check('large response replaced with manifest', manifest.headers['x-sync-encoding'] === 'cdc1' && manifest.headers['x-sync-content-type'].startsWith('application/json'));
    const decoded = Cdc.decodeManifest(new Uint8Array(manifest.body));
    const chunks = await request('POST', '/__sync/chunks', { 'content-type': 'application/octet-stream', 'accept-encoding': 'gzip' }, Buffer.from(Cdc.encodeHashList(decoded.hashes)));
    const plain = await request('POST', '/api/big', { 'content-type': 'application/json' }, '{}');
    check('chunks reassemble the original response', chunks.status === 200 && Buffer.compare(chunks.body, plain.body) === 0, `${plain.body.length} bytes`);

    const unknown = await request('POST', '/__sync/chunks', {}, Buffer.from(Cdc.encodeHashList(['00000000000000000000000000000000'])));
    check('unknown chunk -> 409', unknown.status === 409);

    const spoofed = JSON.parse((await request('GET', '/api/headers', { 'x-real-ip': '10.9.9.9', 'x-forwarded-for': '10.8.8.8' })).body);
    check('client-supplied forwarding headers replaced', spoofed['x-real-ip'] === '127.0.0.1' && spoofed['x-forwarded-for'] === '127.0.0.1', `${spoofed['x-real-ip']} / ${spoofed['x-forwarded-for']}`);
    const small = await request('GET', '/api/state', { 'x-sync-accept': 'cdc1', 'accept-encoding': 'gzip' });
    check('small sync response passes through', small.status === 200 && !small.headers['x-sync-encoding'] && typeof JSON.parse(small.body).aborted === 'number');
    check('every response carries x-sync-proxy', !!small.headers['x-sync-proxy'] && !!manifest.headers['x-sync-proxy'] && !!garbage.headers['x-sync-proxy']);
    check('every response carries x-sync-store', /^[0-9a-f]{16}$/.test(small.headers['x-sync-store'] || '') && small.headers['x-sync-store'] === manifest.headers['x-sync-store']);
    check('scope-resolved responses carry x-sync-scope', /^[0-9a-f]{24}$/.test(manifest.headers['x-sync-scope'] || '') && ok.headers['x-sync-scope'] === manifest.headers['x-sync-scope'] && !small.headers['x-sync-scope']);

    const health = await request('GET', '/__sync/health');
    const healthBody = health.status === 200 ? JSON.parse(health.body) : {};
    check('health endpoint', health.status === 200 && typeof healthBody.rssMB === 'number', JSON.stringify(healthBody));

    const worker = await request('GET', '/__sync/worker.js');
    check('worker served', worker.status === 200 && worker.body.includes('StSyncCdc') && worker.body.includes('onmessage'));

    const huge = await request('GET', '/api/huge?mb=20', { 'x-sync-accept': 'cdc1' });
    const hugeManifest = huge.headers['x-sync-encoding'] === 'cdc1' ? Cdc.decodeManifest(new Uint8Array(huge.body)) : null;
    const hugeTotal = hugeManifest ? hugeManifest.lengths.reduce((a, b) => a + b, 0) : 0;
    check('streamed response without content-length chunked', hugeTotal >= 20 * 1024 * 1024, `${hugeTotal} bytes in ${hugeManifest && hugeManifest.hashes.length} chunks`);

    if (process.env.OFF_FILE) {
        require('fs').writeFileSync(process.env.OFF_FILE, '');
        await new Promise((r) => setTimeout(r, 2500));
        const offSave = await request('POST', '/api/save', { 'content-type': 'application/json', 'x-sync-encoding': 'cdc1' }, Buffer.from(noInline));
        check('off switch: envelope still reconstructed', offSave.status === 200 && JSON.parse(offSave.body).length === data.length && offSave.headers['x-sync-proxy'] === 'off');
        const offChunks = await request('POST', '/__sync/chunks', { 'content-type': 'application/octet-stream' }, Buffer.from(Cdc.encodeHashList(decoded.hashes.slice(0, 3))));
        check('off switch: chunks still served', offChunks.status === 200 && offChunks.body.length > 0);
        const offBig = await request('POST', '/api/big', { 'x-sync-accept': 'cdc1', 'content-type': 'application/json' }, '{}');
        check('off switch: responses pass through', offBig.status === 200 && !offBig.headers['x-sync-encoding']);
        require('fs').unlinkSync(process.env.OFF_FILE);
        await new Promise((r) => setTimeout(r, 2500));
        const onBig = await request('POST', '/api/big', { 'x-sync-accept': 'cdc1', 'content-type': 'application/json' }, '{}');
        check('off switch removed: sync resumes', onBig.headers['x-sync-encoding'] === 'cdc1');
    }

    console.log(failures ? `${failures} FAILED` : 'ALL PASSED');
    process.exit(failures ? 1 : 0);
})();
