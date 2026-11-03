'use strict';

/**
 * st-sync: a reverse proxy in front of SillyTavern that turns full-document uploads/downloads
 * into content-defined chunk deltas. SillyTavern itself is not modified.
 *
 * Resource rules: request and response bodies are never held in memory as a whole; they are
 * processed chunk by chunk (<= 64KB) and spilled to the on-disk chunk store. Whenever the process
 * is over its memory/concurrency/disk budget, or the off switch file exists, it degrades to plain
 * streaming passthrough, which needs no buffering.
 *
 * Node >= 18.15, no dependencies.
 */

const crypto = require('crypto');
const fs = require('fs');
const fsp = fs.promises;
const http = require('http');
const net = require('net');
const path = require('path');
const { PassThrough } = require('stream');
const zlib = require('zlib');
const Cdc = require('./cdc.js');

const MB = 1024 * 1024;
const env = (name, fallback) => (process.env[name] !== undefined ? process.env[name] : fallback);
const config = {
    listenHost: env('ST_SYNC_LISTEN_HOST', '0.0.0.0'),
    listenPort: Number(env('ST_SYNC_LISTEN_PORT', 8000)),
    upstreamHost: env('ST_SYNC_UPSTREAM_HOST', '127.0.0.1'),
    upstreamPort: Number(env('ST_SYNC_UPSTREAM_PORT', 8010)),
    // Only when every client reaches st-sync through a reverse proxy that sets these headers.
    trustForwarded: env('ST_SYNC_TRUST_FORWARDED', '0') === '1',
    storeDir: env('ST_SYNC_STORE_DIR', '/var/lib/st-sync'),
    logFile: env('ST_SYNC_LOG_FILE', '/var/log/st-sync/access.log'),
    offFile: env('ST_SYNC_OFF_FILE', '/etc/st-sync/off'),
    threshold: Number(env('ST_SYNC_THRESHOLD', 64 * 1024)),
    retentionDays: Number(env('ST_SYNC_RETENTION_DAYS', 30)),
    storeMaxBytes: Number(env('ST_SYNC_STORE_MAX_BYTES', 1024 * MB)),
    minFreeDiskBytes: Number(env('ST_SYNC_MIN_FREE_DISK', 3072 * MB)),
    // Live JS + buffer memory; freed memory the allocator keeps is only bounded by the RSS limit.
    liveSoftLimit: Number(env('ST_SYNC_LIVE_SOFT_LIMIT', 64 * MB)),
    rssSoftLimit: Number(env('ST_SYNC_RSS_SOFT_LIMIT', 200 * MB)),
    maxHeavy: Number(env('ST_SYNC_MAX_HEAVY', 3)),
    maxQueue: 64,
    uploadWaitMs: 30 * 1000,
    downloadWaitMs: 15 * 1000,
    // Below this, even uploads stop storing chunks; it is a disaster threshold, not a budget.
    hardMinFreeDiskBytes: 512 * MB,
    // Longer than the shim's confirmation TTL plus the touch granularity, so a chunk a client
    // believes the server has is never evicted for size.
    protectDays: 8,
    maxManifestEntries: Number(env('ST_SYNC_MAX_MANIFEST', 200000)),
    maxHtmlBytes: 8 * MB,
    logMaxBytes: 50 * MB,
};

const SHIM_PATH = '/__sync/shim.js';
const WORKER_PATH = '/__sync/worker.js';
const CHUNKS_PATH = '/__sync/chunks';
const HEALTH_PATH = '/__sync/health';
const MAX_CHUNKS_PER_REQUEST = 4096;
const HOP_BY_HOP = new Set([
    'connection', 'keep-alive', 'proxy-connection', 'transfer-encoding', 'te', 'trailer', 'upgrade',
    'proxy-authenticate', 'proxy-authorization',
]);
const COMPRESSIBLE = /^(text\/|application\/(json|javascript|x-javascript|xml|[\w.+-]*\+json|[\w.+-]*\+xml)|image\/svg\+xml)/i;

const cdcSource = fs.readFileSync(path.join(__dirname, 'cdc.js'), 'utf8');
const version = crypto.createHash('sha1')
    .update(cdcSource)
    .update(fs.readFileSync(path.join(__dirname, 'shim.js')))
    .update(fs.readFileSync(path.join(__dirname, 'worker.js')))
    .digest('hex').slice(0, 12);
const assets = {
    [SHIM_PATH]: Buffer.from(`var ST_SYNC_VERSION = ${JSON.stringify(version)};\n${cdcSource}\n${fs.readFileSync(path.join(__dirname, 'shim.js'), 'utf8')}`),
    [WORKER_PATH]: Buffer.from(`${cdcSource}\n${fs.readFileSync(path.join(__dirname, 'worker.js'), 'utf8')}`),
};
const assetsGzip = Object.fromEntries(Object.entries(assets).map(([k, v]) => [k, zlib.gzipSync(v)]));

const agent = new http.Agent({ keepAlive: true, maxSockets: 128 });

/**
 * Runtime budget state. New chunk work (download manifests) degrades to passthrough when over
 * budget. Uploads are never refused for budget reasons: an XMLHttpRequest caller cannot resend.
 * Heavy work waits for one of maxHeavy slots; a waiting request is not read, so it costs no memory.
 */
const budget = {
    heavy: 0,
    waiting: [],
    off: false,
    diskOk: true,
    freeBytes: Infinity,
    overMemory() {
        const memory = process.memoryUsage();
        return memory.heapUsed + memory.external > config.liveSoftLimit || memory.rss > config.rssSoftLimit;
    },
    /** Whether new download chunking may start (the concurrency limit is waited for separately). */
    syncPossible() {
        return !this.off && this.diskOk && !this.overMemory();
    },
    /** Takes a heavy slot, waiting up to waitMs. Resolves false if none frees up in time. */
    acquire(waitMs) {
        if (this.heavy < config.maxHeavy) {
            this.heavy++;
            return Promise.resolve(true);
        }
        if (this.waiting.length >= config.maxQueue) return Promise.resolve(false);
        return new Promise((resolve) => {
            const waiter = {
                resolve,
                timer: setTimeout(() => {
                    this.waiting.splice(this.waiting.indexOf(waiter), 1);
                    resolve(false);
                }, waitMs),
            };
            this.waiting.push(waiter);
        });
    },
    release() {
        const next = this.waiting.shift();
        if (next) {
            clearTimeout(next.timer);
            next.resolve(true);
        } else {
            this.heavy--;
        }
    },
};

let gcRunning = false;
let lastSizeGc = 0;
function runGc() {
    if (gcRunning) return;
    gcRunning = true;
    lastSizeGc = Date.now();
    store.gc().catch((err) => console.error('[st-sync] gc failed', err)).finally(() => { gcRunning = false; });
}

async function refreshSwitches() {
    budget.off = await fsp.access(config.offFile).then(() => true, () => false);
    try {
        const stat = await fsp.statfs(config.storeDir);
        budget.freeBytes = stat.bavail * stat.bsize;
        budget.diskOk = budget.freeBytes > config.minFreeDiskBytes && store.totalBytes < config.storeMaxBytes;
    } catch {
        budget.diskOk = false;
    }
    if (store.totalBytes >= config.storeMaxBytes && Date.now() - lastSizeGc > 10 * 60 * 1000) runGc();
}

/** Content-addressed chunk store, one directory per SillyTavern user. A chunk file's mtime is its last use. */
class ChunkStore {
    constructor(dir) {
        this.dir = dir;
        /** @type {Map<string, Promise<Map<string, {t: number, s: number}>>>} */
        this.indexes = new Map();
        this.totalBytes = 0;
        this.touchInterval = 6 * 3600 * 1000;
    }

    file(scope, hash) {
        return path.join(this.dir, scope, hash.slice(0, 2), hash);
    }

    index(scope) {
        let pending = this.indexes.get(scope);
        if (!pending) {
            pending = this.loadIndex(scope);
            this.indexes.set(scope, pending);
        }
        return pending;
    }

    async loadIndex(scope) {
        const index = new Map();
        const root = path.join(this.dir, scope);
        await fsp.mkdir(root, { recursive: true });
        for (const sub of await fsp.readdir(root)) {
            const names = (await fsp.readdir(path.join(root, sub)).catch(() => [])).filter((n) => /^[0-9a-f]{32}$/.test(n));
            await mapLimit(names, 8, async (name) => {
                const stat = await fsp.stat(path.join(root, sub, name)).catch(() => null);
                if (!stat) return;
                index.set(name, { t: stat.mtimeMs, s: stat.size });
                this.totalBytes += stat.size;
            });
        }
        return index;
    }

    async has(scope, hash) {
        return (await this.index(scope)).has(hash);
    }

    touch(scope, hash, entry) {
        const now = Date.now();
        if (now - entry.t < this.touchInterval) return;
        entry.t = now;
        const time = new Date(now);
        fsp.utimes(this.file(scope, hash), time, time).catch(() => {});
    }

    async read(scope, hash) {
        const index = await this.index(scope);
        const entry = index.get(hash);
        if (!entry) return null;
        const bytes = await fsp.readFile(this.file(scope, hash)).catch(() => null);
        if (!bytes) {
            index.delete(hash);
            this.totalBytes -= entry.s;
            return null;
        }
        this.touch(scope, hash, entry);
        return bytes;
    }

    async put(scope, hash, bytes) {
        const index = await this.index(scope);
        const existing = index.get(hash);
        if (existing) {
            this.touch(scope, hash, existing);
            return;
        }
        const file = this.file(scope, hash);
        const tmp = `${file}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`;
        await fsp.mkdir(path.dirname(file), { recursive: true });
        await fsp.writeFile(tmp, bytes);
        await fsp.rename(tmp, file);
        if (!index.has(hash)) {
            index.set(hash, { t: Date.now(), s: bytes.length });
            this.totalBytes += bytes.length;
        }
    }

    /**
     * Drops chunks past retention, then the least recently used ones until under the size cap,
     * but never chunks used within protectDays. If those alone exceed the cap, the store stays
     * over it and new download chunking pauses (diskOk) until they age out.
     */
    async gc() {
        const now = Date.now();
        const cutoff = now - config.retentionDays * 24 * 3600 * 1000;
        const protectedSince = now - config.protectDays * 24 * 3600 * 1000;
        const all = [];
        for (const [scope, pending] of this.indexes) {
            for (const [hash, entry] of await pending) all.push([scope, hash, entry]);
        }
        all.sort((a, b) => a[2].t - b[2].t);
        const target = config.storeMaxBytes * 0.8;
        let removed = 0;
        for (const [scope, hash, entry] of all) {
            if (entry.t >= cutoff && (this.totalBytes <= target || entry.t >= protectedSince)) break;
            await fsp.unlink(this.file(scope, hash)).catch(() => {});
            (await this.index(scope)).delete(hash);
            this.totalBytes -= entry.s;
            removed++;
        }
        if (removed) console.log(`[st-sync] gc removed ${removed} chunks, store now ${(this.totalBytes / MB).toFixed(1)}MB`);
    }

    async loadAll() {
        for (const scope of await fsp.readdir(this.dir).catch(() => [])) {
            if (/^[0-9a-f]{24}$/.test(scope)) await this.index(scope);
        }
    }
}

const store = new ChunkStore(config.storeDir);

fs.mkdirSync(path.dirname(config.logFile), { recursive: true });
let logStream = fs.createWriteStream(config.logFile, { flags: 'a' });

function rotateLog() {
    fsp.stat(config.logFile).then((stat) => {
        if (stat.size < config.logMaxBytes) return;
        logStream.end();
        fs.renameSync(config.logFile, `${config.logFile}.1`);
        logStream = fs.createWriteStream(config.logFile, { flags: 'a' });
    }).catch(() => {});
}

async function mapLimit(items, limit, fn) {
    const results = new Array(items.length);
    let next = 0;
    const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
        while (next < items.length) {
            const i = next++;
            results[i] = await fn(items[i], i);
        }
    });
    await Promise.all(workers);
    return results;
}

/** Exact-length reads from a stream without accumulating more than the requested bytes plus one piece. */
function createReader(stream) {
    const iterator = stream[Symbol.asyncIterator]();
    const parts = [];
    let length = 0;
    return async function read(n) {
        if (n === 0) return Buffer.alloc(0);
        while (length < n) {
            const { value, done } = await iterator.next();
            if (done) throw new Error('truncated envelope');
            parts.push(value);
            length += value.length;
        }
        if (parts[0].length >= n) {
            const head = parts[0];
            const out = head.subarray(0, n);
            if (head.length === n) parts.shift();
            else parts[0] = head.subarray(n);
            length -= n;
            return out;
        }
        const out = Buffer.allocUnsafe(n);
        let o = 0;
        while (o < n) {
            const head = parts[0];
            const take = Math.min(head.length, n - o);
            head.copy(out, o, 0, take);
            o += take;
            if (take === head.length) parts.shift();
            else parts[0] = head.subarray(take);
        }
        length -= n;
        return out;
    };
}

function clientIp(req) {
    return (req.socket.remoteAddress || '').replace(/^::ffff:/, '');
}

function acceptsGzip(req) {
    return /\bgzip\b/i.test(req.headers['accept-encoding'] || '');
}

function upstreamHeaders(req, overrides = {}) {
    const headers = {};
    for (const [key, value] of Object.entries(req.headers)) {
        if (HOP_BY_HOP.has(key) || key.startsWith('x-sync-') || key === 'expect') continue;
        headers[key] = value;
    }
    // SillyTavern's forwarded whitelist trusts X-Real-IP, then the first X-Forwarded-For entry,
    // so headers from an untrusted client must not reach it.
    if (config.trustForwarded) {
        const forwarded = req.headers['x-forwarded-for'];
        headers['x-forwarded-for'] = forwarded ? `${forwarded}, ${clientIp(req)}` : clientIp(req);
    } else {
        headers['x-forwarded-for'] = clientIp(req);
        headers['x-real-ip'] = clientIp(req);
    }
    for (const [key, value] of Object.entries(overrides)) {
        if (value === undefined) delete headers[key];
        else headers[key] = value;
    }
    return headers;
}

/** Upstream response headers as a flat raw array, minus hop-by-hop and the given names. */
function responseHeaders(upRes, drop = []) {
    const skip = new Set([...HOP_BY_HOP, ...drop]);
    const out = [];
    const raw = upRes.rawHeaders;
    for (let i = 0; i < raw.length; i += 2) {
        if (!skip.has(raw[i].toLowerCase())) out.push(raw[i], raw[i + 1]);
    }
    return out;
}

function upstreamRequest(req, res, headers) {
    const upReq = http.request({
        host: config.upstreamHost,
        port: config.upstreamPort,
        method: req.method,
        path: req.url,
        agent,
        headers,
    });
    res.on('close', () => {
        if (!res.writableFinished) upReq.destroy();
    });
    upReq.on('error', (err) => fail(res, err));
    return upReq;
}

function fail(res, err) {
    if (err && err.code !== 'ECONNRESET') console.error('[st-sync]', err.message || err);
    if (!res.headersSent) {
        res.writeHead(502, { 'content-type': 'text/plain; charset=utf-8' });
        res.end('st-sync: upstream unavailable');
    } else {
        res.destroy();
    }
}

/** Errors produced before SillyTavern saw the request; the shim resends the original request. */
function syncError(res, status, code) {
    if (res.headersSent) return res.destroy();
    res.writeHead(status, { 'content-type': 'application/json', 'x-sync-error': code, 'cache-control': 'no-store' });
    res.end(JSON.stringify({ error: code }));
}

function shouldCompress(req, contentType, encoding) {
    return acceptsGzip(req) && !encoding && COMPRESSIBLE.test(contentType || '') && req.method !== 'HEAD';
}

function findHeader(rawHeaders, name) {
    for (let i = 0; i < rawHeaders.length; i += 2) {
        if (rawHeaders[i].toLowerCase() === name) return rawHeaders[i + 1];
    }
    return undefined;
}

/** Sends a small, already-buffered body (below the chunking threshold or an HTML page). */
function sendSmall(req, res, status, statusMessage, headers, body) {
    if (body.length > 1024 && shouldCompress(req, findHeader(headers, 'content-type'), findHeader(headers, 'content-encoding'))) {
        body = zlib.gzipSync(body);
        headers.push('Content-Encoding', 'gzip', 'Vary', 'Accept-Encoding');
    }
    headers.push('Content-Length', String(body.length));
    res.writeHead(status, statusMessage, headers);
    res.end(req.method === 'HEAD' ? undefined : body);
}

/** Resolves once the writable can take more data, or has closed. */
function writable(sink) {
    if (!sink.writableNeedDrain) return Promise.resolve();
    return new Promise((resolve) => {
        const done = () => {
            sink.off('drain', done);
            sink.off('close', done);
            sink.off('error', done);
            resolve();
        };
        sink.on('drain', done);
        sink.on('close', done);
        sink.on('error', done);
    });
}

/**
 * Streams the rest of an upstream response to the client, prefixed by bytes already read from it.
 * Gzips it when we asked upstream for identity. Once an async iterator was taken from upRes,
 * the rest must be read through it: pipe() would never receive data.
 */
function streamResponse(req, res, upRes, compressIdentity, prefix = [], iterator = null) {
    const contentType = upRes.headers['content-type'] || '';
    const isEventStream = /^text\/event-stream/i.test(contentType);
    const noBody = upRes.statusCode === 204 || upRes.statusCode === 304 || req.method === 'HEAD';
    let sink = res;
    if (compressIdentity && !isEventStream && !noBody && shouldCompress(req, contentType, upRes.headers['content-encoding'])) {
        const headers = responseHeaders(upRes, ['content-length']);
        headers.push('Content-Encoding', 'gzip', 'Vary', 'Accept-Encoding');
        res.writeHead(upRes.statusCode, upRes.statusMessage, headers);
        sink = zlib.createGzip();
        sink.pipe(res);
        res.on('close', () => sink.destroy());
    } else {
        res.writeHead(upRes.statusCode, upRes.statusMessage, responseHeaders(upRes));
        if (isEventStream) res.flushHeaders();
    }
    for (const piece of prefix) sink.write(piece);
    if (!iterator) return upRes.pipe(sink);
    (async () => {
        for (;;) {
            await writable(sink);
            if (res.destroyed) return upRes.destroy();
            const { value, done } = await iterator.next();
            if (done) return sink.end();
            sink.write(value);
        }
    })().catch((err) => fail(res, err));
}

function passthrough(req, res, log) {
    const upReq = upstreamRequest(req, res, upstreamHeaders(req));
    req.on('data', (chunk) => { log.in += chunk.length; });
    req.pipe(upReq);
    upReq.on('response', (upRes) => streamResponse(req, res, upRes, false));
}

const scopeCache = new Map();

/** Resolves the SillyTavern user behind the request by asking SillyTavern. Sends the denial and returns null if not authenticated. */
async function resolveScope(req, res) {
    // SillyTavern may admit a request by its source address alone (whitelist), so that is part of the key.
    const forwarded = upstreamHeaders(req);
    const key = [req.headers.authorization, req.headers.cookie, forwarded['x-forwarded-for'], forwarded['x-real-ip']].join('\n');
    const cached = scopeCache.get(key);
    if (cached && cached.expires > Date.now()) {
        res.syncScope = cached.scope;
        return cached.scope;
    }

    const { upRes, body } = await new Promise((resolve, reject) => {
        const upReq = http.request({
            host: config.upstreamHost,
            port: config.upstreamPort,
            method: 'GET',
            path: '/api/users/me',
            agent,
            headers: upstreamHeaders(req, {
                'content-length': undefined,
                'content-type': undefined,
                'accept-encoding': 'identity',
                'if-none-match': undefined,
                'if-modified-since': undefined,
            }),
        });
        upReq.on('response', (response) => {
            // The body also carries the avatar as a data URL; only its head is needed.
            const parts = [];
            let size = 0;
            const finish = () => resolve({ upRes: response, body: Buffer.concat(parts) });
            response.on('data', (c) => {
                parts.push(c);
                size += c.length;
                if (size >= 4096) {
                    response.removeAllListeners('end');
                    response.destroy();
                    finish();
                }
            });
            response.on('end', finish);
            response.on('error', reject);
        });
        upReq.on('error', reject);
        upReq.end();
    });

    if (upRes.statusCode !== 200) {
        res.writeHead(upRes.statusCode, responseHeaders(upRes, ['content-length']));
        res.end(body.subarray(0, 4096));
        return null;
    }
    const match = /^\s*\{\s*"handle"\s*:\s*("(?:[^"\\]|\\.)*")/.exec(body.toString('utf8'));
    if (!match) throw new Error('cannot read handle from /api/users/me');
    const handle = JSON.parse(match[1]);
    const scope = crypto.createHash('sha256').update(`st-sync|${handle}`).digest('hex').slice(0, 24);
    if (scopeCache.size > 1000) scopeCache.clear();
    scopeCache.set(key, { scope, expires: Date.now() + 60 * 1000 });
    res.syncScope = scope;
    return scope;
}

/**
 * Upload: parse the envelope as it streams in, store inline chunks on disk, then stream the
 * reconstructed body to SillyTavern chunk by chunk.
 */
async function handleUpload(req, res, log) {
    const encoding = req.headers['x-sync-encoding'];
    if (encoding !== 'cdc1' && encoding !== 'cdc1+gzip') return syncError(res, 400, 'unknown-encoding');
    if (budget.freeBytes < config.hardMinFreeDiskBytes) {
        req.resume();
        return syncError(res, 507, 'disk-full');
    }

    let scope;
    try {
        scope = await resolveScope(req, res);
    } catch (err) {
        console.error('[st-sync] scope lookup failed:', err.message);
        req.resume();
        return syncError(res, 503, 'scope');
    }
    if (!scope) return req.resume();

    // Past the wait the upload proceeds anyway: refusing would fail callers that cannot resend.
    const slot = await budget.acquire(config.uploadWaitMs);
    if (!slot) log.overflow = true;
    try {
        const counted = new PassThrough();
        req.on('data', (chunk) => { log.in += chunk.length; });
        req.pipe(counted);
        const source = encoding === 'cdc1+gzip' ? counted.pipe(zlib.createGunzip()) : counted;
        const read = createReader(source);
        const discard = () => {
            req.unpipe(counted);
            req.resume();
        };

        let hashes;
        let lengths;
        let inlineSet;
        try {
            const header = await read(12);
            if (header.readUInt32LE(0) !== 0x31435953) throw new Error('bad magic');
            const count = header.readUInt32LE(4);
            const inlineCount = header.readUInt32LE(8);
            if (count > config.maxManifestEntries || inlineCount > count) throw new Error('manifest too large');
            const entries = await read(count * Cdc.ENTRY_BYTES);
            const view = new DataView(entries.buffer, entries.byteOffset, entries.byteLength);
            hashes = new Array(count);
            lengths = new Array(count);
            for (let i = 0; i < count; i++) {
                hashes[i] = Cdc.readHash(view, i * Cdc.ENTRY_BYTES);
                lengths[i] = view.getUint32(i * Cdc.ENTRY_BYTES + Cdc.HASH_BYTES, true);
                if (lengths[i] > Cdc.MAX_SIZE) throw new Error('chunk too large');
            }
            const indexBytes = await read(inlineCount * 4);
            inlineSet = new Set();
            for (let k = 0; k < inlineCount; k++) {
                const i = indexBytes.readUInt32LE(k * 4);
                if (i >= count) throw new Error('bad inline index');
                inlineSet.add(i);
            }
            for (let k = 0; k < inlineCount; k++) {
                const i = indexBytes.readUInt32LE(k * 4);
                const bytes = await read(lengths[i]);
                if (Cdc.hash(bytes, 0, bytes.length) !== hashes[i]) {
                    discard();
                    return syncError(res, 400, 'hash-mismatch');
                }
                await store.put(scope, hashes[i], bytes);
            }
        } catch (err) {
            discard();
            return syncError(res, 400, 'bad-envelope');
        }

        const missing = [];
        for (let i = 0; i < hashes.length; i++) {
            if (!inlineSet.has(i) && !(await store.has(scope, hashes[i]))) missing.push(i);
        }
        if (missing.length) {
            res.writeHead(409, { 'content-type': 'application/json', 'x-sync-missing': '1', 'cache-control': 'no-store' });
            return res.end(JSON.stringify({ missing }));
        }

        let total = 0;
        for (const len of lengths) total += len;
        log.orig = total;
        const syncAccept = req.headers['x-sync-accept'] === 'cdc1';
        const upReq = upstreamRequest(req, res, upstreamHeaders(req, {
            'content-length': String(total),
            ...(syncAccept ? { 'accept-encoding': 'identity' } : {}),
        }));
        upReq.on('response', (upRes) => handleResponse(req, res, upRes, log, syncAccept, scope));

        const reads = [];
        const readAhead = 8;
        for (let i = 0; i < hashes.length; i++) {
            while (reads.length < readAhead && i + reads.length < hashes.length) {
                const hash = hashes[i + reads.length];
                reads.push(store.read(scope, hash));
            }
            const bytes = await reads.shift();
            if (!bytes || bytes.length !== lengths[i]) {
                // SillyTavern gets a truncated body and rejects it before any handler runs.
                upReq.removeAllListeners('response');
                upReq.removeAllListeners('error');
                upReq.on('error', () => {});
                upReq.destroy();
                return syncError(res, 409, 'chunk-lost');
            }
            upReq.write(bytes);
            await writable(upReq);
            if (res.destroyed || upReq.destroyed) return;
        }
        upReq.end();
    } finally {
        if (slot) budget.release();
    }
}

/**
 * Response side for requests that accept chunked responses: small or streaming responses are
 * passed through; large ones are chunked to disk as they arrive and replaced with a manifest.
 */
async function handleResponse(req, res, upRes, log, syncAccept, scope) {
    try {
        const contentType = upRes.headers['content-type'] || '';
        const length = Number(upRes.headers['content-length'] || NaN);
        const chunkable = syncAccept && upRes.statusCode === 200 && req.method !== 'HEAD' &&
            !/^text\/event-stream/i.test(contentType) && !upRes.headers['content-encoding'] &&
            !(length < config.threshold) && budget.syncPossible();
        if (!chunkable) return streamResponse(req, res, upRes, syncAccept);

        const head = [];
        let headBytes = 0;
        const iterator = upRes[Symbol.asyncIterator]();
        while (headBytes < config.threshold) {
            const { value, done } = await iterator.next();
            if (done) {
                return sendSmall(req, res, upRes.statusCode, upRes.statusMessage, responseHeaders(upRes, ['content-length']), Buffer.concat(head));
            }
            head.push(value);
            headBytes += value.length;
        }

        try {
            scope = scope || await resolveScope(req, res);
        } catch (err) {
            console.error('[st-sync] scope lookup failed:', err.message);
            return streamResponse(req, res, upRes, true, head, iterator);
        }
        if (!scope) return upRes.destroy();
        const slot = await budget.acquire(config.downloadWaitMs);
        if (!slot || !budget.syncPossible()) {
            if (slot) budget.release();
            return streamResponse(req, res, upRes, true, head, iterator);
        }

        try {
            const chunker = Cdc.createChunker();
            const entries = [];
            let total = 0;
            const storeChunks = async (chunks) => {
                for (const chunk of chunks) {
                    const hash = Cdc.hash(chunk, 0, chunk.length);
                    await store.put(scope, hash, chunk);
                    const entry = Buffer.allocUnsafe(Cdc.ENTRY_BYTES);
                    Cdc.writeHash(new DataView(entry.buffer, entry.byteOffset, entry.byteLength), 0, hash);
                    entry.writeUInt32LE(chunk.length, Cdc.HASH_BYTES);
                    entries.push(entry);
                    total += chunk.length;
                }
            };
            for (const piece of head) await storeChunks(chunker.push(piece));
            for (;;) {
                const { value, done } = await iterator.next();
                if (done) break;
                if (res.destroyed) return upRes.destroy();
                await storeChunks(chunker.push(value));
            }
            await storeChunks(chunker.end());

            const manifestHeader = Buffer.allocUnsafe(8);
            manifestHeader.writeUInt32LE(0x31445953, 0);
            manifestHeader.writeUInt32LE(entries.length, 4);
            const manifest = Buffer.concat([manifestHeader, ...entries]);
            log.mode += '+down';
            log.orig = (log.orig || 0) + total;
            const headers = responseHeaders(upRes, ['content-length', 'content-type', 'etag', 'last-modified']);
            headers.push(
                'Content-Type', 'application/octet-stream',
                'X-Sync-Encoding', 'cdc1',
                'X-Sync-Content-Type', contentType,
                'Cache-Control', 'no-store',
                'Vary', 'X-Sync-Accept',
                'Content-Length', String(manifest.length),
            );
            res.writeHead(200, upRes.statusMessage, headers);
            res.end(manifest);
        } finally {
            budget.release();
        }
    } catch (err) {
        fail(res, err);
    }
}

function forwardSyncAccept(req, res, log) {
    const syncAccept = budget.syncPossible();
    const upReq = upstreamRequest(req, res, upstreamHeaders(req, syncAccept ? { 'accept-encoding': 'identity' } : {}));
    req.on('data', (chunk) => { log.in += chunk.length; });
    req.pipe(upReq);
    upReq.on('response', (upRes) => handleResponse(req, res, upRes, log, syncAccept, null));
}

/** Streams the requested chunks from disk, gzipped. */
async function serveChunks(req, res, log) {
    if (req.method !== 'POST') {
        res.writeHead(405);
        return res.end();
    }
    let scope;
    try {
        scope = await resolveScope(req, res);
    } catch (err) {
        console.error('[st-sync] scope lookup failed:', err.message);
        return syncError(res, 503, 'scope');
    }
    if (!scope) return;
    const parts = [];
    let size = 0;
    for await (const piece of req) {
        size += piece.length;
        if (size > MAX_CHUNKS_PER_REQUEST * Cdc.HASH_BYTES) return syncError(res, 413, 'too-many-chunks');
        parts.push(piece);
    }
    const raw = Buffer.concat(parts);
    log.in = raw.length;
    let hashes;
    try {
        hashes = Cdc.decodeHashList(new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength));
    } catch {
        return syncError(res, 400, 'bad-hash-list');
    }
    const missing = [];
    for (let i = 0; i < hashes.length; i++) {
        if (!(await store.has(scope, hashes[i]))) missing.push(i);
    }
    if (missing.length) {
        res.writeHead(409, { 'content-type': 'application/json', 'x-sync-missing': '1', 'cache-control': 'no-store' });
        return res.end(JSON.stringify({ missing }));
    }

    const headers = ['Content-Type', 'application/octet-stream', 'Cache-Control', 'no-store'];
    let sink = res;
    if (acceptsGzip(req)) {
        headers.push('Content-Encoding', 'gzip');
        sink = zlib.createGzip();
        sink.pipe(res);
        res.on('close', () => sink.destroy());
    }
    res.writeHead(200, headers);
    for (const hash of hashes) {
        const bytes = await store.read(scope, hash);
        if (!bytes) return res.destroy();
        log.orig += bytes.length;
        sink.write(bytes);
        await writable(sink);
        if (res.destroyed) return;
    }
    sink.end();
}

function serveAsset(req, res, pathname) {
    const etag = `"${version}"`;
    if (req.headers['if-none-match'] === etag) {
        res.writeHead(304, { ETag: etag });
        return res.end();
    }
    const versioned = new URL(req.url, 'http://x').searchParams.get('v') === version;
    const gz = acceptsGzip(req);
    const body = gz ? assetsGzip[pathname] : assets[pathname];
    const headers = [
        'Content-Type', 'application/javascript; charset=utf-8',
        'ETag', etag,
        'Cache-Control', versioned ? 'public, max-age=31536000, immutable' : 'no-cache',
        'Content-Length', String(body.length),
    ];
    if (gz) headers.push('Content-Encoding', 'gzip', 'Vary', 'Accept-Encoding');
    res.writeHead(200, headers);
    res.end(body);
}

/** Serves index.html with the shim injected as the first script in <head>. */
function serveHtml(req, res, log) {
    const upReq = upstreamRequest(req, res, upstreamHeaders(req, {
        'accept-encoding': 'identity',
        'if-none-match': undefined,
        'if-modified-since': undefined,
    }));
    upReq.end();
    upReq.on('response', async (upRes) => {
        try {
            const contentType = upRes.headers['content-type'] || '';
            const length = Number(upRes.headers['content-length'] || 0);
            if (upRes.statusCode !== 200 || !/^text\/html/i.test(contentType) || upRes.headers['content-encoding'] || length > config.maxHtmlBytes) {
                return streamResponse(req, res, upRes, true);
            }
            const parts = [];
            for await (const piece of upRes) parts.push(piece);
            const html = Buffer.concat(parts).toString('utf8');
            const tag = `<script src="${SHIM_PATH}?v=${version}"></script>`;
            const injected = /<head[^>]*>/i.test(html) ? html.replace(/<head[^>]*>/i, (m) => m + tag) : tag + html;
            const body = Buffer.from(injected, 'utf8');
            const etag = `W/"sync-${crypto.createHash('sha1').update(body).digest('hex').slice(0, 16)}"`;
            log.orig = body.length;
            if (req.headers['if-none-match'] === etag) {
                res.writeHead(304, { ETag: etag, 'Cache-Control': 'no-cache' });
                return res.end();
            }
            const headers = responseHeaders(upRes, ['content-length', 'etag', 'last-modified', 'cache-control']);
            headers.push('ETag', etag, 'Cache-Control', 'no-cache');
            sendSmall(req, res, 200, upRes.statusMessage, headers, body);
        } catch (err) {
            fail(res, err);
        }
    });
}

function serveHealth(res) {
    const memory = process.memoryUsage();
    const body = JSON.stringify({
        ok: true,
        version,
        off: budget.off,
        diskOk: budget.diskOk,
        heavy: budget.heavy,
        waiting: budget.waiting.length,
        rssMB: Math.round(memory.rss / MB),
        heapMB: Math.round(memory.heapUsed / MB),
        externalMB: Math.round(memory.external / MB),
        storeMB: Math.round(store.totalBytes / MB),
    });
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(body);
}

/** Identifies this chunk store; a new id means every chunk a client remembers may be gone. */
let storeId = '';

function instrument(req, res, log) {
    const tag = budget.off ? 'off' : version;
    const writeHead = res.writeHead;
    res.writeHead = function (status, reason, headers) {
        if (typeof reason !== 'string') {
            headers = reason;
            reason = undefined;
        }
        const extra = ['X-Sync-Proxy', tag, 'X-Sync-Store', storeId];
        if (res.syncScope) extra.push('X-Sync-Scope', res.syncScope);
        if (Array.isArray(headers)) {
            headers = [...headers, ...extra];
        } else {
            headers = { ...(headers || {}) };
            for (let i = 0; i < extra.length; i += 2) headers[extra[i]] = extra[i + 1];
        }
        return writeHead.call(this, status, reason, headers);
    };
    const write = res.write;
    const end = res.end;
    res.write = function (chunk, ...args) {
        if (chunk) log.out += typeof chunk === 'string' ? Buffer.byteLength(chunk) : chunk.length;
        return write.call(this, chunk, ...args);
    };
    res.end = function (chunk, ...args) {
        if (chunk && typeof chunk !== 'function') log.out += typeof chunk === 'string' ? Buffer.byteLength(chunk) : chunk.length;
        return end.call(this, chunk, ...args);
    };
}

async function handle(req, res) {
    const started = Date.now();
    const log = { t: new Date(started).toISOString(), ip: clientIp(req), m: req.method, u: req.url.slice(0, 300), mode: 'pass', in: 0, out: 0, orig: 0 };
    // Which client sent it: the shim marks its fetch and XHR requests; for the rest keep a few
    // headers that tell an XHR, a frame, or another page apart.
    const client = req.headers['x-sync-client'];
    if (client) {
        log.c = String(client).slice(0, 24);
    } else {
        if (req.headers['x-requested-with']) log.xrw = String(req.headers['x-requested-with']).slice(0, 32);
        if (req.headers['sec-fetch-dest']) log.dest = String(req.headers['sec-fetch-dest']).slice(0, 16);
        if (req.headers.referer) log.ref = String(req.headers.referer).replace(/^https?:\/\/[^/]+/, '').slice(0, 80);
    }
    instrument(req, res, log);
    res.on('close', () => {
        log.s = res.statusCode;
        log.ms = Date.now() - started;
        if (!res.writableFinished) log.aborted = true;
        logStream.write(JSON.stringify(log) + '\n');
    });

    try {
        const pathname = req.url.split('?')[0];
        if (pathname === HEALTH_PATH) return serveHealth(res);
        if (pathname === SHIM_PATH || pathname === WORKER_PATH) {
            log.mode = 'asset';
            return serveAsset(req, res, pathname);
        }
        if (req.headers['x-sync-encoding']) {
            log.mode = 'up';
            return await handleUpload(req, res, log);
        }
        if (pathname === CHUNKS_PATH) {
            // Served even when off: a page may still be assembling a manifest it already received.
            log.mode = 'chunks';
            return await serveChunks(req, res, log);
        }
        if (budget.off) return passthrough(req, res, log);
        if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
            log.mode = 'html';
            return serveHtml(req, res, log);
        }
        if (req.headers['x-sync-accept'] === 'cdc1') {
            log.mode = 'sync';
            return forwardSyncAccept(req, res, log);
        }
        return passthrough(req, res, log);
    } catch (err) {
        fail(res, err);
    }
}

function handleUpgrade(req, socket, head) {
    const upstream = net.connect(config.upstreamPort, config.upstreamHost, () => {
        const lines = [`${req.method} ${req.url} HTTP/1.1`];
        for (let i = 0; i < req.rawHeaders.length; i += 2) {
            const name = req.rawHeaders[i].toLowerCase();
            if (name === 'x-forwarded-for' || name === 'x-real-ip') continue;
            lines.push(`${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}`);
        }
        const forwarded = upstreamHeaders(req);
        lines.push(`X-Forwarded-For: ${forwarded['x-forwarded-for']}`);
        if (forwarded['x-real-ip']) lines.push(`X-Real-IP: ${forwarded['x-real-ip']}`);
        upstream.write(lines.join('\r\n') + '\r\n\r\n');
        if (head && head.length) upstream.write(head);
        socket.pipe(upstream).pipe(socket);
    });
    const close = () => {
        socket.destroy();
        upstream.destroy();
    };
    upstream.on('error', close);
    socket.on('error', close);
}

async function main() {
    await fsp.mkdir(config.storeDir, { recursive: true });
    const idFile = path.join(config.storeDir, 'store-id');
    storeId = (await fsp.readFile(idFile, 'utf8').catch(() => '')).trim();
    if (!/^[0-9a-f]{16}$/.test(storeId)) {
        storeId = crypto.randomBytes(8).toString('hex');
        await fsp.writeFile(idFile, storeId);
    }
    store.loadAll().catch((err) => console.error('[st-sync] index load failed', err));
    await refreshSwitches();
    setInterval(() => refreshSwitches().catch(() => {}), 2000).unref();
    setInterval(runGc, 3600 * 1000).unref();
    setInterval(rotateLog, 60 * 1000).unref();

    const server = http.createServer(handle);
    server.requestTimeout = 0;
    server.headersTimeout = 60 * 1000;
    server.keepAliveTimeout = 65 * 1000;
    server.on('upgrade', handleUpgrade);
    server.on('clientError', (err, socket) => socket.destroy());
    server.listen(config.listenPort, config.listenHost, () => {
        console.log(`[st-sync] ${version} listening on ${config.listenHost}:${config.listenPort} -> ${config.upstreamHost}:${config.upstreamPort}, store ${(store.totalBytes / MB).toFixed(1)}MB`);
    });
}

main().catch((err) => {
    console.error('[st-sync] failed to start', err);
    process.exit(1);
});
