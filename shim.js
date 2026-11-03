/**
 * st-sync browser shim. Injected by the proxy before any SillyTavern script runs.
 * Wraps fetch (uploads + downloads) and XMLHttpRequest (uploads only) for same-origin requests,
 * replacing large bodies with chunk manifests and keeping downloaded chunks in IndexedDB.
 *
 * Resource rules: chunking runs in a worker; downloads are assembled in windows of a few MB
 * and handed to the caller as a stream; only downloaded chunks are cached, under a size cap.
 * As soon as a response arrives without the proxy's marker header, the shim switches itself off.
 * Set localStorage['st-sync-disabled'] = '1' to turn it off manually.
 */
(function () {
    'use strict';

    if (window.__stSync || typeof StSyncCdc === 'undefined') return;
    try {
        if (localStorage.getItem('st-sync-disabled') === '1') return;
    } catch { /* storage may be unavailable */ }

    const Cdc = StSyncCdc;
    const MB = 1024 * 1024;
    const THRESHOLD = 64 * 1024;
    const WORKER_THRESHOLD = 1 * MB;
    const WINDOW_BYTES = 4 * MB;
    // Must stay well below the proxy's chunk retention so "the server has it" stays true.
    const CONFIRM_TTL = 7 * 24 * 3600 * 1000;
    const DATA_CAP = 192 * MB;
    const nativeFetch = window.fetch.bind(window);
    const encoder = new TextEncoder();
    const stats = { uploads: 0, uploadBytes: 0, uploadSent: 0, downloads: 0, downloadBytes: 0, downloadReceived: 0, fallbacks: 0 };
    const CLIENT_ID = Math.random().toString(36).slice(2, 10);
    let active = true;

    /** The request never reached SillyTavern's handlers, so the original can be resent. */
    class FallbackError extends Error {}

    /** @param {(name: string) => string|null} header */
    function observeHeaders(header) {
        const marker = header('X-Sync-Proxy');
        if (!marker || marker === 'off') {
            if (active) console.warn('[st-sync] proxy not active, shim switched off');
            active = false;
            return marker;
        }
        store.checkNamespace(header('X-Sync-Store'), header('X-Sync-Scope'));
        return marker;
    }

    function observe(response) {
        return observeHeaders((name) => response.headers.get(name));
    }

    const store = (function createStore() {
        /** @type {Map<string, {t: number, u: number, s: number}>} t: confirmed by server, u: last used, s: stored bytes */
        const known = new Map();
        const memData = new Map();
        let db = null;
        let dataBytes = 0;
        let evicting = false;

        function request(req) {
            return new Promise((resolve, reject) => {
                req.onsuccess = () => resolve(req.result);
                req.onerror = () => reject(req.error);
            });
        }

        function done(tx) {
            return new Promise((resolve, reject) => {
                tx.oncomplete = resolve;
                tx.onerror = () => reject(tx.error);
                tx.onabort = () => reject(tx.error);
            });
        }

        const ready = new Promise((resolve) => {
            if (!window.indexedDB) return resolve(null);
            const req = indexedDB.open('st-sync', 1);
            req.onupgradeneeded = () => {
                req.result.createObjectStore('known');
                req.result.createObjectStore('data');
            };
            req.onsuccess = () => resolve(req.result);
            req.onerror = () => resolve(null);
            req.onblocked = () => resolve(null);
        }).then(async (opened) => {
            db = opened;
            if (!db) return;
            const os = db.transaction('known', 'readonly').objectStore('known');
            const [keys, values] = await Promise.all([request(os.getAllKeys()), request(os.getAll())]);
            keys.forEach((key, i) => {
                known.set(key, values[i]);
                dataBytes += values[i].s || 0;
            });
        }).catch((err) => {
            console.warn('[st-sync] IndexedDB unavailable, using memory only', err);
            db = null;
        });

        function confirmed(hash) {
            const entry = known.get(hash);
            return !!entry && Date.now() - entry.t < CONFIRM_TTL;
        }

        /** Marks hashes (all if null) as no longer known to be on the server; cached bytes stay usable. */
        async function forget(hashes) {
            await ready;
            const tx = db ? db.transaction('known', 'readwrite') : null;
            for (const hash of hashes || [...known.keys()]) {
                const entry = known.get(hash);
                if (!entry || !entry.t) continue;
                entry.t = 0;
                if (tx) tx.objectStore('known').put(entry, hash);
            }
            if (tx) await done(tx);
        }

        // Confirmations are only valid for the chunk store and user they were made against.
        let namespace = null;
        try {
            namespace = localStorage.getItem('st-sync-ns');
        } catch { /* storage may be unavailable */ }
        let namespaceVerified = false;

        function checkNamespace(storeId, scope) {
            if (!storeId) return;
            const [knownStore, knownScope] = (namespace || '/').split('/');
            const next = `${storeId}/${scope || (knownStore === storeId ? knownScope : '')}`;
            if (knownStore !== storeId || (scope && knownScope !== scope)) {
                if (namespace) console.warn('[st-sync] chunk store or user changed, dropping confirmations');
                forget(null).catch(() => {});
            }
            if (next !== namespace) {
                namespace = next;
                try {
                    localStorage.setItem('st-sync-ns', namespace);
                } catch { /* ignore */ }
            }
            if (scope) namespaceVerified = true;
        }

        async function getData(hashes) {
            const out = new Map();
            const wanted = hashes.filter((h) => (known.get(h) || {}).s);
            if (!wanted.length) return out;
            if (!db) {
                for (const h of wanted) if (memData.has(h)) out.set(h, memData.get(h));
                return out;
            }
            const os = db.transaction('data', 'readonly').objectStore('data');
            const values = await Promise.all(wanted.map((h) => request(os.get(h)).catch(() => undefined)));
            wanted.forEach((h, i) => {
                if (values[i] instanceof Uint8Array) out.set(h, values[i]);
            });
            return out;
        }

        /**
         * Marks hashes as present on the server; stores bytes for the entries that carry them.
         * @param {Array<[string, Uint8Array|null]>} entries
         */
        async function remember(entries) {
            const now = Date.now();
            const tx = db ? db.transaction(['known', 'data'], 'readwrite') : null;
            for (const [hash, bytes] of entries) {
                const entry = known.get(hash) || { t: 0, u: 0, s: 0 };
                entry.t = now;
                entry.u = now;
                if (bytes && !entry.s) {
                    const copy = bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength ? bytes : bytes.slice();
                    if (tx) tx.objectStore('data').put(copy, hash);
                    else memData.set(hash, copy);
                    entry.s = copy.length;
                    dataBytes += copy.length;
                }
                known.set(hash, entry);
                if (tx) tx.objectStore('known').put(entry, hash);
            }
            if (tx) await done(tx);
            if (dataBytes > DATA_CAP && !evicting) {
                evicting = true;
                evict().catch(() => {}).finally(() => { evicting = false; });
            }
        }

        async function evict() {
            const stored = [...known].filter(([, e]) => e.s).sort((a, b) => a[1].u - b[1].u);
            const tx = db ? db.transaction(['known', 'data'], 'readwrite') : null;
            for (const [hash, entry] of stored) {
                if (dataBytes <= DATA_CAP * 0.8) break;
                dataBytes -= entry.s;
                entry.s = 0;
                if (tx) {
                    tx.objectStore('data').delete(hash);
                    tx.objectStore('known').put(entry, hash);
                } else {
                    memData.delete(hash);
                }
            }
            if (tx) await done(tx);
        }

        async function clear() {
            await ready;
            known.clear();
            memData.clear();
            dataBytes = 0;
            if (db) {
                const tx = db.transaction(['known', 'data'], 'readwrite');
                tx.objectStore('known').clear();
                tx.objectStore('data').clear();
                await done(tx);
            }
        }

        return {
            ready, confirmed, forget, getData, remember, clear, checkNamespace,
            namespaceVerified: () => namespaceVerified,
            info: () => ({ chunks: known.size, dataBytes, indexedDB: !!db, namespace, namespaceVerified }),
        };
    })();

    const splitter = (function createSplitter() {
        let worker = null;
        let broken = typeof Worker !== 'function';
        let nextId = 0;
        const pending = new Map();

        function getWorker() {
            if (worker || broken) return worker;
            try {
                worker = new Worker(`/__sync/worker.js?v=${encodeURIComponent(typeof ST_SYNC_VERSION === 'string' ? ST_SYNC_VERSION : '')}`);
                worker.onmessage = (event) => {
                    const job = pending.get(event.data.id);
                    if (!job) return;
                    pending.delete(event.data.id);
                    job(event.data);
                };
                worker.onerror = () => {
                    broken = true;
                    worker = null;
                    for (const job of pending.values()) job({ error: 'worker failed' });
                    pending.clear();
                };
            } catch {
                broken = true;
            }
            return worker;
        }

        /**
         * @param {Uint8Array} bytes must own its whole buffer; it is transferred and handed back
         * @returns {Promise<{bytes: Uint8Array, parts: {hashes: string[], starts: number[], ends: number[]}}>}
         */
        return async function split(bytes) {
            const w = bytes.length >= WORKER_THRESHOLD ? getWorker() : null;
            if (!w) return { bytes, parts: Cdc.split(bytes) };
            const result = await new Promise((resolve) => {
                const id = nextId++;
                pending.set(id, resolve);
                w.postMessage({ id, buffer: bytes.buffer }, [bytes.buffer]);
            });
            if (result.error || !result.buffer) {
                throw new FallbackError(result.error || 'worker lost the body');
            }
            return { bytes: new Uint8Array(result.buffer), parts: { hashes: result.hashes, starts: result.starts, ends: result.ends } };
        };
    })();

    function isSameOrigin(url) {
        try {
            const parsed = new URL(url, location.href);
            return parsed.origin === location.origin && !parsed.pathname.startsWith('/__sync/');
        } catch {
            return false;
        }
    }

    /** @returns {Promise<{bytes: Uint8Array, type: string|null}|null>} bytes own their buffer; null when the body type is not handled */
    async function bodyBytes(body) {
        if (typeof body === 'string') return { bytes: encoder.encode(body), type: 'text/plain;charset=UTF-8' };
        if (body instanceof ArrayBuffer) return { bytes: new Uint8Array(body.slice(0)), type: null };
        if (ArrayBuffer.isView(body)) return { bytes: new Uint8Array(body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength)), type: null };
        if (typeof Blob !== 'undefined' && body instanceof Blob) return { bytes: new Uint8Array(await body.arrayBuffer()), type: body.type || null };
        return null;
    }

    async function gzipBytes(bytes) {
        const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream('gzip'));
        return new Uint8Array(await new Response(stream).arrayBuffer());
    }

    /** Envelope with every chunk the server is not known to have (or all of them), plus any it reported missing. */
    async function buildEnvelope(bytes, parts, extraInline, inlineAll = false) {
        const inline = new Set(extraInline || []);
        const seen = new Set();
        parts.hashes.forEach((hash, i) => {
            if (seen.has(hash)) return;
            seen.add(hash);
            if (inlineAll || !store.confirmed(hash)) inline.add(i);
        });
        const indices = [...inline].sort((a, b) => a - b);
        let body = Cdc.encodeUpload(bytes, parts, indices);
        let encoding = 'cdc1';
        if (typeof CompressionStream === 'function' && body.length > 4096) {
            body = await gzipBytes(body);
            encoding = 'cdc1+gzip';
        }
        return { body, encoding };
    }

    async function prepareUpload(bytes, inlineAll = false) {
        await store.ready;
        try {
            const result = await splitter(bytes);
            return { ...result, envelope: await buildEnvelope(result.bytes, result.parts, null, inlineAll) };
        } catch (err) {
            throw err instanceof FallbackError ? err : new FallbackError(String(err));
        }
    }

    function confirmUploaded(parts) {
        store.remember(parts.hashes.map((h) => [h, null])).catch((err) => console.warn('[st-sync] cache write failed', err));
    }

    async function uploadChunked(url, init, headers, bytes) {
        let { bytes: body, parts, envelope } = await prepareUpload(bytes);
        for (let attempt = 0; attempt < 2; attempt++) {
            const sendHeaders = new Headers(headers);
            sendHeaders.set('X-Sync-Encoding', envelope.encoding);
            const response = await nativeFetch(url, { ...init, headers: sendHeaders, body: envelope.body });
            if (!observe(response)) {
                // Answered by SillyTavern directly: its body parser rejected the envelope before any handler ran.
                if ([400, 413, 415].includes(response.status)) throw new FallbackError('proxy gone');
                return response;
            }
            if (response.status === 409 && response.headers.get('X-Sync-Missing')) {
                const { missing } = await response.json();
                store.forget(missing.map((i) => parts.hashes[i])).catch(() => {});
                envelope = await buildEnvelope(body, parts, missing);
                continue;
            }
            if (response.headers.get('X-Sync-Error')) throw new FallbackError(response.headers.get('X-Sync-Error'));
            stats.uploads++;
            stats.uploadBytes += body.length;
            stats.uploadSent += envelope.body.length;
            if (response.status < 500) confirmUploaded(parts);
            return response;
        }
        throw new FallbackError('chunks still missing after retry');
    }

    async function fetchChunks(hashes, signal) {
        for (let attempt = 0; ; attempt++) {
            const response = await nativeFetch('/__sync/chunks', {
                method: 'POST',
                headers: { 'Content-Type': 'application/octet-stream' },
                body: Cdc.encodeHashList(hashes),
                cache: 'no-store',
                credentials: 'same-origin',
                signal,
            });
            if (response.ok) return new Uint8Array(await response.arrayBuffer());
            if (attempt >= 1) throw new TypeError(`st-sync: chunk fetch failed (${response.status})`);
        }
    }

    /** Turns a manifest response back into the original response, streamed window by window. */
    async function decodeResponse(response, signal) {
        if (response.headers.get('X-Sync-Encoding') !== 'cdc1') return response;
        const manifestBytes = new Uint8Array(await response.arrayBuffer());
        const { hashes, lengths } = Cdc.decodeManifest(manifestBytes);
        await store.ready;

        let total = 0;
        for (const len of lengths) total += len;
        let index = 0;
        let received = manifestBytes.length;

        const body = new ReadableStream({
            async pull(controller) {
                try {
                    if (index >= hashes.length) {
                        stats.downloads++;
                        stats.downloadBytes += total;
                        stats.downloadReceived += received;
                        controller.close();
                        return;
                    }
                    let end = index;
                    let windowBytes = 0;
                    while (end < hashes.length && windowBytes < WINDOW_BYTES) windowBytes += lengths[end++];

                    const lengthOf = new Map();
                    for (let i = index; i < end; i++) lengthOf.set(hashes[i], lengths[i]);
                    const unique = [...lengthOf.keys()];
                    const have = await store.getData(unique);
                    const missing = unique.filter((h) => !have.has(h) || have.get(h).length !== lengthOf.get(h));
                    const fetched = new Map();
                    if (missing.length) {
                        const data = await fetchChunks(missing, signal);
                        received += data.length;
                        let o = 0;
                        for (const hash of missing) {
                            const len = lengthOf.get(hash);
                            const piece = data.slice(o, o + len);
                            o += len;
                            if (piece.length !== len || Cdc.hash(piece, 0, len) !== hash) throw new TypeError('st-sync: corrupted chunk');
                            have.set(hash, piece);
                            fetched.set(hash, piece);
                        }
                    }
                    await store.remember(unique.map((h) => [h, fetched.get(h) || null]));
                    for (let i = index; i < end; i++) controller.enqueue(have.get(hashes[i]));
                    index = end;
                } catch (err) {
                    controller.error(err);
                }
            },
        }, { highWaterMark: 0 });

        const headers = new Headers(response.headers);
        const contentType = headers.get('X-Sync-Content-Type');
        headers.delete('X-Sync-Encoding');
        headers.delete('X-Sync-Content-Type');
        if (contentType) headers.set('Content-Type', contentType);
        else headers.delete('Content-Type');
        headers.set('Content-Length', String(total));
        const result = new Response(body, { status: response.status, statusText: response.statusText, headers });
        Object.defineProperty(result, 'url', { value: response.url });
        Object.defineProperty(result, 'redirected', { value: response.redirected });
        return result;
    }

    async function syncFetch(input, init) {
        if (!active || !(typeof input === 'string' || input instanceof URL) || !isSameOrigin(String(input))) {
            return nativeFetch(input, init);
        }
        const options = { ...(init || {}) };
        const headers = new Headers(options.headers || undefined);
        let body = null;
        if (options.body != null) {
            body = await bodyBytes(options.body);
            if (!body) return nativeFetch(input, init);
        }
        headers.set('X-Sync-Accept', 'cdc1');
        headers.set('X-Sync-Client', `${CLIENT_ID}.f`);

        let response;
        if (body && body.bytes.length >= THRESHOLD) {
            if (!headers.has('Content-Type') && body.type) headers.set('Content-Type', body.type);
            try {
                response = await uploadChunked(input, options, headers, body.bytes);
            } catch (err) {
                if (!(err instanceof FallbackError)) throw err;
                stats.fallbacks++;
                console.warn('[st-sync] falling back to a plain request:', err.message);
                return nativeFetch(input, init);
            }
        } else {
            response = await nativeFetch(input, { ...options, headers });
            observe(response);
        }
        return decodeResponse(response, options.signal);
    }

    window.fetch = function fetch(input, init) {
        return syncFetch(input, init);
    };

    const xhrProto = XMLHttpRequest.prototype;
    const nativeOpen = xhrProto.open;
    const nativeSetRequestHeader = xhrProto.setRequestHeader;
    const nativeSend = xhrProto.send;

    xhrProto.open = function (method, url, async) {
        this.__stSync = { url: String(url), async: arguments.length < 3 || !!async, contentType: false };
        return nativeOpen.apply(this, arguments);
    };

    xhrProto.setRequestHeader = function (name, value) {
        if (this.__stSync && String(name).toLowerCase() === 'content-type') this.__stSync.contentType = true;
        return nativeSetRequestHeader.apply(this, arguments);
    };

    xhrProto.send = function (body) {
        const state = this.__stSync;
        if (!active || !state || !isSameOrigin(state.url)) return nativeSend.apply(this, arguments);
        nativeSetRequestHeader.call(this, 'X-Sync-Client', `${CLIENT_ID}.x`);
        if (!state.async || typeof body !== 'string' || body.length < THRESHOLD / 4) return nativeSend.apply(this, arguments);
        const bytes = encoder.encode(body);
        if (bytes.length < THRESHOLD) return nativeSend.apply(this, arguments);
        const xhr = this;
        // An XHR cannot be retried behind the caller's back. It relies on server-side chunks only
        // once this page has seen which store and user it talks to; the proxy never evicts chunks
        // younger than the confirmation TTL, so a 409 here means the store lost data.
        prepareUpload(bytes, !store.namespaceVerified()).then(({ bytes: prepared, parts, envelope }) => {
            if (xhr.readyState !== XMLHttpRequest.OPENED) return;
            nativeSetRequestHeader.call(xhr, 'X-Sync-Encoding', envelope.encoding);
            if (!state.contentType) nativeSetRequestHeader.call(xhr, 'Content-Type', 'text/plain;charset=UTF-8');
            xhr.addEventListener('load', () => {
                observeHeaders((name) => xhr.getResponseHeader(name));
                if (xhr.status === 409 || xhr.getResponseHeader('X-Sync-Error')) {
                    console.warn('[st-sync] xhr upload refused, dropping confirmations', xhr.status);
                    store.forget(null).catch(() => {});
                    return;
                }
                stats.uploads++;
                stats.uploadBytes += prepared.length;
                stats.uploadSent += envelope.body.length;
                confirmUploaded(parts);
            });
            nativeSend.call(xhr, envelope.body);
        }, (err) => {
            console.warn('[st-sync] xhr envelope failed, sending plain body', err);
            if (xhr.readyState === XMLHttpRequest.OPENED) nativeSend.call(xhr, body);
        });
    };

    window.__stSync = { stats, store, info: () => ({ active, ...stats, ...store.info() }) };
})();
