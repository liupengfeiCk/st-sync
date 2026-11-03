/**
 * Content-defined chunking and 128-bit chunk hashing shared by the proxy (Node) and the shim (browser).
 * Both sides must produce identical boundaries and hashes, so this file must stay dependency-free
 * and must not use APIs that differ between Node and browsers.
 */
(function (root, factory) {
    if (typeof module === 'object' && module.exports) {
        module.exports = factory();
    } else {
        root.StSyncCdc = factory();
    }
})(typeof self !== 'undefined' ? self : this, function () {
    'use strict';

    const MIN_SIZE = 2 * 1024;
    const MAX_SIZE = 64 * 1024;
    // 13 high bits must be zero: average chunk ~8KB, and the high bits depend on the last 32 bytes.
    const BOUNDARY_MASK = 0xFFF80000;
    const HASH_BYTES = 16;

    const GEAR = new Uint32Array(256);
    let seed = 0x9E3779B9;
    for (let i = 0; i < 256; i++) {
        seed ^= seed << 13; seed >>>= 0;
        seed ^= seed >>> 17;
        seed ^= seed << 5; seed >>>= 0;
        GEAR[i] = seed;
    }

    /**
     * @param {Uint8Array} data
     * @returns {number[]} exclusive end offset of every chunk
     */
    function boundaries(data) {
        const ends = [];
        const n = data.length;
        let start = 0;
        while (start < n) {
            const limit = Math.min(start + MAX_SIZE, n);
            let end = limit;
            let h = 0;
            for (let i = start + MIN_SIZE; i < limit; i++) {
                h = ((h << 1) + GEAR[data[i]]) >>> 0;
                if ((h & BOUNDARY_MASK) === 0) {
                    end = i + 1;
                    break;
                }
            }
            ends.push(end);
            start = end;
        }
        return ends;
    }

    /**
     * Incremental version of boundaries(): feed data in pieces, get the same chunks back.
     * Holds at most MAX_SIZE bytes plus the latest piece.
     */
    function createChunker() {
        let buf = new Uint8Array(0);
        let pos = 0;
        let scan = MIN_SIZE;
        let h = 0;

        function take(len, out) {
            out.push(buf.slice(pos, pos + len));
            pos += len;
            scan = MIN_SIZE;
            h = 0;
        }

        function drain(final) {
            const out = [];
            for (;;) {
                const available = buf.length - pos;
                const limit = pos + Math.min(MAX_SIZE, available);
                // Locals: closure variables in the per-byte loop are several times slower.
                const data = buf;
                let i = pos + scan;
                let hash = h;
                let found = -1;
                for (; i < limit; i++) {
                    hash = ((hash << 1) + GEAR[data[i]]) >>> 0;
                    if ((hash & BOUNDARY_MASK) === 0) {
                        found = i + 1 - pos;
                        break;
                    }
                }
                scan = i - pos;
                h = hash;
                if (found > 0) {
                    take(found, out);
                } else if (available >= MAX_SIZE) {
                    take(MAX_SIZE, out);
                } else {
                    if (final && available) take(available, out);
                    return out;
                }
            }
        }

        return {
            push(data) {
                const available = buf.length - pos;
                const merged = new Uint8Array(available + data.length);
                merged.set(buf.subarray(pos), 0);
                merged.set(data, available);
                buf = merged;
                pos = 0;
                return drain(false);
            },
            end() {
                return drain(true);
            },
        };
    }

    function rotl(x, r) {
        return (x << r) | (x >>> (32 - r));
    }

    function fmix(h) {
        h ^= h >>> 16;
        h = Math.imul(h, 0x85ebca6b);
        h ^= h >>> 13;
        h = Math.imul(h, 0xc2b2ae35);
        h ^= h >>> 16;
        return h;
    }

    /**
     * MurmurHash3 x86_128 of data[start, end).
     * @returns {string} 32 hex chars
     */
    function hash(data, start, end) {
        const len = end - start;
        const blocks = len >>> 4;
        const c1 = 0x239b961b, c2 = 0xab0e9789, c3 = 0x38b34ae5, c4 = 0xa1e38b93;
        let h1 = 0, h2 = 0, h3 = 0, h4 = 0;
        let k1, k2, k3, k4;

        for (let b = 0; b < blocks; b++) {
            const o = start + b * 16;
            k1 = data[o] | (data[o + 1] << 8) | (data[o + 2] << 16) | (data[o + 3] << 24);
            k2 = data[o + 4] | (data[o + 5] << 8) | (data[o + 6] << 16) | (data[o + 7] << 24);
            k3 = data[o + 8] | (data[o + 9] << 8) | (data[o + 10] << 16) | (data[o + 11] << 24);
            k4 = data[o + 12] | (data[o + 13] << 8) | (data[o + 14] << 16) | (data[o + 15] << 24);

            k1 = Math.imul(k1, c1); k1 = rotl(k1, 15); k1 = Math.imul(k1, c2); h1 ^= k1;
            h1 = rotl(h1, 19); h1 = (h1 + h2) | 0; h1 = (Math.imul(h1, 5) + 0x561ccd1b) | 0;
            k2 = Math.imul(k2, c2); k2 = rotl(k2, 16); k2 = Math.imul(k2, c3); h2 ^= k2;
            h2 = rotl(h2, 17); h2 = (h2 + h3) | 0; h2 = (Math.imul(h2, 5) + 0x0bcaa747) | 0;
            k3 = Math.imul(k3, c3); k3 = rotl(k3, 17); k3 = Math.imul(k3, c4); h3 ^= k3;
            h3 = rotl(h3, 15); h3 = (h3 + h4) | 0; h3 = (Math.imul(h3, 5) + 0x96cd1c35) | 0;
            k4 = Math.imul(k4, c4); k4 = rotl(k4, 18); k4 = Math.imul(k4, c1); h4 ^= k4;
            h4 = rotl(h4, 13); h4 = (h4 + h1) | 0; h4 = (Math.imul(h4, 5) + 0x32ac3b17) | 0;
        }

        const t = start + blocks * 16;
        k1 = 0; k2 = 0; k3 = 0; k4 = 0;
        switch (len & 15) {
            case 15: k4 ^= data[t + 14] << 16;
            case 14: k4 ^= data[t + 13] << 8;
            case 13: k4 ^= data[t + 12];
                k4 = Math.imul(k4, c4); k4 = rotl(k4, 18); k4 = Math.imul(k4, c1); h4 ^= k4;
            case 12: k3 ^= data[t + 11] << 24;
            case 11: k3 ^= data[t + 10] << 16;
            case 10: k3 ^= data[t + 9] << 8;
            case 9: k3 ^= data[t + 8];
                k3 = Math.imul(k3, c3); k3 = rotl(k3, 17); k3 = Math.imul(k3, c4); h3 ^= k3;
            case 8: k2 ^= data[t + 7] << 24;
            case 7: k2 ^= data[t + 6] << 16;
            case 6: k2 ^= data[t + 5] << 8;
            case 5: k2 ^= data[t + 4];
                k2 = Math.imul(k2, c2); k2 = rotl(k2, 16); k2 = Math.imul(k2, c3); h2 ^= k2;
            case 4: k1 ^= data[t + 3] << 24;
            case 3: k1 ^= data[t + 2] << 16;
            case 2: k1 ^= data[t + 1] << 8;
            case 1: k1 ^= data[t];
                k1 = Math.imul(k1, c1); k1 = rotl(k1, 15); k1 = Math.imul(k1, c2); h1 ^= k1;
        }

        h1 ^= len; h2 ^= len; h3 ^= len; h4 ^= len;
        h1 = (h1 + h2) | 0; h1 = (h1 + h3) | 0; h1 = (h1 + h4) | 0;
        h2 = (h2 + h1) | 0; h3 = (h3 + h1) | 0; h4 = (h4 + h1) | 0;
        h1 = fmix(h1); h2 = fmix(h2); h3 = fmix(h3); h4 = fmix(h4);
        h1 = (h1 + h2) | 0; h1 = (h1 + h3) | 0; h1 = (h1 + h4) | 0;
        h2 = (h2 + h1) | 0; h3 = (h3 + h1) | 0; h4 = (h4 + h1) | 0;

        return hex32(h1) + hex32(h2) + hex32(h3) + hex32(h4);
    }

    function hex32(x) {
        return (x >>> 0).toString(16).padStart(8, '0');
    }

    function writeHash(view, offset, hex) {
        for (let i = 0; i < 4; i++) {
            view.setUint32(offset + i * 4, parseInt(hex.slice(i * 8, i * 8 + 8), 16), true);
        }
    }

    function readHash(view, offset) {
        let out = '';
        for (let i = 0; i < 4; i++) {
            out += hex32(view.getUint32(offset + i * 4, true));
        }
        return out;
    }

    /**
     * Splits data into chunks.
     * @param {Uint8Array} data
     * @returns {{hashes: string[], starts: number[], ends: number[]}}
     */
    function split(data) {
        const ends = boundaries(data);
        const starts = new Array(ends.length);
        const hashes = new Array(ends.length);
        let start = 0;
        for (let i = 0; i < ends.length; i++) {
            starts[i] = start;
            hashes[i] = hash(data, start, ends[i]);
            start = ends[i];
        }
        return { hashes, starts, ends };
    }

    // Manifest entry: 16-byte hash + uint32 length (little endian).
    const ENTRY_BYTES = HASH_BYTES + 4;
    const UPLOAD_MAGIC = 0x31435953; // "SYC1"
    const DOWNLOAD_MAGIC = 0x31445953; // "SYD1"

    /**
     * Upload envelope: magic | count | inlineCount | entries | inline indices | inline bytes.
     * @param {Uint8Array} data original body
     * @param {{hashes: string[], starts: number[], ends: number[]}} parts
     * @param {number[]} inline indices of chunks whose bytes are included
     */
    function encodeUpload(data, parts, inline) {
        const count = parts.hashes.length;
        let inlineBytes = 0;
        for (const i of inline) inlineBytes += parts.ends[i] - parts.starts[i];
        const headerBytes = 12 + count * ENTRY_BYTES + inline.length * 4;
        const out = new Uint8Array(headerBytes + inlineBytes);
        const view = new DataView(out.buffer);
        view.setUint32(0, UPLOAD_MAGIC, true);
        view.setUint32(4, count, true);
        view.setUint32(8, inline.length, true);
        let o = 12;
        for (let i = 0; i < count; i++) {
            writeHash(view, o, parts.hashes[i]);
            view.setUint32(o + HASH_BYTES, parts.ends[i] - parts.starts[i], true);
            o += ENTRY_BYTES;
        }
        for (const i of inline) {
            view.setUint32(o, i, true);
            o += 4;
        }
        for (const i of inline) {
            out.set(data.subarray(parts.starts[i], parts.ends[i]), o);
            o += parts.ends[i] - parts.starts[i];
        }
        return out;
    }

    /**
     * @param {Uint8Array} buf
     * @returns {{hashes: string[], lengths: number[], inline: Map<number, Uint8Array>}}
     */
    function decodeUpload(buf) {
        const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
        if (buf.byteLength < 12 || view.getUint32(0, true) !== UPLOAD_MAGIC) {
            throw new Error('bad upload envelope');
        }
        const count = view.getUint32(4, true);
        const inlineCount = view.getUint32(8, true);
        let o = 12;
        if (buf.byteLength < o + count * ENTRY_BYTES + inlineCount * 4) {
            throw new Error('truncated upload envelope');
        }
        const hashes = new Array(count);
        const lengths = new Array(count);
        for (let i = 0; i < count; i++) {
            hashes[i] = readHash(view, o);
            lengths[i] = view.getUint32(o + HASH_BYTES, true);
            o += ENTRY_BYTES;
        }
        const indices = new Array(inlineCount);
        for (let i = 0; i < inlineCount; i++) {
            indices[i] = view.getUint32(o, true);
            o += 4;
        }
        const inline = new Map();
        for (const i of indices) {
            if (i >= count || o + lengths[i] > buf.byteLength) {
                throw new Error('bad inline chunk');
            }
            inline.set(i, buf.subarray(o, o + lengths[i]));
            o += lengths[i];
        }
        return { hashes, lengths, inline };
    }

    /** Download envelope: magic | count | entries. */
    function encodeManifest(parts) {
        const count = parts.hashes.length;
        const out = new Uint8Array(8 + count * ENTRY_BYTES);
        const view = new DataView(out.buffer);
        view.setUint32(0, DOWNLOAD_MAGIC, true);
        view.setUint32(4, count, true);
        let o = 8;
        for (let i = 0; i < count; i++) {
            writeHash(view, o, parts.hashes[i]);
            view.setUint32(o + HASH_BYTES, parts.ends[i] - parts.starts[i], true);
            o += ENTRY_BYTES;
        }
        return out;
    }

    function decodeManifest(buf) {
        const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
        if (buf.byteLength < 8 || view.getUint32(0, true) !== DOWNLOAD_MAGIC) {
            throw new Error('bad manifest');
        }
        const count = view.getUint32(4, true);
        if (buf.byteLength < 8 + count * ENTRY_BYTES) {
            throw new Error('truncated manifest');
        }
        const hashes = new Array(count);
        const lengths = new Array(count);
        let o = 8;
        for (let i = 0; i < count; i++) {
            hashes[i] = readHash(view, o);
            lengths[i] = view.getUint32(o + HASH_BYTES, true);
            o += ENTRY_BYTES;
        }
        return { hashes, lengths };
    }

    function encodeHashList(hashes) {
        const out = new Uint8Array(hashes.length * HASH_BYTES);
        const view = new DataView(out.buffer);
        hashes.forEach((h, i) => writeHash(view, i * HASH_BYTES, h));
        return out;
    }

    function decodeHashList(buf) {
        const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
        if (buf.byteLength % HASH_BYTES !== 0) {
            throw new Error('bad hash list');
        }
        const out = [];
        for (let o = 0; o < buf.byteLength; o += HASH_BYTES) {
            out.push(readHash(view, o));
        }
        return out;
    }

    return {
        MIN_SIZE, MAX_SIZE, HASH_BYTES, ENTRY_BYTES,
        boundaries, createChunker, hash, split, writeHash, readHash,
        encodeUpload, decodeUpload,
        encodeManifest, decodeManifest,
        encodeHashList, decodeHashList,
    };
});
