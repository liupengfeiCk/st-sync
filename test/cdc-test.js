'use strict';

const crypto = require('crypto');
const Cdc = require('../cdc.js');

let failures = 0;
function check(name, ok, detail = '') {
    if (!ok) failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'} ${name}${detail ? ' — ' + detail : ''}`);
}

const data = crypto.randomBytes(4 * 1024 * 1024);
const parts = Cdc.split(data);
const sizes = parts.ends.map((e, i) => e - parts.starts[i]);
check('chunks cover the input', parts.ends[parts.ends.length - 1] === data.length && parts.starts[0] === 0);
check('chunk sizes within bounds', sizes.slice(0, -1).every((s) => s >= Cdc.MIN_SIZE && s <= Cdc.MAX_SIZE),
    `avg ${Math.round(data.length / sizes.length)} bytes`);
check('hashes are deterministic', Cdc.split(data).hashes.join() === parts.hashes.join());
check('empty input', Cdc.split(new Uint8Array(0)).hashes.length === 0);
check('hash distinguishes tails', Cdc.hash(Buffer.from('abc'), 0, 3) !== Cdc.hash(Buffer.from('abd'), 0, 3));
check('hash depends on length', Cdc.hash(Buffer.from([0]), 0, 1) !== Cdc.hash(Buffer.from([0, 0]), 0, 2));

const edited = Buffer.concat([data.subarray(0, 2_000_000), Buffer.from('inserted bytes'), data.subarray(2_000_000)]);
const editedParts = Cdc.split(edited);
const known = new Set(parts.hashes);
const fresh = editedParts.hashes.filter((h) => !known.has(h)).length;
check('insertion only changes nearby chunks', fresh <= 3, `${fresh} new of ${editedParts.hashes.length}`);

const envelope = Cdc.encodeUpload(edited, editedParts, [1, 5]);
const decoded = Cdc.decodeUpload(envelope);
check('upload envelope round trip', decoded.hashes.join() === editedParts.hashes.join() &&
    Buffer.compare(Buffer.from(decoded.inline.get(5)), edited.subarray(editedParts.starts[5], editedParts.ends[5])) === 0);
const manifest = Cdc.decodeManifest(Cdc.encodeManifest(parts));
check('manifest round trip', manifest.hashes.join() === parts.hashes.join() && manifest.lengths.join() === sizes.join());
check('hash list round trip', Cdc.decodeHashList(Cdc.encodeHashList(parts.hashes)).join() === parts.hashes.join());

for (const pieceSize of [1, 777, 65536, 1 << 20]) {
    const input = pieceSize === 1 ? data.subarray(0, 300000) : data;
    const chunker = Cdc.createChunker();
    const streamed = [];
    for (let o = 0; o < input.length; o += pieceSize) streamed.push(...chunker.push(input.subarray(o, o + pieceSize)));
    streamed.push(...chunker.end());
    const expected = Cdc.split(input).hashes;
    const got = streamed.map((c) => Cdc.hash(c, 0, c.length));
    check(`streaming chunker matches batch (pieces of ${pieceSize})`, got.join() === expected.join(), `${got.length} chunks`);
}
const textLike = Buffer.from(JSON.stringify(Array.from({ length: 50000 }, (_, i) => ({ i, s: 'same text '.repeat(i % 7) }))));
{
    const chunker = Cdc.createChunker();
    const streamed = [];
    for (let o = 0; o < textLike.length; o += 4096) streamed.push(...chunker.push(textLike.subarray(o, o + 4096)));
    streamed.push(...chunker.end());
    check('streaming chunker matches batch on JSON text', streamed.map((c) => Cdc.hash(c, 0, c.length)).join() === Cdc.split(textLike).hashes.join());
}

const big = crypto.randomBytes(32 * 1024 * 1024);
const t0 = process.hrtime.bigint();
Cdc.split(big);
console.log(`split 32MB in ${Number(process.hrtime.bigint() - t0) / 1e6 | 0} ms (node)`);

console.log(failures ? `${failures} FAILED` : 'ALL PASSED');
process.exit(failures ? 1 : 0);
