'use strict';

// Minimal stand-in for SillyTavern used by the st-sync tests.
const http = require('http');
const crypto = require('crypto');
const zlib = require('zlib');

const port = Number(process.env.PORT || 18355);
const requireAuth = process.env.REQUIRE_AUTH === '1';
const state = { lastBody: Buffer.alloc(0), lastHeaders: {}, aborted: 0, bigVersion: 0 };

function bigDocument(version) {
    const items = [];
    for (let i = 0; i < 4000; i++) {
        items.push({ id: i, name: `item-${i}`, text: crypto.createHash('sha256').update(String(i)).digest('hex').repeat(3) });
    }
    items[1234].text = `changed-in-version-${version}`;
    return Buffer.from(JSON.stringify({ version: 0, items }));
}

const pageHtml = require('fs').readFileSync(require('path').join(__dirname, 'page.html'));

function readBody(req) {
    return new Promise((resolve) => {
        const parts = [];
        req.on('data', (c) => parts.push(c));
        req.on('end', () => resolve(Buffer.concat(parts)));
    });
}

function send(req, res, status, type, body) {
    const headers = { 'Content-Type': type };
    if (/gzip/.test(req.headers['accept-encoding'] || '') && body.length > 1024) {
        body = zlib.gzipSync(body);
        headers['Content-Encoding'] = 'gzip';
    }
    headers['Content-Length'] = body.length;
    res.writeHead(status, headers);
    res.end(body);
}

http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x');
    if (requireAuth && req.headers.authorization !== 'Basic dTpw') {
        res.writeHead(401, { 'WWW-Authenticate': 'Basic realm="test"' });
        return res.end('auth required');
    }
    switch (url.pathname) {
        case '/':
            return send(req, res, 200, 'text/html; charset=utf-8', pageHtml);
        case '/api/users/me':
            // Like SillyTavern: the avatar comes inline as a data URL.
            return send(req, res, 200, 'application/json', Buffer.from(JSON.stringify({ handle: 'default-user', name: 'User', avatar: 'data:image/png;base64,' + 'A'.repeat(100000) })));
        case '/api/headers':
            return send(req, res, 200, 'application/json', Buffer.from(JSON.stringify(req.headers)));
        case '/api/big':
            return send(req, res, 200, 'application/json; charset=utf-8', bigDocument(Number(url.searchParams.get('v') || 0)));
        case '/api/save': {
            const body = await readBody(req);
            if (/json/.test(req.headers['content-type'] || '')) {
                // Like express.json(): a body that is not JSON never reaches the handler.
                try {
                    JSON.parse(body.toString());
                } catch {
                    return send(req, res, 400, 'text/html', Buffer.from('Bad Request'));
                }
            }
            state.lastBody = body;
            state.lastHeaders = req.headers;
            const digest = crypto.createHash('sha1').update(body).digest('hex');
            return send(req, res, 200, 'application/json', Buffer.from(JSON.stringify({ length: body.length, sha1: digest, type: req.headers['content-type'] || null })));
        }
        case '/api/huge': {
            // Streams N MB of deterministic, mostly unique JSON-like text without Content-Length.
            const total = Number(url.searchParams.get('mb') || 100) * 1024 * 1024;
            const seed = url.searchParams.get('seed') || '0';
            res.writeHead(200, { 'Content-Type': 'application/json' });
            let sent = 0;
            let n = 0;
            const pump = () => {
                while (sent < total) {
                    const line = Buffer.from(`{"n":${n++},"h":"${crypto.createHash('sha256').update(seed + ':' + n).digest('hex').repeat(8)}"},\n`);
                    sent += line.length;
                    if (!res.write(line)) return res.once('drain', pump);
                }
                res.end();
            };
            return pump();
        }
        case '/api/sink': {
            let length = 0;
            const hash = crypto.createHash('sha1');
            req.on('data', (c) => { length += c.length; hash.update(c); });
            req.on('end', () => send(req, res, 200, 'application/json', Buffer.from(JSON.stringify({ length, sha1: hash.digest('hex') }))));
            return;
        }
        case '/api/last':
            return send(req, res, 200, 'application/octet-stream', state.lastBody);
        case '/api/sse': {
            res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
            let n = 0;
            const timer = setInterval(() => {
                res.write(`data: ${n}\n\n`);
                if (++n === 5) {
                    clearInterval(timer);
                    res.end();
                }
            }, 200);
            return;
        }
        case '/api/slow': {
            req.on('close', () => { if (!res.writableFinished) state.aborted++; });
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write('data: start\n\n');
            setTimeout(() => res.end('data: end\n\n'), 5000);
            return;
        }
        case '/api/state':
            return send(req, res, 200, 'application/json', Buffer.from(JSON.stringify({ aborted: state.aborted, lastLength: state.lastBody.length, lastEncoding: state.lastHeaders['x-sync-encoding'] || null })));
        default:
            res.writeHead(404);
            res.end();
    }
}).listen(port, '127.0.0.1', () => console.log(`fake upstream on ${port}`));
