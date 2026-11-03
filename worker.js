/* st-sync worker: chunks and hashes request bodies off the page's main thread. Loaded after cdc.js. */
self.onmessage = function (event) {
    const { id, buffer } = event.data;
    try {
        const parts = StSyncCdc.split(new Uint8Array(buffer));
        self.postMessage({ id, buffer, hashes: parts.hashes, starts: parts.starts, ends: parts.ends }, [buffer]);
    } catch (err) {
        self.postMessage({ id, buffer, error: String(err) }, [buffer]);
    }
};
