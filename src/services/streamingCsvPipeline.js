'use strict';
/**
 * Streaming CSV Pipeline — Main-Thread Orchestrator
 *
 * Coordinates the high-performance WASM+streaming CSV import:
 *
 *   1. Opens the file with file.stream() (true ReadableStream — no full
 *      buffering, constant memory regardless of file size).
 *   2. Coalesces small stream chunks (~64 KB each) into ~4 MB buffers.
 *   3. Sends each buffer to the streamingParseWorker via Transferable
 *      (zero-copy hand-off — the ArrayBuffer moves to the worker without
 *      being cloned).
 *   4. Collects batched row objects back from the worker.
 *   5. Reports progress throughout.
 *
 * Performance budget for 1 GB CSV on a mid-range machine:
 *   • File streaming:   ~3-5 s   (SSD read throughput)
 *   • Byte scanning:    ~2-4 s   (WASM) / ~4-8 s (JS fallback)
 *   • Field decoding:   ~8-12 s  (TextDecoder per-field, not per-chunk)
 *   • Type coercion:    ~3-5 s
 *   • Object creation:  ~5-8 s
 *   Total:              ~21-34 s (WASM) / ~23-38 s (JS fallback)
 */

import { logger } from '../../logger.js';
import StreamingParseWorker from './streamingParseWorker.js?worker';

// ── Constants ──────────────────────────────────────────────────────────────
const MAX_ROWS = 5_000_000;
const COALESCE_TARGET = 4 * 1024 * 1024; // 4 MB — sweet spot for worker throughput

// ── Chunk Buffer ───────────────────────────────────────────────────────────

/**
 * Accumulates small ReadableStream chunks into a larger buffer before
 * flushing to the worker.  Avoids per-64 KB postMessage overhead.
 */
class ChunkBuffer {
  constructor(targetSize) {
    this.target = targetSize;
    this.buf = new Uint8Array(targetSize);
    this.pos = 0;
  }

  /** Append bytes from a stream chunk. Grows the buffer if needed. */
  append(data) {
    const needed = this.pos + data.length;
    if (needed > this.buf.length) {
      const next = new Uint8Array(Math.max(this.buf.length * 2, needed));
      next.set(this.buf.subarray(0, this.pos));
      this.buf = next;
    }
    this.buf.set(data, this.pos);
    this.pos += data.length;
  }

  shouldFlush() {
    return this.pos >= this.target;
  }

  /** Return the accumulated bytes as a trimmed copy and reset. */
  flush() {
    const out = this.buf.slice(0, this.pos);
    this.pos = 0;
    return out;
  }

  hasData() {
    return this.pos > 0;
  }
}

// ── File → Worker Streaming ────────────────────────────────────────────────

/**
 * Stream file bytes to the worker in coalesced ~4 MB chunks.
 * Uses file.stream() for true streaming — constant memory, no full read.
 */
async function streamFileToWorker(file, worker, onProgress) {
  const reader = file.stream().getReader();
  const buffer = new ChunkBuffer(COALESCE_TARGET);
  const totalBytes = file.size;
  let bytesRead = 0;

  try {
    for (;;) {
      const { done, value } = await reader.read();

      if (done) {
        // Flush remaining buffer as the final chunk
        if (buffer.hasData()) {
          const chunk = buffer.flush();
          worker.postMessage(
            { type: 'CHUNK', bytes: chunk, isLast: true },
            [chunk.buffer]
          );
        } else {
          worker.postMessage({
            type: 'CHUNK',
            bytes: new Uint8Array(0),
            isLast: true,
          });
        }
        break;
      }

      buffer.append(value);
      bytesRead += value.length;

      // Report reading progress (0 – 50 %)
      onProgress(Math.round((bytesRead / totalBytes) * 50));

      // Flush when buffer reaches target size
      if (buffer.shouldFlush()) {
        const chunk = buffer.flush();
        worker.postMessage(
          { type: 'CHUNK', bytes: chunk, isLast: false },
          [chunk.buffer] // Transferable — zero-copy to worker
        );
      }
    }
  } finally {
    reader.releaseLock();
  }
}

// ── Public API ─────────────────────────────────────────────────────────────

/**
 * Stream-parse a CSV file using the WASM+streaming worker pipeline.
 *
 * @param {File} file         CSV file to parse
 * @param {string} delimiter  Field delimiter (default ',')
 * @param {(pct: number) => void} onProgress  Progress callback (0-100)
 * @returns {Promise<{
 *   rows: object[],
 *   headers: string[],
 *   rowCapReached: boolean,
 *   wasmUsed: boolean
 * }>}
 */
export function streamParseCsv(file, delimiter, onProgress) {
  return new Promise((resolve, reject) => {
    const worker = new StreamingParseWorker();
    const allRows = [];
    let finalHeaders = null;
    let rowCapReached = false;
    let wasmUsed = false;
    let resolved = false; // Guard against duplicate DONE (STOP + final CHUNK)

    worker.onmessage = e => {
      const msg = e.data;

      switch (msg.type) {
        case 'READY':
          wasmUsed = msg.wasm;
          logger.info(
            `Streaming worker ready — WASM scanner: ${wasmUsed ? 'ACTIVE' : 'JS fallback'}`
          );
          // Worker is ready — start streaming file data to it
          streamFileToWorker(file, worker, onProgress).catch(err => {
            worker.terminate();
            reject(err);
          });
          break;

        case 'BATCH':
          if (!rowCapReached) {
            const remaining = MAX_ROWS - allRows.length;
            const rows = msg.rows;
            const count = Math.min(rows.length, remaining);

            // Push rows without spread to avoid call-stack limits
            for (let i = 0; i < count; i++) {
              allRows.push(rows[i]);
            }

            if (allRows.length >= MAX_ROWS) {
              rowCapReached = true;
              logger.warn(
                `Row cap reached (${MAX_ROWS.toLocaleString()}). Remaining rows skipped.`
              );
              // Tell worker to stop parsing — no point processing the rest
              // of the file if we'll discard the rows anyway.
              worker.postMessage({ type: 'STOP' });
            }
          }

          finalHeaders = msg.headers;

          // Report parsing progress (50 – 95 %)
          // Estimate total rows from file size and average bytes/row
          const avgBytesPerRow = Math.max(80, file.size / Math.max(1, msg.rowCount));
          const estTotalRows = file.size / avgBytesPerRow;
          const parsePct = Math.min(
            45,
            (msg.rowCount / Math.max(1, estTotalRows)) * 45
          );
          onProgress(Math.round(50 + parsePct));
          break;

        case 'DONE':
          if (resolved) break; // Ignore duplicate DONE from STOP + final CHUNK race
          resolved = true;
          finalHeaders = msg.headers;
          onProgress(100);
          worker.terminate();
          logger.info(
            `Streaming parse complete: ${msg.rowCount.toLocaleString()} rows` +
              (wasmUsed ? ' (WASM)' : ' (JS)')
          );
          resolve({
            rows: allRows,
            headers: finalHeaders || [],
            rowCapReached,
            wasmUsed,
          });
          break;

        case 'ERROR':
          worker.terminate();
          reject(new Error(msg.message));
          break;
      }
    };

    worker.onerror = err => {
      worker.terminate();
      reject(new Error(err.message || 'Streaming worker error'));
    };

    // Kick off — worker will load WASM and respond with READY
    onProgress(1);
    worker.postMessage({ type: 'INIT', delimiter });
  });
}
