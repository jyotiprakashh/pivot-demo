'use strict';
/**
 * Streaming CSV Parse Worker
 *
 * High-performance CSV parser that operates on raw Uint8Array chunks.
 * The key insight: we NEVER convert the entire chunk to a string.
 * Line-break scanning and delimiter scanning happen at the byte level;
 * only individual field values are decoded with TextDecoder.
 *
 * For a 1 GB file this avoids creating a ~2 GB UTF-16 string on the
 * main thread, which is the single biggest source of memory pressure
 * and GC pauses in the old pipeline.
 *
 * Uses WASM acceleration for the line-scanning hot loop when the
 * compiled csvStreamParser.wasm is available; otherwise falls back to
 * an optimised JS byte scanner that V8 JIT-compiles to near-native.
 *
 * ── Protocol ────────────────────────────────────────────────────────
 *   IN:  INIT  { delimiter: string }
 *   IN:  CHUNK { bytes: Uint8Array (Transferable), isLast: boolean }
 *
 *   OUT: READY { wasm: boolean }
 *   OUT: BATCH { rows: object[], headers: string[], rowCount: number }
 *   OUT: DONE  { headers: string[], rowCount: number }
 *   OUT: ERROR { message: string }
 */

// ── Constants ──────────────────────────────────────────────────────────────
const NEWLINE = 10; // '\n'
const CARRIAGE = 13; // '\r'
const QUOTE = 34; // '"'
const SPACE = 32;
const TAB = 9;
const BATCH_SIZE = 10000;
const WASM_MAX_INPUT = 8 * 1024 * 1024; // Must match C source INPUT_BUF_SIZE

const decoder = new TextDecoder('utf-8');

// ── Parser state ───────────────────────────────────────────────────────────
let headers = null;
let leftover = null; // Uint8Array — incomplete line from previous chunk
let quoteState = false; // true = inside a quoted field at end of leftover
let delimByte = 44; // ASCII of delimiter (default ',')
let rowCount = 0;
let pendingRows = [];

// ── WASM scanner (optional) ────────────────────────────────────────────────
let wasm = null;
let wasmInputView = null; // Uint8Array into WASM input_buf
let wasmBreaksView = null; // Int32Array into WASM break_positions

async function tryLoadWasm() {
  try {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), 3000); // 3 s timeout

    const resp = await fetch('/wasm/csvStreamParser.wasm', {
      signal: controller.signal,
    });
    clearTimeout(tid);
    if (!resp.ok) return false;

    const bytes = await resp.arrayBuffer();
    const { instance } = await WebAssembly.instantiate(bytes, {});
    const exp = instance.exports;

    // Handle both prefixed (_name) and unprefixed (name) export styles
    const getInputPtr = exp.get_input_ptr || exp._get_input_ptr;
    const getBreaksPtr = exp.get_breaks_ptr || exp._get_breaks_ptr;
    const scanLines = exp.scan_lines || exp._scan_lines;
    const getBreakCount = exp.get_break_count || exp._get_break_count;

    if (!getInputPtr || !getBreaksPtr || !scanLines || !getBreakCount) {
      return false;
    }

    const mem = exp.memory;
    const inputPtr = getInputPtr();
    const breaksPtr = getBreaksPtr();

    wasmInputView = new Uint8Array(mem.buffer, inputPtr, WASM_MAX_INPUT);
    // 2M max breaks as defined in C source
    wasmBreaksView = new Int32Array(mem.buffer, breaksPtr, 2 * 1024 * 1024);

    wasm = { scanLines, getBreakCount, mem };
    return true;
  } catch (_e) {
    return false;
  }
}

// ── Line-Break Scanning ────────────────────────────────────────────────────

/**
 * Find positions of '\n' bytes that are OUTSIDE quoted fields.
 *
 * @param {Uint8Array} bytes
 * @param {number} start
 * @param {number} end           (exclusive)
 * @param {boolean} initQuote    true if scan starts inside quotes
 * @returns {{ breaks: number[], finalQuoteState: boolean }}
 */
function scanLineBreaks(bytes, start, end, initQuote) {
  const len = end - start;

  // ── WASM path ──────────────────────────────────────────────────────────
  if (wasm && len <= WASM_MAX_INPUT) {
    // Copy chunk into WASM linear memory
    wasmInputView.set(bytes.subarray(start, end), 0);

    const finalQ = wasm.scanLines(len, initQuote ? 1 : 0);
    const count = wasm.getBreakCount();

    // Read break positions, adjusting back to original array offsets
    const breaks = new Array(count);
    for (let i = 0; i < count; i++) {
      breaks[i] = start + wasmBreaksView[i];
    }
    return { breaks, finalQuoteState: finalQ === 1 };
  }

  // ── JS fallback — tight integer-only loop ──────────────────────────────
  const breaks = [];
  let inQ = initQuote;

  for (let i = start; i < end; i++) {
    const b = bytes[i];
    if (b === QUOTE) {
      inQ = !inQ;
    } else if (!inQ && b === NEWLINE) {
      breaks.push(i);
    }
  }

  return { breaks, finalQuoteState: inQ };
}

// ── Field Extraction ───────────────────────────────────────────────────────

/**
 * Extract fields from a single CSV line (byte range).
 * Only decodes individual field sub-arrays — NOT the whole line.
 *
 * @param {Uint8Array} bytes
 * @param {number} start   inclusive
 * @param {number} end     exclusive (position of '\n' or buffer end)
 * @param {number} delim   delimiter byte
 * @returns {string[]}
 */
function extractFields(bytes, start, end, delim) {
  // Strip trailing \r
  if (end > start && bytes[end - 1] === CARRIAGE) end--;

  const fields = [];
  let fieldStart = start;
  let inQ = false;

  for (let i = start; i <= end; i++) {
    const atEnd = i === end;
    const b = atEnd ? 0 : bytes[i];

    if (!atEnd && b === QUOTE) {
      inQ = !inQ;
      continue;
    }

    if (atEnd || (!inQ && b === delim)) {
      let fs = fieldStart;
      let fe = i;

      // Trim leading/trailing whitespace bytes
      while (fs < fe && (bytes[fs] === SPACE || bytes[fs] === TAB)) fs++;
      while (fe > fs && (bytes[fe - 1] === SPACE || bytes[fe - 1] === TAB))
        fe--;

      // Handle quoted field (strip outer quotes, unescape "")
      if (fe - fs >= 2 && bytes[fs] === QUOTE && bytes[fe - 1] === QUOTE) {
        const raw = decoder.decode(bytes.subarray(fs + 1, fe - 1));
        fields.push(
          raw.indexOf('""') !== -1 ? raw.replaceAll('""', '"') : raw
        );
      } else {
        fields.push(fs < fe ? decoder.decode(bytes.subarray(fs, fe)) : '');
      }

      fieldStart = i + 1;
    }
  }

  return fields;
}

// ── Type Coercion ──────────────────────────────────────────────────────────

/**
 * Coerce a string value to its JS type (number / boolean / null / string).
 * Uses charCode checks to skip Number() for obviously non-numeric strings.
 */
function coerceValue(val) {
  if (val === '') return val;

  // Boolean / null — only check short strings
  if (val.length <= 5) {
    const low = val.toLowerCase();
    if (low === 'true') return true;
    if (low === 'false') return false;
    if (low === 'null') return null;
  }

  // Fast numeric pre-check: first char must be digit, '-', or '.'
  const c0 = val.charCodeAt(0);
  if ((c0 >= 48 && c0 <= 57) || c0 === 45 || c0 === 46) {
    const n = +val; // unary + is faster than Number()
    if (n === n && isFinite(n)) return n; // n === n ≡ !isNaN(n)
  }

  return val;
}

// ── Chunk Processing ───────────────────────────────────────────────────────

function flushBatch(force) {
  if (pendingRows.length >= BATCH_SIZE || (force && pendingRows.length > 0)) {
    self.postMessage({
      type: 'BATCH',
      rows: pendingRows,
      headers,
      rowCount,
    });
    pendingRows = [];
  }
}

function processLine(bytes, start, end) {
  const fields = extractFields(bytes, start, end, delimByte);

  // First non-empty line becomes the header row
  if (!headers) {
    headers = fields;
    return;
  }

  // Build row object with type coercion
  const row = {};
  const hLen = headers.length;
  const fLen = fields.length;
  for (let i = 0; i < hLen; i++) {
    row[headers[i]] = i < fLen ? coerceValue(fields[i]) : null;
  }

  pendingRows.push(row);
  rowCount++;

  if (pendingRows.length >= BATCH_SIZE) {
    flushBatch(false);
  }
}

function processChunk(chunk, isLast) {
  // ── Combine with leftover from previous chunk ──────────────────────────
  let bytes;
  if (leftover && leftover.length > 0) {
    bytes = new Uint8Array(leftover.length + chunk.length);
    bytes.set(leftover, 0);
    bytes.set(chunk, leftover.length);
    leftover = null;
  } else {
    bytes = chunk;
  }

  if (bytes.length === 0) {
    if (isLast) {
      flushBatch(true);
      self.postMessage({ type: 'DONE', headers, rowCount });
    }
    return;
  }

  // ── Scan for line breaks ───────────────────────────────────────────────
  const { breaks, finalQuoteState } = scanLineBreaks(
    bytes,
    0,
    bytes.length,
    quoteState
  );

  // No complete lines found in this chunk
  if (breaks.length === 0) {
    if (isLast) {
      // Treat remaining bytes as the final line
      if (bytes.length > 0) {
        processLine(bytes, 0, bytes.length);
      }
      flushBatch(true);
      self.postMessage({ type: 'DONE', headers, rowCount });
    } else {
      // Buffer everything for the next chunk
      leftover = bytes;
      quoteState = finalQuoteState;
    }
    return;
  }

  // ── Process each complete line ─────────────────────────────────────────
  let lineStart = 0;
  for (let i = 0; i < breaks.length; i++) {
    const lineEnd = breaks[i];

    // Skip empty lines (\n or \r\n with nothing else)
    const isEmpty =
      lineEnd <= lineStart ||
      (lineEnd === lineStart + 1 && bytes[lineStart] === CARRIAGE);

    if (!isEmpty) {
      processLine(bytes, lineStart, lineEnd);
    }

    lineStart = lineEnd + 1;
  }

  // ── Save leftover (bytes after last line break) ────────────────────────
  if (lineStart < bytes.length) {
    leftover = bytes.slice(lineStart);
    // After the last line break (which is outside quotes by definition),
    // we know the quote state resets to false.
    quoteState = false;
  } else {
    leftover = null;
    quoteState = false;
  }

  // ── Handle final chunk ─────────────────────────────────────────────────
  if (isLast) {
    if (leftover && leftover.length > 0) {
      // Check leftover has real content (not just whitespace / CR)
      let hasContent = false;
      for (let i = 0; i < leftover.length; i++) {
        const b = leftover[i];
        if (b !== CARRIAGE && b !== NEWLINE && b !== SPACE) {
          hasContent = true;
          break;
        }
      }
      if (hasContent) {
        processLine(leftover, 0, leftover.length);
      }
      leftover = null;
    }

    flushBatch(true);
    self.postMessage({ type: 'DONE', headers, rowCount });
  } else {
    flushBatch(false);
  }
}

// ── Message Handler ────────────────────────────────────────────────────────

let stopped = false; // Set by STOP message — skip further chunk processing

self.onmessage = async e => {
  const msg = e.data;

  switch (msg.type) {
    case 'INIT': {
      // Reset state
      headers = null;
      leftover = null;
      quoteState = false;
      rowCount = 0;
      pendingRows = [];
      stopped = false;
      delimByte = (msg.delimiter || ',').charCodeAt(0);

      // Try loading WASM scanner (non-blocking, 3 s timeout)
      const hasWasm = await tryLoadWasm();

      self.postMessage({ type: 'READY', wasm: hasWasm });
      break;
    }

    case 'CHUNK': {
      if (stopped) break; // Main thread hit row cap — discard remaining data
      try {
        processChunk(msg.bytes, msg.isLast);
      } catch (err) {
        self.postMessage({
          type: 'ERROR',
          message: err.message || 'Parse error in streaming worker',
        });
      }
      break;
    }

    case 'STOP': {
      // Main thread reached MAX_ROWS — stop processing and signal completion
      stopped = true;
      leftover = null;
      flushBatch(true);
      self.postMessage({ type: 'DONE', headers, rowCount });
      break;
    }
  }
};
