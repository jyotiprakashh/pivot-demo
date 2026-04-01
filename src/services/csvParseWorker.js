'use strict';
import { splitCSVLine, coerceValue, getWasmLoaderInstance } from './csvParsingUtils.js';

let headers = null;
let leftover = '';
let wasmInstance = null;
let isWasmLoaded = false;

const BATCH_SIZE = 5000; // Smaller batches = less structured-clone overhead per message

async function initializeWasm() {
  if (!isWasmLoaded) {
    wasmInstance = await getWasmLoaderInstance();
    isWasmLoaded = !!wasmInstance;
  }
}

/**
 * Parse a chunk of CSV text using indexOf-based line scanning.
 * This avoids allocating a huge array via text.split(/\r?\n/).
 *
 * Returns an array of row objects parsed from this chunk.
 * Manages `leftover` (partial trailing line) across chunks.
 */
function parseChunkText(text, delimiter, isLastChunk) {
  const combined = leftover + text;
  const rows = [];
  let lineStart = 0;
  let lineEnd;

  // Scan for newlines using indexOf — no array allocation
  while ((lineEnd = combined.indexOf('\n', lineStart)) !== -1) {
    // Handle \r\n line endings
    const end = (lineEnd > 0 && combined[lineEnd - 1] === '\r') ? lineEnd - 1 : lineEnd;
    if (end > lineStart) {
      const line = combined.substring(lineStart, end);
      const fields = splitCSVLine(line, delimiter);

      if (!headers) {
        headers = fields;
      } else {
        const obj = {};
        for (let j = 0; j < headers.length; j++) {
          obj[headers[j]] = coerceValue(
            j < fields.length ? fields[j] : '',
            wasmInstance
          );
        }
        rows.push(obj);
      }
    }
    lineStart = lineEnd + 1;
  }

  // Handle remaining text after the last newline
  if (isLastChunk) {
    if (lineStart < combined.length) {
      const line = combined.substring(lineStart);
      if (line.length > 0) {
        const fields = splitCSVLine(line, delimiter);
        if (!headers) {
          headers = fields;
        } else {
          const obj = {};
          for (let j = 0; j < headers.length; j++) {
            obj[headers[j]] = coerceValue(
              j < fields.length ? fields[j] : '',
              wasmInstance
            );
          }
          rows.push(obj);
        }
      }
    }
    leftover = '';
  } else {
    // Buffer the partial line for the next chunk
    leftover = combined.substring(lineStart);
  }

  return rows;
}

self.onmessage = async function (e) {
  const msg = e.data;

  switch (msg.type) {
    case 'RESET':
      headers = null;
      leftover = '';
      break;

    case 'PARSE_CHUNK':
      await initializeWasm();

      try {
        const rows = parseChunkText(msg.text, msg.delimiter || ',', msg.isLastChunk);

        // Send rows in small batches to reduce structured-clone pressure
        let offset = 0;
        while (offset < rows.length) {
          const batchEnd = Math.min(offset + BATCH_SIZE, rows.length);
          const batch = rows.slice(offset, batchEnd);
          const isLast = msg.isLastChunk && batchEnd >= rows.length;

          self.postMessage({
            type: 'CHUNK_BATCH',
            chunkId: msg.chunkId,
            rows: batch,
            headers: headers ? [...headers] : [],
            isLastBatch: isLast,
          });

          offset = batchEnd;
        }

        // If no rows were produced but this is the last chunk, signal completion
        if (rows.length === 0 && msg.isLastChunk) {
          self.postMessage({
            type: 'CHUNK_DONE',
            chunkId: msg.chunkId,
            headers: headers ? [...headers] : [],
            rowCount: 0,
          });
        }
      } catch (err) {
        self.postMessage({
          type: 'CHUNK_ERROR',
          chunkId: msg.chunkId,
          error: err.message,
        });
      }
      break;
  }
};
