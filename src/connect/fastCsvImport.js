'use strict';
import { logger } from '../../logger.js';
import { appState } from '../../state.js';
import {
  showLoadingIndicator,
  updateLoadingProgress,
  hideLoadingIndicator,
  showImportNotification,
} from './fileConnect.js';
import { ChartEngine } from '@mindfiredigital/pivothead-analytics';
import { Chart } from 'chart.js';
import { initializeFilters, resetFilters } from '../ui/filters.js';
import { initializeAnalyticsTab } from '../chart/chartModule.js';
import CsvParseWorker from '../services/csvParseWorker.js?worker';
import { streamParseCsv } from '../services/streamingCsvPipeline.js';

// ── Constants ────────────────────────────────────────────────────────────────

const MAX_FILE_SIZE = 1024 * 1024 * 1024; // 1 GB hard cap
const MAX_ROWS = 5_000_000; // Safety cap to prevent OOM on main thread
const WASM_STREAM_THRESHOLD = 10 * 1024 * 1024; // 10 MB — above this use WASM+streaming

// Chunk sizes per tier
const SMALL_FILE_LIMIT = 5 * 1024 * 1024; // 5 MB
const MEDIUM_FILE_LIMIT = 50 * 1024 * 1024; // 50 MB
const MEDIUM_CHUNK = 2 * 1024 * 1024; // 2 MB chunks for 5-50 MB files
const LARGE_CHUNK = 8 * 1024 * 1024; // 8 MB chunks for >50 MB files

// ── Helpers ──────────────────────────────────────────────────────────────────

function pickFile(accept) {
  return new Promise(resolve => {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = accept;
    input.onchange = () => resolve(input.files[0] || null);
    input.click();
  });
}

// Cardinality limits for pivot axis auto-detection.
// Row axis: at most this many unique values in the 500-row sample.
// Column axis: stricter — fewer columns = more readable table.
// Numeric fields with fewer unique values than MEASURE_CARDINALITY_MIN are
// treated as categorical dimensions (e.g. fitness_level 1–5, priority 1–3)
// rather than continuous measures.
const MAX_ROW_CARDINALITY    = 100;
const MAX_COLUMN_CARDINALITY = 30;
const MEASURE_CARDINALITY_MIN = 15;

function autoDetectLayout(data, columns) {
  const SAMPLE_SIZE = Math.min(data.length, 500);
  const sample = data.slice(0, SAMPLE_SIZE);

  // Compute unique-value count and numeric flag for each field.
  const fieldInfo = {};
  columns.forEach(col => {
    const vals = sample.map(r => r[col]).filter(v => v != null);
    const isNumeric = vals.length > 0 && vals.every(v => typeof v === 'number');
    fieldInfo[col] = { isNumeric, uniqueCount: new Set(vals).size };
  });

  // Measures: numeric fields with enough variety to be truly continuous.
  // Numeric fields with very few unique values (e.g. fitness_level 1–5) are
  // better used as row/column dimensions than summed as measures.
  const measures = columns.filter(col => {
    const { isNumeric, uniqueCount } = fieldInfo[col];
    return isNumeric && uniqueCount >= MEASURE_CARDINALITY_MIN;
  });
  const measureSet = new Set(measures);

  // Dimensions: everything that isn't a continuous measure.
  const dims = columns.filter(c => !measureSet.has(c));

  // Row axis: first dimension whose cardinality is manageable.
  // High-cardinality string fields like "name" or "body" are useless as pivot
  // rows — they create one group per record and hang the engine.
  let rowField = null;
  for (const d of dims) {
    if (fieldInfo[d].uniqueCount <= MAX_ROW_CARDINALITY) {
      rowField = d;
      break;
    }
  }

  // Column axis: first dimension (different from the row field) with low
  // enough cardinality to render as table columns without flooding the DOM.
  let columnField = null;
  for (const d of dims) {
    if (d !== rowField && fieldInfo[d].uniqueCount <= MAX_COLUMN_CARDINALITY) {
      columnField = d;
      break;
    }
  }

  return {
    rows: rowField
      ? [{ uniqueName: rowField, caption: rowField }]
      : [{ uniqueName: '__all__', caption: 'All' }],
    columns: columnField
      ? [{ uniqueName: columnField, caption: columnField }]
      : [{ uniqueName: '__all__', caption: 'All' }],
    measures: measures.slice(0, 3).map(f => ({
      uniqueName: f,
      caption: f,
      aggregation: 'sum',
    })),
  };
}

/** Yield to the browser so the UI stays responsive. */
function yieldToMain() {
  return new Promise(resolve => setTimeout(resolve, 0));
}

// ═════════════════════════════════════════════════════════════════════════════
// Unified Worker-Based Parsing
//
// ALL tiers parse inside a Web Worker to keep the main thread free.
// The only difference is how the file is read and sent to the worker:
//
//   <= 5 MB   → Read whole file, send as single chunk (Web Worker)
//   5 – 50 MB → Read whole file, send in 2 MB text chunks (WASM in Worker)
//   > 50 MB   → Stream 8 MB blob slices, send each to Worker (WASM + Streaming)
// ═════════════════════════════════════════════════════════════════════════════

/**
 * Sends the file content to the worker in chunks appropriate for the file size.
 * The worker handles parsing, WASM coercion, and batching results back.
 */
async function sendFileToWorker(file, worker, delimiter, onProgress) {
  const size = file.size;

  if (size <= SMALL_FILE_LIMIT) {
    // ── TIER 1: Small file — single chunk ────────────────────────────────
    onProgress(10);
    const text = await file.text();
    onProgress(30);
    worker.postMessage({
      type: 'PARSE_CHUNK',
      chunkId: 0,
      text,
      delimiter,
      isFirstChunk: true,
      isLastChunk: true,
    });
  } else {
    // ── TIER 2 & 3: Chunked reading ─────────────────────────────────────
    const chunkSize = size <= MEDIUM_FILE_LIMIT ? MEDIUM_CHUNK : LARGE_CHUNK;
    const totalChunks = Math.ceil(size / chunkSize);

    for (let i = 0; i < totalChunks; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, size);
      const chunkText = await file.slice(start, end).text();

      worker.postMessage({
        type: 'PARSE_CHUNK',
        chunkId: i,
        text: chunkText,
        delimiter,
        isFirstChunk: i === 0,
        isLastChunk: i === totalChunks - 1,
      });

      // Report reading progress (0–50% of total progress)
      onProgress(Math.round(((i + 1) / totalChunks) * 50));
    }
  }
}

/**
 * Core parsing function used by all tiers.
 * Spawns a Web Worker, sends file data in chunks, and accumulates results.
 */
function parseInWorker(file, delimiter, onProgress, tier) {
  return new Promise((resolve, reject) => {
    const worker = new CsvParseWorker();
    const allRows = [];
    let finalHeaders = null;
    let rowCapReached = false;

    worker.onmessage = e => {
      const msg = e.data;

      if (msg.type === 'CHUNK_BATCH') {
        if (!rowCapReached) {
          const remaining = MAX_ROWS - allRows.length;
          const rowsToAdd = msg.rows.length <= remaining ? msg.rows : msg.rows.slice(0, remaining);

          // Safe accumulation — avoid spread operator which has call-stack limits
          for (let i = 0; i < rowsToAdd.length; i++) {
            allRows.push(rowsToAdd[i]);
          }

          if (allRows.length >= MAX_ROWS) {
            rowCapReached = true;
            logger.warn(`Row cap reached (${MAX_ROWS.toLocaleString()}). Remaining rows skipped.`);
          }
        }

        finalHeaders = msg.headers;

        // Report parsing progress (50–95% of total)
        const estimatedRows = Math.max(1, file.size / 100);
        onProgress(50 + Math.min(45, (allRows.length / estimatedRows) * 45));

        if (msg.isLastBatch) {
          onProgress(100);
          worker.terminate();
          resolve({ rows: allRows, headers: finalHeaders, rowCapReached });
        }
      } else if (msg.type === 'CHUNK_DONE') {
        onProgress(100);
        worker.terminate();
        resolve({ rows: allRows, headers: finalHeaders, rowCapReached });
      } else if (msg.type === 'CHUNK_ERROR') {
        worker.terminate();
        reject(new Error(msg.error));
      }
    };

    worker.onerror = err => {
      worker.terminate();
      reject(new Error(err.message || 'Worker error'));
    };

    // Reset worker state, then begin sending file data
    worker.postMessage({ type: 'RESET' });
    sendFileToWorker(file, worker, delimiter, onProgress).catch(err => {
      worker.terminate();
      reject(err);
    });
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// Raw Data Preview — shown below processed table after CSV import
// ═════════════════════════════════════════════════════════════════════════════

function renderRawDataPreview(data, headers) {
  const existing = document.getElementById('raw-data-preview');
  if (existing) existing.remove();

  const myTable = document.getElementById('myTable');
  if (!myTable) return;

  const rawSection = document.createElement('div');
  rawSection.id = 'raw-data-preview';
  rawSection.style.cssText = 'margin-top: 24px;';

  // Section header
  const sectionHeader = document.createElement('div');
  sectionHeader.style.cssText = `
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 16px; background: linear-gradient(180deg, #1e293b, #0f172a);
    color: #f1f5f9; border-radius: 8px 8px 0 0; font-family: Inter, sans-serif;
  `;
  sectionHeader.innerHTML = `
    <div style="display:flex;align-items:center;gap:8px;">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <rect x="3" y="3" width="18" height="18" rx="2"/>
        <path d="M3 9h18M9 21V9"/>
      </svg>
      <strong>Raw Data</strong>
      <span style="font-size:12px;color:#94a3b8;margin-left:8px;">
        ${data.length.toLocaleString()} rows &middot; ${headers.length} columns
      </span>
    </div>
  `;
  rawSection.appendChild(sectionHeader);

  // Table wrapper with scroll
  const wrapper = document.createElement('div');
  wrapper.style.cssText = `
    max-height: 400px; overflow: auto; border: 1px solid #e2e8f0;
    border-top: none; border-radius: 0 0 8px 8px; background: #fff;
  `;

  const table = document.createElement('table');
  table.style.cssText = `
    width: 100%; border-collapse: collapse; font-size: 13px;
    font-family: Inter, -apple-system, BlinkMacSystemFont, sans-serif;
  `;

  // Use DocumentFragment to avoid repeated reflows
  const fragment = document.createDocumentFragment();

  // Header row
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  headers.forEach(h => {
    const th = document.createElement('th');
    th.textContent = h.charAt(0).toUpperCase() + h.slice(1);
    th.style.cssText = `
      padding: 10px 14px; background: #f8fafc; border-bottom: 2px solid #e2e8f0;
      border-right: 1px solid #e2e8f0; text-align: left; font-weight: 600;
      color: #475569; position: sticky; top: 0; z-index: 1; white-space: nowrap;
    `;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  // Body — limit to 50 rows for fast rendering
  const tbody = document.createElement('tbody');
  const previewCount = Math.min(50, data.length);

  for (let i = 0; i < previewCount; i++) {
    const row = data[i];
    const tr = document.createElement('tr');
    if (i % 2 !== 0) tr.style.background = '#f8fafc';

    headers.forEach(h => {
      const td = document.createElement('td');
      const val = row[h];
      td.textContent =
        val === null || val === undefined
          ? ''
          : typeof val === 'number'
            ? String(val)
            : String(val);
      td.style.cssText = `
        padding: 8px 14px; border-bottom: 1px solid #f1f5f9;
        border-right: 1px solid #f1f5f9; color: #334155; white-space: nowrap;
        max-width: 300px; overflow: hidden; text-overflow: ellipsis;
      `;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  }

  if (data.length > previewCount) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = headers.length;
    td.style.cssText =
      'padding: 12px; text-align: center; color: #94a3b8; font-style: italic; background: #fafbfc;';
    td.textContent = `Showing ${previewCount} of ${data.length.toLocaleString()} rows. Switch to Raw Data view for full data with virtual scrolling.`;
    tr.appendChild(td);
    tbody.appendChild(tr);
  }

  table.appendChild(tbody);
  fragment.appendChild(table);
  wrapper.appendChild(fragment);
  rawSection.appendChild(wrapper);

  // Insert after #myTable — single DOM insertion
  myTable.parentNode.insertBefore(rawSection, myTable.nextSibling);
}

// ═════════════════════════════════════════════════════════════════════════════
// Public API
// ═════════════════════════════════════════════════════════════════════════════

/**
 * Fast CSV import with tiered parsing strategy.
 * All tiers use a Web Worker for off-main-thread parsing with WASM coercion.
 *
 *   <= 5 MB   → Worker (single chunk)
 *   5 – 50 MB → Worker (2 MB text chunks with WASM)
 *   > 50 MB   → Worker (8 MB streaming blob slices with WASM)
 *
 * Max file size: 1 GB
 */
export async function fastCsvImport() {
  const file = await pickFile('.csv,.txt');
  if (!file) return;

  // Validate file size
  if (file.size > MAX_FILE_SIZE) {
    showImportNotification(
      {
        error: `File size (${(file.size / (1024 * 1024)).toFixed(0)} MB) exceeds the 1 GB limit.`,
      },
      false
    );
    return;
  }

  const delimiter = ',';
  const sizeMB = file.size / (1024 * 1024);
  console.log('WASM_STREAM_THRESHOLD:', WASM_STREAM_THRESHOLD);
  console.log('File size:', sizeMB);
  const useWasmStream = file.size > WASM_STREAM_THRESHOLD;
  const tier = useWasmStream ? 'wasm-streaming' : 'worker';
  console.log('Tier:', tier);

  logger.info(
    `Fast CSV import: ${file.name} (${sizeMB.toFixed(1)} MB) — strategy: ${tier}`
  );

  showLoadingIndicator(`Importing ${file.name} (${sizeMB.toFixed(1)} MB)...`);
  const t0 = performance.now();

  try {
    const parsed = useWasmStream
      ? await streamParseCsv(file, delimiter, updateLoadingProgress)
      : await parseInWorker(file, delimiter, updateLoadingProgress, tier);

    const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
    logger.info(
      `Parsed ${parsed.rows.length.toLocaleString()} rows in ${elapsed}s (${tier})${parsed.rowCapReached ? ' [CAPPED]' : ''}`
    );

    if (!parsed.rows.length) {
      hideLoadingIndicator();
      showImportNotification({ error: 'No data rows found in file.' }, false);
      return;
    }

    // ── Feed data into PivotEngine ───────────────────────────────────────────
    const layout = autoDetectLayout(parsed.rows, parsed.headers);

    appState.currentData = parsed.rows;
    appState.rawDataColumnOrder = null;
    appState.pagination.currentPage = 1;
    appState.currentViewMode = 'processed';

    // Update the switch button text
    const switchButton = document.getElementById('switchView');
    if (switchButton) {
      switchButton.textContent = 'Switch to Raw Data';
    }

    if (typeof appState.onFormatTable === 'function') {
      // Pass parsed data directly to formatTable — avoids the expensive
      // updateDataSource() call which would process all rows a second time
      // and double memory usage on large files.
      appState.onFormatTable(
        {
          rows: layout.rows,
          columns: layout.columns,
          measures: layout.measures,
        },
        parsed.rows
      );

      await yieldToMain();
    }

    // Yield before rendering raw preview
    await yieldToMain();

    // Show raw data table below processed table
    renderRawDataPreview(parsed.rows, parsed.headers);

    // Yield before chart engine init
    await yieldToMain();

    // Re-initialise chart engine
    if (appState.chartEngine) {
      appState.chartEngine.dispose();
    }
    appState.chartEngine = new ChartEngine(appState.pivotEngine, {
      chartInstance: Chart,
      defaultStyle: { colorScheme: appState.currentPalette },
    });
    appState.chartService = appState.chartEngine.getChartService();
    appState.analyticsTabInitialized = false;

    const analyticsTab = document.getElementById('analytics-tab');
    if (analyticsTab && analyticsTab.classList.contains('active')) {
      initializeAnalyticsTab();
    }

    resetFilters();
    initializeFilters();

    hideLoadingIndicator();

    const notificationResult = {
      success: true,
      fileName: file.name,
      fileSize: file.size,
      recordCount: parsed.rows.length,
      columns: parsed.headers,
      parseTime: performance.now() - t0,
      performanceMode: useWasmStream
        ? `wasm-streaming${parsed.wasmUsed ? ' (WASM)' : ' (JS fallback)'}`
        : tier,
    };

    if (parsed.rowCapReached) {
      notificationResult.validationErrors = [
        `File contained more than ${MAX_ROWS.toLocaleString()} rows. Only the first ${MAX_ROWS.toLocaleString()} rows were loaded.`,
      ];
    }

    showImportNotification(notificationResult, true);
  } catch (error) {
    hideLoadingIndicator();
    logger.error('Fast CSV import failed:', error);
    showImportNotification(
      { success: false, error: error.message || 'Unknown error' },
      false
    );
  }
}
