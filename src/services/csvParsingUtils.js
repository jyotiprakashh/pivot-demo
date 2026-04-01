
import { logger } from '../../logger.js';
import {
  loadLocalCsvWasm,
  isModuleLoaded,
  getLocalWasmAdapter,
} from './localCsvWasmLoader.js';

let _wasmAdapter = null;
let _wasmReady = false;

/**
 * Initializes and returns the WASM loader singleton.
 * Loads csvParser.wasm directly via @assemblyscript/loader —
 * no dependency on @mindfiredigital/pivothead for WASM.
 * Falls back to pure-JS coercion if WASM is unavailable.
 */
export async function getWasmLoaderInstance() {
  if (_wasmReady) return _wasmAdapter;
  try {
    const ok = await loadLocalCsvWasm();
    _wasmReady = ok && isModuleLoaded();
    _wasmAdapter = _wasmReady ? getLocalWasmAdapter() : null;
    if (_wasmReady) {
      logger.info('WASM loaded in utility (local loader, no package dependency)');
    }
  } catch (err) {
    logger.warn('WASM load failed in utility, will use JS fallback:', err.message);
    _wasmReady = false;
    _wasmAdapter = null;
  }
  return _wasmAdapter;
}

/**
 * Splits a CSV line into fields using index tracking and substring extraction.
 * Avoids character-by-character string concatenation which causes GC pressure.
 *
 * @param {string} line The CSV line to split.
 * @param {string} delimiter The field delimiter.
 * @returns {string[]} An array of trimmed field strings.
 */
export function splitCSVLine(line, delimiter) {
  const fields = [];
  const len = line.length;
  let i = 0;

  while (i <= len) {
    let fieldStart = i;
    let fieldEnd;

    if (i < len && line[i] === '"') {
      // Quoted field — find the closing quote
      i++; // skip opening quote
      const parts = [];
      let segStart = i;

      while (i < len) {
        if (line[i] === '"') {
          if (i + 1 < len && line[i + 1] === '"') {
            // Escaped quote — capture segment up to here, skip the pair
            parts.push(line.substring(segStart, i));
            parts.push('"');
            i += 2;
            segStart = i;
          } else {
            // Closing quote
            parts.push(line.substring(segStart, i));
            i++; // skip closing quote
            break;
          }
        } else {
          i++;
        }
      }

      // Skip to delimiter or end
      while (i < len && line[i] !== delimiter) i++;
      fields.push(parts.join('').trim());
    } else {
      // Unquoted field — scan to next delimiter
      while (i < len && line[i] !== delimiter) i++;
      fields.push(line.substring(fieldStart, i).trim());
    }

    i++; // skip delimiter
  }

  return fields;
}

/**
 * Coerces a string value to its appropriate type (number, boolean, null, or string).
 * Uses WASM for faster number/type detection if available.
 *
 * @param {string} val The string value to coerce.
 * @param {object | null} wasm The WASM loader instance, if available.
 * @returns {string | number | boolean | null} The coerced value.
 */
export function coerceValue(val, wasm) {
  if (val === '') return val; // Keep empty strings as empty strings

  if (wasm) {
    // Use WASM for faster type detection and number parsing
    const t = wasm.detectFieldType(val);
    if (t === 1) return wasm.parseNumber(val); // number
    if (t === 2) return val === 'true' || val === 'TRUE'; // boolean
    if (t === 3) return null; // null / empty (as per WASM's definition)
    return val; // string
  }

  // Pure JS fallback if WASM is not available or failed to load
  if (val === 'true') return true;
  if (val === 'false') return false;
  if (val === 'null' || val === 'NULL') return null;
  const n = Number(val);
  if (val !== '' && !isNaN(n) && isFinite(n)) return n;
  return val;
}
