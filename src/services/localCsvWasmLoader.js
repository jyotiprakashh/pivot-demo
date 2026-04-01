'use strict';
/**
 * localCsvWasmLoader.js
 *
 * Standalone replacement for the package's WasmLoader.
 * Loads /wasm/csvParser.wasm directly using @assemblyscript/loader —
 * no import from @mindfiredigital/pivothead required.
 *
 * Provides the same minimal surface that csvParsingUtils.js needs:
 *   detectFieldType(val: string) → number  (0=str 1=num 2=bool 3=null)
 *   parseNumber(val: string)     → number
 *   isModuleLoaded()             → boolean
 */

const WASM_URL = '/wasm/csvParser.wasm';

let _mod = null;      // AssemblyScript instantiated module
let _loaded = false;
let _loadPromise = null;

// ── Loader ────────────────────────────────────────────────────────────────────

async function _loadModule() {
  // Dynamic import keeps @assemblyscript/loader out of the main bundle until needed
  const { instantiate } = await import('@assemblyscript/loader');

  let wasmBinary;
  try {
    const resp = await fetch(WASM_URL);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    wasmBinary = await resp.arrayBuffer();
  } catch (err) {
    throw new Error(`Failed to fetch ${WASM_URL}: ${err.message}`);
  }

  // Validate WASM magic bytes (0x00 0x61 0x73 0x6D)
  const header = new Uint8Array(wasmBinary, 0, 4);
  if (header[0] !== 0x00 || header[1] !== 0x61 || header[2] !== 0x73 || header[3] !== 0x6d) {
    throw new Error(`Invalid WASM file at ${WASM_URL}`);
  }

  _mod = await instantiate(wasmBinary, {
    env: {
      abort(_msg, _file, line, col) {
        console.warn(`[csvParser.wasm] abort at ${line}:${col}`);
      },
    },
  });

  _loaded = true;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Initialise once.  Safe to call multiple times — returns the same promise.
 * Returns true on success, false if WASM is unavailable or failed.
 */
export async function loadLocalCsvWasm() {
  if (_loaded) return true;
  if (_loadPromise) return _loadPromise.then(() => _loaded).catch(() => false);

  _loadPromise = _loadModule();
  try {
    await _loadPromise;
    return true;
  } catch (err) {
    console.warn('[localCsvWasmLoader] WASM load failed, JS fallback active:', err.message);
    _loaded = false;
    _loadPromise = null;
    return false;
  }
}

/**
 * Whether the module is loaded and ready.
 */
export function isModuleLoaded() {
  return _loaded;
}

/**
 * detectFieldType(val) → 0=string 1=number 2=boolean 3=null
 * Mirrors WasmLoader.detectFieldType()
 */
export function detectFieldType(val) {
  if (!_loaded || !_mod) throw new Error('WASM not loaded');
  const ptr = _mod.exports.__newString(val);
  return _mod.exports.detectFieldType(ptr);
}

/**
 * parseNumber(val) → number (NaN if not a number)
 * Mirrors WasmLoader.parseNumber()
 */
export function parseNumber(val) {
  if (!_loaded || !_mod) throw new Error('WASM not loaded');
  const ptr = _mod.exports.__newString(val);
  return _mod.exports.parseNumber(ptr);
}

/**
 * Returns an adapter object with the same duck-type surface as the package's
 * WasmLoader instance, so callers don't need to change their coercion code.
 */
export function getLocalWasmAdapter() {
  if (!_loaded) return null;
  return {
    isModuleLoaded,
    detectFieldType,
    parseNumber,
  };
}
