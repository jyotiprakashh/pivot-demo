/**
 * Logger for simple-js-demo.
 * Uses Winston when running in Node.js (vite build). Falls back to
 * structured console logging in the browser.
 * Control log level via LOG_LEVEL env var (default: "info").
 */

const LABEL = 'pivothead-simple-js-demo';

const LEVEL_ORDER = { error: 0, warn: 1, info: 2, debug: 3 };

function resolveLevel() {
  const envLevel =
    typeof process !== 'undefined' ? (process.env.LOG_LEVEL ?? 'info') : 'info';
  return LEVEL_ORDER[envLevel] ?? LEVEL_ORDER['info'];
}

function buildConsoleLogger() {
  const maxLevel = resolveLevel();
  const log = (method, lvl, msg, ...args) => {
    if ((LEVEL_ORDER[lvl] ?? 99) <= maxLevel) {
      method(`[${LABEL}] ${lvl.toUpperCase()}: ${msg}`, ...args);
    }
  };
  return {
    error: (msg, ...args) =>
      log(console.error.bind(console), 'error', msg, ...args),
    warn: (msg, ...args) =>
      log(console.warn.bind(console), 'warn', msg, ...args),
    info: (msg, ...args) =>
      log(console.info.bind(console), 'info', msg, ...args),
    debug: (msg, ...args) =>
      log(console.debug.bind(console), 'debug', msg, ...args),
  };
}

export const logger = buildConsoleLogger();
