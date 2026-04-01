import { logger } from './logger.js';
import { defineConfig } from 'vite';
import { resolve } from 'path';
import { copyFileSync, mkdirSync, existsSync } from 'fs';

export default defineConfig({
  worker: {
    format: 'es',
  },
  resolve: {
    alias: {
      '@types': resolve(__dirname, 'src'),
    },
    conditions: ['import', 'module', 'browser', 'default'],
  },
  server: {
    port: 5173,
    strictPort: false,
    fs: {
      strict: false,
    },
  },
  plugins: [
    {
      name: 'copy-wasm-files',
      buildStart() {
        const wasmDir = resolve(__dirname, 'public/wasm');
        if (!existsSync(wasmDir)) {
          mkdirSync(wasmDir, { recursive: true });
        }

        // csvParser.wasm — type detection/number parsing (AssemblyScript, from package)
        const csvParserSrc = resolve(
          __dirname,
          'node_modules/@mindfiredigital/pivothead/dist/wasm/csvParser.wasm'
        );
        const csvParserDest = resolve(__dirname, 'public/wasm/csvParser.wasm');
        if (existsSync(csvParserSrc)) {
          copyFileSync(csvParserSrc, csvParserDest);
          logger.info('✅ Copied csvParser.wasm to public/wasm/');
        } else {
          logger.warn('⚠️ csvParser.wasm not found at:', csvParserSrc);
        }

        // csvStreamParser.wasm — byte-level line scanner (built locally from src/assembly/)
        // Build it first: node scripts/compile-wat.js  OR  ./scripts/build-wasm.sh
        const streamParserDest = resolve(__dirname, 'public/wasm/csvStreamParser.wasm');
        if (!existsSync(streamParserDest)) {
          logger.warn('⚠️ csvStreamParser.wasm not found in public/wasm/.');
          logger.warn('   Build it with: node scripts/compile-wat.js');
          logger.warn('   Streaming worker will fall back to JS line scanner.');
        }
      },
    },
  ],
});
