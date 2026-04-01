#!/usr/bin/env node
/**
 * Compile csvStreamParser.wat → csvStreamParser.wasm using the wabt npm package.
 * Run: node scripts/compile-wat.js
 */
const fs = require('fs');
const path = require('path');

async function main() {
  const wabt = await require('wabt')();

  const watPath = path.resolve(__dirname, '../src/assembly/csvStreamParser.wat');
  const outDir = path.resolve(__dirname, '../public/wasm');
  const outPath = path.join(outDir, 'csvStreamParser.wasm');

  const watSource = fs.readFileSync(watPath, 'utf8');
  const module = wabt.parseWat('csvStreamParser.wat', watSource);
  module.validate();

  const { buffer } = module.toBinary({ write_debug_names: false });

  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outPath, Buffer.from(buffer));

  console.log(`WASM compiled: ${outPath} (${buffer.byteLength} bytes)`);

  module.destroy();
}

main().catch(err => {
  console.error('Compilation failed:', err.message);
  process.exit(1);
});
