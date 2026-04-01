#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Build the high-performance CSV line-scanner WASM module.
#
# Prerequisites (install one):
#   • Emscripten  — https://emscripten.org/docs/getting_started/
#   • Clang 15+   — with wasm32 target (apt install clang lld)
#
# Usage:
#   chmod +x scripts/build-wasm.sh
#   ./scripts/build-wasm.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

SRC="$PROJECT_DIR/src/assembly/csvStreamParser.c"
OUT_DIR="$PROJECT_DIR/public/wasm"
OUT="$OUT_DIR/csvStreamParser.wasm"

mkdir -p "$OUT_DIR"

if command -v emcc &>/dev/null; then
    echo "Building WASM with Emscripten..."
    emcc "$SRC" -O3 \
        -s STANDALONE_WASM=1 \
        -s INITIAL_MEMORY=33554432 \
        --no-entry \
        -s "EXPORTED_FUNCTIONS=['_get_input_ptr','_get_breaks_ptr','_scan_lines','_get_break_count']" \
        -o "$OUT"

elif command -v clang &>/dev/null; then
    echo "Building WASM with Clang..."
    clang --target=wasm32 -O3 -nostdlib \
        -Wl,--no-entry \
        -Wl,--export=get_input_ptr \
        -Wl,--export=get_breaks_ptr \
        -Wl,--export=scan_lines \
        -Wl,--export=get_break_count \
        -Wl,--initial-memory=33554432 \
        -o "$OUT" \
        "$SRC"
else
    echo "ERROR: Neither emcc (Emscripten) nor clang found."
    echo ""
    echo "Install one of:"
    echo "  Emscripten → https://emscripten.org/docs/getting_started/"
    echo "  Clang      → sudo apt install clang lld"
    exit 1
fi

BYTES=$(wc -c < "$OUT")
echo "WASM built successfully: $OUT ($BYTES bytes)"
