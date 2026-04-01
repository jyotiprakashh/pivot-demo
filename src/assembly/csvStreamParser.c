/**
 * High-Performance CSV Line Scanner — WebAssembly Module
 *
 * Scans raw byte arrays for line-break positions while correctly tracking
 * quoted fields.  This is the critical hot-loop that benefits most from
 * WASM: a tight integer-only scan over linear memory with no allocations.
 *
 * ─── Build with Emscripten ──────────────────────────────────────────────
 *   emcc src/assembly/csvStreamParser.c -O3          \
 *       -s STANDALONE_WASM=1                         \
 *       -s INITIAL_MEMORY=33554432                   \
 *       --no-entry                                   \
 *       -s "EXPORTED_FUNCTIONS=[                     \
 *            '_get_input_ptr',                       \
 *            '_get_breaks_ptr',                      \
 *            '_scan_lines',                          \
 *            '_get_break_count'                      \
 *          ]"                                        \
 *       -o public/wasm/csvStreamParser.wasm
 *
 * ─── Build with Clang (wasm32 target) ───────────────────────────────────
 *   clang --target=wasm32 -O3 -nostdlib              \
 *       -Wl,--no-entry                               \
 *       -Wl,--export=get_input_ptr                   \
 *       -Wl,--export=get_breaks_ptr                  \
 *       -Wl,--export=scan_lines                      \
 *       -Wl,--export=get_break_count                 \
 *       -Wl,--initial-memory=33554432                \
 *       -o public/wasm/csvStreamParser.wasm          \
 *       src/assembly/csvStreamParser.c
 *
 * ─── Memory layout ─────────────────────────────────────────────────────
 *   input_buf        : 8 MB   — JS copies chunk bytes here before scan
 *   break_positions  : 8 MB   — scan_lines writes line-break offsets here
 *   break_count      : 4 B    — number of breaks found by last scan
 *
 * ─── JS usage ───────────────────────────────────────────────────────────
 *   const inputPtr  = exports.get_input_ptr();
 *   const breaksPtr = exports.get_breaks_ptr();
 *   const mem       = exports.memory;
 *
 *   new Uint8Array(mem.buffer, inputPtr).set(chunkBytes, 0);
 *   const finalQ = exports.scan_lines(chunkBytes.length, quoteState);
 *   const count  = exports.get_break_count();
 *   const breaks = new Int32Array(mem.buffer, breaksPtr, count);
 */

#define INPUT_BUF_SIZE   (8 * 1024 * 1024)   /* 8 MB max chunk        */
#define MAX_LINE_BREAKS  (2 * 1024 * 1024)   /* 2 M lines per chunk   */

#define CHAR_QUOTE   34   /* '"'  */
#define CHAR_NEWLINE 10   /* '\n' */

/* ── Fixed buffers in linear memory ──────────────────────────────────── */

static unsigned char input_buf[INPUT_BUF_SIZE];
static int           break_positions[MAX_LINE_BREAKS];
static int           break_count = 0;

/* ── Exported accessors ──────────────────────────────────────────────── */

/**
 * Pointer to the input buffer.
 * JS copies raw chunk bytes here before calling scan_lines().
 */
unsigned char *get_input_ptr(void) {
    return input_buf;
}

/**
 * Pointer to the results array.
 * After scan_lines() returns, JS reads break offsets from here.
 */
int *get_breaks_ptr(void) {
    return break_positions;
}

/**
 * Number of line breaks found by the last scan_lines() call.
 */
int get_break_count(void) {
    return break_count;
}

/* ── Core scanner ────────────────────────────────────────────────────── */

/**
 * Scan input_buf[0..len) for '\n' bytes that are outside quoted fields.
 *
 * @param  len          Bytes to scan (must be <= INPUT_BUF_SIZE).
 * @param  quote_state  1 if the scan starts inside a quoted field, else 0.
 * @return              Final quote state: 1 = still in quotes, 0 = not.
 *
 * Results are written to break_positions[0..break_count).
 */
int scan_lines(int len, int quote_state) {
    int count = 0;
    int in_q  = quote_state;

    for (int i = 0; i < len; i++) {
        unsigned char c = input_buf[i];

        if (c == CHAR_QUOTE) {
            in_q = !in_q;
        } else if (!in_q && c == CHAR_NEWLINE) {
            if (count < MAX_LINE_BREAKS) {
                break_positions[count++] = i;
            }
        }
    }

    break_count = count;
    return in_q;
}
