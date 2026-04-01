(module
  ;; ── Memory ──────────────────────────────────────────────────────────────
  ;; 512 pages = 32 MB  (enough for 8 MB input + 8 MB breaks + headroom)
  (memory (export "memory") 512)

  ;; ── Layout constants ────────────────────────────────────────────────────
  ;; input_buf     : offset 0          .. 8 MB   (8388608 bytes)
  ;; break_positions: offset 8388608   .. 16 MB  (2M × 4-byte i32 = 8388608)
  ;; break_count   : offset 16777216   (single i32)

  (global $INPUT_OFFSET     i32 (i32.const 0))
  (global $BREAKS_OFFSET    i32 (i32.const 8388608))
  (global $COUNT_OFFSET     i32 (i32.const 16777216))
  (global $MAX_LINE_BREAKS  i32 (i32.const 2097152))  ;; 2 * 1024 * 1024

  ;; ── get_input_ptr() → i32 ──────────────────────────────────────────────
  (func (export "get_input_ptr") (result i32)
    global.get $INPUT_OFFSET
  )

  ;; ── get_breaks_ptr() → i32 ────────────────────────────────────────────
  (func (export "get_breaks_ptr") (result i32)
    global.get $BREAKS_OFFSET
  )

  ;; ── get_break_count() → i32 ───────────────────────────────────────────
  (func (export "get_break_count") (result i32)
    global.get $COUNT_OFFSET
    i32.load
  )

  ;; ── scan_lines(len: i32, quote_state: i32) → i32 (final quote state) ─
  ;;
  ;; Scans input_buf[0..len) for '\n' (0x0A) bytes outside quoted fields.
  ;; Writes line-break offsets into break_positions[].
  ;; Stores the count at $COUNT_OFFSET.
  ;; Returns 1 if still inside quotes at the end, else 0.
  ;;
  (func (export "scan_lines") (param $len i32) (param $quote_state i32) (result i32)
    (local $i i32)
    (local $count i32)
    (local $in_q i32)
    (local $byte i32)

    ;; Initialise
    (local.set $i (i32.const 0))
    (local.set $count (i32.const 0))
    (local.set $in_q (local.get $quote_state))

    ;; Main scanning loop
    (block $break
      (loop $loop
        ;; if (i >= len) break
        (br_if $break (i32.ge_u (local.get $i) (local.get $len)))

        ;; byte = input_buf[i]
        (local.set $byte
          (i32.load8_u (i32.add (global.get $INPUT_OFFSET) (local.get $i)))
        )

        ;; if (byte == 34)  →  '"'  toggle quote state
        (if (i32.eq (local.get $byte) (i32.const 34))
          (then
            (local.set $in_q
              (i32.xor (local.get $in_q) (i32.const 1))
            )
          )
          (else
            ;; if (!in_q && byte == 10)  →  '\n'  record line break
            (if (i32.and
                  (i32.eqz (local.get $in_q))
                  (i32.eq (local.get $byte) (i32.const 10))
                )
              (then
                ;; if (count < MAX_LINE_BREAKS)
                (if (i32.lt_u (local.get $count) (global.get $MAX_LINE_BREAKS))
                  (then
                    ;; break_positions[count] = i
                    (i32.store
                      (i32.add
                        (global.get $BREAKS_OFFSET)
                        (i32.mul (local.get $count) (i32.const 4))
                      )
                      (local.get $i)
                    )
                    ;; count++
                    (local.set $count (i32.add (local.get $count) (i32.const 1)))
                  )
                )
              )
            )
          )
        )

        ;; i++
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $loop)
      )
    )

    ;; Store break_count
    (i32.store (global.get $COUNT_OFFSET) (local.get $count))

    ;; Return final quote state
    (local.get $in_q)
  )
)
