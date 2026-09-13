# fl5_corpus — 5-arm layout benchmark, portable to aarch64

Two binaries. Build both with `./build.sh`, run both, hand back the four output
files. Nothing here is x86-specific; `build.sh` picks `-march` from `uname -m`.

## Read this first: the working-set ladder is about to change

**Please build and smoke-test now, but hold the full production run.** Two
problems with the current three points were found after this harness was written,
and both are being fixed:

1. **The point labelled `DRAM` is not DRAM.** The x86 reference numbers were
   taken on a Xeon 6975P-C (Granite Rapids) with **2 MiB of L2 per core and
   480 MiB of shared L3**. A 32 MiB working set is comfortably L3-resident, so
   that row measures L3 bandwidth, not memory. Reaching DRAM on that machine
   needs roughly a gigabyte. Wherever you see `DRAM` in output or in the x86
   reference files, read it as **L3**, and do not describe those figures as
   memory-bound in anything you write up.
2. **The three points bracket the realistic range instead of covering it.** A
   120 MB file with 100 columns gives ~1.2 MB per column chunk, and integer
   columns run well under that average — call it 300 KB. Unpacking expands
   packed bytes by `32/W`, so a 300 KB integer chunk decodes to 300 KB at
   *W*=32, 600 KB at *W*=16, 1.2 MB at *W*=8 and 2.4 MB at *W*=4. The realistic
   span is therefore ~300 KB to 2.4 MB, and there is no measurement between
   400 KiB and 32 MiB.

Points near 1.5 MiB, 4 MiB and ≥1 GiB are being added, plus a concurrency sweep
(the single-thread numbers give one core the entire L3 and memory controller,
which a real parallel scan does not). Building and smoke-testing now is still
worth doing — whether this compiles at all on aarch64 is the open risk, and it is
independent of which points get measured. **Smoke test with a single dataset**
(`./fl5_corpus ClientIP /tmp/smoke.csv`) rather than the full corpus, and report
whether it builds, runs, and exits zero.

## Build

The harness now lives in-tree at `<arrow>/fl5_corpus/`, so `build.sh` finds the
checkout itself. Only the build directory needs pointing at:

```bash
export ARROW_BUILD=/path/to/your/arrow/build   # has src/arrow/util/config.h and
                                               # release/libarrow.so
export XSIMD=$ARROW_BUILD/_deps/xsimd-src/include   # optional; this is the default
./build.sh
```

`build.sh` preflights the three headers it needs and fails with a specific
message naming the missing one, rather than emitting a wall of compiler errors.
The checkout must be on the branch carrying
`cpp/src/arrow/util/fastlanes/interleaved_pfor.h` — the harness includes it
directly.

## Run

```bash
# usage: ./fl5_corpus [dataset-filter] [csv-path]
#   filter "all" (or omitted) runs all 43; any other string is a substring match
taskset -c 2 ./fl5_corpus all fl5_corpus_arm.csv > fl5_corpus_arm.txt 2> fl5_corpus_arm.err
taskset -c 2 ./seq_granularity > seq_granularity_arm.txt
echo "exit=$?"
```

One run produces both the human table (stdout) and the CSV. Pin to a core and
keep the machine otherwise idle; the whole thing takes a few minutes.

`fl5_corpus` exits non-zero if the `fl_unpk`-vs-`intlv` identity check fails, so
a clean exit is meaningful — please report the exit status. Its header also
prints which `fl_tpos` implementation was compiled in; on aarch64 expect to see
the `UNFUSED fallback` banner, which confirms caveat 1 below rather than
indicating a build problem.

## What the two binaries are for

`fl5_corpus` runs five arms over 43 generated columns at three working sets
(16 KiB / 400 KiB / 32 MiB of output — labelled L1 / L2 / `DRAM`, but see the
correction at the top: the third is L3):

| arm | layout | order out |
|---|---|---|
| `seq_scal` | continuous LSB-first | file |
| `seq_simd` | continuous LSB-first | file |
| `intlv` | interleaved 32×32 | file |
| `fl_unpk` | interleaved, FL_ORDER | FL_ORDER |
| `fl_tpos` | interleaved, FL_ORDER | file |

`intlv` and `fl_unpk` run identical kernel code against grids that were merely
*filled* differently at encode time, so their speeds must agree — that is the
harness's self-check, and it should read 1.00x.

`seq_granularity` is the calibration probe, and it is **not optional**. On x86 it
showed that calling Arrow's exported `unpack_bias` once per 1024-value block
rather than once per buffer costs **1.27x at L1 and L2** — a handicap the
`seq_simd` arm pays and the header-inlined interleaved arms do not. Without it,
`intlv/seq_simd` reads 1.43x at L2 when the layout-only effect is ~1.13x. The
aarch64 ratios need the same correction with a locally-measured divisor; do not
reuse the x86 1.27x.

## Two things to know before interpreting aarch64 results

1. **`fl_tpos` is the unfused fallback on aarch64.** The fused
   FL_ORDER-to-file-order kernel (`UnpackBlockFlToFileOrder`) is guarded by
   `ARROW_TRANSPOSED_DELTA_AVX2` and depends on the
   `vpunpckldq`/`vperm2i128` ladder. On aarch64 the decoder falls back to
   unpack-into-a-4-KiB-scratch-grid then `Transpose32x32`. On x86 that path
   measures ~21.6 GiB/s against the fused kernel's 31.30, so **any aarch64
   `fl_tpos` number is a lower bound**, and it must not be compared against the
   x86 `fl_tpos` figure as though the same kernel ran. `intlv` and `fl_unpk` are
   fully portable and carry no such caveat.
2. **aarch64 never had the AVX-512 dispatch bug.** Arrow's
   `bpacking_simd_avx512.cc` is 6.26x slower than its AVX2 kernel and 0.67x of
   scalar, and `ARROW_RUNTIME_SIMD_LEVEL` defaults to `MAX`, so x86 machines
   preferred it. That is the whole explanation for "PFOR is 30 GB/s on ARM and
   6 GiB/s on x86" — it is a bug on one side, not an architecture difference.
   The x86 numbers here were taken with that dispatch capped at 256 bits.

## Files to hand back

`fl5_corpus_arm.txt`, `fl5_corpus_arm.csv`, `fl5_corpus_arm.err`,
`seq_granularity_arm.txt`. The `.csv` is what generates the document tables
(`gen_tables.py` consumes it; columns are
`dataset,point,n,avg_bit_width,cr,seq_scal,seq_simd,intlv,fl_unpk,fl_tpos`).

x86 reference outputs for comparison are checked in alongside:
`fl5_corpus_x86.{txt,csv}` and `seq_granularity.txt`.
