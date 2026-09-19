# layout_benchmark — five bit-unpacking decoders, one store-only reference

One binary, no arguments. It decodes 43 generated integer columns at six
working-set sizes with five decoders plus a store-only reference, and writes a
human table to stdout and `layout_benchmark.csv` to the current directory.

Portable to x86-64 and aarch64; `build.sh` picks `-march` from `uname -m`.

## Build

Needs a configured and built Arrow in the same checkout (the harness links
`libarrow` and includes headers from it).

```bash
git clone -b pgaur_interleavedPlusFastLanesDelta https://github.com/prtkgaur/arrow.git
cd arrow/cpp && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DARROW_BUILD_TESTS=OFF \
         -DARROW_BUILD_BENCHMARKS=OFF -DARROW_PARQUET=OFF \
         -DARROW_DEPENDENCY_SOURCE=BUNDLED
cmake --build . -j

cd ../../fl5_corpus
ARROW_BUILD=$(pwd)/../cpp/build ./build.sh
```

## Run

```bash
taskset -c 2 ./layout_benchmark > layout_benchmark.txt 2> layout_benchmark.err
echo "exit=$?"
```

Pin it to one core, keep the machine otherwise idle, and give it a few minutes.
Hand back `layout_benchmark.txt`, `layout_benchmark.csv`, `layout_benchmark.err`
and the exit status; the CSV is what generates the tables.

## What it runs

| name | layout | order written |
|---|---|---|
| `seq_scal` | continuous LSB-first, scalar | file |
| `seq_simd` | continuous LSB-first, vectorized | file |
| `intlv` | interleaved 32x32 | file |
| `fl_unpk` | interleaved, FL_ORDER | FL_ORDER |
| `fl_tpos` | interleaved, FL_ORDER, transposed back | file |
| `pure_st` | no unpacking, writes the same bytes | file |

`intlv` and `fl_unpk` run the same kernel over grids that differ only in how
encode filled them, so their speeds should agree. The `unpk/int` column prints
that ratio on every line, and a validity table at the end turns it into a
per-working-set-size decision: the control has to tie in aggregate, four columns
in five have to tie on their own, and dropping the ones that do not must leave
`intlv/seq_simd` where it was. Points that fail are named, so read that table
before quoting any figure. The binary exits non-zero only if no point passes, so
report the exit status too.

Two things about the output to know before comparing runs:

- The header prints which `fl_tpos` implementation was compiled in. The fused
  FL_ORDER-to-file-order kernel is x86-only today; on aarch64 you get the
  `UNFUSED fallback` banner, and any aarch64 `fl_tpos` figure is a lower bound
  that must not be compared against an x86 one as though the same kernel ran.
  `intlv` and `fl_unpk` are portable and carry no such caveat.
- `pure_st` is a store-only reference and not a ceiling. It is regularly slower
  than the decoders writing the same bytes, and it moves 2.07x between gcc and
  clang on fixed hardware, so it bounds its own codegen and nothing else.

Each point names both its input and output footprint, because the decode call is
held at page scale (a Parquet data page is ~1 MiB of encoded bytes, so a 32-MiB
decode call occurs in no reader) while the *stream* is what grows past cache.

Checked in alongside: `fl5_corpus_x86.{txt,csv}`, recorded on x86 by an earlier
version of this harness. Its three points are labelled L1/L2/DRAM by output size
alone and it carries no store-only reference, so its rows do not correspond to the
six points above. Read it for the x86 `fl_tpos` kernel and for the ratios within a
row, not as a working-set comparison against this ladder.
