# PFOR layout re-run: build and run instructions

The first run of this sweep could not answer the layout question. Two defects in
what you were handed caused that; both are fixed in this branch.

## What was wrong, and why the numbers came out the way they did

**1. The candidate kernel was never compiled.** The fused FL_ORDER-to-file-order
transpose — `UnpackBlockFlToFileOrder` in
`arrow/util/fastlanes/interleaved_pfor.h` — is hand-written `__m256i`
intrinsics behind this gate chain:

```
transposed_delta.h:82   #if defined(__AVX2__)
                        #define ARROW_TRANSPOSED_DELTA_AVX2 1
interleaved_pfor.h:235  #define ARROW_FASTLANES_FUSED_FL_UNPACK 1   (inside that block)
```

`-DARROW_SIMD_LEVEL` defaults to `SSE4_2` on x86. At that level `__AVX2__` is
never defined, so `ARROW_FASTLANES_FUSED_FL_UNPACK` does not exist, and the
decoder takes a different path entirely: it materialises a 4 KiB
`uint32_t scratch[1024]` grid, then calls `Transpose32x32(scratch, out)` to read
it back in file order. That is 128 extra vector stores plus 128 extra vector
loads per 1024-value block that the fused kernel does entirely in registers.

So the ~0.55x you measured came from the scratch fallback rather than from the
fused kernel. Reproduced here: at your footprint the fallback costs 2.35x and
the fused kernel 1.08x, so your 0.551 sits in the fallback regime.

`-DARROW_SIMD_LEVEL=AVX2` fixes it. Verified in
`cmake_modules/SetupCxxFlags.cmake:505-511`: that level appends
`-march=haswell -mavx2` to `CXX_COMMON_FLAGS` for the whole build, which is what
defines `__AVX2__` and switches the fused kernel on. Without it the fused kernel
is not in the binary at all, so treat the flag as required rather than as tuning.

The same flag independently un-handicaps the *grid unpack*, which is a separate
issue with the same cause: `fastlanes::UnpackBlock` contains zero intrinsics and
has no dispatch table (`MakeUnpackTable` in `util/pfor/pfor.cc` is indexed by bit
width 1..32, with no CPU-feature dimension), so its register width is frozen at
compile time and `ARROW_USER_SIMD_LEVEL` cannot reach it. The sequential decoder
it is compared against *does* dispatch at runtime, all the way to AVX2. A
default build therefore races a 256-bit sequential kernel against a 128-bit grid
kernel and reports the ratio as a layout result.

**2. One control was missing, and one working-set size is not enough.** The next
section covers both. These bear on what the run can conclude at all, not only on
the speeds it reports.

## The four benchmarks, and why all four are required

| name | benchmark | what it is |
|---|---|---|
| `seq` | `BM_PforPlainSeqDecode` | continuous layout, delta declined — **the baseline** |
| `intlv` | `BM_InterleavedPforDecode` | grid filled in file order — **control, unshippable** |
| `fl_unpk` | `BM_InterleavedPforFlOrderRawDecode` | grid filled the paper's way, handed back unpermuted — **control, violates Parquet's positional contract** |
| `fl_tpos` | `BM_InterleavedPforFlOrderDecode` | same grid, permuted back to file order by the fused transpose — **the candidate, and the only ratio that answers the question** |

`fl_unpk` was missing from the first run, and without it a flat `fl_tpos/seq` is
ambiguous: it can mean the grid's bit-unpacking is no cheaper, or that it is much
cheaper and the permutation spends the whole win. Those two want different fixes,
and the candidate's number alone cannot tell them apart.

At 16 KiB here the two separate: the grid wins 1.75x and the permutation costs
1.77x. Neither of those is visible without `fl_unpk`.

The comparison now also sweeps five working-set sizes (`LayoutArgs` in
`pfor_comparison_benchmark.cc`) instead of the single 400 KiB point:

| values | decoded output | why this point |
|---|---|---|
| 4096 | 16 KiB | L1-resident — where the grid's compute advantage is visible at all |
| 102400 | 400 KiB | the historical point; low end of a realistic column chunk |
| 393216 | 1.5 MiB | still inside a 2 MiB L2; top of realistic |
| 1048576 | 4 MiB | spilled L2 |
| 8388608 | 32 MiB | L3-resident, deliberately not DRAM |

The grid's advantage is a compute effect, so whether it converts to time depends
on whether stores are the limit at a given footprint. Here it measures 1.76x at
16 KiB and much less at the larger points, but do not read that fall-off as a
residency result yet, and do not treat it as something your run is expected to
reproduce. This harness shares one process-wide output buffer but still lets
each benchmark allocate its own encoded input, so input-address-mod-4096 varies
between benchmarks and with footprint (the allocator places larger requests
differently). A later harness on this branch that also carves inputs out of a
shared arena — `fl5_corpus/layout_benchmark.cpp` — traced a 1.36x-to-0.47x swing
between two adjacent bit widths, at its largest footprint only, to exactly that.
What the five points buy is the shape across footprints; whether the shape is
real is part of what is being asked, and a single mid-size point cannot show it
either way.

## Build

Two builds, one per register width. **The AVX2 one is the one that matters** —
do it first and stop there if you only have time for one.

```bash
git fetch origin
git checkout pgaur_interleavedPlusFastLanesDelta
git pull

for LEVEL in AVX2 SSE4_2; do
  cmake -S cpp -B build-x86-$LEVEL \
    -DCMAKE_BUILD_TYPE=Release \
    -DARROW_SIMD_LEVEL=$LEVEL \
    -DARROW_PARQUET=ON \
    -DARROW_BUILD_BENCHMARKS=ON \
    -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON \
    -DARROW_BUILD_TESTS=OFF
  cmake --build build-x86-$LEVEL --target parquet-pfor-comparison-benchmark -j
done
```

Please run the `SSE4_2` leg too. It is the control that shows how much of the
result is register width rather than layout, and it is also what a
default-configured Arrow ships today. Expect the candidate to collapse in it;
that reading is wanted, so send it either way.

## Verify the binary before spending time running it

One second, and it is what catches a build where the flag did not take.

```bash
nm -C build-x86-AVX2/release/parquet-pfor-comparison-benchmark \
  | grep -c UnpackBlockFlToFileOrder
```

- **non-zero** → the fused candidate kernel is in the binary. Good.
- **zero** → `ARROW_SIMD_LEVEL` did not take. Do not run it; the candidate will
  silently measure the scratch fallback again.

The sweep script also emits a per-binary `objdump` register census into
`machine.txt`, so the tarball carries the register width as data. For reference,
what a correct AVX2 build looks like here:

```
InterleavedPforDecode:        ymm=2543  zmm=0
UnpackBlockFlToFileOrder<12>: ymm=2311        <- present at all = fused kernel exists
```

`UnpackBlockFlToFileOrder` is `__m256i` by hand, so it stays `ymm` even in a
512-bit build. A 512 leg would therefore widen the sequential kernel and not the
candidate, which is why it is not part of the headline.

## Run

```bash
./cpp/src/parquet/x86_register_width_sweep.sh \
  AVX2=build-x86-AVX2/release/parquet-pfor-comparison-benchmark \
  SSE4_2=build-x86-SSE4_2/release/parquet-pfor-comparison-benchmark
```

Send back the single `x86_register_width_sweep_<host>_<date>.tar.gz` it writes.

This run is longer than the first: the four layout benchmarks now cover five
footprints each.

If your CPU has a mobile/boost power profile, the largest footprints are the
ones most likely to drift thermally. `machine.txt` records the governor, and the
per-column tables print the worst CV alongside every ratio, so drift shows up in
the table instead of disappearing into it.

## Read the results before sending them

```bash
tar xzf x86_register_width_sweep_*.tar.gz
python3 cpp/src/parquet/pfor_layout_tables.py combined_results.json
```

That prints, per build width: the four benchmarks across all five footprints and
all 43 columns, the geomean over columns, and two self-checks. It fails loudly
if a benchmark or the ladder is missing, so it will tell you immediately if the
build did not take.

Two things worth checking in the output yourself:

- **`fl_unpk/intlv` must be ~1.000** at every footprint. Those two run the
  identical `PackBlock`/`UnpackBlock` pair over the same number of wire bytes
  against a grid that was merely *filled* differently at encode time. If they
  disagree by more than a few percent, something is wrong with the run and the
  gap bounds what every other ratio can mean. The script prints this check
  explicitly at the end of each width.
- **`tax` = `fl_unpk/fl_tpos`** is the permutation cost. Near 1.0 means the
  fused kernel is working; near 2.3 means you are in the scratch fallback and
  the binary was mis-built.

## What is being asked of the data

**At what working-set size, if any, does `fl_tpos/seq` exceed 1.0 on your
silicon, and how does the `fl_unpk` win compare to the `tax` that collects
it?** The claim under test on
this branch is that the grid's unpacking is genuinely much cheaper in L1, and
that the positional permutation costs about as much as the win — which would
mean the layout does not pay for Parquet as specified. Whether either effect
survives at larger footprints is the least settled part of that, for the
allocation reason above, so your machine is most useful as a second reading of
the footprint shape.
