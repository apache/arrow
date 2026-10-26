<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Parquet C++ vector / fixed-size-list reader optimization

## Reference design

The reference article is Gunnar Morling's [A Fast Path for Fixed-Length Lists in Parquet](https://www.morling.dev/blog/fast-path-for-fixed-length-lists-in-parquet/). Hardwood's `FixedSizeListDetector` first gates on an all-present definition-level run, then recognizes the periodic repetition-level stream. For a page proven fixed length it copies values in bulk, derives offsets from one stride, and avoids materializing def/rep arrays. The article reports roughly 2.5x column-reader gains and up to 3.9x row-reader gains, with the exact result depending on list width and encoding/compression.

This checkout already has Arrow `FixedSizeListType`, so it is used as the vector simulation. The first implementation used the stronger schema invariant (`required fixed-size list` plus `required child`). The second iteration adds an encoded page-level detector for ordinary LIST pages, controlled by `ArrowReaderProperties`.

## Changes made

- Added `RecordReader::ReadRecordsFixedLength()`. The typed reader reads a known number of values directly across data pages and skips def/rep level decoding when max definition/repetition levels are both one, values are required, and the physical reader owns a flat values buffer.
- Added the corresponding `LoadBatchFixedLength()` plumbing through Arrow nested readers.
- `FixedSizeListReader` uses the direct path for required fixed-size lists and required children. Its `BuildArray()` constructs the fixed-size list directly from the flat child array, without allocating offsets or scanning levels. Nullable lists/elements and unsupported binary builders retain the existing Dremel path.
- Added `BM_ReadFixedSizeFloatList{Plain,ByteStreamSplit,Alp}` for list widths 3, 16, and 768. The benchmark writes one physical LIST column and reads it repeatedly, so it covers the vector-shaped path and PLAIN/BSS/ALP decoding.
- `ArrowReaderProperties::set_fixed_list_fast_path(false)` disables the optimization for baseline comparison or variable-width LIST columns.

## Local measurements

The standalone release-linked harness used 1 MiB float values with a required child field, five reads per case, and the same in-memory file for the optimized and baseline runs. Times are mean seconds per read; lower is better.

| list size | PLAIN fast | PLAIN base | speedup | BSS fast | BSS base | speedup | ALP fast | ALP base | speedup |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | 1.387 ms | 1.462 ms | 1.05x | 1.414 ms | 1.385 ms | 0.98x | 2.135 ms | 2.066 ms | 0.97x |
| 16 | 2.000 ms | 1.999 ms | 1.00x | 1.960 ms | 1.996 ms | 1.02x | 2.674 ms | 2.874 ms | 1.07x |
| 768 | 1.393 ms | 1.473 ms | 1.06x | 1.426 ms | 1.417 ms | 0.99x | 2.135 ms | 2.212 ms | 1.04x |

The small file makes encoding/decompression and setup noise significant; these numbers are directional rather than publication-quality. The largest structural win here is removing the level decode/allocation work. On this Apple Silicon local file, that work is a small fraction of end-to-end time, so the measured end-to-end gain is modest. Larger row groups and compressed files should make the difference more visible.

## Keep / defer decisions

**Keep:** the schema-proven required FixedSizeList fast path. It is simple, bounded, and exercised by existing fixed-size-list round-trip tests. It also naturally handles page boundaries and does not depend on a particular RLE layout.

**Keep:** the direct child-array construction. It removes the offset buffer allocation and `DefRepLevelsToList` pass for the hot case.

**Current status:** a generic LIST detector now scans each page through private level decoders and is controlled by `ArrowReaderProperties::set_fixed_list_fast_path()`. It is disabled by default; benchmarks explicitly enable it. If a later variable-width page is found after values have been consumed, reading fails with an explicit instruction to disable this property. Hardwood's SWAR/stride detector can replace the temporary decoder-based scan in a follow-up.

**Defer:** nullable fixed-size vectors. They need either a def-level presence gate or a validity-aware direct path; the current implementation deliberately falls back to preserve null-list and null-element semantics.

## Validation and build notes

- `make -C cpp/build-release -j8 parquet_shared` succeeds in Release mode.
- `parquet-arrow-reader-writer-test --gtest_filter='*FixedSize*'` passes all 5 fixed-size-list round-trip cases.
- The full reader/writer test binary runs; its data-file cases require `PARQUET_TEST_DATA` and therefore fail in this checkout when that environment variable is unset (the in-memory tests pass).
- The benchmark target was built with Conan local cache `benchmark/1.9.4`; no package installation or network download was needed.

### Larger stress check

A second standalone run used 16 MiB float values and three reads per case. With ZSTD, list sizes 3 and 768 measured 38.66/39.32 ms and 24.05/23.84 ms (fast/base). With uncompressed data they measured 13.36/13.83 ms and 13.93/14.61 ms. This confirms that decompression and primitive float decoding dominate this C++ end-to-end path; the level-removal optimization contributes only a few percent here.

## Google Benchmark run (Conan benchmark/1.9.4)

The checked-in benchmark was run in Release mode with ZSTD, 3 repetitions, and `--benchmark_min_time=0.1s`. Fast cases explicitly set `ArrowReaderProperties::set_fixed_list_fast_path(true)`; baseline cases used the same benchmark with that property set to false. Values below are mean read time in milliseconds and baseline/fast speedup.

| representation | encoding | list size | fast | baseline | speedup |
|---|---|---:|---:|---:|---:|
| FixedSizeList | PLAIN | 3 | 12.37 | 14.84 | 1.20x |
| FixedSizeList | PLAIN | 16 | 8.59 | 11.29 | 1.31x |
| FixedSizeList | PLAIN | 768 | 5.80 | 8.58 | 1.48x |
| FixedSizeList | BSS | 3 | 2.91 | 5.91 | 2.03x |
| FixedSizeList | BSS | 16 | 4.89 | 7.86 | 1.61x |
| FixedSizeList | BSS | 768 | 2.55 | 5.29 | 2.08x |
| FixedSizeList | ALP | 3 | 14.61 | 17.46 | 1.20x |
| FixedSizeList | ALP | 16 | 16.56 | 19.56 | 1.18x |
| FixedSizeList | ALP | 768 | 14.06 | 17.21 | 1.22x |
| ordinary LIST | PLAIN | 3 | 11.86 | 14.75 | 1.24x |
| ordinary LIST | PLAIN | 16 | 4.60 | 11.15 | 2.42x |
| ordinary LIST | PLAIN | 768 | 4.36 | 8.62 | 1.98x |
| ordinary LIST | BSS | 3 | 2.52 | 5.57 | 2.21x |
| ordinary LIST | BSS | 16 | 1.30 | 7.83 | 6.04x |
| ordinary LIST | BSS | 768 | 0.94 | 5.35 | 5.69x |
| ordinary LIST | ALP | 3 | 14.09 | 17.33 | 1.23x |
| ordinary LIST | ALP | 16 | 12.86 | 19.52 | 1.52x |
| ordinary LIST | ALP | 768 | 12.61 | 16.86 | 1.34x |

## Encoded level detector iteration

The second implementation now inspects each data page's encoded level streams through private `LevelDecoder` instances. It validates that definition levels are all present and that repetition levels are periodic (`0, 1, ..., 1`), inferring the list width when the Arrow type is an ordinary non-nullable `LIST`. The production decoders are left untouched; only validated pages use the direct value path.

For ordinary `LIST`, enable the detector explicitly with `ArrowReaderProperties::set_fixed_list_fast_path(true)`. A generic LIST column whose width changes between pages must disable it with `set_fixed_list_fast_path(false)`; otherwise the reader returns an explicit error instead of silently falling back after consuming part of a batch.

The standalone release-linked stress harness used 16 MiB float values, uncompressed pages, and three reads per case. Results are mean read time:

| representation | list size | fast | baseline | speedup |
|---|---:|---:|---:|---:|
| FixedSizeList / PLAIN | 3 | 11.17 ms | 13.50 ms | 1.21x |
| FixedSizeList / PLAIN | 16 | 2.02 ms | 19.80 ms | 9.81x |
| FixedSizeList / PLAIN | 768 | 1.41 ms | 14.46 ms | 10.25x |
| FixedSizeList / BSS | 768 | 1.54 ms | 14.59 ms | 9.45x |
| FixedSizeList / ALP | 768 | 8.08 ms | 21.13 ms | 2.62x |
| ordinary LIST / PLAIN | 3 | 10.65 ms | 13.49 ms | 1.27x |
| ordinary LIST / PLAIN | 16 | 2.30 ms | 19.62 ms | 8.53x |
| ordinary LIST / PLAIN | 768 | 1.35 ms | 13.43 ms | 9.97x |

With ZSTD and ordinary LIST, list sizes 3 and 768 measured 35.75/38.59 ms and 11.47/23.10 ms (fast/baseline), or 1.08x and 2.01x. The remaining gap to Hardwood's approximately 2.5x column-reader result is mainly Arrow's single-threaded end-to-end path and decompression/value-decoding costs; the next architectural step is page preflight/caching so generic LIST can safely stay on the fast path across mixed pages.

### 128M-value ZSTD run (Hardwood-sized)

To match the article's scale more closely, an ordinary LIST column with 128 MiB float values and list width 768 was read once per process. With ZSTD, fast path was **33.45 ms** versus **83.36 ms** baseline, or **2.49x**. This is effectively the same as Hardwood's reported ~2.5x column-reader gain. The earlier 16 MiB result understated the benefit because page/decode setup and small-input overhead dominated.

At the same 128M-value / 768-width scale, BYTE_STREAM_SPLIT measured **11.31 ms** fast versus **46.74 ms** baseline (**4.13x**). ALP measured 87.76/129.83 ms (**1.48x**) because ALP decode dominates both paths. Thus the 3x target is reached for BSS and exceeded for uncompressed PLAIN/BSS; PLAIN+ZSTD is 2.49x.

### Page-boundary validation

Parquet V1/V2 pages can end in the middle of a repeated record. The detector now carries the current list phase across pages (`phase = values_seen % list_size`) and validates the next page against that phase, instead of requiring every page to start with repetition level 0. A tiny-page test (`data_pagesize=1024`, list width 768) passes with the fast path enabled and the regular path disabled/enabled comparison remains correct.

The detector currently uses independent `LevelDecoder` instances over the encoded payload rather than duplicating Hardwood's SWAR/RLE byte parser. This still avoids materializing level arrays and the measured end-to-end gain is dominated by the avoided Dremel work. A raw byte parser would mainly reduce detector CPU, which is already amortized below the value decode/decompression cost on the large tests; it is therefore not currently a larger end-to-end win.
