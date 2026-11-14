<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Parquet Thrift parser optimization experiments

**Exploratory snapshot — DO NOT REVIEW OR MERGE.** This branch preserves code,
measurements and unsuccessful ideas for future reference. It is not a complete
feature implementation or a production-ready replacement for the current reader.
The limitations below are part of the experiment record.

## Motivation and reading paths

The starting point is the Apache Arrow article
[Parquet Metadata in Rust](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/).
It reports improvements of up to 9x for particular pathological workloads. Those
results motivated this exploration; they are not a directly reproduced C++ target.
There is no same-machine, same-input Rust comparison in this record.

The experiment separates three kinds of work:

1. **Generated footer API:** libthrift parsing plus the existing `FileMetaData`
   initialization. Set `PARQUET_USE_GENERATED_THRIFT=1` for this baseline.
2. **Direct footer API:** a custom compact-protocol cursor fills a borrowed internal
   representation, then builds a schema tree for the existing metadata accessors.
   This snapshot selects that path by default for eligible plaintext footers.
3. **Summary API:** `FastFileMetaData` discovers footer counts and `created_by`,
   traversing past row groups without retaining column metadata. It cannot supply
   column statistics, offsets or a `SchemaDescriptor`.

The summary API does less work. Its timings must not be presented as an equivalent
full-metadata speedup. Reading data pages is outside these footer benchmarks.

## Implemented directions

| Direction | Code in this snapshot | Intended benefit / qualification |
| --- | --- | --- |
| Custom compact reader | `thrift_compact_reader.{h,cc}` | Direct field decoding and unknown-field skipping without generated protocol glue. |
| Borrowed footer fields | `Direct*` structures and retained `Buffer` ownership | Schema names, paths, statistics binary values, key/value pairs and selected footer strings refer to footer storage. The raw-pointer entry point makes a bulk owning copy. |
| Direct schema construction | `schema::UnflattenDirect` | Builds the schema tree without a generated `SchemaElement` vector. Logical annotations still use some generated structures. |
| On-demand compatibility objects | `metadata.cc` | Generated metadata remains available for serialization and fallback; some statistics and public string accessors materialize owning objects on demand. |
| Selective statistics | `ReaderProperties` and the compact reader | Can omit column/encoding/size statistics or retain them for selected leaf columns. This is statistics selection, not full column or row-group projection. |
| Encoding-statistics mask | `thrift_encoding_stats_as_mask` | Retains an encoding-presence bitset instead of the original list. Page types and counts are not preserved by this experimental policy. |
| Summary-only footer API | `FastFileMetaData` | Avoids schema-tree and column-object construction while retaining summary counts. |
| Varint common path | `CompactInput::ReadVarint` | Specialized one-byte case; the measurements below do not isolate its individual contribution. |
| Page header, page index and bloom header decoding | `ThriftDeserializer` compact-reader dispatch | These objects use custom readers but still populate generated structs. They are not measured by the footer-only table. |

Page headers still use the existing `Peek`/retry/`Advance` loop. This is not a
resumable incremental decoder. A page-statistics skip option was started, but its
end-to-end forwarding is incomplete and must not be described as fully implemented.
Encrypted footer and encrypted-column compatibility paths also remain unfinished.

## Reproducible measurement

Validation and measurements below were rerun on **October 23, 2026**, using:

- Source baseline: `ca5564e007a209e251b9d4648af4bc6a53c68c4f` plus this experiment.
- Apple M5 Pro, 48 GiB memory; Apple Clang 21.0.0; CMake Release build.
- Existing CMake configuration with tests enabled and encryption not required.
- Seven samples per mode, preceded by one warm-up per mode; mode order rotates
  each round. Results are medians, with construction and destruction timed separately.
- The same retained in-memory footer buffer is passed to each mode. File I/O,
  synthetic generation and initial buffer creation are excluded from the timer.
- Counts are checked against the summary API in every mode. This is a timing and
  basic consistency check, not an exhaustive semantic-equivalence test.

The standalone driver is
[`experiments/thrift_parser_benchmark.cc`](experiments/thrift_parser_benchmark.cc).
It is intentionally outside the default build and uses only synthetic values when
generating a footer. It prints aggregate sizes, counts and times, never source
paths, schema names, statistics values or key-value metadata.

From the repository root, after configuring `cpp/build-release`:

```sh
(cd cpp/build-release && make -j8)
c++ -std=c++20 -O3 -Icpp/src -Icpp/build-release/src \
  cpp/src/parquet/experiments/thrift_parser_benchmark.cc \
  -Lcpp/build-release/release \
  -Wl,-rpath,"$PWD/cpp/build-release/release" \
  -lparquet -larrow -o /tmp/thrift_parser_benchmark
/tmp/thrift_parser_benchmark --synthetic 100000 20 7
```

This is a Unix/macOS-oriented experiment driver, not a portable benchmark target.
Libraries using nonstandard runtime dependencies may need additional linker flags.
The generator creates metadata only: 100,000 optional String columns, 20 one-row
row groups, synthetic min/max values and encoding-statistics entries. It does not
write a complete Parquet data file. Its serialized footer is **170,453,566 bytes**.
This fixture is newly reproducible here; it differs from earlier local experiments.

Workload A has these aggregate properties only:

| Property | Value |
| --- | ---: |
| File bytes | 536,809,024 |
| Footer bytes | 154,661,319 |
| Leaf columns | 83,925 |
| Row groups | 8 |

Its source file, identifiers, schema and data are not included. Public reproduction
uses the synthetic generator; Workload A is a supplemental local observation.

### Footer construction time

| Mode | Synthetic footer (ms) | Workload A (ms) |
| --- | ---: | ---: |
| Generated footer API | 459.995 | 260.994 |
| Direct footer API, full statistics | 285.512 | 163.663 |
| Direct API, encoding-presence mask | 284.650 | 164.140 |
| Direct API, all three statistics groups omitted | 272.452 | 157.567 |
| Direct API, statistics retained only for leaf column 0 | 278.880 | 165.778 |
| Summary-only API | 187.604 | 102.205 |

Full direct construction is **1.61x** faster on the synthetic footer and **1.59x**
on Workload A compared with generated construction. Statistics-skipping policies
change what is retained and show workload-dependent results. In particular, the
encoding mask and single-column statistics policy do not consistently improve open
time across both inputs.

Summary construction is **2.45x** and **2.55x** faster than generated construction,
respectively, while omitting column metadata and schema-tree construction. These
figures do not demonstrate a 9x C++ improvement or explain the Rust/C++ difference.
The seven-sample results do not establish confidence intervals or an optimization
contribution breakdown.

### Metadata destruction time

| Mode | Synthetic footer (ms) | Workload A (ms) |
| --- | ---: | ---: |
| Generated footer API | 54.317 | 40.373 |
| Direct footer API, full statistics | 52.368 | 31.219 |
| Encoding-presence mask | 33.958 | 27.157 |
| All three statistics groups omitted | 34.416 | 21.094 |
| Statistics retained only for leaf column 0 | 35.366 | 21.930 |
| Summary-only API | 0.001 | 0.001 |

Keeping destruction separate avoids hiding the allocation/lifetime costs of the
retained metadata. No peak-RSS or allocation-count study was completed.

## Earlier exploration and uncompleted ideas

Earlier exploratory runs used different fixtures and timing boundaries. For
example, an earlier 100,000-String-column, 20-row-group footer was 185,780,176 bytes;
it is not byte-for-byte identical to the new synthetic fixture. Those runs should
not be combined with the table above into a causal optimization progression.

A **summary schema-count reuse** API was tried and removed. Recorded times were
245.44 vs 246.28 ms for an earlier wide synthetic workload and 100.57 vs 101.45 ms
for Workload A (without vs with reuse). These small differences did not justify
keeping that API. This was not an implementation of full `SchemaDescriptor` reuse.

The following ideas were discussed but have no completed implementation and
controlled A/B measurement in this snapshot. They must not be labelled as tested
and rejected for lack of benefit:

- Full schema reuse, full row-group/column projection and a resumable input decoder.
- PMR/arena allocation, specialized flat-schema parsing and SIMD varint decoding.
- mmap footer access and metadata caching.
- Borrowed page-index/header/bloom APIs and removal of all generated compatibility
  structures. The bloom-filter payload itself is not a Thrift object.

## Validation and remaining work

The following were run against this snapshot on October 23, 2026:

| Check | Result |
| --- | --- |
| `make -j8` in the configured Release build | Passed |
| `parquet-internals-test` | 354 passed |
| `parquet-reader-test` | 154 passed, 4 skipped |
| `parquet-arrow-reader-writer-test` | 847 passed, 7 skipped; 1 disabled |
| `parquet-schema-test` | 43 passed |
| `parquet-file-deserialize-test` | 33 passed |

The added summary tests check counts, retained-buffer lifetime and rejection of
invalid/truncated buffers. Test runs use the repository's standard Arrow and
Parquet test-data environment variables. The local configuration does not validate
every encryption/compression path.

Known gaps before any production proposal include:

- Custom-cursor string/container limits, malformed-input handling and resource
  limits need a dedicated audit; fallback is not a substitute for this audit.
- Equality in some direct metadata proxies compares only a subset of fields.
  Existing API semantic equivalence has not been established.
- Borrowed storage, mutable caches, serialization and concurrent accessor use need
  further lifetime and thread-safety review.
- Statistics policies and environment overrides are not uniformly applied across
  every fallback/header path. An encoding-presence mask is not the complete
  encoding-statistics contract.
- `FastFileMetaData` traverses omitted wire fields; it is neither constant-time
  footer discovery nor a full metadata validator. `Skip(Binary)` still creates
  temporary strings in this snapshot.
- Public API/ABI compatibility, Meson integration, portability, fuzzing and
  representative end-to-end performance have not been completed.

This branch intentionally retains the experimental paths and their limitations
for later investigation. Passing the listed tests is not a claim that this is a
complete or mergeable feature.
