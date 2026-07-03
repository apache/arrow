<!---
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

# AI Agent Development Guidelines for the Arrow C++ Library

## Project Overview

The Arrow C++ library is the core implementation of the
[Apache Arrow](https://arrow.apache.org/) columnar format. It lives at `cpp/`
inside the `apache/arrow` monorepo, which also contains Python (`python/`),
R (`r/`), Ruby, C/GLib, MATLAB, and other language implementations.

This means:

- Many other implementations bind to this library. Python (`python/`) and R
  (`r/`) build against the C++ library from `cpp/`, and Ruby, C/GLib, and
  others depend on it as well.
- A change here can affect every downstream binding, so fix problems at the
  right layer. A bug reported in Python or R may actually need to be fixed in
  C++ (`cpp/`), or the reverse.
- The C++ library is split into modular components (core Arrow, Compute, Acero,
  Dataset, Parquet, Flight, Gandiva) that can be built and tested
  independently.

## AI Disclosure

Any pull request that used generative AI tools must disclose this. Include a
note in the PR description stating which AI tools were used. The project also
asks that you only submit AI-assisted PRs you can debug and own yourself, that
you match the codebase's style and conventions, and that you are upfront about
what was AI-generated. AI agents should never tag or ping maintainers.

## Do Not Edit Generated Files

Some files under `cpp/` are generated and must not be edited by hand:

- `cpp/src/generated/` — Flatbuffers (`*_generated.h`) and Thrift
  (`parquet_types.*`) sources generated from the format definitions. These are
  excluded from formatting and linting.
- `cpp/src/arrow/util/config.h` — generated at build time from
  `config.h.cmake`. Edit the `.cmake` template, not the generated header.

## Development Setup

Arrow C++ uses CMake and builds out-of-source. Building requires a
C++20-capable compiler (on Linux, gcc 12 or higher), CMake 3.25 or higher, and
either `ninja` or `make`.

### Building

The quickest way to get a development build is with one of the CMake presets.
List them with `cmake --list-presets` (from the `cpp/` directory) and inspect
one with `cmake -N --preset <name>`.

```bash
cd cpp && mkdir build && cd build
# Debug build, library only (no tests):
cmake .. --preset ninja-debug-minimal
# Or a debug build that also builds tests:
cmake .. --preset ninja-debug-basic   # or ninja-debug for more optional components
cmake --build .
```

- `ninja-debug-minimal` builds only the core library (`ARROW_BUILD_TESTS=OFF`).
- `ninja-debug-basic` and `ninja-debug` enable tests (`ARROW_BUILD_TESTS=ON`)
  and set `ARROW_EXTRA_ERROR_CONTEXT=ON`, which adds context to
  `RETURN_NOT_OK` / `ASSERT_OK` failures — very helpful during development.

You can pass extra options after the preset, e.g.
`cmake .. --preset ninja-debug-basic -DARROW_PARQUET=ON`.

### Testing

Tests use GoogleTest and live next to the sources they cover, named `*_test.cc`
(for example, `cpp/src/arrow/buffer_test.cc` compiles to the `arrow-buffer-test`
executable). They are built by the tests-enabled presets above, or with
`-DARROW_BUILD_TESTS=ON`.

From the build directory:

```bash
# Run the whole suite in parallel:
ctest -j16 --output-on-failure
# Run only the unit tests, or only the Parquet tests:
ctest -L unittest
ctest -L parquet
# Run a single test by name (regex match):
ctest -R arrow-buffer-test
```

You can also run a test executable directly — it lives under the build-type
directory, e.g. `debug/arrow-buffer-test` for a debug preset — and pass
GoogleTest flags such as `--gtest_filter=` to select individual cases.

Benchmarks follow the `*_benchmark.cc` convention and require a
benchmarks-enabled build (`-DARROW_BUILD_BENCHMARKS=ON`); build in `Release`
mode for meaningful numbers.

### Code Style and Linting

Arrow C++ follows
[Google's C++ Style Guide](https://google.github.io/styleguide/cppguide.html)
with a few modifications (see the
[C++ conventions docs](https://arrow.apache.org/docs/dev/developers/cpp/conventions.html)):

- Line length is relaxed to 90 characters.
- Use the `NULLPTR` macro (from `arrow/util/macros.h`) instead of `nullptr` in
  public headers.
- Prefer pointers for output and input/output parameters.
- Return `arrow::Status` or `arrow::Result<T>` instead of throwing exceptions;
  use `DCHECK` (from `arrow/util/logging.h`) for internal invariants.
- Doxygen docstrings use `///` and the infinitive mood ("Allocate a buffer",
  not "Allocates a buffer").

Formatting is enforced with clang-format (pinned to 18.1.8) and linting with
cpplint, both run through pre-commit; `CPPLINT.cfg` holds the cpplint filters.
Run the C++ checks with:

```bash
pre-commit run --show-diff-on-failure --color=always --all-files cpp
```

CI runs the same checks in the "Dev / Lint" pipeline. A debug build uses
`-DBUILD_WARNING_LEVEL=CHECKIN` by default, which treats compiler warnings as
errors.

## File Structure

| Path | Contents |
|------|----------|
| `cpp/src/arrow/` | Core Arrow library (arrays, buffers, data types, IO, IPC, ...) |
| `cpp/src/arrow/compute/` | Compute kernels and expression engine |
| `cpp/src/arrow/acero/` | Acero streaming execution engine |
| `cpp/src/arrow/dataset/` | Dataset API (multi-file, partitioned reads) |
| `cpp/src/arrow/flight/` | Arrow Flight RPC framework |
| `cpp/src/parquet/` | Apache Parquet implementation and Arrow integration |
| `cpp/src/generated/` | **Generated** — Flatbuffers/Thrift sources, do not edit |
| `cpp/cmake_modules/` | Shared CMake modules and helper functions |
| `cpp/examples/` | Standalone examples of using the Arrow C++ API |
| `*_test.cc` | GoogleTest unit tests, next to the code they cover |
| `*_benchmark.cc` | Google Benchmark microbenchmarks |

## Pull Requests

### Title Format

```
GH-<issue-number>: [C++] <description>
```

For example: `GH-50341: [C++] Add AGENTS.md to C++ dir`

### PR Description

Use all four template sections:

```markdown
### Rationale for this change

[1-2 sentences: why is this change needed?]

### What changes are included in this PR?

[1-2 sentences: what was changed?]

### Are these changes tested?

[How the changes were tested]

### Are there any user-facing changes?

[Yes/No with brief explanation]
```

Do not skip any section.

## Git Workflow

This project uses a fork-based workflow:

- **Push to your fork** (`origin`), never directly to `upstream`
  (`apache/arrow`).
- **Create PRs** from your fork to `upstream/main`.
- Fetch latest upstream before creating a branch:

  ```bash
  git fetch upstream
  git checkout upstream/main
  git checkout -b GH-<issue>-description
  ```
