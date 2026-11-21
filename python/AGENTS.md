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

# AI Agent Development Guidelines for PyArrow

## Project Overview

PyArrow is the Python interface to the [Apache Arrow](https://arrow.apache.org/)
C++ library. It lives at `python/` inside the `apache/arrow` monorepo alongside
the C++ library (`cpp/`), R (`r/`), and other language implementations.

Key points:

- PyArrow wraps the C++ library using [Cython](https://cython.org/). Most
  performance-critical code lives in `.pyx` / `.pxi` files that are compiled at
  build time.
- Some bugs or features require changes in C++ (`cpp/`), not just Python.
  Fix problems at the right layer.
- The Python bindings call C++ via Cython extension modules; pure-Python helpers
  live alongside them in `python/pyarrow/`.

## AI Disclosure

Any pull request that used generative AI tools must disclose this. Include a
note in the PR description stating which AI tools were used.

## Cython Source Files

The Cython source files (`.pyx`, `.pxi`, `.pxd`) are **hand-written** source
code that Cython compiles to C extensions at build time. Edit them directly when
modifying the Python bindings; a rebuild is required before changes take effect
(see [Building PyArrow](#building-pyarrow) below).

## Type Stubs

The type stub files (`pyarrow-stubs/pyarrow/*.pyi`) are **hand-written** and
maintained alongside the implementation. Their docstrings can be updated from
the compiled runtime:

```bash
# From python/ (requires a built pyarrow)
python scripts/update_stub_docstrings.py <install_prefix> pyarrow-stubs/pyarrow
```

Include any regenerated stub changes in your PR when modifying public APIs or
docstrings.

## Development Setup

### Conda (Linux / macOS / Windows)

```bash
git clone https://github.com/apache/arrow.git
cd arrow

# Initialize submodules (required for test data paths below)
git submodule update --init

# Set test-data paths
export PARQUET_TEST_DATA="${PWD}/cpp/submodules/parquet-testing/data"
export ARROW_TEST_DATA="${PWD}/testing/data"

conda create -y -n pyarrow-dev -c conda-forge \
    --file ci/conda_env_unix.txt \
    --file ci/conda_env_cpp.txt \
    --file ci/conda_env_python.txt \
    python=3.12
conda activate pyarrow-dev
```

### Building PyArrow

Build Arrow C++ first, then PyArrow:

```bash
# Build Arrow C++
mkdir -p cpp/build && cd cpp/build
cmake -GNinja \
    -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DATASET=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=ON \
    -DARROW_IPC=ON \
    -DARROW_JSON=ON \
    -DARROW_ORC=ON \
    -DARROW_PARQUET=ON \
    -DARROW_SUBSTRAIT=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZSTD=ON \
    ..
ninja install
cd ../..

# Build PyArrow in editable mode
cd python
pip install -e ".[test]" --no-build-isolation
```

After editing `.pyx` / `.pxi` files, re-run `pip install -e . --no-build-isolation`
to recompile — a bare `import pyarrow` will not pick up Cython changes without
a rebuild.

See the full guide at
https://arrow.apache.org/docs/developers/python/building.html for platform-
specific instructions, including Windows.

## Testing

Tests use [pytest](https://docs.pytest.org/). After building:

```bash
cd python
python -m pytest pyarrow/tests/
```

Run a single test file:

```bash
python -m pytest pyarrow/tests/test_array.py
```

### Optional Test Groups

Many tests are disabled by default and gated behind feature groups. Enable them
with `--enable-<group>` or disable with `--disable-<group>`:

```bash
# Enable Parquet tests
python -m pytest pyarrow/tests/ --enable-parquet

# Enable hypothesis tests
python -m pytest pyarrow/tests/ --enable-hypothesis

# Disable a group that is on by default
python -m pytest pyarrow/tests/ --disable-requires_testing_data
```

Common groups: `dataset`, `flight`, `gandiva`, `hdfs`, `hypothesis`,
`large_memory`, `orc`, `parquet`, `parquet_encryption`, `s3`, `substrait`.
The full list and their defaults are in `python/pyarrow/conftest.py`.

Install test requirements if needed:

```bash
pip install -r requirements-test.txt
```

## Code Style

PyArrow follows a PEP 8–like style similar to the
[pandas project](https://github.com/pandas-dev/pandas). Formatting and linting
are enforced via `pre-commit` using the `python` alias (covers autopep8,
flake8, and cython-lint):

```bash
# Format and lint the python/ directory
pre-commit run --show-diff-on-failure --color=always --all-files python
```

Always run this before submitting a PR.

## Type Checking

PyArrow ships type stubs in `pyarrow-stubs/`. Run type checkers from
`python/`:

```bash
# mypy
mypy

# pyright
pyright

# ty
ty check
```

All three configurations live in `pyproject.toml` under `[tool.mypy]`,
`[tool.pyright]`, and `[tool.ty]`.

## File Structure

| Path | Contents |
|------|----------|
| `python/pyarrow/*.pyx` | Cython extension modules (compiled) |
| `python/pyarrow/*.pxi` | Cython include files (included by `.pyx` files) |
| `python/pyarrow/*.pxd` | Cython declaration files |
| `python/pyarrow/*.py` | Pure-Python modules |
| `python/pyarrow/tests/` | pytest test suite |
| `python/pyarrow/src/` | C++ source files used only by PyArrow |
| `python/pyarrow-stubs/` | Type stubs (`.pyi`) for static analysis |
| `python/scripts/` | Developer utility scripts |
| `python/pyproject.toml` | Build and tool configuration |
| `python/requirements-test.txt` | Test dependencies |

## Pull Requests

### Title Format

```
GH-<issue-number>: [Python] <description>
```

Example: `GH-50342: [Python] Add AGENTS.md`

### PR Description

Use all four sections from the PR template:

```markdown
### Rationale for this change

[Why is this change needed?]

### What changes are included in this PR?

[What was changed?]

### Are these changes tested?

[Yes / No — and why]

### Are there any user-facing changes?

[Yes / No — and what they are]
```

## Further Resources

- [Building PyArrow](https://arrow.apache.org/docs/developers/python/building.html)
- [Developing PyArrow](https://arrow.apache.org/docs/developers/python/development.html)
- [New Contributor's Guide](https://arrow.apache.org/docs/developers/guide/index.html)
- [Contributing Overview](https://arrow.apache.org/docs/developers/overview.html)
- Root [`AGENTS.md`](../AGENTS.md) for repo-wide conventions
