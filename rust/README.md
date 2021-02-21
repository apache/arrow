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

# Native Rust implementation of Apache Arrow

[![Coverage Status](https://codecov.io/gh/apache/arrow/rust/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/arrow?branch=master)

Welcome to the implementation of Arrow, the popular in-memory columnar format, in Rust.

This part of the Arrow project is divided in 4 main components:

| Crate     | Description | Documentation |
|-----------|-------------|---------------|
|Arrow        | Core functionality (memory layout, arrays, low level computations) | [(README)](arrow/README.md) |
|Parquet      | Parquet support | [(README)](parquet/README.md) |
|Arrow-flight | Arrow data between processes | [(README)](arrow-flight/README.md) |
|DataFusion   | In-memory query engine with SQL support | [(README)](datafusion/README.md) |

Independently, they support a vast array of functionality for in-memory computations.

Together, they allow users to write an SQL query or a `DataFrame` (using `datafusion` crate), run it against a parquet file (using `parquet` crate), evaluate it in-memory using Arrow's columnar format (using the `arrow` crate), and send to another process (using `arrow-flight` crate).

Generally speaking, the `arrow` crate offers the  functionality to develop code that uses Arrow arrays, and `datafusion` offers most operations typically found in SQL, with the notable exceptions of:

* `join`
* `window` functions

There are too many features to enumerate here, but some notable mentions:

* `Arrow` implements all formats in the specification except certain dictionaries
* `Arrow` supports SIMD operations to some of its vertical operations
* `DataFusion` supports `async` execution
* `DataFusion` supports user-defined functions, aggregates, and whole execution nodes

You can find more details about each crate on their respective READMEs.

## Developer's guide to Arrow Rust

### How to compile

This is a standard cargo project with workspaces. To build it, you need to have `rust` and `cargo`:

```bash
cd /rust && cargo build
```

You can also use rust's official docker image:

```bash
docker run --rm -v $(pwd)/rust:/rust -it rust /bin/bash -c "cd /rust && cargo build"
```

The command above assumes that are in the root directory of the project, not in the same
directory as this README.md.

You can also compile specific workspaces,

```bash
cd /rust/arrow && cargo build
```

### Git Submodules

Before running tests and examples, it is necessary to set up the local development environment.

The tests rely on test data that is contained in git submodules.

To pull down this data run the following:

```bash
git submodule update --init
```

This populates data in two git submodules:

- `../cpp/submodules/parquet_testing/data` (sourced from https://github.com/apache/parquet-testing.git)
- `../testing` (sourced from https://github.com/apache/arrow-testing)

By default, `cargo test` will look for these directories at their
standard location. The following Env vars can be used to override the
location should you choose

```bash
# Optionaly specify a different location for test data
export PARQUET_TEST_DATA=$(cd ../cpp/submodules/parquet-testing/data; pwd)
export ARROW_TEST_DATA=$(cd ../testing/data; pwd)
```

From here on, this is a pure Rust project and `cargo` can be used to run tests, benchmarks, docs and examples as usual.


### Running the tests

Run tests using the Rust standard `cargo test` command:

```bash
# run all tests.
cargo test


# run only tests for the arrow crate
cargo test -p arrow
```

## Code Formatting

Our CI uses `rustfmt` to check code formatting. Before submitting a
PR be sure to run the following and check for lint issues:

```bash
cargo +stable fmt --all -- --check
```

## Clippy Lints

We recommend using `clippy` for checking lints during development. While we do not yet enforce `clippy` checks, we recommend not introducing new `clippy` errors or warnings.

Run the following to check for clippy lints.

```
cargo clippy
```

If you use Visual Studio Code with the `rust-analyzer` plugin, you can enable `clippy` to run each time you save a file. See https://users.rust-lang.org/t/how-to-use-clippy-in-vs-code-with-rust-analyzer/41881.

One of the concerns with `clippy` is that it often produces a lot of false positives, or that some recommendations may hurt readability. We do not have a policy of which lints are ignored, but if you disagree with a `clippy` lint, you may disable the lint and briefly justify it.

Search for `allow(clippy::` in the codebase to identify lints that are ignored/allowed. We currently prefer ignoring lints on the lowest unit possible.
* If you are introducing a line that returns a lint warning or error, you may disable the lint on that line.
* If you have several lints on a function or module, you may disable the lint on the function or module.
* If a lint is pervasive across multiple modules, you may disable it at the crate level.

## Git Pre-Commit Hook

We can use [git pre-commit hook](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks) to automate various kinds of git pre-commit checking/formatting.

Suppose you are in the root directory of the project.

First check if the file already exists:

```bash
ls -l .git/hooks/pre-commit
```

If the file already exists, to avoid mistakenly **overriding**, you MAY have to check
the link source or file content. Else if not exist, let's safely soft link [pre-commit.sh](pre-commit.sh) as file `.git/hooks/pre-commit`:

```
ln -s  ../../rust/pre-commit.sh .git/hooks/pre-commit
```

If sometimes you want to commit without checking, just run `git commit` with `--no-verify`:

```bash
git commit --no-verify -m "... commit message ..."
```
