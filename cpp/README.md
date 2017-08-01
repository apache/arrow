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

# Arrow C++

## System setup

Arrow uses CMake as a build configuration system. Currently, it supports in-source and
out-of-source builds with the latter one being preferred.

Build Arrow requires:

* A C++11-enabled compiler. On Linux, gcc 4.8 and higher should be sufficient.
* CMake
* Boost

On Ubuntu/Debian you can install the requirements with:

```shell
sudo apt-get install cmake \
     libboost-dev \
     libboost-filesystem-dev \
     libboost-system-dev
```

On OS X, you can use [Homebrew][1]:

```shell
brew install boost cmake
```

If you are developing on Windows, see the [Windows developer guide][2].

## Building Arrow

Simple debug build:

    mkdir debug
    cd debug
    cmake ..
    make unittest

Simple release build:

    mkdir release
    cd release
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make unittest

Detailed unit test logs will be placed in the build directory under `build/test-logs`.

### Building/Running benchmarks

Follow the directions for simple build except run cmake
with the `--ARROW_BUILD_BENCHMARKS` parameter set correctly:

    cmake -DARROW_BUILD_BENCHMARKS=ON ..

and instead of make unittest run either `make; ctest` to run both unit tests
and benchmarks or `make runbenchmark` to run only the benchmark tests.

Benchmark logs will be placed in the build directory under `build/benchmark-logs`.

### Third-party environment variables

To set up your own specific build toolchain, here are the relevant environment
variables

* Boost: `BOOST_ROOT`
* Googletest: `GTEST_HOME` (only required to build the unit tests)
* gflags: `GFLAGS_HOME` (only required to build the unit tests)
* Google Benchmark: `GBENCHMARK_HOME` (only required if building benchmarks)
* Flatbuffers: `FLATBUFFERS_HOME` (only required for the IPC extensions)
* Hadoop: `HADOOP_HOME` (only required for the HDFS I/O extensions)
* jemalloc: `JEMALLOC_HOME`
* brotli: `BROTLI_HOME`, can be disabled with `-DARROW_WITH_BROTLI=off`
* lz4: `LZ4_HOME`, can be disabled with `-DARROW_WITH_LZ4=off`
* snappy: `SNAPPY_HOME`, can be disabled with `-DARROW_WITH_SNAPPY=off`
* zlib: `ZLIB_HOME`, can be disabled with `-DARROW_WITH_ZLIB=off`
* zstd: `ZSTD_HOME`, can be disabled with `-DARROW_WITH_ZSTD=off`

If you have all of your toolchain libraries installed at the same prefix, you
can use the environment variable `$ARROW_BUILD_TOOLCHAIN` to automatically set
all of these variables. Note that `ARROW_BUILD_TOOLCHAIN` will not set
`BOOST_ROOT`, so if you have custom Boost installation, you must set this
environment variable separately.

### Building Python integration library

The `arrow_python` shared library can be built by passing `-DARROW_PYTHON=on`
to CMake. This must be installed or in your library load path to be able to
build pyarrow, the Arrow Python bindings.

The Python library must be built against the same Python version for which you
are building pyarrow, e.g. Python 2.7 or Python 3.6. NumPy must also be
installed.

### API documentation

To generate the (html) API documentation, run the following command in the apidoc
directoy:

    doxygen Doxyfile

This requires [Doxygen](http://www.doxygen.org) to be installed.

## Development

This project follows [Google's C++ Style Guide][3] with minor exceptions. We do
not encourage anonymous namespaces and we relax the line length restriction to
90 characters.

### Error Handling and Exceptions

For error handling, we use `arrow::Status` values instead of throwing C++
exceptions. Since the Arrow C++ libraries are intended to be useful as a
component in larger C++ projects, using `Status` objects can help with good
code hygiene by making explicit when a function is expected to be able to fail.

For expressing invariants and "cannot fail" errors, we use DCHECK macros
defined in `arrow/util/logging.h`. These checks are disabled in release builds
and are intended to catch internal development errors, particularly when
refactoring. These macros are not to be included in any public header files.

Since we do not use exceptions, we avoid doing expensive work in object
constructors. Objects that are expensive to construct may often have private
constructors, with public static factory methods that return `Status`.

There are a number of object constructors, like `arrow::Schema` and
`arrow::RecordBatch` where larger STL container objects like `std::vector` may
be created. While it is possible for `std::bad_alloc` to be thrown in these
constructors, the circumstances where they would are somewhat esoteric, and it
is likely that an application would have encountered other more serious
problems prior to having `std::bad_alloc` thrown in a constructor.

## Continuous Integration

Pull requests are run through travis-ci for continuous integration.  You can avoid
build failures by running the following checks before submitting your pull request:

    make unittest
    make lint
    # The next command may change your code.  It is recommended you commit
    # before running it.
    make format # requires clang-format is installed

Note that the clang-tidy target may take a while to run.  You might consider
running clang-tidy separately on the files you have added/changed before
invoking the make target to reduce iteration time.  Also, it might generate warnings
that aren't valid.  To avoid these you can use add a line comment `// NOLINT`. If
NOLINT doesn't suppress the warnings, you add the file in question to
the .clang-tidy-ignore file.  This will allow `make check-clang-tidy` to pass in
travis-CI (but still surface the potential warnings in `make clang-tidy`).   Ideally,
both of these options would be used rarely.  Current known uses-cases whent hey are required:

*  Parameterized tests in google test.

[1]: https://brew.sh/
[2]: https://github.com/apache/arrow/blob/master/cpp/apidoc/Windows.md
[3]: https://google.github.io/styleguide/cppguide.html