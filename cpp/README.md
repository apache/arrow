<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
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
     libboost-regex-dev \
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

* Googletest: `GTEST_HOME` (only required to build the unit tests)
* Google Benchmark: `GBENCHMARK_HOME` (only required if building benchmarks)
* Flatbuffers: `FLATBUFFERS_HOME` (only required for the IPC extensions)
* Hadoop: `HADOOP_HOME` (only required for the HDFS I/O extensions)
* jemalloc: `JEMALLOC_HOME` (only required for the jemalloc-based memory pool)

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

## Continuous Integration

Pull requests are run through travis-ci for continuous integration.  You can avoid
build failures by running the following checks before submitting your pull request:

    make unittest
    make lint
    # The next two commands may change your code.  It is recommended you commit
    # before running them.
    make clang-tidy # requires clang-tidy is installed
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
[2]: https://github.com/apache/arrow/blob/master/cpp/doc/Windows.md
