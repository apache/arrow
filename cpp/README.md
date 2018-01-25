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
git clone https://github.com/apache/arrow.git
cd arrow
brew update && brew bundle --file=c_glib/Brewfile
```

If you are developing on Windows, see the [Windows developer guide][2].

## Building Arrow

Simple debug build:

    git clone https://github.com/apache/arrow.git
    cd arrow/cpp
    mkdir debug
    cd debug
    cmake ..
    make unittest

Simple release build:

    git clone https://github.com/apache/arrow.git
    cd arrow/cpp
    mkdir release
    cd release
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make unittest

Detailed unit test logs will be placed in the build directory under `build/test-logs`.

On some Linux distributions, running the test suite might require setting an
explicit locale. If you see any locale-related errors, try setting the
environment variable (which requires the `locales` package or equivalent):

```
export LC_ALL="en_US.UTF-8"
```

### Statically linking to Arrow on Windows

The Arrow headers on Windows static library builds (enabled by the CMake
option `ARROW_BUILD_STATIC`) use the preprocessor macro `ARROW_STATIC` to
suppress dllimport/dllexport marking of symbols. Projects that statically link
against Arrow on Windows additionally need this definition. The Unix builds do
not use the macro.

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

### Building Python integration library (optional)

The optional `arrow_python` shared library can be built by passing
`-DARROW_PYTHON=on` to CMake. This must be installed or in your library load
path to be able to build pyarrow, the Arrow Python bindings.

The Python library must be built against the same Python version for which you
are building pyarrow, e.g. Python 2.7 or Python 3.6. NumPy must also be
installed.

### Building GPU extension library (optional)

The optional `arrow_gpu` shared library can be built by passing
`-DARROW_GPU=on`. This requires a CUDA installation to build, and to use many
of the functions you must have a functioning GPU. Currently only CUDA
functionality is supported, though if there is demand we can also add OpenCL
interfaces in this library as needed.

The CUDA toolchain used to build the library can be customized by using the
`$CUDA_HOME` environment variable.

This library is still in Alpha stages, and subject to API changes without
deprecation warnings.

### Building Apache ORC integration (optional)

The optional arrow reader for the Apache ORC format (found in the
`arrow::adapters::orc` namespace) can be built by passing `-DARROW_ORC=on`.
This is currently not supported on windows. Note that this functionality is
still in Alpha stages, and subject to API changes without deprecation warnings.

### API documentation

To generate the (html) API documentation, run the following command in the apidoc
directoy:

    doxygen Doxyfile

This requires [Doxygen](http://www.doxygen.org) to be installed.

## Development

This project follows [Google's C++ Style Guide][3] with minor exceptions. We do
not encourage anonymous namespaces and we relax the line length restriction to
90 characters.

### Memory Pools

We provide a default memory pool with `arrow::default_memory_pool()`. As a
matter of convenience, some of the array builder classes have constructors
which use the default pool without explicitly passing it. You can disable these
constructors in your application (so that you are accounting properly for all
memory allocations) by defining `ARROW_NO_DEFAULT_MEMORY_POOL`.

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

### Extra debugging help

If you use the CMake option `-DARROW_EXTRA_ERROR_CONTEXT=ON` it will compile
the libraries with extra debugging information on error checks inside the
`RETURN_NOT_OK` macro. In unit tests with `ASSERT_OK`, this will yield error
outputs like:


```
../src/arrow/ipc/ipc-read-write-test.cc:609: Failure
Failed
NotImplemented: ../src/arrow/ipc/ipc-read-write-test.cc:574 code: writer->WriteRecordBatch(batch)
../src/arrow/ipc/writer.cc:778 code: CheckStarted()
../src/arrow/ipc/writer.cc:755 code: schema_writer.Write(&dictionaries_)
../src/arrow/ipc/writer.cc:730 code: WriteSchema()
../src/arrow/ipc/writer.cc:697 code: WriteSchemaMessage(schema_, dictionary_memo_, &schema_fb)
../src/arrow/ipc/metadata-internal.cc:651 code: SchemaToFlatbuffer(fbb, schema, dictionary_memo, &fb_schema)
../src/arrow/ipc/metadata-internal.cc:598 code: FieldToFlatbuffer(fbb, *schema.field(i), dictionary_memo, &offset)
../src/arrow/ipc/metadata-internal.cc:508 code: TypeToFlatbuffer(fbb, *field.type(), &children, &layout, &type_enum, dictionary_memo, &type_offset)
Unable to convert type: decimal(19, 4)
```

### Deprecations and API Changes

We use the compiler definition `ARROW_NO_DEPRECATED_API` to disable APIs that
have been deprecated. It is a good practice to compile third party applications
with this flag to proactively catch and account for API changes.

### Keeping includes clean with include-what-you-use

We have provided a `build-support/iwyu/iwyu.sh` convenience script for invoking
Google's [include-what-you-use][4] tool, also known as IWYU. This includes
various suppressions for more informative output. After building IWYU
(following instructions in the README), you can run it on all files by running:

```shell
CC="clang-4.0" CXX="clang++-4.0" cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
../build-support/iwyu/iwyu.sh all
```

This presumes that `include-what-you-use` and `iwyu_tool.py` are in your
`$PATH`. If you compiled IWYU using a different version of clang, then
substitute the version number above accordingly. The results of this script are
logged to a temporary file, whose location can be found by examining the shell
output:

```
...
Logging IWYU to /tmp/arrow-cpp-iwyu.gT7XXV
...
```

### Linting

We require that you follow a certain coding style in the C++ code base.
You can check your code abides by that coding style by running:

    make lint

You can also fix any formatting errors automatically:

    make format

These commands require `clang-format-4.0` (and not any other version).
You may find the required packages at http://releases.llvm.org/download.html
or use the Debian/Ubuntu APT repositories on https://apt.llvm.org/.

Also, if under a Python 3 environment, you need to install a compatible
version of `cpplint` using `pip install cpplint`.

## Continuous Integration

Pull requests are run through travis-ci for continuous integration.  You can avoid
build failures by running the following checks before submitting your pull request:

    make unittest
    make lint
    # The next command may change your code.  It is recommended you commit
    # before running it.
    make format # requires clang-format is installed

We run our CI builds with more compiler warnings enabled for the Clang
compiler. Please run CMake with

`-DBUILD_WARNING_LEVEL=CHECKIN`

to avoid failures due to compiler warnings.

Note that the clang-tidy target may take a while to run.  You might consider
running clang-tidy separately on the files you have added/changed before
invoking the make target to reduce iteration time.  Also, it might generate warnings
that aren't valid.  To avoid these you can add a line comment `// NOLINT`. If
NOLINT doesn't suppress the warnings, you add the file in question to
the .clang-tidy-ignore file.  This will allow `make check-clang-tidy` to pass in
travis-CI (but still surface the potential warnings in `make clang-tidy`). Ideally,
both of these options would be used rarely. Current known uses-cases when they are required:

*  Parameterized tests in google test.

[1]: https://brew.sh/
[2]: https://github.com/apache/arrow/blob/master/cpp/apidoc/Windows.md
[3]: https://google.github.io/styleguide/cppguide.html
[4]: https://github.com/include-what-you-use/include-what-you-use
