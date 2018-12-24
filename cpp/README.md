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

# Apache Arrow C++ codebase

This directory contains the code and build system for the Arrow C++ libraries,
as well as for the C++ libraries for Apache Parquet.

## System setup

Arrow uses CMake as a build configuration system. Currently, it supports
in-source and out-of-source builds with the latter one being preferred.

Building Arrow requires:

* A C++11-enabled compiler. On Linux, gcc 4.8 and higher should be sufficient.
* CMake 3.2 or higher
* Boost

On Ubuntu/Debian you can install the requirements with:

```shell
sudo apt-get install \
     autoconf \
     build-essential \
     cmake \
     libboost-dev \
     libboost-filesystem-dev \
     libboost-regex-dev \
     libboost-system-dev
```

On Alpine Linux:

```shell
apk add autoconf \
        bash \
        boost-dev \
        cmake \
        g++ \
        gcc \
        make
```

On macOS, you can use [Homebrew][1]:

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
    cmake -DARROW_BUILD_TESTS=ON ..
    make unittest

Simple release build:

    git clone https://github.com/apache/arrow.git
    cd arrow/cpp
    mkdir release
    cd release
    cmake -DARROW_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release ..
    make unittest

If you do not need to build the test suite, you can omit the
`ARROW_BUILD_TESTS` option (the default is not to build the unit tests).

Detailed unit test logs will be placed in the build directory under
`build/test-logs`.

On some Linux distributions, running the test suite might require setting an
explicit locale. If you see any locale-related errors, try setting the
environment variable (which requires the `locales` package or equivalent):

```
export LC_ALL="en_US.UTF-8"
```

## Modular Build Targets

Since there are several major parts of the C++ project, we have provided
modular CMake targets for building each library component, group of unit tests
and benchmarks, and their dependencies:

* `make arrow` for Arrow core libraries
* `make parquet` for Parquet libraries
* `make gandiva` for Gandiva (LLVM expression compiler) libraries
* `make plasma` for Plasma libraries, server

To build the unit tests or benchmarks, add `-tests` or `-benchmarks` to the
target name. So `make arrow-tests` will build the Arrow core unit tests. Using
the `-all` target, e.g. `parquet-all`, will build everything.

If you wish to only build and install one or more project subcomponents, we
have provided the CMake option `ARROW_OPTIONAL_INSTALL` to only install targets
that have been built. For example, if you only wish to build the Parquet
libraries, its tests, and its dependencies, you can run:

```
cmake .. -DARROW_PARQUET=ON -DARROW_OPTIONAL_INSTALL=ON -DARROW_BUILD_TESTS=ON
make parquet
make install
```

If you omit an explicit target when invoking `make`, all targets will be built.

## Parquet Development Notes

To build the C++ libraries for Apache Parquet, add the flag
`-DARROW_PARQUET=ON` when invoking CMake. The Parquet libraries and unit tests
can be built with the `parquet` make target:

```shell
make parquet
```

Running `ctest -L unittest` will run all built C++ unit tests, while `ctest -L
parquet` will run only the Parquet unit tests. The unit tests depend on an
environment variable `PARQUET_TEST_DATA` that depends on a git submodule to the
repository https://github.com/apache/parquet-testing:

```shell
git submodule update --init
export PARQUET_TEST_DATA=$ARROW_ROOT/cpp/submodules/parquet-testing/data
```

Here `$ARROW_ROOT` is the absolute path to the Arrow codebase.

### Statically linking to Arrow on Windows

The Arrow headers on Windows static library builds (enabled by the CMake
option `ARROW_BUILD_STATIC`) use the preprocessor macro `ARROW_STATIC` to
suppress dllimport/dllexport marking of symbols. Projects that statically link
against Arrow on Windows additionally need this definition. The Unix builds do
not use the macro.

### Building/Running benchmarks

Follow the directions for simple build except run cmake
with the `--ARROW_BUILD_BENCHMARKS` parameter set correctly:

    cmake -DARROW_BUILD_TESTS=ON -DARROW_BUILD_BENCHMARKS=ON ..

and instead of make unittest run either `make; ctest` to run both unit tests
and benchmarks or `make benchmark` to run only the benchmark tests.

Benchmark logs will be placed in the build directory under `build/benchmark-logs`.

### Testing with LLVM AddressSanitizer

To use AddressSanitizer (ASAN) to find bad memory accesses or leaks with LLVM,
pass `-DARROW_USE_ASAN=ON` when building. You must use clang to compile with
ASAN, and `ARROW_USE_ASAN` is mutually-exclusive with the valgrind option
`ARROW_TEST_MEMCHECK`.

### Building/Running fuzzers

Fuzzers can help finding unhandled exceptions and problems with untrusted input
that may lead to crashes, security issues and undefined behavior. They do this
by generating random input data and observing the behavior of the executed
code. To build the fuzzer code, LLVM is required (GCC-based compilers won't
work). You can build them using the following code:

    cmake -DARROW_FUZZING=ON -DARROW_USE_ASAN=ON ..

`ARROW_FUZZING` will enable building of fuzzer executables as well as enable the
addition of coverage helpers via `ARROW_USE_COVERAGE`, so that the fuzzer can observe
the program execution.

It is also wise to enable some sanitizers like `ARROW_USE_ASAN` (see above), which
activates the address sanitizer. This way, we ensure that bad memory operations
provoked by the fuzzer will be found early. You may also enable other sanitizers as
well. Just keep in mind that some of them do not work together and some may result
in very long execution times, which will slow down the fuzzing procedure.

Now you can start one of the fuzzer, e.g.:

    ./debug/debug/ipc-fuzzing-test

This will try to find a malformed input that crashes the payload and will show the
stack trace as well as the input data. After a problem was found this way, it should
be reported and fixed. Usually, the fuzzing process cannot be continued until the
fix is applied, since the fuzzer usually converts to the problem again.

If you build fuzzers with ASAN, you need to set the `ASAN_SYMBOLIZER_PATH`
environment variable to the absolute path of `llvm-symbolizer`, which is a tool
that ships with LLVM.

```shell
export ASAN_SYMBOLIZER_PATH=$(type -p llvm-symbolizer)
```

Note that some fuzzer builds currently reject paths with a version qualifier
(like `llvm-sanitizer-5.0`). To overcome this, set an appropriate symlink
(here, when using LLVM 5.0):

```shell
ln -sf /usr/bin/llvm-sanitizer-5.0 /usr/bin/llvm-sanitizer
```

There are some problems that may occur during the compilation process:

- libfuzzer was not distributed with your LLVM: `ld: file not found: .../libLLVMFuzzer.a`
- your LLVM is too old: `clang: error: unsupported argument 'fuzzer' to option 'fsanitize='`

### Third-party dependencies and configuration

Arrow depends on a number of third-party libraries. We support these in a few
ways:

* Building dependencies from source by downloading archives from the internet
* Building dependencies from source using from local archives (to allow offline
  builds)
* Building with locally-installed libraries

See [thirdparty/README.md][5] for details about these options and how to
configure your build toolchain.

### Building Python integration library (optional)

The optional `arrow_python` shared library can be built by passing
`-DARROW_PYTHON=on` to CMake. This must be installed or in your library load
path to be able to build pyarrow, the Arrow Python bindings.

The Python library must be built against the same Python version for which you
are building pyarrow, e.g. Python 2.7 or Python 3.6. NumPy must also be
installed.

### Building CUDA extension library (optional)

The optional `arrow_cuda` shared library can be built by passing
`-DARROW_CUDA=on`. This requires a CUDA installation to build, and to use many
of the functions you must have a functioning CUDA-compatible GPU.

The CUDA toolchain used to build the library can be customized by using the
`$CUDA_HOME` environment variable.

This library is still in Alpha stages, and subject to API changes without
deprecation warnings.

### Building Apache ORC integration (optional)

The optional arrow reader for the Apache ORC format (found in the
`arrow::adapters::orc` namespace) can be built by passing `-DARROW_ORC=on`.
This is currently not supported on windows. Note that this functionality is
still in Alpha stages, and subject to API changes without deprecation warnings.

### Building and developing Gandiva (optional)

The Gandiva library supports compiling and evaluating expressions on arrow
data. It uses LLVM for doing just-in-time compilation of the expressions.

In addition to the arrow dependencies, gandiva requires :
* On linux, gcc 4.9 or higher C++11-enabled compiler.
* LLVM

On Ubuntu/Debian you can install these requirements with:

```shell
sudo apt-add-repository -y "deb http://llvm.org/apt/trusty/ llvm-toolchain-trusty-6.0 main"
sudo apt-get update -qq
sudo apt-get install llvm-6.0-dev
```

On macOS, you can use [Homebrew][1]:

```shell
brew install llvm@6
```

The optional `gandiva` libraries and tests can be built by passing
`-DARROW_GANDIVA=on`.

```shell
cmake .. -DARROW_GANDIVA=ON -DARROW_BUILD_TESTS=ON
make
ctest -L gandiva
```

This library is still in Alpha stages, and subject to API changes without
deprecation warnings.

### API documentation

To generate the (html) API documentation, run the following command in the apidoc
directory:

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

### Header files

We use the `.h` extension for C++ header files. Any header file name not
containing `internal` is considered to be a public header, and will be
automatically installed by the build.

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
substitute the version number above accordingly.

We have provided a Docker-based IWYU to make it easier to run these
checks. This can be run using the docker-compose setup in the `dev/` directory

```shell
# If you have not built the base image already
docker build -t arrow_integration_xenial_base -f dev/docker_common/Dockerfile.xenial.base .

dev/run_docker_compose.sh iwyu
```

### Linting

We require that you follow a certain coding style in the C++ code base.
You can check your code abides by that coding style by running:

    make lint

You can also fix any formatting errors automatically:

    make format

These commands require `clang-format-6.0` (and not any other version).
You may find the required packages at http://releases.llvm.org/download.html
or use the Debian/Ubuntu APT repositories on https://apt.llvm.org/. On macOS
with [Homebrew][1] you can get it via `brew install llvm@6`.

Depending on how you installed clang-format, the build system may not be able
to find it. You can provide an explicit path to your LLVM installation (or the
root path for the clang tools) with the environment variable
`$CLANG_TOOLS_PATH` or by passing `-DClangTools_PATH=$PATH_TO_CLANG_TOOLS` when
invoking CMake.

## Checking for ABI and API stability

To build ABI compliance reports, you need to install the two tools
`abi-dumper` and `abi-compliance-checker`.

Build Arrow C++ in Debug mode, alternatively you could use `-Og` which also
builds with the necessary symbols but includes a bit of code optimization.
Once the build has finished, you can generate ABI reports using:

```
abi-dumper -lver 9 debug/libarrow.so -o ABI-9.dump
```

The above version number is freely selectable. As we want to compare versions,
you should now `git checkout` the version you want to compare it to and re-run
the above command using a different version number. Once both reports are
generated, you can build a comparision report using

```
abi-compliance-checker -l libarrow -d1 ABI-PY-9.dump -d2 ABI-PY-10.dump
```

The report is then generated in `compat_reports/libarrow` as a HTML.

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

## CMake version requirements

We support CMake 3.2 and higher. Some features require a newer version of CMake:

* Building the benchmarks requires 3.6 or higher
* Building zstd from source requires 3.7 or higher
* Building Gandiva JNI bindings requires 3.11 or higher

[1]: https://brew.sh/
[2]: https://github.com/apache/arrow/blob/master/cpp/apidoc/Windows.md
[3]: https://google.github.io/styleguide/cppguide.html
[4]: https://github.com/include-what-you-use/include-what-you-use
[5]: https://github.com/apache/arrow/blob/master/cpp/thirdparty/README.md
