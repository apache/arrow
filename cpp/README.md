# Arrow C++

## Setup Build Environment

Arrow uses CMake as a build configuration system. Currently, it supports in-source and
out-of-source builds with the latter one being preferred.

Arrow requires a C++11-enabled compiler. On Linux, gcc 4.8 and higher should be
sufficient.

To build the thirdparty build dependencies, run:

```
./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh
source ./thirdparty/set_thirdparty_env.sh
```

You can also run from the root of the C++ tree

```
source setup_build_env.sh
```

Arrow is configured to use the `thirdparty` directory by default for its build
dependencies. To set up a custom toolchain see below.

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
