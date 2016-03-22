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

