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
    make
    ctest

Simple release build:

    mkdir release
    cd release
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make
    ctest

### Third-party environment variables

To set up your own specific build toolchain, here are the relevant environment
variables

* Googletest: `GTEST_HOME` (only required to build the unit tests)
