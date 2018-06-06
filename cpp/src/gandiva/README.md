# Gandiva C++

## System setup

Gandiva uses CMake as a build configuration system. Currently, it supports
out-of-source builds only.

Build Gandiva requires:

* A C++11-enabled compiler. On Linux, gcc 4.8 and higher should be sufficient.
* CMake
* LLVM
* Arrow
* Boost
* Protobuf

On macOS, you can use [Homebrew][1]:

```shell
brew install cmake llvm boost protobuf
```

To install arrow, follow the steps in the [arrow Readme][2].
## Building Gandiva

Debug build :

```shell
git clone https://github.com/dremio/gandiva.git
cd gandiva/cpp
mkdir debug
cd debug
cmake ..
make test
```

Release build :

```shell
git clone https://github.com/dremio/gandiva.git
cd gandiva/cpp
mkdir release
cd release
cmake .. -DCMAKE_BUILD_TYPE=Release
make test
```

## Validating code style

We follow the [google cpp code style][3]. To validate compliance,

```shell
cd debug
make lint
```

[1]: https://brew.sh/
[2]: https://github.com/apache/arrow/tree/master/cpp
[3]: https://google.github.io/styleguide/cppguide.html
