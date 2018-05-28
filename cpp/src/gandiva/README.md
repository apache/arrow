# Gandiva C++

## System setup

Gandiva uses CMake as a build configuration system. Currently, it supports 
in-source and out-of-source builds with the latter one being preferred.

Build Gandiva requires:

* A C++11-enabled compiler. On Linux, gcc 4.8 and higher should be sufficient.
* CMake
* LLVM 
* Arrow
* GTest
* Boost

On OS X, you can use [Homebrew][1]:

```shell
brew install cmake llvm boost
```

## Building Gandiva

    mkdir build
    cd build 
    cmake ..
    make
    
## Validating code style

We follow the [google cpp code style][2]. To validate compliance,

    make lint

[1]: https://brew.sh/
[2]: https://google.github.io/styleguide/cppguide.html
