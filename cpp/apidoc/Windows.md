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

# Developing Arrow C++ on Windows

## System setup, conda, and conda-forge

Since some of the Arrow developers work in the Python ecosystem, we are
investing time in maintaining the thirdparty build dependencies for Arrow and
related C++ libraries using the conda package manager. Others are free to add
other development instructions for Windows here.

### conda and package toolchain

[Miniconda][1] is a minimal Python distribution including the conda package
manager. To get started, download and install a 64-bit distribution.

We recommend using packages from [conda-forge][2].
Launch cmd.exe and run following commands:

```shell
conda config --add channels conda-forge
```

Now, you can bootstrap a build environment

```shell
conda create -n arrow-dev cmake git boost-cpp flatbuffers rapidjson cmake thrift-cpp snappy zlib brotli gflags lz4-c zstd -c conda-forge
```

***Note:***
> *Make sure to get the `conda-forge` build of `gflags` as the
  naming of the library differs from that in the `defaults` channel*

Activate just created conda environment with pre-installed packages from
previous step:

```shell
activate arrow-dev
```

We are using [cmake][4] tool to support Windows builds.
To allow cmake to pick up 3rd party dependencies, you should set
`ARROW_BUILD_TOOLCHAIN` environment variable to contain `Library` folder
path of new created on previous step `arrow-dev` conda environment.
For instance, if `Miniconda` was installed to default destination, `Library`
folder path for `arrow-dev` conda environment will be as following:

```shell
C:\Users\YOUR_USER_NAME\Miniconda3\envs\arrow-dev\Library
```

To set `ARROW_BUILD_TOOLCHAIN` environment variable visible only for current terminal session you can run following:
```shell
set ARROW_BUILD_TOOLCHAIN=C:\Users\YOUR_USER_NAME\Miniconda3\envs\arrow-dev\Library
```

To validate value of `ARROW_BUILD_TOOLCHAIN` environment variable you can run following terminal command:
```shell
echo %ARROW_BUILD_TOOLCHAIN%
```

As alternative to `ARROW_BUILD_TOOLCHAIN`, it's possible to configure path
to each 3rd party dependency separately by setting appropriate environment
variable:

`FLATBUFFERS_HOME` variable with path to `flatbuffers` installation
`RAPIDJSON_HOME` variable with path to `rapidjson` installation
`GFLAGS_HOME` variable with path to `gflags` installation
`SNAPPY_HOME` variable with path to `snappy` installation
`ZLIB_HOME` variable with path to `zlib` installation
`BROTLI_HOME` variable with path to `brotli` installation
`LZ4_HOME` variable with path to `lz4` installation
`ZSTD_HOME` variable with path to `zstd` installation

### Customize static libraries names lookup of 3rd party dependencies

If you decided to use pre-built 3rd party dependencies libs, it's possible to
configure Arrow's cmake build script to search for customized names of 3rd
party static libs.

`zlib`. Pass `-DARROW_ZLIB_VENDORED=OFF` to enable lookup of custom zlib
build. Set `ZLIB_HOME` environment variable. Pass
`-DZLIB_MSVC_STATIC_LIB_SUFFIX=%ZLIB_SUFFIX%` to link with z%ZLIB_SUFFIX%.lib

`brotli`. Set `BROTLY_HOME` environment variable. Pass
`-DBROTLI_MSVC_STATIC_LIB_SUFFIX=%BROTLI_SUFFIX%` to link with
brotli*%BROTLI_SUFFIX%.lib.

`snappy`. Set `SNAPPY_HOME` environment variable. Pass
`-DSNAPPY_MSVC_STATIC_LIB_SUFFIX=%SNAPPY_SUFFIX%` to link with
snappy%SNAPPY_SUFFIX%.lib.

`lz4`. Set `LZ4_HOME` environment variable. Pass
`-LZ4_MSVC_STATIC_LIB_SUFFIX=%LZ4_SUFFIX%` to link with
lz4%LZ4_SUFFIX%.lib.

`zstd`. Set `ZSTD_HOME` environment variable. Pass
`-ZSTD_MSVC_STATIC_LIB_SUFFIX=%ZSTD_SUFFIX%` to link with
zstd%ZSTD_SUFFIX%.lib.

### Visual Studio

Microsoft provides the free Visual Studio Community edition. When doing
development, you must launch the developer command prompt using

#### Visual Studio 2015

```"C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64```

#### Visual Studio 2017

```"C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64```

It's easiest to configure a console emulator like [cmder][3] to automatically
launch this when starting a new development console.

## Building with NMake

Activate your conda build environment:

```
activate arrow-dev
```

Change working directory in cmd.exe to the root directory of Arrow and
do an out of source build using `nmake`:

```
cd cpp
mkdir build
cd build
cmake -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=Release ..
nmake
```

When using conda, only release builds are currently supported.

## Build using Visual Studio (MSVC) Solution Files

Activate your conda build environment:

```
activate arrow-dev
```

Change working directory in cmd.exe to the root directory of Arrow and
do an out of source build by generating a MSVC solution:

```
cd cpp
mkdir build
cd build
cmake -G "Visual Studio 14 2015 Win64" -DCMAKE_BUILD_TYPE=Release ..
cmake --build . --config Release
```

## Debug build

To build Debug version of Arrow you should have pre-insalled Debug version of
boost libs.

It's recommended to configure cmake build with following variables for Debug build:

`-DARROW_BOOST_USE_SHARED=OFF` - enables static linking with boost debug libs and
simplifies run-time loading of 3rd parties. (Recommended)

`-DBOOST_ROOT` - sets the root directory of boost libs. (Optional)

`-DBOOST_LIBRARYDIR` - sets the directory with boost lib files. (Optional)

Command line to build Arrow in Debug might look as following:

```
cd cpp
mkdir build
cd build
cmake -G "Visual Studio 14 2015 Win64" ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=Debug ^
      -DBOOST_ROOT=C:/local/boost_1_63_0  ^
      -DBOOST_LIBRARYDIR=C:/local/boost_1_63_0/lib64-msvc-14.0 ^
      ..
cmake --build . --config Debug
```

To get the latest build instructions, you can reference [msvc-build.bat][5], which is used by automated Appveyor builds.


[1]: https://conda.io/miniconda.html
[2]: https://conda-forge.github.io/
[3]: http://cmder.net/
[4]: https://cmake.org/
[5]: https://github.com/apache/arrow/blob/master/ci/msvc-build.bat
