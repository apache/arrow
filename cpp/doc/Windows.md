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

# Developing Arrow C++ on Windows

## System setup, conda, and conda-forge

Since some of the Arrow developers work in the Python ecosystem, we are
investing time in maintaining the thirdparty build dependencies for Arrow and
related C++ libraries using the conda package manager. Others are free to add
other development instructions for Windows here.

### Visual Studio

Microsoft provides the free Visual Studio 2017 Community edition. When doing
development, you must launch the developer command prompt using

```"C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64```

It's easiest to configure a console emulator like [cmder][3] to automatically
launch this when starting a new development console.

### conda and package toolchain

[Miniconda][1] is a minimal Python distribution including the conda package
manager. To get started, download and install a 64-bit distribution.

We recommend using packages from [conda-forge][2]

```shell
conda config --add channels conda-forge
```

Now, you can bootstrap a build environment

```shell
conda create -n arrow-dev cmake git boost
```

## Building with NMake

Activate your conda build environment:

```
activate arrow-dev
```

Now, do an out of source build using `nmake`:

```
cd cpp
mkdir build
cd build
cmake -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=Release ..
nmake
```

When using conda, only release builds are currently supported.

## Build using Visual Studio (MSVC) Solution Files

To build on the command line by instead generating a MSVC solution, instead
run:

```
cmake -G "Visual Studio 14 2015 Win64" -DCMAKE_BUILD_TYPE=Release ..
cmake --build . --config Release
```

[1]: https://conda.io/miniconda.html
[2]: https://conda-forge.github.io/
[3]: http://cmder.net/