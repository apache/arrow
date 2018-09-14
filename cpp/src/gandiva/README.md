# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
* re2

On macOS, you can use [Homebrew][1]:

```shell
brew install cmake llvm boost protobuf re2
```

To install arrow, follow the steps in the [arrow Readme][2].
## Building Gandiva

Debug build :

```shell
git clone https://github.com/apache/arrow.git
cd cpp/src/gandiva
mkdir debug
cd debug
cmake ..
make
ctest
```

Release build :

```shell
git clone https://github.com/apache/arrow.git
cd cpp/src/gandiva
mkdir release
cd release
cmake .. -DCMAKE_BUILD_TYPE=Release
make
ctest
```

## Validating code style

We follow the [google cpp code style][3]. To validate compliance,

```shell
cd debug
make stylecheck
```

## Fixing code style

```shell
cd debug
make stylefix
```

[1]: https://brew.sh/
[2]: https://github.com/apache/arrow/tree/master/cpp
[3]: https://google.github.io/styleguide/cppguide.html
