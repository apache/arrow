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

# Arrow C++ Thirdparty Dependencies

The version numbers for our third-party dependencies are listed in
`thirdparty/versions.txt`. This is used by the CMake build system as well as
the dependency downloader script (see below), which can be used to set up
offline builds.

## Configuring your own build toolchain

To set up your own specific build toolchain, here are the relevant environment
variables

* Boost: `BOOST_ROOT`
* Googletest: `GTEST_HOME` (only required to build the unit tests)
* gflags: `GFLAGS_HOME` (only required to build the unit tests)
* Google Benchmark: `GBENCHMARK_HOME` (only required if building benchmarks)
* Flatbuffers: `FLATBUFFERS_HOME` (only required for -DARROW_IPC=on, which is
  the default)
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

## Configuring for offline builds

If you do not use the above variables to direct the Arrow build system to
preinstalled dependencies, they will be built automatically by the build
system. The source archive for each dependency will be downloaded via the
internet, which can cause issues in environments with limited access to the
internet.

To enable offline builds, you can download the source artifacts yourself and
use environment variables of the form `ARROW_$LIBRARY_URL` to direct the build
system to read from a local file rather than accessing the internet.

To make this easier for you, we have prepared a script
`thirdparty/download_dependencies.sh` which will download the correct version
of each dependency to a directory of your choosing. It will print a list of
bash-style environment variable statements at the end to use for your build
script:

```shell
$ ./thirdparty/download_dependencies $HOME/arrow-thirdparty-deps
# some output omitted

# Environment variables for offline Arrow build
export ARROW_BOOST_URL=$HOME/arrow-thirdparty-deps/boost.tar.gz
export ARROW_GTEST_URL=$HOME/arrow-thirdparty-deps/gtest.tar.gz
export ARROW_GFLAGS_URL=$HOME/arrow-thirdparty-deps/gflags.tar.gz
export ARROW_GBENCHMARK_URL=$HOME/arrow-thirdparty-deps/gbenchmark.tar.gz
export ARROW_FLATBUFFERS_URL=$HOME/arrow-thirdparty-deps/flatbuffers.tar.gz
export ARROW_RAPIDJSON_URL=$HOME/arrow-thirdparty-deps/rapidjson.tar.gz
export ARROW_SNAPPY_URL=$HOME/arrow-thirdparty-deps/snappy.tar.gz
export ARROW_BROTLI_URL=$HOME/arrow-thirdparty-deps/brotli.tar.gz
export ARROW_LZ4_URL=$HOME/arrow-thirdparty-deps/lz4.tar.gz
export ARROW_ZLIB_URL=$HOME/arrow-thirdparty-deps/zlib.tar.gz
export ARROW_ZSTD_URL=$HOME/arrow-thirdparty-deps/zstd.tar.gz
export ARROW_PROTOBUF_URL=$HOME/arrow-thirdparty-deps/protobuf.tar.gz
export ARROW_GRPC_URL=$HOME/arrow-thirdparty-deps/grpc.tar.gz
export ARROW_ORC_URL=$HOME/arrow-thirdparty-deps/orc.tar.gz
```
