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

* brotli: `BROTLI_HOME`, can be disabled with `-DARROW_WITH_BROTLI=off`
* Boost: `BOOST_ROOT`
* double-conversion: `DOUBLE_CONVERSION_HOME`
* Googletest: `GTEST_HOME` (only required to build the unit tests)
* gflags: `GFLAGS_HOME` (only required to build the unit tests)
* glog: `GLOG_HOME` (only required if `ARROW_USE_GLOG=ON`)
* Google Benchmark: `GBENCHMARK_HOME` (only required if building benchmarks)
* Flatbuffers: `FLATBUFFERS_HOME` (only required for -DARROW_IPC=on, which is
  the default)
* Hadoop: `HADOOP_HOME` (only required for the HDFS I/O extensions)
* jemalloc: `JEMALLOC_HOME`
* lz4: `LZ4_HOME`, can be disabled with `-DARROW_WITH_LZ4=off`
* Apache ORC: `ORC_HOME`
* protobuf: `PROTOBUF_HOME`
* rapidjson: `RAPIDJSON_HOME`
* re2: `RE2_HOME` (only required to build Gandiva currently)
* snappy: `SNAPPY_HOME`, can be disabled with `-DARROW_WITH_SNAPPY=off`
* thrift: `THRIFT_HOME`
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
# Download tarballs into `$HOME/arrow-thirdparty-deps`
$ ./thirdparty/download_dependencies $HOME/arrow-thirdparty
# Environment variables for offline Arrow build
export ARROW_BOOST_URL=$HOME/arrow-thirdparty/boost-1.67.0.tar.gz
export ARROW_BROTLI_URL=$HOME/arrow-thirdparty/brotli-v0.6.0.tar.gz
export ARROW_DOUBLE_CONVERSION_URL=$HOME/arrow-thirdparty/double-conversion-v3.1.1.tar.gz
export ARROW_FLATBUFFERS_URL=$HOME/arrow-thirdparty/flatbuffers-02a7807dd8d26f5668ffbbec0360dc107bbfabd5.tar.gz
export ARROW_GBENCHMARK_URL=$HOME/arrow-thirdparty/gbenchmark-v1.4.1.tar.gz
export ARROW_GFLAGS_URL=$HOME/arrow-thirdparty/gflags-v2.2.0.tar.gz
export ARROW_GLOG_URL=$HOME/arrow-thirdparty/glog-v0.3.5.tar.gz
export ARROW_GRPC_URL=$HOME/arrow-thirdparty/grpc-v1.14.1.tar.gz
export ARROW_GTEST_URL=$HOME/arrow-thirdparty/gtest-1.8.0.tar.gz
export ARROW_LZ4_URL=$HOME/arrow-thirdparty/lz4-v1.7.5.tar.gz
export ARROW_ORC_URL=$HOME/arrow-thirdparty/orc-1.5.4.tar.gz
export ARROW_PROTOBUF_URL=$HOME/arrow-thirdparty/protobuf-v3.6.1.tar.gz
export ARROW_RAPIDJSON_URL=$HOME/arrow-thirdparty/rapidjson-v1.1.0.tar.gz
export ARROW_RE2_URL=$HOME/arrow-thirdparty/re2-2018-10-01.tar.gz
export ARROW_SNAPPY_URL=$HOME/arrow-thirdparty/snappy-1.1.3.tar.gz
export ARROW_THRIFT_URL=$HOME/arrow-thirdparty/thrift-0.11.0.tar.gz
export ARROW_ZLIB_URL=$HOME/arrow-thirdparty/zlib-1.2.8.tar.gz
export ARROW_ZSTD_URL=$HOME/arrow-thirdparty/zstd-v1.3.7.tar.gz
```

This can be automated by using inline source/eval:

```shell
$ source <(./thirdparty/download_dependencies $HOME/arrow-thirdparty-deps)
```

You can then invoke CMake to create the build directory and it will use the
declared environment variable pointing to downloaded archives instead of
downloading them (one for each build dir!).
