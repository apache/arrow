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

ARG base
FROM ${base}

RUN apt-get update && \
    apt-get install -y -q \
        clang-7 \
        libclang-7-dev \
        clang-format-7 \
        clang-tidy-7 \
        clang-tools-7 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget -O /usr/local/bin/fuzzit https://github.com/fuzzitdev/fuzzit/releases/latest/download/fuzzit_Linux_x86_64 && \
    chmod a+x /usr/local/bin/fuzzit

ENV ARROW_FUZZING="ON" \
    ARROW_USE_ASAN="ON" \
    CC="clang-7" \
    CXX="clang++-7" \
    ARROW_BUILD_TYPE="RelWithDebInfo" \
    ARROW_FLIGHT="OFF" \
    ARROW_GANDIVA="OFF" \
    ARROW_ORC="OFF" \
    ARROW_PARQUET="OFF" \
    ARROW_PLASMA="OFF" \
    ARROW_WITH_BZ2="OFF" \
    ARROW_WITH_ZSTD="OFF" \
    ARROW_BUILD_BENCHMARKS="OFF" \
    ARROW_BUILD_UTILITIES="OFF"
