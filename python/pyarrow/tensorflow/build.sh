#!/usr/bin/env bash

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

set -ex

PYARROW_TENSORFLOW_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

TF_CFLAGS=$(python -c 'import tensorflow as tf; print(" ".join(tf.sysconfig.get_compile_flags()))')
TF_LFLAGS=$(python -c 'import tensorflow as tf; print(" ".join(tf.sysconfig.get_link_flags()))')

if [ "$(uname)" == "Darwin" ]; then
    TF_CFLAGS="-undefined dynamic_lookup ${TF_CFLAGS}"
fi

NDEBUG="-DNDEBUG"

g++ -std=c++11 -g -shared $PYARROW_TENSORFLOW_DIR/plasma_op.cc -o $PYARROW_TENSORFLOW_DIR/plasma_op.so \
    ${NDEBUG} \
    `pkg-config --cflags --libs plasma arrow arrow-python` \
    -fPIC \
    ${TF_CFLAGS[@]} \
    ${TF_LFLAGS[@]} \
    -O2
