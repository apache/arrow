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

wget -O - http://llvm.org/apt/llvm-snapshot.gpg.key|apt-key add -
apt-add-repository -y \
     "deb http://llvm.org/apt/xenial/ llvm-toolchain-xenial-6.0 main"
apt-get update -qq
apt-get install -y -q libclang-6.0-dev clang-6.0 clang-format-6.0 clang-tidy-6.0
