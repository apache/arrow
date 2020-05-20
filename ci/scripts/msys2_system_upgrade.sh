#!/usr/bin/env bash
#
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

set -eux

# Update pacman manually from old MSYS2.
# See also: https://github.com/msys2/MSYS2-packages/issues/1960
pacman --noconfirm --refresh --sync zstd
wget -q http://repo.msys2.org/msys/x86_64/pacman-5.2.1-7-x86_64.pkg.tar.zst
zstd -d pacman-5.2.1-7-x86_64.pkg.tar.zst
gzip pacman-5.2.1-7-x86_64.pkg.tar
pacman --noconfirm --upgrade ./pacman-5.2.1-7-x86_64.pkg.tar.gz

pacman \
  --noconfirm \
  --sync \
  -uu \
  -yy
