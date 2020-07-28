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

# https://www.msys2.org/news/#2020-06-29-new-packagers
msys2_keyring_pkg=msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz
for suffix in "" ".sig"; do
  curl \
    --location \
    --remote-name \
    --show-error \
    --silent \
    https://repo.msys2.org/msys/x86_64/${msys2_keyring_pkg}${suffix}
done
pacman-key --verify ${msys2_keyring_pkg}.sig
pacman \
  --noconfirm \
  --upgrade \
  ${msys2_keyring_pkg}


pacman \
  --noconfirm \
  --refresh \
  --refresh \
  --sync \
  --sysupgrade \
  --sysupgrade
