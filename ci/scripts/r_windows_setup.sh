#!/bin/bash

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

if [ "$RTOOLS_VERSION" = "35" ]; then
  # Use rtools-backports if building with rtools35
  curl https://raw.githubusercontent.com/r-windows/rtools-backports/master/pacman.conf > /etc/pacman.conf
  # Update keys: https://www.msys2.org/news/#2020-06-29-new-packagers
  msys2_repo_base_url=https://repo.msys2.org/msys
  # Mirror
  msys2_repo_base_url=https://sourceforge.net/projects/msys2/files/REPOS/MSYS2
  curl -OSsL "${msys2_repo_base_url}/x86_64/msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz"
  pacman -U --noconfirm msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz && rm msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz
  # Use sf.net instead of http://repo.msys2.org/ temporary.
  sed -i -e "s,^Server = http://repo\.msys2\.org/msys,Server = ${msys2_repo_base_url},g" \
    /etc/pacman.conf
  pacman --noconfirm -Scc
  pacman --noconfirm -Syy
  # For actions/cache
  pacman --noconfirm -S zstd
else
  # Uncomment L38-41 if you're testing a new rtools dependency that hasn't yet sync'd to CRAN
  # curl https://raw.githubusercontent.com/r-windows/rtools-packages/master/pacman.conf > /etc/pacman.conf
  # curl -OSsl "http://repo.msys2.org/msys/x86_64/msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz"
  # pacman -U --noconfirm msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz && rm msys2-keyring-r21.b39fb11-1-any.pkg.tar.xz
  # pacman --noconfirm -Scc

  pacman --noconfirm -Syy
fi

which tar
which pacman
which zstd

"$(dirname $0)/ccache_setup.sh"
echo "CCACHE_DIR=$(cygpath --absolute --windows ccache)" >> $GITHUB_ENV
