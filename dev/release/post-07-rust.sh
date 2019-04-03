#!/bin/bash
# -*- indent-tabs-mode: nil; sh-indentation: 2; sh-basic-offset: 2 -*-
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
#
set -e
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  exit
fi

version=$1

: ${INSTALL_RUST:=no}

if [ "${INSTALL_RUST}" == "yes" ]; then
  export RUSTUP_HOME="$(pwd)/release-rustup"
  export CARGO_HOME="${RUSTUP_HOME}"
  rm -rf "${RUSTUP_HOME}"
  curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path
  export PATH="${RUSTUP_HOME}/bin:$PATH"
  source "${RUSTUP_HOME}/env"
  rustup default stable
  cargo login
fi

archive_name=apache-arrow-${version}
tar_gz=${archive_name}.tar.gz
rm -f ${tar_gz}
curl \
  --remote-name \
  --fail \
  https://www-us.apache.org/dist/arrow/arrow-${version}/${tar_gz}
rm -rf ${archive_name}
tar xf ${tar_gz}
modules=()
for cargo_toml in ${archive_name}/rust/*/Cargo.toml; do
  module_dir=$(dirname ${cargo_toml})
  pushd ${module_dir}
  cargo publish --allow-dirty
  modules+=($(basename ${module_dir}))
  popd
done
popd
rm -rf ${archive_name}
rm -f ${tar_gz}

if [ "${INSTALL_RUST}" == "yes" ]; then
  rm -rf "${RUSTUP_HOME}"
fi

echo "Success! The released packages are available here:"
for module in ${modules}; do
  echo "  https://crates.io/crates/${module}/${version}"
done
