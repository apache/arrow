#!/bin/bash
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

set -exu

if [ $# -lt 2 ]; then
  echo "Usage: $0 X.Y.Z IS_RC"
  echo "       $0 X.Y.Z IS_RC BINTRAY_REPOSITORY"
  echo " e.g.: $0 0.13.0 yes           # Verify 0.13.0 RC"
  echo " e.g.: $0 0.13.0 no            # Verify 0.13.0"
  echo " e.g.: $0 0.13.0 yes kou/arrow # Verify 0.13.0 RC at https://bintray.com/kou/arrow"
  exit 1
fi

VERSION="$1"
IS_RC="$2"
BINTRAY_REPOSITORY="${3:-apache/arrow}"

deb_version="${VERSION}-1"

export DEBIAN_FRONTEND=noninteractive

apt update
apt install -y -V \
  apt-transport-https \
  curl \
  gnupg \
  lsb-release

code_name="$(lsb_release --codename --short)"
distribution="$(lsb_release --id --short | tr 'A-Z' 'a-z')"
bintray_base_url="https://dl.bintray.com/${BINTRAY_REPOSITORY}/${distribution}"
if [ "${IS_RC}" = "yes" ]; then
  bintray_base_url="${bintray_base_url}-rc"
fi

have_signed_by=yes
have_python=yes
have_gandiva=yes
need_llvm_apt=no
case "${distribution}-${code_name}" in
  debian-*)
    sed \
      -i"" \
      -e "s/ main$/ main contrib non-free/g" \
      /etc/apt/sources.list
    cat <<APT_LINE > /etc/apt/sources.list.d/backports.list
deb http://deb.debian.org/debian ${code_name}-backports main
APT_LINE
    need_llvm_apt=yes
    ;;
  ubuntu-xenial)
    have_signed_by=no
    need_llvm_apt=yes
    ;;
  ubuntu-trusty)
    have_signed_by=no
    have_python=no
    have_gandiva=no
    ;;
esac

if [ "${have_signed_by}" = "yes" ]; then
  keyring_path="/usr/share/keyrings/apache-arrow-keyring.gpg"
  curl \
    --output "${keyring_path}" \
    "${bintray_base_url}/apache-arrow-keyring.gpg"
  cat <<APT_LINE > /etc/apt/sources.list.d/apache-arrow.list
deb [arch=amd64 signed-by=${keyring_path}] ${bintray_base_url}/ ${code_name} main
deb-src [signed-by=${keyring_path}] ${bintray_base_url}/ ${code_name} main
APT_LINE
else
  curl https://dist.apache.org/repos/dist/dev/arrow/KEYS | apt-key add -
  cat <<APT_LINE > /etc/apt/sources.list.d/apache-arrow.list
deb [arch=amd64] ${bintray_base_url}/ ${code_name} main
deb-src ${bintray_base_url}/ ${code_name} main
APT_LINE
fi

if [ "${need_llvm_apt}" = "yes" ]; then
  curl https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
  cat <<APT_LINE > /etc/apt/sources.list.d/llvm.list
deb http://apt.llvm.org/${code_name}/ llvm-toolchain-${code_name}-7 main
deb-src http://apt.llvm.org/${code_name}/ llvm-toolchain-${code_name}-7 main
APT_LINE
fi

apt update

apt install -y -V libarrow-glib-dev=${deb_version}
apt install -y -V libarrow-glib-doc=${deb_version}

if [ "${have_python}" = "yes" ]; then
  apt install -y -V libarrow-python-dev=${deb_version}
fi

apt install -y -V libplasma-glib-dev=${deb_version}
apt install -y -V libplasma-glib-doc=${deb_version}
# apt install -y -V plasma-store-server=${deb_version}

if [ "${have_gandiva}" = "yes" ]; then
  apt install -y -V libgandiva-glib-dev=${deb_version}
  apt install -y -V libgandiva-glib-doc=${deb_version}
fi

apt install -y -V libparquet-glib-dev=${deb_version}
apt install -y -V libparquet-glib-doc=${deb_version}
