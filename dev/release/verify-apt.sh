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
  curl \
  lsb-release

code_name="$(lsb_release --codename --short)"
distribution="$(lsb_release --id --short | tr 'A-Z' 'a-z')"
bintray_base_url="https://dl.bintray.com/${BINTRAY_REPOSITORY}/${distribution}"
if [ "${IS_RC}" = "yes" ]; then
  bintray_base_url="${bintray_base_url}-rc"
fi

have_flight=yes
have_gandiva=yes
have_plasma=yes
case "${distribution}-${code_name}" in
  debian-stretch)
    sed \
      -i"" \
      -e "s/ main$/ main contrib non-free/g" \
      /etc/apt/sources.list
    cat <<APT_LINE > /etc/apt/sources.list.d/backports.list
deb http://deb.debian.org/debian ${code_name}-backports main
APT_LINE
    ;;
  debian-buster)
    sed \
      -i"" \
      -e "s/ main$/ main contrib non-free/g" \
      /etc/apt/sources.list
    ;;
  ubuntu-xenial)
    have_flight=no
    ;;
esac
if [ "$(arch)" = "aarch64" ]; then
  have_gandiva=no
  have_plasma=no
fi

keyring_archive_base_name="apache-arrow-archive-keyring-latest-${code_name}.deb"
curl \
  --output "${keyring_archive_base_name}" \
  "${bintray_base_url}/${keyring_archive_base_name}"
apt install -y -V "./${keyring_archive_base_name}"
if [ "${BINTRAY_REPOSITORY}" = "apache/arrow" ]; then
  if [ "${IS_RC}" = "yes" ]; then
    sed \
      -i"" \
      -e "s,^URIs: \\(.*\\)/,URIs: \\1-rc/,g" \
      /etc/apt/sources.list.d/apache-arrow.sources
  fi
else
  sed \
    -i"" \
    -e "s,^URIs: .*,URIs: ${bintray_base_url}/,g" \
    /etc/apt/sources.list.d/apache-arrow.sources
fi

apt update

apt install -y -V libarrow-glib-dev=${deb_version}
apt install -y -V libarrow-glib-doc=${deb_version}

if [ "${have_flight}" = "yes" ]; then
  apt install -y -V libarrow-flight-dev=${deb_version}
fi

apt install -y -V libarrow-python-dev=${deb_version}

if [ "${have_plasma}" = "yes" ]; then
  apt install -y -V libplasma-glib-dev=${deb_version}
  apt install -y -V libplasma-glib-doc=${deb_version}
  apt install -y -V plasma-store-server=${deb_version}
fi

if [ "${have_gandiva}" = "yes" ]; then
  apt install -y -V libgandiva-glib-dev=${deb_version}
  apt install -y -V libgandiva-glib-doc=${deb_version}
fi

apt install -y -V libparquet-glib-dev=${deb_version}
apt install -y -V libparquet-glib-doc=${deb_version}
