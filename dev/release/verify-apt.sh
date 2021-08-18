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

set -exu

if [ $# -lt 2 ]; then
  echo "Usage: $0 VERSION rc"
  echo "       $0 VERSION release"
  echo "       $0 VERSION local"
  echo " e.g.: $0 0.13.0 rc           # Verify 0.13.0 RC"
  echo " e.g.: $0 0.13.0 release      # Verify 0.13.0"
  echo " e.g.: $0 0.13.0-dev20210203 local # Verify 0.13.0-dev20210203 on local"
  exit 1
fi

VERSION="$1"
TYPE="$2"

local_prefix="/arrow/dev/tasks/linux-packages"

export DEBIAN_FRONTEND=noninteractive

apt update
apt install -y -V \
  curl \
  lsb-release

code_name="$(lsb_release --codename --short)"
distribution="$(lsb_release --id --short | tr 'A-Z' 'a-z')"
artifactory_base_url="https://apache.jfrog.io/artifactory/arrow/${distribution}"
if [ "${TYPE}" = "rc" ]; then
  artifactory_base_url+="-rc"
fi

have_flight=yes
have_plasma=yes
workaround_missing_packages=()
case "${distribution}-${code_name}" in
  debian-*)
    sed \
      -i"" \
      -e "s/ main$/ main contrib non-free/g" \
      /etc/apt/sources.list
    ;;
esac
if [ "$(arch)" = "aarch64" ]; then
  have_plasma=no
fi

if [ "${TYPE}" = "local" ]; then
  case "${VERSION}" in
    *-dev*)
      package_version="$(echo "${VERSION}" | sed -e 's/-dev\(.*\)$/~dev\1/g')"
      ;;
    *-rc*)
      package_version="$(echo "${VERSION}" | sed -e 's/-rc.*$//g')"
      ;;
    *)
      package_version="${VERSION}"
      ;;
  esac
  package_version+="-1"
  apt_source_path="${local_prefix}/apt/repositories"
  apt_source_path+="/${distribution}/pool/${code_name}/main"
  apt_source_path+="/a/apache-arrow-apt-source"
  apt_source_path+="/apache-arrow-apt-source_${package_version}_all.deb"
  apt install -y -V "${apt_source_path}"
else
  package_version="${VERSION}-1"
  apt_source_base_name="apache-arrow-apt-source-latest-${code_name}.deb"
  curl \
    --output "${apt_source_base_name}" \
    "${artifactory_base_url}/${apt_source_base_name}"
  apt install -y -V "./${apt_source_base_name}"
fi

if [ "${TYPE}" = "local" ]; then
  sed \
    -i"" \
    -e "s,^URIs: .*$,URIs: file://${local_prefix}/apt/repositories/${distribution},g" \
    /etc/apt/sources.list.d/apache-arrow.sources
  keys="${local_prefix}/KEYS"
  if [ -f "${keys}" ]; then
    gpg \
      --no-default-keyring \
      --keyring /usr/share/keyrings/apache-arrow-apt-source.gpg \
      --import "${keys}"
  fi
else
  if [ "${TYPE}" = "rc" ]; then
    sed \
      -i"" \
      -e "s,^URIs: \\(.*\\)/,URIs: \\1-rc/,g" \
      /etc/apt/sources.list.d/apache-arrow.sources
  fi
fi

apt update

apt install -y -V libarrow-glib-dev=${package_version}
required_packages=()
required_packages+=(cmake)
required_packages+=(g++)
required_packages+=(git)
required_packages+=(${workaround_missing_packages[@]})
apt install -y -V ${required_packages[@]}
mkdir -p build
cp -a /arrow/cpp/examples/minimal_build build
pushd build/minimal_build
cmake .
make -j$(nproc)
./arrow_example
popd

apt install -y -V libarrow-glib-dev=${package_version}
apt install -y -V libarrow-glib-doc=${package_version}

if [ "${have_flight}" = "yes" ]; then
  apt install -y -V libarrow-flight-glib-dev=${package_version}
  apt install -y -V libarrow-flight-glib-doc=${package_version}
fi

apt install -y -V libarrow-python-dev=${package_version}

if [ "${have_plasma}" = "yes" ]; then
  apt install -y -V libplasma-glib-dev=${package_version}
  apt install -y -V libplasma-glib-doc=${package_version}
  apt install -y -V plasma-store-server=${package_version}
fi

apt install -y -V libgandiva-glib-dev=${package_version}
apt install -y -V libgandiva-glib-doc=${package_version}

apt install -y -V libparquet-glib-dev=${package_version}
apt install -y -V libparquet-glib-doc=${package_version}
