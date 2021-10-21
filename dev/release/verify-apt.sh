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
  echo "       $0 VERSION staging-rc"
  echo "       $0 VERSION release"
  echo "       $0 VERSION staging-release"
  echo "       $0 VERSION local"
  echo " e.g.: $0 0.13.0 rc                # Verify 0.13.0 RC"
  echo " e.g.: $0 0.13.0 staging-rc        # Verify 0.13.0 RC on staging"
  echo " e.g.: $0 0.13.0 release           # Verify 0.13.0"
  echo " e.g.: $0 0.13.0 staging-release   # Verify 0.13.0 on staging"
  echo " e.g.: $0 0.13.0-dev20210203 local # Verify 0.13.0-dev20210203 on local"
  exit 1
fi

VERSION="$1"
TYPE="$2"

local_prefix="/arrow/dev/tasks/linux-packages"


echo "::group::Prepare repository"

export DEBIAN_FRONTEND=noninteractive

APT_INSTALL="apt install -y -V --no-install-recommends"

apt update
${APT_INSTALL} \
  ca-certificates \
  curl \
  lsb-release

code_name="$(lsb_release --codename --short)"
distribution="$(lsb_release --id --short | tr 'A-Z' 'a-z')"
artifactory_base_url="https://apache.jfrog.io/artifactory/arrow/${distribution}"
case "${TYPE}" in
  rc|staging-rc|staging-release)
    suffix=${TYPE%-release}
    artifactory_base_url+="-${suffix}"
    ;;
esac

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
  ${APT_INSTALL} "${apt_source_path}"
else
  package_version="${VERSION}-1"
  apt_source_base_name="apache-arrow-apt-source-latest-${code_name}.deb"
  curl \
    --output "${apt_source_base_name}" \
    "${artifactory_base_url}/${apt_source_base_name}"
  ${APT_INSTALL} "./${apt_source_base_name}"
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
  case "${TYPE}" in
    rc|staging-rc|staging-release)
      suffix=${TYPE%-release}
      sed \
        -i"" \
        -e "s,^URIs: \\(.*\\)/,URIs: \\1-${suffix}/,g" \
        /etc/apt/sources.list.d/apache-arrow.sources
      ;;
  esac
fi

apt update

echo "::endgroup::"


echo "::group::Test Apache Arrow C++"
${APT_INSTALL} libarrow-dev=${package_version}
required_packages=()
required_packages+=(cmake)
required_packages+=(g++)
required_packages+=(git)
required_packages+=(make)
required_packages+=(pkg-config)
required_packages+=(${workaround_missing_packages[@]})
${APT_INSTALL} ${required_packages[@]}
mkdir -p build
cp -a /arrow/cpp/examples/minimal_build build
pushd build/minimal_build
cmake .
make -j$(nproc)
./arrow_example
popd
echo "::endgroup::"


echo "::group::Test Apache Arrow GLib"
${APT_INSTALL} libarrow-glib-dev=${package_version}
${APT_INSTALL} libarrow-glib-doc=${package_version}
echo "::endgroup::"


if [ "${have_flight}" = "yes" ]; then
  echo "::group::Test Apache Arrow Flight"
  ${APT_INSTALL} libarrow-flight-glib-dev=${package_version}
  ${APT_INSTALL} libarrow-flight-glib-doc=${package_version}
  echo "::endgroup::"
fi


echo "::group::Test libarrow-python"
${APT_INSTALL} libarrow-python-dev=${package_version}
echo "::endgroup::"


if [ "${have_plasma}" = "yes" ]; then
  echo "::group::Test Plasma"
  ${APT_INSTALL} libplasma-glib-dev=${package_version}
  ${APT_INSTALL} libplasma-glib-doc=${package_version}
  ${APT_INSTALL} plasma-store-server=${package_version}
  echo "::endgroup::"
fi


echo "::group::Test Gandiva"
${APT_INSTALL} libgandiva-glib-dev=${package_version}
${APT_INSTALL} libgandiva-glib-doc=${package_version}
echo "::endgroup::"


echo "::group::Test Parquet"
${APT_INSTALL} libparquet-glib-dev=${package_version}
${APT_INSTALL} libparquet-glib-doc=${package_version}
echo "::endgroup::"
