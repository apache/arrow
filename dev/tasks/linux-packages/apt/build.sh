#!/usr/bin/env bash
# -*- sh-indentation: 2; sh-basic-offset: 2 -*-
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

LANG=C

set -u

run()
{
  "$@"
  if test $? -ne 0; then
    echo "Failed $@"
    exit 1
  fi
}

. /host/env.sh

umask "${UMASK}"

if [ -n "${CPU_LIST:-}" ]; then
  taskset -a -c "${CPU_LIST}"
fi

if [ -n "${FAKETIME:-}" ]; then
  lib_dir="/usr/lib/$(dpkg-architecture -q DEB_BUILD_MULTIARCH)"
  libfaketime="${lib_dir}/faketime/libfaketime.so.1"
  if [ ! -f "${libfaketime}" ]; then
    echo "You must install libfaktime: ${libfaketime} doesn't exist"
    exit 1
  fi
  export LD_PRELOAD="${libfaketime}"
fi

distribution=$(lsb_release --id --short | tr 'A-Z' 'a-z')
code_name=$(lsb_release --codename --short)
case "${distribution}" in
  debian)
    component=main
    ;;
  ubuntu)
    component=universe
    ;;
esac
architecture=$(dpkg-architecture -q DEB_BUILD_ARCH)

debuild_options=()
dpkg_buildpackage_options=(-us -uc)

build_root_dir="/build"
run mkdir -p "${build_root_dir}"
run pushd "${build_root_dir}"
find . -not -path ./ccache -a -not -path "./ccache/*" -delete
if which ccache > /dev/null 2>&1; then
  export CCACHE_COMPILERCHECK=content
  export CCACHE_COMPRESS=1
  export CCACHE_COMPRESSLEVEL=6
  export CCACHE_DIR="${PWD}/ccache"
  export CCACHE_MAXSIZE=500M
  ccache --show-stats --verbose || :
  debuild_options+=(-eCCACHE_COMPILERCHECK)
  debuild_options+=(-eCCACHE_COMPRESS)
  debuild_options+=(-eCCACHE_COMPRESSLEVEL)
  debuild_options+=(-eCCACHE_DIR)
  debuild_options+=(-eCCACHE_MAXSIZE)
  if [ -d /usr/lib/ccache ] ;then
    debuild_options+=(--prepend-path=/usr/lib/ccache)
  fi
fi
build_dir=$(mktemp --directory --tmpdir="${build_root_dir}" package.XXXXX)
run pushd "${build_dir}"
run cp /host/tmp/${PACKAGE}-${VERSION}.tar.gz \
  ${PACKAGE}_${VERSION}.orig.tar.gz
run tar xfz ${PACKAGE}_${VERSION}.orig.tar.gz
case "${VERSION}" in
  *~dev*)
    run mv ${PACKAGE}-$(echo $VERSION | sed -e 's/~dev/-dev/') \
        ${PACKAGE}-${VERSION}
    ;;
  *~rc*)
    run mv ${PACKAGE}-$(echo $VERSION | sed -r -e 's/~rc[0-9]+//') \
        ${PACKAGE}-${VERSION}
    ;;
esac
run pushd ${PACKAGE}-${VERSION}/
platform="${distribution}-${code_name}"
if [ -d "/host/tmp/debian.${platform}-${architecture}" ]; then
  run cp -rp "/host/tmp/debian.${platform}-${architecture}" debian
elif [ -d "/host/tmp/debian.${platform}" ]; then
  run cp -rp "/host/tmp/debian.${platform}" debian
else
  run cp -rp "/host/tmp/debian" debian
fi
: ${DEB_BUILD_OPTIONS:="parallel=$(nproc)"}
# DEB_BUILD_OPTIONS="${DEB_BUILD_OPTIONS} noopt"
export DEB_BUILD_OPTIONS
df -h
if [ "${DEBUG:-no}" = "yes" ]; then
  run debuild "${debuild_options[@]}" "${dpkg_buildpackage_options[@]}"
else
  run debuild "${debuild_options[@]}" "${dpkg_buildpackage_options[@]}" > /dev/null
fi
df -h
if which ccache > /dev/null 2>&1; then
  ccache --show-stats --verbose || :
fi
run popd

repositories="/host/repositories"
package_initial=$(echo "${PACKAGE}" | sed -e 's/\(.\).*/\1/')
pool_dir="${repositories}/${distribution}/pool/${code_name}/${component}/${package_initial}/${PACKAGE}"
run mkdir -p "${pool_dir}/"
run \
  find . \
  -maxdepth 1 \
  -type f \
  -not -path '*.build' \
  -exec cp '{}' "${pool_dir}/" ';'

run chown -R "$(stat --format "%u:%g" "${repositories}")" "${repositories}"

run popd
rm -rf "${build_dir}"
