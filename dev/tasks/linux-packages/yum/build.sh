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

set -u

run()
{
  "$@"
  if test $? -ne 0; then
    echo "Failed $@"
    exit 1
  fi
}

rpmbuild_options=

. /host/env.sh

if grep -q amazon /etc/system-release-cpe; then
  distribution=$(cut -d ":" -f 5 /etc/system-release-cpe | tr '_' '-')
  distribution_version=$(cut -d ":" -f 6 /etc/system-release-cpe)
elif grep -q oracle /etc/system-release-cpe; then
  distribution=oracle-linux
  distribution_version=$(cut -d ":" -f 5 /etc/system-release-cpe)
else
  distribution=$(cut -d ":" -f 4 /etc/system-release-cpe)
  distribution_version=$(cut -d ":" -f 5 /etc/system-release-cpe)
fi
distribution_version=$(echo ${distribution_version} | sed -e 's/\..*$//g')
if grep -q 'CentOS Stream' /etc/system-release; then
  distribution_version+="-stream"
fi

architecture="$(arch)"
lib_directory=/usr/lib64
case "${architecture}" in
  i*86)
    architecture=i386
    lib_directory=/usr/lib
    ;;
esac

run mkdir -p /build
run cd /build
find . -not -path ./ccache -a -not -path "./ccache/*" -delete
if which ccache > /dev/null 2>&1; then
  export CCACHE_COMPILERCHECK=content
  export CCACHE_COMPRESS=1
  export CCACHE_COMPRESSLEVEL=6
  export CCACHE_MAXSIZE=500M
  export CCACHE_DIR="${PWD}/ccache"
  ccache --show-stats --verbose || :
  if [ -d "${lib_directory}/ccache" ]; then
    PATH="${lib_directory}/ccache:$PATH"
  fi
fi

run mkdir -p rpmbuild
run cd
rm -rf rpmbuild
run ln -fs /build/rpmbuild ./
if [ -x /usr/bin/rpmdev-setuptree ]; then
  rm -rf .rpmmacros
  run rpmdev-setuptree
else
  run cat <<RPMMACROS > ~/.rpmmacros
%_topdir ${HOME}/rpmbuild
RPMMACROS
  run mkdir -p rpmbuild/SOURCES
  run mkdir -p rpmbuild/SPECS
  run mkdir -p rpmbuild/BUILD
  run mkdir -p rpmbuild/RPMS
  run mkdir -p rpmbuild/SRPMS
fi

repositories="/host/repositories"
repository="${repositories}/${distribution}/${distribution_version}"
rpm_dir="${repository}/${architecture}/Packages"
srpm_dir="${repository}/source/SRPMS"
run mkdir -p "${rpm_dir}" "${srpm_dir}"

# for debug
# rpmbuild_options="$rpmbuild_options --define 'optflags -O0 -g3'"

if [ -n "${SOURCE_ARCHIVE}" ]; then
  case "${RELEASE}" in
    0.dev*)
      source_archive_base_name=$( \
        echo ${SOURCE_ARCHIVE} | sed -e 's/\.tar\.gz$//')
      run tar xf /host/tmp/${SOURCE_ARCHIVE} \
        --transform="s,^[^/]*,${PACKAGE},"
      run mv \
          ${PACKAGE} \
          ${source_archive_base_name}
      run tar czf \
          rpmbuild/SOURCES/${SOURCE_ARCHIVE} \
          ${source_archive_base_name}
      run rm -rf ${source_archive_base_name}
      ;;
    *)
      run cp /host/tmp/${SOURCE_ARCHIVE} rpmbuild/SOURCES/
      ;;
  esac
else
  run cp /host/tmp/${PACKAGE}-${VERSION}.* rpmbuild/SOURCES/
fi
if [ -x /host/prepare-sources.sh ]; then
  /host/prepare-sources.sh
fi

run cp \
    /host/tmp/${PACKAGE}.spec \
    rpmbuild/SPECS/

df -h

run cat <<BUILD > build.sh
#!/usr/bin/env bash

rpmbuild -ba ${rpmbuild_options} rpmbuild/SPECS/${PACKAGE}.spec
BUILD
run chmod +x build.sh
if [ -n "${SCL:-}" ]; then
  run cat <<WHICH_STRIP > which-strip.sh
#!/usr/bin/env bash

which strip
WHICH_STRIP
  run chmod +x which-strip.sh
  run cat <<USE_SCL_STRIP >> ~/.rpmmacros
%__strip $(run scl enable ${SCL} ./which-strip.sh)
USE_SCL_STRIP
  if [ "${DEBUG:-no}" = "yes" ]; then
    run scl enable ${SCL} ./build.sh
  else
    run scl enable ${SCL} ./build.sh > /dev/null
  fi
else
  if [ "${DEBUG:-no}" = "yes" ]; then
    run ./build.sh
  else
    run ./build.sh > /dev/null
  fi
fi

df -h

if which ccache > /dev/null 2>&1; then
  ccache --show-stats --verbose || :
fi

run mv rpmbuild/RPMS/*/* "${rpm_dir}/"
run mv rpmbuild/SRPMS/* "${srpm_dir}/"

run chown -R "$(stat --format "%u:%g" "${repositories}")" "${repositories}"
