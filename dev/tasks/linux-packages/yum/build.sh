#!/bin/sh
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

distribution=$(cut -d " " -f 1 /etc/redhat-release | tr "A-Z" "a-z")
if grep -q Linux /etc/redhat-release; then
  distribution_version=$(cut -d " " -f 4 /etc/redhat-release)
else
  distribution_version=$(cut -d " " -f 3 /etc/redhat-release)
fi
distribution_version=$(echo ${distribution_version} | sed -e 's/\..*$//g')

architecture="$(arch)"
case "${architecture}" in
  i*86)
    architecture=i386
    ;;
esac

if [ -x /usr/bin/rpmdev-setuptree ]; then
  rm -rf .rpmmacros
  run rpmdev-setuptree
else
  run cat <<EOM > ~/.rpmmacros
%_topdir ${HOME}/rpmbuild
EOM
  run mkdir -p ~/rpmbuild/SOURCES
  run mkdir -p ~/rpmbuild/SPECS
  run mkdir -p ~/rpmbuild/BUILD
  run mkdir -p ~/rpmbuild/RPMS
  run mkdir -p ~/rpmbuild/SRPMS
fi

repository="/host/repositories/${distribution}/${distribution_version}"
rpm_dir="${repository}/${architecture}/Packages"
srpm_dir="${repository}/source/SRPMS"
run mkdir -p "${rpm_dir}" "${srpm_dir}"

# for debug
# rpmbuild_options="$rpmbuild_options --define 'optflags -O0 -g3'"

cd

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
run cp \
    /host/tmp/${PACKAGE}.spec \
    rpmbuild/SPECS/

run cat <<BUILD > build.sh
#!/bin/bash

rpmbuild -ba ${rpmbuild_options} rpmbuild/SPECS/${PACKAGE}.spec
BUILD
run chmod +x build.sh
if [ -n "${DEVTOOLSET_VERSION:-}" ]; then
  run cat <<WHICH_STRIP > which-strip.sh
#!/bin/bash

which strip
WHICH_STRIP
  run chmod +x which-strip.sh
  run cat <<USE_DEVTOOLSET_STRIP >> ~/.rpmmacros
%__strip $(run scl enable devtoolset-${DEVTOOLSET_VERSION} ./which-strip.sh)
USE_DEVTOOLSET_STRIP
  if [ "${DEBUG:-no}" = "yes" ]; then
    run scl enable devtoolset-${DEVTOOLSET_VERSION} ./build.sh
  else
    run scl enable devtoolset-${DEVTOOLSET_VERSION} ./build.sh > /dev/null
  fi
else
  if [ "${DEBUG:-no}" = "yes" ]; then
    run ./build.sh
  else
    run ./build.sh > /dev/null
  fi
fi

run mv rpmbuild/RPMS/*/* "${rpm_dir}/"
run mv rpmbuild/SRPMS/* "${srpm_dir}/"
