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

artifactory_base_url="https://apache.jfrog.io/artifactory/arrow"

distribution=$(. /etc/os-release && echo "${ID}")
distribution_version=$(. /etc/os-release && echo "${VERSION_ID}")
distribution_prefix="centos"

cmake_package=cmake
cmake_command=cmake
have_flight=yes
have_gandiva=yes
have_glib=yes
have_parquet=yes
have_python=yes
install_command="dnf install -y --enablerepo=powertools"

case "${distribution}-${distribution_version}" in
  amzn-2)
    cmake_package=cmake3
    cmake_command=cmake3
    have_flight=no
    have_gandiva=no
    have_python=no
    install_command="yum install -y"
    distribution_prefix="amazon-linux"
    amazon-linux-extras install epel -y
    ;;
  centos-7)
    cmake_package=cmake3
    cmake_command=cmake3
    have_flight=no
    have_gandiva=no
    install_command="yum install -y"
    ;;
esac
if [ "$(arch)" = "aarch64" ]; then
  have_gandiva=no
fi

if [ "${TYPE}" = "local" ]; then
  case "${VERSION}" in
    *-dev*)
      package_version="$(echo "${VERSION}" | sed -e 's/-dev\(.*\)$/-0.dev\1/g')"
      ;;
    *-rc*)
      package_version="$(echo "${VERSION}" | sed -e 's/-rc.*$//g')"
      package_version+="-1"
      ;;
    *)
      package_version="${VERSION}-1"
      ;;
  esac
  release_path="${local_prefix}/yum/repositories"
  case "${distribution}" in
    amzn)
      package_version+=".${distribution}${distribution_version}"
      release_path+="/amazon-linux"
      amazon-linux-extras install -y epel
      ;;
    *)
      package_version+=".el${distribution_version}"
      release_path+="/centos"
      ;;
  esac
  release_path+="/${distribution_version}/$(arch)/Packages"
  release_path+="/apache-arrow-release-${package_version}.noarch.rpm"
  ${install_command} "${release_path}"
else
  package_version="${VERSION}"
  if [ "${TYPE}" = "rc" ]; then
    distribution_prefix+="-rc"
  fi
  ${install_command} \
    ${artifactory_base_url}/${distribution_prefix}/${distribution_version}/apache-arrow-release-latest.rpm
fi

if [ "${TYPE}" = "local" ]; then
  sed \
    -i"" \
    -e "s,baseurl=https://apache\.jfrog\.io/artifactory/arrow/,baseurl=file://${local_prefix}/yum/repositories/,g" \
    /etc/yum.repos.d/Apache-Arrow.repo
  keys="${local_prefix}/KEYS"
  if [ -f "${keys}" ]; then
    cp "${keys}" /etc/pki/rpm-gpg/RPM-GPG-KEY-Apache-Arrow
  fi
else
  if [ "${TYPE}" = "rc" ]; then
    sed \
      -i"" \
      -e "s,/centos/,/centos-rc/,g" \
      -e "s,/amazon-linux/,/amazon-linux-rc/,g" \
      /etc/yum.repos.d/Apache-Arrow.repo
  fi
fi

${install_command} --enablerepo=epel arrow-devel-${package_version}
${install_command} \
  ${cmake_package} \
  gcc-c++ \
  git \
  libarchive \
  make
mkdir -p build
cp -a /arrow/cpp/examples/minimal_build build
pushd build/minimal_build
${cmake_command} .
make -j$(nproc)
./arrow_example
popd

if [ "${have_glib}" = "yes" ]; then
  ${install_command} --enablerepo=epel arrow-glib-devel-${package_version}
  ${install_command} --enablerepo=epel arrow-glib-doc-${package_version}
fi

if [ "${have_python}" = "yes" ]; then
  ${install_command} --enablerepo=epel arrow-python-devel-${package_version}
fi

if [ "${have_glib}" = "yes" ]; then
  ${install_command} --enablerepo=epel plasma-glib-devel-${package_version}
  ${install_command} --enablerepo=epel plasma-glib-doc-${package_version}
else
  ${install_command} --enablerepo=epel plasma-devel-${package_version}
fi

if [ "${have_flight}" = "yes" ]; then
  ${install_command} --enablerepo=epel arrow-flight-glib-devel-${package_version}
  ${install_command} --enablerepo=epel arrow-flight-glib-doc-${package_version}
fi

if [ "${have_gandiva}" = "yes" ]; then
  if [ "${have_glib}" = "yes" ]; then
    ${install_command} --enablerepo=epel gandiva-glib-devel-${package_version}
    ${install_command} --enablerepo=epel gandiva-glib-doc-${package_version}
  else
    ${install_command} --enablerepo=epel gandiva-devel-${package_version}
  fi
fi

if [ "${have_parquet}" = "yes" ]; then
  if [ "${have_glib}" = "yes" ]; then
    ${install_command} --enablerepo=epel parquet-glib-devel-${package_version}
    ${install_command} --enablerepo=epel parquet-glib-doc-${package_version}
  else
    ${install_command} --enablerepo=epel parquet-devel-${package_version}
  fi
fi
