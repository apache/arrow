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

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOP_SOURCE_DIR="${SOURCE_DIR}/../.."
local_prefix="${TOP_SOURCE_DIR}/dev/tasks/linux-packages"

artifactory_base_url="https://apache.jfrog.io/artifactory/arrow"

distribution=$(. /etc/os-release && echo "${ID}")
distribution_version=$(. /etc/os-release && echo "${VERSION_ID}" | grep -o "^[0-9]*")
repository_version="${distribution_version}"

cmake_package=cmake
cmake_command=cmake
devtoolset=
scl_package=
have_arrow_libs=no
have_flight=yes
have_gandiva=yes
have_glib=yes
have_parquet=yes
have_python=yes
have_ruby=yes
ruby_devel_packages=(ruby-devel)
install_command="dnf install -y --enablerepo=crb"
uninstall_command="dnf remove -y"
clean_command="dnf clean"
info_command="dnf info --enablerepo=crb"

echo "::group::Prepare repository"

case "${distribution}-${distribution_version}" in
  almalinux-8)
    distribution_prefix="almalinux"
    have_arrow_libs=yes
    ruby_devel_packages+=(redhat-rpm-config)
    install_command="dnf install -y --enablerepo=powertools"
    info_command="dnf info --enablerepo=powertools"
    ;;
  almalinux-*)
    distribution_prefix="almalinux"
    ruby_devel_packages+=(redhat-rpm-config)
    ;;
  amzn-2)
    distribution_prefix="amazon-linux"
    cmake_package=cmake3
    cmake_command=cmake3
    if [ "$(arch)" != "aarch64" ]; then
      have_arrow_libs=yes
    fi
    have_flight=no
    have_gandiva=no
    have_python=no
    have_ruby=no
    install_command="yum install -y"
    uninstall_command="yum remove -y"
    clean_command="yum clean"
    info_command="yum info"
    amazon-linux-extras install epel -y
    ;;
  centos-7)
    distribution_prefix="centos"
    cmake_package=cmake3
    cmake_command=cmake3
    devtoolset=11
    scl_package=centos-release-scl-rh
    have_arrow_libs=yes
    have_flight=no
    have_gandiva=no
    have_python=no
    have_ruby=no
    install_command="yum install -y"
    uninstall_command="yum remove -y"
    clean_command="yum clean"
    info_command="yum info"
    ;;
  centos-8)
    distribution_prefix="centos"
    repository_version+="-stream"
    ruby_devel_packages+=(redhat-rpm-config)
    install_command="dnf install -y --enablerepo=powertools"
    info_command="dnf info --enablerepo=powertools"
    ;;
  centos-*)
    distribution_prefix="centos"
    repository_version+="-stream"
    ;;
esac
if [ "$(arch)" = "aarch64" ]; then
  have_flight=no
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
    almalinux)
      package_version+=".el${distribution_version}"
      release_path+="/almalinux"
      ;;
    amzn)
      package_version+=".${distribution}${distribution_version}"
      release_path+="/amazon-linux"
      amazon-linux-extras install -y epel
      ;;
    centos)
      package_version+=".el${distribution_version}"
      release_path+="/centos"
      ;;
  esac
  release_path+="/${repository_version}/$(arch)/Packages"
  release_path+="/apache-arrow-release-${package_version}.noarch.rpm"
  ${install_command} "${release_path}"
else
  package_version="${VERSION}"
  case "${TYPE}" in
    rc|staging-rc|staging-release)
      suffix=${TYPE%-release}
      distribution_prefix+="-${suffix}"
      ;;
  esac
  ${install_command} \
    ${artifactory_base_url}/${distribution_prefix}/${repository_version}/apache-arrow-release-latest.rpm
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
  case "${TYPE}" in
    rc|staging-rc|staging-release)
      suffix=${TYPE%-release}
      sed \
        -i"" \
        -e "s,/almalinux/,/almalinux-${suffix}/,g" \
        -e "s,/centos/,/centos-${suffix}/,g" \
        -e "s,/amazon-linux/,/amazon-linux-${suffix}/,g" \
        /etc/yum.repos.d/Apache-Arrow.repo
      ;;
  esac
fi

echo "::endgroup::"


echo "::group::Test Apache Arrow C++"
${install_command} --enablerepo=epel arrow-devel-${package_version}
if [ -n "${devtoolset}" ]; then
  ${install_command} ${scl_package}
fi
${install_command} \
  ${cmake_package} \
  git \
  libarchive \
  pkg-config
if [ -n "${devtoolset}" ]; then
  ${install_command} \
    devtoolset-${devtoolset}-gcc-c++ \
    devtoolset-${devtoolset}-make
  . /opt/rh/devtoolset-${devtoolset}/enable
else
  ${install_command} \
    gcc-c++ \
    make
fi
mkdir -p build
cp -a "${TOP_SOURCE_DIR}/cpp/examples/minimal_build" build/
pushd build/minimal_build
${cmake_command} .
make -j$(nproc)
./arrow-example
c++ -std=c++17 -o arrow-example example.cc $(pkg-config --cflags --libs arrow)
./arrow-example
popd
echo "::endgroup::"

if [ "${have_glib}" = "yes" ]; then
  echo "::group::Test Apache Arrow GLib"
  export G_DEBUG=fatal-warnings

  ${install_command} --enablerepo=epel arrow-glib-devel-${package_version}
  ${install_command} --enablerepo=epel arrow-glib-doc-${package_version}

  ${install_command} vala
  cp -a "${TOP_SOURCE_DIR}/c_glib/example/vala" build/
  pushd build/vala
  valac --pkg arrow-glib --pkg posix build.vala
  ./build
  popd

  if [ "${have_ruby}" = "yes" ]; then
    ${install_command} "${ruby_devel_packages[@]}"
    gem install gobject-introspection
    ruby -r gi -e "p GI.load('Arrow')"
  fi
  echo "::endgroup::"
fi

if [ "${have_flight}" = "yes" ]; then
  echo "::group::Test Apache Arrow Flight"
  ${install_command} --enablerepo=epel arrow-flight-glib-devel-${package_version}
  ${install_command} --enablerepo=epel arrow-flight-glib-doc-${package_version}
  if [ "${have_ruby}" = "yes" ]; then
    ruby -r gi -e "p GI.load('ArrowFlight')"
  fi
  echo "::endgroup::"

  echo "::group::Test Apache Arrow Flight SQL"
  ${install_command} --enablerepo=epel arrow-flight-sql-glib-devel-${package_version}
  ${install_command} --enablerepo=epel arrow-flight-sql-glib-doc-${package_version}
  if [ "${have_ruby}" = "yes" ]; then
    ruby -r gi -e "p GI.load('ArrowFlightSQL')"
  fi
  echo "::endgroup::"
fi

echo "::group::Test Plasma"
if [ "${have_glib}" = "yes" ]; then
  ${install_command} --enablerepo=epel plasma-glib-devel-${package_version}
  ${install_command} --enablerepo=epel plasma-glib-doc-${package_version}
  if [ "${have_ruby}" = "yes" ]; then
    ruby -r gi -e "p GI.load('Plasma')"
  fi
else
  ${install_command} --enablerepo=epel plasma-devel-${package_version}
fi
echo "::endgroup::"

if [ "${have_gandiva}" = "yes" ]; then
  echo "::group::Test Gandiva"
  if [ "${have_glib}" = "yes" ]; then
    ${install_command} --enablerepo=epel gandiva-glib-devel-${package_version}
    ${install_command} --enablerepo=epel gandiva-glib-doc-${package_version}
    if [ "${have_ruby}" = "yes" ]; then
      ruby -r gi -e "p GI.load('Gandiva')"
    fi
  else
    ${install_command} --enablerepo=epel gandiva-devel-${package_version}
  fi
  echo "::endgroup::"
fi

if [ "${have_parquet}" = "yes" ]; then
  echo "::group::Test Apache Parquet"
  if [ "${have_glib}" = "yes" ]; then
    ${install_command} --enablerepo=epel parquet-glib-devel-${package_version}
    ${install_command} --enablerepo=epel parquet-glib-doc-${package_version}
    if [ "${have_ruby}" = "yes" ]; then
      ruby -r gi -e "p GI.load('Parquet')"
    fi
  else
    ${install_command} --enablerepo=epel parquet-devel-${package_version}
  fi
  echo "::endgroup::"
fi

echo "::group::Test coexistence with old library"
${uninstall_command} apache-arrow-release
if ${install_command} \
     https://apache.jfrog.io/artifactory/arrow/${distribution_prefix}/${repository_version}/apache-arrow-release-latest.rpm; then
  ${clean_command} all
  if [ "${have_arrow_libs}" = "yes" ]; then
    ${install_command} arrow-libs
  else
    major_version=$(echo ${VERSION} | grep -E -o '^[0-9]+')
    previous_major_version="$((${major_version} - 1))"
    if ${info_command} arrow${previous_major_version}-libs; then
      ${install_command} arrow${previous_major_version}-libs
    fi
  fi
fi
echo "::endgroup::"
