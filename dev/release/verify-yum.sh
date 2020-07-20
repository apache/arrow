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

bintray_base_url="https://dl.bintray.com/${BINTRAY_REPOSITORY}/centos"
if [ "${IS_RC}" = "yes" ]; then
  bintray_base_url="${bintray_base_url}-rc"
fi

if [ -f /etc/os-release ]; then
  distribution=$(. /etc/os-release && echo "${ID}")
  distribution_version=$(. /etc/os-release && echo "${VERSION_ID}")
else
  distribution=centos
  distribution_version=6
fi

have_flight=yes
have_gandiva=yes
have_glib=yes
have_parquet=yes
install_command="dnf install -y --enablerepo=PowerTools"
case "${distribution}-${distribution_version}" in
  centos-6)
    have_flight=no
    have_gandiva=no
    have_glib=no
    have_parquet=no
    install_command="yum install -y"
    ;;
  centos-7)
    have_flight=no
    have_gandiva=no
    install_command="yum install -y"
    ;;
esac
if [ "$(arch)" = "aarch64" ]; then
  have_gandiva=no
fi

${install_command} \
  ${bintray_base_url}/${distribution_version}/apache-arrow-release-latest.rpm
if [ "${BINTRAY_REPOSITORY}" = "apache/arrow" ]; then
  if [ "${IS_RC}" = "yes" ]; then
    sed \
      -i"" \
      -e "s,/centos/,/centos-rc/,g" \
      /etc/yum.repos.d/Apache-Arrow.repo
  fi
else
  sed \
    -i"" \
    -e "s,baseurl=https://apache.bintray.com/arrow/centos,baseurl=${bintray_base_url},g" \
    /etc/yum.repos.d/Apache-Arrow.repo
fi

if [ "${have_glib}" = "yes" ]; then
  ${install_command} --enablerepo=epel arrow-glib-devel-${VERSION}
  ${install_command} --enablerepo=epel arrow-glib-doc-${VERSION}
fi
${install_command} --enablerepo=epel arrow-python-devel-${VERSION}

if [ "${have_glib}" = "yes" ]; then
  ${install_command} --enablerepo=epel plasma-glib-devel-${VERSION}
  ${install_command} --enablerepo=epel plasma-glib-doc-${VERSION}
else
  ${install_command} --enablerepo=epel plasma-devel-${VERSION}
fi

if [ "${have_flight}" = "yes" ]; then
  ${install_command} --enablerepo=epel arrow-flight-devel-${VERSION}
fi

if [ "${have_gandiva}" = "yes" ]; then
  if [ "${have_glib}" = "yes" ]; then
    ${install_command} --enablerepo=epel gandiva-glib-devel-${VERSION}
    ${install_command} --enablerepo=epel gandiva-glib-doc-${VERSION}
  else
    ${install_command} --enablerepo=epel gandiva-devel-${VERSION}
  fi
fi

if [ "${have_parquet}" = "yes" ]; then
  if [ "${have_glib}" = "yes" ]; then
    ${install_command} --enablerepo=epel parquet-glib-devel-${VERSION}
    ${install_command} --enablerepo=epel parquet-glib-doc-${VERSION}
  else
    ${install_command} --enablerepo=epel parquet-devel-${VERSION}
  fi
fi
