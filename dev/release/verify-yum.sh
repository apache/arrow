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

bintray_base_url="https://dl.bintray.com/${BINTRAY_REPOSITORY}/centos"
if [ "${IS_RC}" = "yes" ]; then
  bintray_base_url="${bintray_base_url}-rc"
fi

distribution=$(cut -d " " -f 1 /etc/redhat-release | tr "A-Z" "a-z")
if grep -q Linux /etc/redhat-release; then
  distribution_version=$(cut -d " " -f 4 /etc/redhat-release)
else
  distribution_version=$(cut -d " " -f 3 /etc/redhat-release)
fi
distribution_version=$(echo ${distribution_version} | sed -e 's/\..*$//g')

have_glib=yes
have_parquet=yes
case "${distribution}-${distribution_version}" in
  centos-6)
    have_glib=no
    have_parquet=no
    ;;
esac

cat <<REPO > /etc/yum.repos.d/Apache-Arrow.repo
[apache-arrow]
name=Apache Arrow
baseurl=${bintray_base_url}/\$releasever/\$basearch/
gpgcheck=1
enabled=1
gpgkey=${bintray_base_url}/RPM-GPG-KEY-apache-arrow
REPO

yum install -y epel-release

if [ "${have_glib}" = "yes" ]; then
  yum install -y --enablerepo=epel arrow-glib-devel-${VERSION}
  yum install -y --enablerepo=epel arrow-glib-doc-${VERSION}
fi
yum install -y --enablerepo=epel arrow-python-devel-${VERSION}

if [ "${have_glib}" = "yes" ]; then
  yum install -y --enablerepo=epel plasma-glib-devel-${VERSION}
  yum install -y --enablerepo=epel plasma-glib-doc-${VERSION}
else
  yum install -y --enablerepo=epel plasma-devel-${VERSION}
fi

if [ "${have_parquet}" = "yes" ]; then
  if [ "${have_glib}" = "yes" ]; then
    yum install -y --enablerepo=epel parquet-glib-devel-${VERSION}
    yum install -y --enablerepo=epel parquet-glib-doc-${VERSION}
  else
    yum install -y --enablerepo=epel parquet-devel-${VERSION}
  fi
fi
