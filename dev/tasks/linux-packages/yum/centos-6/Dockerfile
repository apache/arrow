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

FROM centos:6

ARG DEBUG

ENV \
  SRPM_DOWNLOAD_URL=http://vault.centos.org/7.6.1810/os/Source/SPackages \
  LIBARCHIVE_SRPM_BASE=libarchive-3.1.2-10.el7_2.src.rpm

RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "--quiet") && \
  yum update -y ${quiet} && \
  yum install -y ${quiet} \
    epel-release && \
  yum install -y \
    autoconf268 \
    wget \
    yum-utils && \
  yum groupinstall -y ${quiet} "Development Tools" && \
  wget ${quiet} ${SRPM_DOWNLOAD_URL}/${LIBARCHIVE_SRPM_BASE} && \
  yum-builddep -y ${quiet} --nogpgcheck ${LIBARCHIVE_SRPM_BASE} && \
  rpm -i ${quiet} ${LIBARCHIVE_SRPM_BASE} && \
  sed -i'' -e 's/^run_testsuite$/:/g' ~/rpmbuild/SPECS/libarchive.spec && \
  AUTOM4TE=autom4te268 \
    rpmbuild -bb ${quiet} \
    ~/rpmbuild/SPECS/libarchive.spec && \
  yum install -y ${quiet} ~/rpmbuild/RPMS/*/libarchive-3.*.rpm && \
  rm -rf ${LIBARCHIVE_SRPM_BASE} ~/rpmbuild/ && \
  yum install -y ${quiet} \
    centos-release-scl && \
  yum install -y ${quiet} \
    boost-devel \
    cmake3 \
    devtoolset-6 \
    git \
    gobject-introspection-devel \
    gtk-doc \
    pkg-config \
    python34 \
    python34-devel \
    python34-numpy \
    rpm-build \
    rpmdevtools \
    tar && \
  yum clean ${quiet} all
