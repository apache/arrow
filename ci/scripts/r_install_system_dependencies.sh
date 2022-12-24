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

set -ex

: ${ARROW_SOURCE_HOME:=/arrow}

if [ "$ARROW_S3" == "ON" ] || [ "$ARROW_GCS" == "ON" ] || [ "$ARROW_R_DEV" == "TRUE" ]; then
  # Figure out what package manager we have
  if [ "`which dnf`" ]; then
    PACKAGE_MANAGER=dnf
  elif [ "`which yum`" ]; then
    PACKAGE_MANAGER=yum
  elif [ "`which zypper`" ]; then
    PACKAGE_MANAGER=zypper
  else
    PACKAGE_MANAGER=apt-get
    apt-get update
  fi

  # Install curl and OpenSSL for S3/GCS support
  #
  # We need to install all dependencies explicitly to use "pkg-config
  # --static --libs libcurl" result. Because libcurl-dev/libcurl-devel
  # don't depend on packages that are only needed for "pkg-config
  # --static".
  case "$PACKAGE_MANAGER" in
    apt-get)
      # "pkg-config --static --libs libcurl" has
      #   * -lnghttp2
      #   * -lidn2
      #   * -lrtmp
      #   * -lssh or -lssh2
      #   * -lpsl
      #   * -lssl
      #   * -lcrypto
      #   * -lgssapi_krb5
      #   * -lkrb5
      #   * -lk5crypto
      #   * -lcom_err
      #   * -lldap
      #   * -llber
      #   * -lzstd
      #   * -lbrotlidec
      #   * -lz
      apt-get install -y \
              libbrotli-dev \
              libcurl4-openssl-dev \
              libidn2-dev \
              libkrb5-dev \
              libldap-dev \
              libnghttp2-dev \
              libpsl-dev \
              librtmp-dev \
              libssh-dev \
              libssh2-1-dev \
              libssl-dev \
              libzstd-dev
      ;;
    dnf|yum)
      # "pkg-config --static --libs libcurl" has -lidl, -lssh2 and -lldap
      $PACKAGE_MANAGER install -y \
                       libcurl-devel \
                       libidn-devel \
                       libssh2-devel \
                       openldap-devel \
                       openssl-devel
      ;;
    zypper)
      # "pkg-config --static --libs libcurl" has
      #   * -lnghttp2
      #   * -lidn2
      #   * -lssh
      #   * -lpsl
      #   * -lssl
      #   * -lcrypto
      #   * -lgssapi_krb5
      #   * -lkrb5
      #   * -lk5crypto
      #   * -lcom_err
      #   * -lldap
      #   * -llber
      #   * -lzstd
      #   * -lbrotlidec
      #   * -lz
      $PACKAGE_MANAGER install -y \
                       krb5-devel \
                       libbrotli-devel \
                       libcurl-devel \
                       libidn2-devel \
                       libnghttp2-devel \
                       libpsl-devel \
                       libssh-devel \
                       libzstd-devel \
                       openldap2-devel \
                       openssl-devel
      ;;
    *)
      $PACKAGE_MANAGER install -y libcurl-devel openssl-devel
      ;;
  esac

  # The Dockerfile should have put this file here
  if [ "$ARROW_S3" == "ON" ] && [ -f "${ARROW_SOURCE_HOME}/ci/scripts/install_minio.sh" ] && [ "`which wget`" ]; then
    "${ARROW_SOURCE_HOME}/ci/scripts/install_minio.sh" latest /usr/local
  fi

  if [ "$ARROW_GCS" == "ON" ] && [ -f "${ARROW_SOURCE_HOME}/ci/scripts/install_gcs_testbench.sh" ]; then
    case "$PACKAGE_MANAGER" in
      zypper)
        # python3 is Python 3.6 on OpenSUSE 15.3.
        # PyArrow supports Python 3.7 or later.
        $PACKAGE_MANAGER install -y python39-pip
        ln -s /usr/bin/python3.9 /usr/local/bin/python
        ln -s /usr/bin/pip3.9 /usr/local/bin/pip
        ;;
      *)
        $PACKAGE_MANAGER install -y python3-pip
        ln -s /usr/bin/python3 /usr/local/bin/python
        ln -s /usr/bin/pip3 /usr/local/bin/pip
        ;;
    esac
    "${ARROW_SOURCE_HOME}/ci/scripts/install_gcs_testbench.sh" default
  fi
fi
