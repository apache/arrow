#!/bin/sh
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

# A script to install dependencies required for release
# verification on Red Hat Enterprise Linux 10 clones in particular
# on AlmaLinux 10

set -exu

dnf -y install 'dnf-command(config-manager)'
dnf -y update
dnf -y groupinstall "Development Tools"
dnf -y install \
  cmake \
  git \
  gobject-introspection-devel \
  libcurl-devel \
  llvm-devel \
  llvm-toolset \
  ncurses-devel \
  ninja-build \
  openssl-devel \
  python3-devel \
  ruby-devel \
  sqlite-devel \
  vala-devel \
  wget \
  which

python3 -m ensurepip --upgrade
