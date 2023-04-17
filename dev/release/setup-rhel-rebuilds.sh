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
# verification Red Hat Enterprise Linux 8 clones in particular
# on AlmaLinux 8 and Rocky Linux 8

set -exu

dnf -y install 'dnf-command(config-manager)'
dnf config-manager --set-enabled powertools
dnf -y update
dnf -y module disable nodejs
dnf -y module enable nodejs:16
dnf -y module disable ruby
dnf -y module enable ruby:2.7
dnf -y groupinstall "Development Tools"
dnf -y install \
  cmake \
  git \
  gobject-introspection-devel \
  java-1.8.0-openjdk-devel \
  libcurl-devel \
  llvm-devel \
  llvm-toolset \
  maven \
  ncurses-devel \
  ninja-build \
  nodejs \
  openssl-devel \
  python38-devel \
  python38-pip \
  ruby-devel \
  sqlite-devel \
  vala-devel \
  wget \
  which

npm install -g yarn

python3 -m pip install -U pip
alternatives --set python /usr/bin/python3
