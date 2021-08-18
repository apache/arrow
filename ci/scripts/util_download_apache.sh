#!/usr/bin/env bash
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

set -x

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <apache tarball path> <target directory>"
  exit 1
fi

tarball_path=$1
target_dir=$2

APACHE_MIRRORS=(
  "http://www.apache.org/dyn/closer.cgi?action=download&filename="
  "https://downloads.apache.org"
  "https://apache.claz.org"
  "https://apache.cs.utah.edu"
  "https://apache.mirrors.lucidnetworks.net"
  "https://apache.osuosl.org"
  "https://ftp.wayne.edu/apache"
  "https://mirror.olnevhost.net/pub/apache"
  "https://mirrors.gigenet.com/apache"
  "https://mirrors.koehn.com/apache"
  "https://mirrors.ocf.berkeley.edu/apache"
  "https://mirrors.sonic.net/apache"
  "https://us.mirrors.quenda.co/apache"
)

mkdir -p "${target_dir}"

for mirror in ${APACHE_MIRRORS[*]}
do
  curl -SL "${mirror}/${tarball_path}" | tar -xzf - -C "${target_dir}"
  if [ $? == 0 ]; then
    exit 0
  fi
done

exit 1
