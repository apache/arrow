#!/bin/bash -x
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

# TODO(bkietz) reuse this
# python/manylinux1/scripts/build_thrift.sh
# python/manylinux201x/scripts/build_thrift.sh

tarball_path=$1

APACHE_MIRRORS=(
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

for mirror in ${APACHE_MIRRORS[*]}
do
  tarball=`basename $tarball_path`
  echo "curl -SL $mirror/$tarball_path -o $tarball"
  if `tar -xf $tarball`
  then
    exit 0
  fi
done

exit 1
