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

set -eu

apt update
apt install -y attr

# usage:
# ./data.sh [source] [destination] [count] [stripeunit]

source=${1}
destination=${2}
count=${3}
stripe=${4}

for ((i=0 ; i<=${count} ; i++)); do
    touch ${destination}.${i}
    setfattr -n ceph.file.layout.object_size -v ${stripe} ${destination}.${i}
    echo "copying ${source} to ${destination}.${i}"
    cp ${source} ${destination}.${i}
done

sleep 2

# For 4MB: 4194304 object size
# For 8MB: 8388608 object size
# For 16MB:  16777216 object size
# For 32MB:  33554432 object size
# For 64MB:  67108864 object size
