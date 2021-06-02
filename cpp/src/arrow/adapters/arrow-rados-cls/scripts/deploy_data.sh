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

if [[ $# -lt 4 ]] ; then
    echo "./deploy_data.sh [source] [destination] [count] [stripeunit]"
    echo " Use the below chart to find the stripunit you need to use: "
    echo " "
    echo "For 4MB files: 4194304"
    echo "For 8MB files: 8388608"
    echo "For 16MB files: 16777216"
    echo "For 32MB files: 33554432"
    echo "For 64MB files: 67108864"
    echo "For 128MB files: 134217728"
    exit 1
fi

apt update
apt install -y attr

source=${1}
destination=${2}
count=${3}
stripe=${4}

mkdir -p ${destination}

for ((i=1 ; i<=${count} ; i++)); do
    uuid=$(uuidgen)
    filename=${destination}/${uuid}
    touch ${filename}
    setfattr -n ceph.file.layout.object_size -v ${stripe} ${filename}
    echo "copying ${source} to ${filename}"
    cp ${source} ${filename}
done

sleep 2
