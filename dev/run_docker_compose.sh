#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ $# -lt 1 ]; then
    echo "This script takes one argument - the docker service to run" >&2
    exit 1
fi

# Make sure this is always run in the directory above Arrow root
cd $(dirname "${BASH_SOURCE}")/../..

if [ ! -d arrow ]; then
    echo "Please make sure that the top level Arrow directory" >&2
    echo "is named 'arrow'"
    exit 1
fi

if [ ! -d parquet-cpp ]; then
    echo "Please clone the Parquet repo next to the Arrow repo" >&2
    exit 1
fi

GID=$(id -g ${USERNAME})
docker-compose -f arrow/dev/docker-compose.yml run \
               --rm -u "${UID}:${GID}" "${1}"
