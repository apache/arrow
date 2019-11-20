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

set -e

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <spark version>"
  exit 1
fi

spark=$1

source activate testenv

mkdir /spark
wget -q -O - https://github.com/apache/spark/archive/${spark}.tar.gz | tar -xzf - --strip-components=1 -C /spark

# patch spark to build with current Arrow Java
# patch -d /spark -p1 -i /arrow/ci/scripts/ARROW-6429.patch
rm /arrow/ci/scripts/ARROW-6429.patch
