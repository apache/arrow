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

# check that optional pyarrow modules are available
# because pytest would just skip the pyarrow tests
python -c "import pyarrow.orc"
python -c "import pyarrow.parquet"

pushd /tmp
git clone https://github.com/blue-yonder/turbodbc.git
pushd turbodbc
git submodule update --init --recursive

service postgresql start
sudo -u postgres psql -U postgres -c 'CREATE DATABASE test_db;'
sudo -u postgres psql -U postgres -c 'ALTER USER postgres WITH PASSWORD '\''password'\'';'
export ODBCSYSINI="$(pwd)/travis/odbc/"

mkdir build
pushd build
cmake -DCMAKE_INSTALL_PREFIX=./dist -DPYTHON_EXECUTABLE=`which python` -GNinja ..
ninja install

# TODO(ARROW-5074)
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/conda/lib"

ctest --output-on-failure
