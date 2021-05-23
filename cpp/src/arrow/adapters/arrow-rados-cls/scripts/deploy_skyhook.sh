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
apt install -y python3 python3-pip python3-venv python3-numpy cmake libradospp-dev rados-objclass-dev

git clone --branch rados-dataset-dev https://github.com/uccross/arrow /tmp/arrow
cd /tmp/arrow
mkdir cpp/debug
cd cpp/debug

cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_CLS=ON -DARROW_PARQUET=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZLIB=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_PYTHON=ON -DARROW_DATASET=ON -DARROW_CSV=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_ZSTD=ON ..
make -j4 install

export PYARROW_BUILD_TYPE=Debug
export PYARROW_WITH_DATASET=1
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_RADOS=1

cd /tmp/arrow/python
pip3 install -r requirements-build.txt -r requirements-test.txt
pip3 install wheel
python3 setup.py build_ext --inplace --bundle-arrow-cpp bdist_wheel
pip3 install --upgrade dist/*.whl

cd /tmp/arrow/cpp/debug/debug
for ((i=1; i<=3; i++)); do
  scp libcls* node${i}:/usr/lib/rados-classes/
  scp libarrow* node${i}:/usr/lib/
  scp libparquet* node${i}:/usr/lib/
  ssh node${i} systemctl restart ceph-osd.target
done

export LD_LIBRARY_PATH=/usr/local/lib
cp /usr/local/lib/libparq* /usr/lib/
