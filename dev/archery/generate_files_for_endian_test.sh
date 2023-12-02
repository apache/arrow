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

# This script generates json and arrow files of each type (e.g. primitive) for integration endian test
# Usage: generate_files_for_endian_test.sh
#        ARROW_CPP_EXE_PATH : where Arrow C++ binaries can be found
#        TMP_DIR            : where files will be generated

set -e

: ${ARROW_CPP_EXE_PATH:=/arrow/cpp/build/debug/}
: ${TMP_DIR:=/tmp/arrow}

json_dir=$TMP_DIR/arrow.$$
mkdir -p $json_dir

archery integration --stop-on-error --run-ipc --with-cpp=1 --tempdir=$json_dir

for f in $json_dir/*.json ; do
    $ARROW_CPP_EXE_PATH/arrow-json-integration-test -mode JSON_TO_ARROW -json $f -arrow ${f%.*}.arrow_file -integration true ;
done
for f in $json_dir/*.arrow_file ; do
    $ARROW_CPP_EXE_PATH/arrow-file-to-stream $f > ${f%.*}.stream;
done
for f in $json_dir/*.json ; do
    gzip $f ;
done
echo "The files are under $json_dir"
