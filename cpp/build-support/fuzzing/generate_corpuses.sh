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

# Generate and pack seed corpus files, for OSS-Fuzz

if [ $# -ne 1 ]; then
    echo "Usage: $0 <build output dir>"
    exit 1
fi

set -ex

CORPUS_DIR=/tmp/corpus
ARROW_ROOT=$(cd $(dirname $BASH_SOURCE)/../../..; pwd)
ARROW_CPP=$ARROW_ROOT/cpp
OUT=$1

# NOTE: name of seed corpus output file should be "<FUZZ TARGET>-seed_corpus.zip"
# where "<FUZZ TARGET>" is the exact name of the fuzz target executable the
# seed corpus is generated for.

IPC_INTEGRATION_FILES=$(find ${ARROW_ROOT}/testing/data/arrow-ipc-stream/integration -name "*.stream")

rm -rf ${CORPUS_DIR}
${OUT}/arrow-ipc-generate-fuzz-corpus -stream ${CORPUS_DIR}
# Several IPC integration files can have the same name, make sure
# they all appear in the corpus by numbering the duplicates.
cp --backup=numbered ${IPC_INTEGRATION_FILES} ${CORPUS_DIR}
${ARROW_CPP}/build-support/fuzzing/pack_corpus.py ${CORPUS_DIR} ${OUT}/arrow-ipc-stream-fuzz_seed_corpus.zip

rm -rf ${CORPUS_DIR}
${OUT}/arrow-ipc-generate-fuzz-corpus -file ${CORPUS_DIR}
${ARROW_CPP}/build-support/fuzzing/pack_corpus.py ${CORPUS_DIR} ${OUT}/arrow-ipc-file-fuzz_seed_corpus.zip

rm -rf ${CORPUS_DIR}
${OUT}/arrow-ipc-generate-tensor-fuzz-corpus -stream ${CORPUS_DIR}
${ARROW_CPP}/build-support/fuzzing/pack_corpus.py ${CORPUS_DIR} ${OUT}/arrow-ipc-tensor-stream-fuzz_seed_corpus.zip

rm -rf ${CORPUS_DIR}
${OUT}/parquet-arrow-generate-fuzz-corpus ${CORPUS_DIR}
# Add Parquet testing examples
cp ${ARROW_CPP}/submodules/parquet-testing/data/*.parquet ${CORPUS_DIR}
${ARROW_CPP}/build-support/fuzzing/pack_corpus.py ${CORPUS_DIR} ${OUT}/parquet-arrow-fuzz_seed_corpus.zip
