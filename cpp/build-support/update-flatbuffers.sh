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
#

# Run this from cpp/ directory. flatc is expected to be in your path

set -o pipefail
set -o errexit
set -o nounset

CWD="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SOURCE_DIR=$CWD/../src
PYTHON_SOURCE_DIR=$CWD/../../python
FORMAT_DIR=$CWD/../../format
FLATC="flatc"

OUT_DIR="$SOURCE_DIR/generated"
FILES=($(find $FORMAT_DIR -name '*.fbs'))
FILES+=($SOURCE_DIR/arrow/ipc/feather.fbs)

COMPUTE_IR_FILES=($(find $FORMAT_DIR/experimental/computeir -name '*.fbs'))
COMPUTE_IR_FILES+=($FORMAT_DIR/Schema.fbs)

$FLATC --cpp --cpp-std c++11 \
  --scoped-enums \
  -o $SOURCE_DIR/generated \
  "${FILES[@]}"

# Only generate compute IR files for Python for now
$FLATC --python \
  -o $PYTHON_SOURCE_DIR/generated \
  "${COMPUTE_IR_FILES[@]}"

PLASMA_FBS=($SOURCE_DIR/plasma/{plasma,common}.fbs)

$FLATC --cpp --cpp-std c++11 \
  -o $SOURCE_DIR/plasma \
  --gen-object-api \
  --scoped-enums \
  "${PLASMA_FBS[@]}"
