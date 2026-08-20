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

set -euo pipefail

CWD="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SOURCE_DIR="$CWD/../src"
#PYTHON_SOURCE_DIR="$CWD/../../python" # Unused ShellCheck (SC2034)
FORMAT_DIR="$CWD/../../format"
#TOP="$FORMAT_DIR/.." # Unused ShellCheck (SC2034)
FLATC="flatc --cpp --cpp-std c++11 --scoped-enums"

OUT_DIR="$SOURCE_DIR/generated"
# Avoid word splitting (SC2207) while maintaining Bash 3 compatibility.
# See: https://www.shellcheck.net/wiki/SC2207
FILES=()
while IFS= read -r file; do
  FILES+=("$file")
done < <(find "$FORMAT_DIR" -name '*.fbs')
FILES+=("$SOURCE_DIR/arrow/ipc/feather.fbs")

$FLATC -o "$OUT_DIR" "${FILES[@]}"
