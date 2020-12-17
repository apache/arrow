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

#!/bin/sh

# This file is git pre-commit hook.
# Copy or soft link it as file .git/hooks/pre-commit.

function GREEN() {
	echo "\033[0;32m$@\033[0m"
}

function RED() {
	echo "\033[0;31m$@\033[0m"
}

MSG="git pre-commit hook"
RUST_DIR="rust"
CARGO_FMT="cargo +stable fmt --all"

NUM_CHANGES=$(git diff --cached --name-only "${RUST_DIR}" |
	grep -e ".*/*.rs$" -o -e ".*/rustfmt.toml$" |
	awk '{print $1}' |
	wc -l)

if [ ${NUM_CHANGES} -eq 0 ]; then
	exit 0
fi

cd ${RUST_DIR}
echo "$(GREEN INFO) ${MSG}: check format of *.rs files ..."

$CARGO_FMT -q -- --check 2>/dev/null

if [ $? -eq 0 ]; then
	echo "$(GREEN INFO) ${MSG}: $(GREEN check passed)"
	exit 0
else
	echo "$(RED FAIL) ${MSG}: check $(RED failed), git commit $(RED aborted)"
	echo "$(GREEN INFO) ${MSG}: please run: $(GREEN ${CARGO_FMT})"
	exit 1
fi
