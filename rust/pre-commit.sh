#!/bin/bash

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

# This file is git pre-commit hook.
#
# Soft link it as git hook under top dir of apache arrow git repository:
# $ ln -s  ../../rust/pre-commit.sh .git/hooks/pre-commit
#
# This file be run directly:
# $ ./pre-commit.sh

function RED() {
	echo "\033[0;31m$@\033[0m"
}

function GREEN() {
	echo "\033[0;32m$@\033[0m"
}

function BYELLOW() {
	echo "\033[1;33m$@\033[0m"
}

RUST_DIR="rust"

# env GIT_DIR is set by git when run a pre-commit hook.
if [ -z "${GIT_DIR}" ]; then
	GIT_DIR=$(git rev-parse --show-toplevel)
fi

cd ${GIT_DIR}/${RUST_DIR}

NUM_CHANGES=$(git diff --cached --name-only . |
	grep -e ".*/*.rs$" |
	awk '{print $1}' |
	wc -l)

if [ ${NUM_CHANGES} -eq 0 ]; then
	echo -e "$(GREEN INFO): no staged changes in *.rs, $(GREEN skip cargo fmt/clippy)"
	exit 0
fi

# 1. cargo clippy

echo -e "$(GREEN INFO): cargo clippy ..."

# Cargo clippy always return exit code 0, and `tee` doesn't work.
# So let's just run cargo clippy.
cargo clippy
echo -e "$(GREEN INFO): cargo clippy done"

# 2. cargo fmt: format with nightly and stable.

CHANGED_BY_CARGO_FMT=false
echo -e "$(GREEN INFO): cargo fmt with nightly and stable ..."

for version in nightly stable; do
	CMD="cargo +${version} fmt"
	${CMD} --all -q -- --check 2>/dev/null
	if [ $? -ne 0 ]; then
		${CMD} --all
		echo -e "$(BYELLOW WARN): ${CMD} changed some files"
		CHANGED_BY_CARGO_FMT=true
	fi
done

if ${CHANGED_BY_CARGO_FMT}; then
	echo -e "$(RED FAIL): git commit $(RED ABORTED), please have a look and run git add/commit again"
	exit 1
fi

exit 0
