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

# NOTE: colorized output may not work as expected.
#       I've seen the difference between /bin/sh and GUN bash 5 on macOS.

function RED() {
	echo "\033[0;31m$@\033[0m"
}

function GREEN() {
	echo "\033[0;32m$@\033[0m"
}

function BYELLOW() {
	echo "\033[1;33m$@\033[0m"
}

MSG="git pre-commit hook"
RUST_DIR="rust"
CARGO_FMT="cargo +stable fmt --all"
CARGO_CLIPPY="cargo clippy"

NUM_CHANGES=$(git diff --cached --name-only "${RUST_DIR}" |
	grep -e ".*/*.rs$" -o -e "^rustfmt.toml$" -o -e "^rust-toolchain$" |
	awk '{print $1}' |
	wc -l)

if [ ${NUM_CHANGES} -eq 0 ]; then
	echo -e "$(GREEN INFO) ${MSG}: no changes in: *rs, rustfmt.toml, rust-toolchain, skip fmt/clippy"
	#exit 0
fi

cd ${RUST_DIR}

# 1. Abort on cargo fmt error.

echo -e "$(GREEN INFO) ${MSG}: cargo fmt checking ..."

$CARGO_FMT -q -- --check 2>/dev/null
if [ $? -eq 0 ]; then
	echo -e "$(GREEN INFO) ${MSG}: $(GREEN cargo fmt check passed)"
else
	echo -e "$(GREEN INFO) ${MSG}: cargo fmt formatting ..."
	$CARGO_FMT
	echo
	echo -e "$(BYELLOW WARN) ${MSG}: cargo fmt $(BYELLOW fixed some files)"
fi

# 2. Warn on cargo clippy errors/warnings.

echo
echo "===================================================="
echo
echo -e "$(GREEN INFO) ${MSG}: cargo clippy ..."
echo

# Cargo clippy always return exit code 0, and tee doesnt work.
# So let's just run cargo clippy.
$CARGO_CLIPPY
echo
echo -e "$(BYELLOW WARN) ${MSG}: please try fix $(BYELLOW clippy issues) if any"
echo

exit 0
