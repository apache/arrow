#!/bin/bash -e
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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Change to the toplevel Rust directory
pushd $DIR/../../

# As of 2020-12-06, the snapshot flatc version is not changed since "1.12.0",
# so let's build flatc from source.

echo "Build flatc from source ..."

FB_URL="https://github.com/google/flatbuffers"
FB_COMMIT="05192553f434d10c5f585aeb6a07a55a6ac702a5"
FB_DIR="rust/arrow/.flatbuffers"
FLATC="$FB_DIR/bazel-bin/flatc"

if [ -z $(which bazel) ]; then
    echo "bazel is required to build flatc"
    exit 1
fi

echo "Bazel version: $(bazel version | head -1 | awk -F':' '{print $2}')"

if [ ! -e $FB_DIR ]; then
    echo "git clone $FB_URL ..."
    git clone -b master --no-tag --depth 1 $FB_URL $FB_DIR
else
    echo "git pull $FB_URL ..."
    git -C $FB_DIR pull
fi

echo "hard reset to $FB_COMMIT"
git -C $FB_DIR reset --hard $FB_COMMIT

pushd $FB_DIR
echo "run: bazel build :flatc ..."
bazel build :flatc
popd

# Execute the code generation:
$FLATC --filename-suffix "" --rust -o rust/arrow/src/ipc/gen/ format/*.fbs

# Now the files are wrongly named so we have to change that.
popd
pushd $DIR/src/ipc/gen

PREFIX=$(cat <<'HEREDOC'
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![allow(dead_code)]
#![allow(unused_imports)]

use std::{cmp::Ordering, mem};
use flatbuffers::EndianScalar;

HEREDOC
)

SCHEMA_IMPORT="\nuse crate::ipc::gen::Schema::*;"
SPARSE_TENSOR_IMPORT="\nuse crate::ipc::gen::SparseTensor::*;"
TENSOR_IMPORT="\nuse crate::ipc::gen::Tensor::*;"

# For flatbuffer(1.12.0+), remove: use crate::${name}::\*;
names=("File" "Message" "Schema" "SparseTensor" "Tensor")

# Remove all generated lines we don't need
for f in `ls *.rs`; do

    if [[ $f == "mod.rs" ]]; then
        continue
    fi

    echo "Modifying: $f"
    sed -i '' '/extern crate flatbuffers;/d' $f
    sed -i '' '/use self::flatbuffers::EndianScalar;/d' $f
    sed -i '' '/\#\[allow(unused_imports, dead_code)\]/d' $f
    sed -i '' '/pub mod org {/d' $f
    sed -i '' '/pub mod apache {/d' $f
    sed -i '' '/pub mod arrow {/d' $f
    sed -i '' '/pub mod flatbuf {/d' $f
    sed -i '' '/}  \/\/ pub mod flatbuf/d' $f
    sed -i '' '/}  \/\/ pub mod arrow/d' $f
    sed -i '' '/}  \/\/ pub mod apache/d' $f
    sed -i '' '/}  \/\/ pub mod org/d' $f
    sed -i '' '/use std::mem;/d' $f
    sed -i '' '/use std::cmp::Ordering;/d' $f

    # required by flatc 1.12.0+
    sed -i '' "/\#\!\[allow(unused_imports, dead_code)\]/d" $f
    for name in ${names[@]}; do
        sed -i '' "/use crate::${name}::\*;/d" $f
        sed -i '' "s/use self::flatbuffers::Verifiable;/use flatbuffers::Verifiable;/g" $f
    done

    # Replace all occurrences of "type__" with "type_", "TYPE__" with "TYPE_".
    sed -i '' 's/type__/type_/g' $f
    sed -i '' 's/TYPE__/TYPE_/g' $f

    # Some files need prefixes
    if [[ $f == "File.rs" ]]; then 
        # Now prefix the file with the static contents
        echo -e "${PREFIX}" "${SCHEMA_IMPORT}" | cat - $f > temp && mv temp $f
    elif [[ $f == "Message.rs" ]]; then
        echo -e "${PREFIX}" "${SCHEMA_IMPORT}" "${SPARSE_TENSOR_IMPORT}" "${TENSOR_IMPORT}" | cat - $f > temp && mv temp $f
    elif [[ $f == "SparseTensor.rs" ]]; then
        echo -e "${PREFIX}" "${SCHEMA_IMPORT}" "${TENSOR_IMPORT}" | cat - $f > temp && mv temp $f
    elif [[ $f == "Tensor.rs" ]]; then
        echo -e "${PREFIX}" "${SCHEMA_IMPORT}" | cat - $f > temp && mv temp $f
    else
        echo "${PREFIX}" | cat - $f > temp && mv temp $f
    fi
done

# Return back to base directory
popd
cargo +stable fmt -- src/ipc/gen/*

echo "=== TIPS ==="
echo "Let's manually fix rustdoc of SparseTensorIndexCSF::indptrType:"
echo 'prepend the tree with ```text, and append the tree with ```'
cat <<TREE_EOF
    /// \`\`\`text
    ///         0          1
    ///        / \         |
    ///       0   1        1
    ///      /   / \       |
    ///     0   0   1      1
    ///    /|  /|   |    /| |
    ///   1 2 0 2   0   0 1 2
    /// \`\`\`
TREE_EOF