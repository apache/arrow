#!/usr/bin/env python

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

import hashlib
import pathlib
import sys
from io import StringIO


template = """\
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

#pragma once
#include <cstdint>

namespace parquet {{
namespace internal {{

constexpr uint64_t GEARHASH_TABLE[8][256] = {{
{content}}};

}}  // namespace internal
}}  // namespace parquet
"""


def generate_hash(n: int, seed: int):
    value = bytes([seed] * 64 + [n] * 64)
    hasher = hashlib.md5(value)
    return hasher.hexdigest()[:16]


def generate_hashtable(seed: int, length=256, comma=True):
    table = [generate_hash(n, seed=seed) for n in range(length)]

    out = StringIO()
    out.write(f"    {{// seed = {seed}\n")
    for i in range(0, length, 4):
        values = [f"0x{value}" for value in table[i:i + 4]]
        values = ", ".join(values)
        out.write(f"     {values}")
        if i < length - 4:
            out.write(",\n")
    out.write("}")

    return out.getvalue()


def generate_header(ntables=8, relative_path="column_chunker_hashtable.h"):
    path = pathlib.Path(__file__).parent / relative_path

    tables = [generate_hashtable(seed) for seed in range(ntables)]
    text = template.format(content=",\n".join(tables))
    path.write_text(text)


if __name__ == "__main__":
    ntables = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    generate_header(ntables)