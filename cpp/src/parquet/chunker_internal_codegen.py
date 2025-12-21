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

"""
Produce the given number gearhash tables for rolling hash calculations.

Each table consists of 256 64-bit integer values and by default 8 tables are
produced. The tables are written to a header file that can be included in the
C++ code.

The generated numbers are deterministic "random" numbers created by MD5 hashing
a fixed seed and the table index. This ensures that the tables are the same
across different runs and platforms. The function of generating the numbers is
less important as long as they have sufficiently uniform distribution.

Reference implementations:
- https://github.com/Borelset/destor/blob/master/src/chunking/fascdc_chunking.c
- https://github.com/nlfiedler/fastcdc-rs/blob/master/examples/table64.rs

Usage:
    python chunker_internal_codegen.py [ntables]

    ntables: Number of gearhash tables to generate (default 8), the
             the C++ implementation expects 8 tables so this should not be
             changed unless the C++ code is also updated.

    The generated header file is written to ./chunker_internal_generated.h
"""

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

namespace parquet::internal {{

constexpr int64_t kNumGearhashTables = {ntables};

constexpr uint64_t kGearhashTable[{ntables}][256] = {{
{content}}};

}}  // namespace parquet::internal
"""


def generate_hash(n: int, seed: int):
    """Produce predictable hash values for a given seed and n using MD5.

    The value can be arbitrary as long as it is deterministic and has a uniform
    distribution. The MD5 hash is used to produce a 16 character hexadecimal
    string which is then converted to a 64-bit integer.
    """
    value = bytes([seed] * 64 + [n] * 64)
    hasher = hashlib.md5(value)
    return hasher.hexdigest()[:16]


def generate_hashtable(seed: int, length=256):
    """Generate and render a single gearhash table."""
    table = [generate_hash(n, seed=seed) for n in range(length)]

    out = StringIO()
    out.write(f"    {{// seed = {seed}\n")
    for i in range(0, length, 4):
        values = [f"0x{value}" for value in table[i : i + 4]]
        values = ", ".join(values)
        out.write(f"     {values}")
        if i < length - 4:
            out.write(",\n")
    out.write("}")

    return out.getvalue()


def generate_header(ntables=8, relative_path="chunker_internal_generated.h"):
    """Generate a header file with multiple gearhash tables."""
    path = pathlib.Path(__file__).parent / relative_path
    tables = [generate_hashtable(seed) for seed in range(ntables)]
    content = ",\n".join(tables)
    text = template.format(ntables=ntables, content=content)
    path.write_text(text)


if __name__ == "__main__":
    ntables = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    generate_header(ntables)
