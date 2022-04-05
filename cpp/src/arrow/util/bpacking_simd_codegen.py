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

# Usage:
#   python bpacking_simd_codegen.py 128 > bpacking_simd128_generated.h
#   python bpacking_simd_codegen.py 256 > bpacking_simd256_generated.h
#   python bpacking_simd_codegen.py 512 > bpacking_simd512_generated.h

from functools import partial
import sys
from textwrap import dedent, indent


class UnpackGenerator:

    def __init__(self, simd_width):
        self.simd_width = simd_width
        if simd_width % 32 != 0:
            raise("SIMD bit width should be a multiple of 32")
        self.simd_byte_width = simd_width // 8

    def print_unpack_bit0_func(self):
        print(
            "inline static const uint32_t* unpack0_32(const uint32_t* in, uint32_t* out) {")
        print("  memset(out, 0x0, 32 * sizeof(*out));")
        print("  out += 32;")
        print("")
        print("  return in;")
        print("}")


    def print_unpack_bit32_func(self):
        print(
            "inline static const uint32_t* unpack32_32(const uint32_t* in, uint32_t* out) {")
        print("  memcpy(out, in, 32 * sizeof(*out));")
        print("  in += 32;")
        print("  out += 32;")
        print("")
        print("  return in;")
        print("}")

    def print_unpack_bit_func(self, bit):
        def p(code):
            print(indent(code, prefix='  '))

        shift = 0
        shifts = []
        in_index = 0
        inls = []
        mask = (1 << bit) - 1
        bracket = "{"

        print(f"inline static const uint32_t* unpack{bit}_32(const uint32_t* in, uint32_t* out) {{")
        p(dedent(f"""\
            uint32_t mask = 0x{mask:0x};

            simd_batch masks(mask);
            simd_batch words, shifts;
            simd_batch results;
            """))

        def safe_load(index):
            return f"SafeLoad<uint32_t>(in + {index})"

        for i in range(32):
            if shift + bit == 32:
                shifts.append(shift)
                inls.append(safe_load(in_index))
                in_index += 1
                shift = 0
            elif shift + bit > 32:  # cross the boundary
                inls.append(
                    f"{safe_load(in_index)} >> {shift} | {safe_load(in_index + 1)} << {32 - shift}")
                in_index += 1
                shift = bit - (32 - shift)
                shifts.append(0)  # zero shift
            else:
                shifts.append(shift)
                inls.append(safe_load(in_index))
                shift += bit

        bytes_per_batch = self.simd_byte_width
        words_per_batch = bytes_per_batch // 4

        one_word_template = dedent("""\
            words = simd_batch{{ {words} }};
            shifts = simd_batch{{ {shifts} }};
            results = (words >> shifts) & masks;
            results.store_unaligned(out);
            out += {words_per_batch};
            """)

        for start in range(0, 32, words_per_batch):
            stop = start + words_per_batch;
            p(f"""// extract {bit}-bit bundles {start} to {stop - 1}""")
            p(one_word_template.format(
                words=", ".join(inls[start:stop]),
                shifts=", ".join(map(str, shifts[start:stop])),
                words_per_batch=words_per_batch))

        p(dedent(f"""\
            in += {bit};
            return in;"""))
        print("}")


def print_copyright():
    print(dedent("""\
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
        """))


def print_note():
    print("// Automatically generated file; DO NOT EDIT.")
    print()


def main(simd_width):
    print_copyright()
    print_note()

    struct_name = f"UnpackBits{simd_width}"

    # NOTE: templating the UnpackBits struct on the dispatch level avoids
    # potential name collisions if there are several UnpackBits generations
    # with the same SIMD width on a given architecture.

    print(dedent(f"""\
        #pragma once

        #include <cstdint>
        #include <cstring>

        #include <xsimd/xsimd.hpp>

        #include "arrow/util/dispatch.h"
        #include "arrow/util/ubsan.h"

        namespace arrow {{
        namespace internal {{
        namespace {{

        using ::arrow::util::SafeLoad;

        template <DispatchLevel level>
        struct {struct_name} {{

        using simd_batch = xsimd::make_sized_batch_t<uint32_t, {simd_width//32}>;
        """))

    gen = UnpackGenerator(simd_width)
    gen.print_unpack_bit0_func()
    print()
    for i in range(1, 32):
        gen.print_unpack_bit_func(i)
        print()
    gen.print_unpack_bit32_func()
    print()

    print(dedent(f"""\
        }};  // struct {struct_name}

        }}  // namespace
        }}  // namespace internal
        }}  // namespace arrow
        """))


if __name__ == '__main__':
    usage = f"""Usage: {__file__} <SIMD bit-width>"""
    if len(sys.argv) != 2:
        raise ValueError(usage)
    try:
        simd_width = int(sys.argv[1])
    except ValueError:
        raise ValueError(usage)

    main(simd_width)
