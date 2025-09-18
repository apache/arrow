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
#   python bpacking_simd_codegen.py 128 > bpacking_simd128_generated_internal.h
#   python bpacking_simd_codegen.py 256 > bpacking_simd256_generated_internal.h
#   python bpacking_simd_codegen.py 512 > bpacking_simd512_generated_internal.h

import dataclasses
import sys
from textwrap import dedent, indent


LICENSE = """// Licensed to the Apache Software Foundation (ASF) under one
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
"""

HEADER = """
#pragma once

#include <cstdint>
#include <cstring>

#include <xsimd/xsimd.hpp>

#include "arrow/util/ubsan.h"

namespace arrow::internal {

using ::arrow::util::SafeLoadAs;
"""

FOOTER = """
}  // namespace arrow::internal
"""


@dataclasses.dataclass
class UnpackGenerator:
    out_bit_width: int
    simd_bit_width: int

    @property
    def simd_byte_width(self) -> int:
        return self.simd_bit_width // 8

    @property
    def simd_value_count(self) -> int:
        return self.simd_bit_width // self.out_bit_width

    @property
    def out_byte_width(self) -> int:
        return self.out_bit_width // 8

    @property
    def out_type(self) -> str:
        return f"uint{self.out_bit_width}_t"

    def __post_init__(self):
        if self.simd_bit_width % self.out_bit_width != 0:
            raise ("SIMD bit width should be a multiple of output width")

    def print_unpack_signature(self, bit: int | None) -> str:
        if bit is None:
            print("template<int kBit>")
            static = "static "
            specialized = ""
            end = ";"
        else:
            print("template<>")
            static = ""
            specialized = f"<{bit}>"
            end = " {"

        print(
            f"{static}const uint8_t* unpack{specialized}"
            f"(const uint8_t* in, {self.out_type}* out){end}"
        )

    def print_struct_header(self):
        print("template<>")
        print(f"struct Simd{self.simd_bit_width}Unpacker<{self.out_type}> {{")
        print()
        self.print_unpack_signature(None)

    def print_unpack_bit0_func(self):
        self.print_unpack_signature(0)
        print(f"  std::memset(out, 0x0, {self.out_bit_width} * sizeof(*out));")
        print(f"  out += {self.out_bit_width};")
        print("  return in;")
        print("}")

    def print_unpack_bitmax_func(self):
        self.print_unpack_signature(self.out_bit_width)
        print(f"  std::memcpy(out, in, {self.out_bit_width} * sizeof(*out));")
        print(f"  in += {self.out_byte_width} * {self.out_bit_width};")
        print(f"  out += {self.out_bit_width};")
        print("  return in;")
        print("}")

    def print_unpack_bit_func(self, bit: int):
        self.print_unpack_signature(bit)

        def p(code, level=1):
            print(indent(code, prefix="  " * level))

        mask = (1 << bit) - 1

        p(
            dedent(f"""\
            using simd_batch = xsimd::make_sized_batch_t<{self.out_type}, {self.simd_value_count}>;

            constexpr {self.out_type} kMask = 0x{mask:0x};

            simd_batch masks(kMask);
            simd_batch words, shifts;
            simd_batch results;
            """)
        )

        def safe_load(index):
            return f"SafeLoadAs<{self.out_type}>(in + {self.out_byte_width} * {index})"

        def static_cast_as_needed(str):
            if self.out_bit_width < 32:
                return f"static_cast<{self.out_type}>({str})"
            return str

        shift = 0
        shifts = []
        in_index = 0
        inls = []

        for i in range(self.out_bit_width):
            if shift + bit == self.out_bit_width:
                shifts.append(shift)
                inls.append(safe_load(in_index))
                in_index += 1
                shift = 0
            elif shift + bit > self.out_bit_width:  # cross the boundary
                inls.append(
                    static_cast_as_needed(
                        f"{safe_load(in_index)} >> {shift} "
                        f"| {safe_load(in_index + 1)} << {self.out_bit_width - shift}"
                    )
                )
                in_index += 1
                shift = bit - (self.out_bit_width - shift)
                shifts.append(0)  # zero shift
            else:
                shifts.append(shift)
                inls.append(safe_load(in_index))
                shift += bit

        one_word_template = dedent("""\
            shifts = simd_batch{{ {shifts} }};
            results = (words >> shifts) & masks;
            results.store_unaligned(out);
            out += {words_per_batch};
            """)

        for start in range(0, self.out_bit_width, self.simd_value_count):
            stop = start + self.simd_value_count
            p(f"""// extract {bit}-bit bundles {start} to {stop - 1}""")
            p("words = simd_batch{")
            for word_part in inls[start:stop]:
                p(f"{word_part},", level=2)
            p("};")
            p(
                one_word_template.format(
                    shifts=", ".join(map(str, shifts[start:stop])),
                    words_per_batch=self.simd_value_count,
                )
            )

        p(
            dedent(f"""\
            in += {bit} * {self.out_byte_width};
            return in;""")
        )
        print("}")

    def print_all(self):
        self.print_struct_header()
        print()

        self.print_unpack_bit0_func()
        print()
        for i in range(1, self.out_bit_width):
            self.print_unpack_bit_func(i)
            print()
        self.print_unpack_bitmax_func()

        print("};  // struct Unpacker")


def print_note():
    print("// WARNING: this file is generated, DO NOT EDIT.")
    print("// Usage:")
    print(f"//   python {' '.join(sys.orig_argv[1:])}")


def main(simd_width, outputs):
    print(LICENSE)
    print_note()
    print(HEADER)

    print("template<typename Uint>")
    print(f"struct Simd{simd_width}Unpacker;")
    print()

    for out_width in outputs:
        gen = UnpackGenerator(out_width, simd_width)
        gen.print_all()
        print()

    print(FOOTER)


if __name__ == "__main__":
    usage = f"""Usage: {__file__} <SIMD bit-width>"""
    if len(sys.argv) != 2:
        raise ValueError(usage)
    try:
        simd_width = int(sys.argv[1])
    except ValueError:
        raise ValueError(usage)

    main(simd_width, [16, 32])
