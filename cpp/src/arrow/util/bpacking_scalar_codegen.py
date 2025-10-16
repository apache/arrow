#!/bin/python

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

# This script is modified from its original version in GitHub. Original source:
# https://github.com/lemire/FrameOfReference/blob/146948b6058a976bc7767262ad3a2ce201486b93/scripts/turbopacking64.py

# Usage:
#   python bpacking_scalar_codegen.py > bpacking_scalar_generated_internal.h


import dataclasses
import sys


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

#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"

namespace arrow::internal {
namespace {

template <typename Int>
Int LoadInt(const uint8_t* in) {
  return bit_util::FromLittleEndian(util::SafeLoadAs<Int>(in));
}
"""

FOOTER = """
}  // namespace
}  // namespace arrow::internal
"""


@dataclasses.dataclass
class ScalarUnpackGenerator:
    out_type: str
    # Used to reduce the number of bytes read on odd bit width by reading
    # the last input in half width.
    smart_halve: bool

    @property
    def out_bit_width(self) -> int:
        if self.out_type.startswith("uint"):
            return int(self.out_type.removeprefix("uint").removesuffix("_t"))
        raise NotImplementedError(f"Unsupported type {self.out_type}")

    @property
    def out_byte_width(self) -> int:
        return self.out_bit_width // 8

    @property
    def out_type_half(self) -> str:
        return f"uint{self.out_bit_width // 2}_t"

    @property
    def struct_name(self) -> str:
        return "ScalarUnpackerForWidth"

    def struct_specialization(self, bit: int) -> str:
        return f"{self.struct_name}<{self.out_type}, {bit}>"

    def print_struct_declaration(self):
        print("template<typename Uint, int kBitWidth>")
        print(f"struct {self.struct_name};")

    @property
    def total_in_values(self) -> int:
        """How many values are we going to unpack?"""
        if self.smart_halve:
            return self.out_bit_width // 2
        return self.out_bit_width

    def total_out_values(self, bit: int) -> int:
        return (
            self.total_in_values * bit + self.out_bit_width - 1
        ) // self.out_bit_width

    def total_out_bytes(self, bit: int) -> int:
        return (
            self.total_in_values * bit + self.out_byte_width - 1
        ) // self.out_byte_width

    def print_unpack_k(self, bit: int) -> None:
        print(
            f"  static const uint8_t* unpack(const uint8_t* in, {self.out_type}* out) {{"
        )

        print(
            f"    constexpr {self.out_type} mask = "
            f"(({self.out_type}{{1}} << {bit}) - {self.out_type}{{1}});"
        )
        print()
        maskstr = " & mask"

        for k in range(self.total_out_values(bit) - 1):
            print(
                f"    const auto w{k} = LoadInt<{self.out_type}>("
                f"in + {k} * {self.out_byte_width});"
            )

        k = self.total_out_values(bit) - 1
        use_smart_halving = self.smart_halve and bit % 2 == 1
        if use_smart_halving:
            print(
                f"    const auto w{k} = static_cast<{self.out_type}>(LoadInt<{self.out_type_half}>("
                f"in + {k} * {self.out_byte_width}));"
            )
        else:
            print(
                f"    const auto w{k} = LoadInt<{self.out_type}>("
                f"in + {k} * {self.out_byte_width});"
            )

        for j in range(self.total_in_values):
            firstword = j * bit // self.out_bit_width
            secondword = (j * bit + bit - 1) // self.out_bit_width
            firstshift = (j * bit) % self.out_bit_width
            firstshiftstr = f" >> {firstshift}"
            if firstshift == 0:
                firstshiftstr = ""  # no need
            if firstword == secondword:
                if firstshift + bit == self.out_bit_width:
                    print(f"    out[{j}] = w{firstword}{firstshiftstr};")
                else:
                    print(f"    out[{j}] = (w{firstword}{firstshiftstr}){maskstr};")
            else:
                secondshift = self.out_bit_width - firstshift
                print(
                    f"    out[{j}] = ((w{firstword}{firstshiftstr}) | "
                    f"(w{firstword + 1} << {secondshift})){maskstr};"
                )
        print()

        if use_smart_halving:
            print(
                f"    return in + ({self.total_out_values(bit) - 1} * {self.out_byte_width}"
                f" + {self.out_byte_width // 2});"
            )
        else:
            print(
                f"    return in + ({self.total_out_values(bit)} * {self.out_byte_width});"
            )
        print("  }")

    def print_struct_k(self, bit: int) -> None:
        print("template<>")
        print(f"struct {self.struct_specialization(bit)} {{")
        print()
        print(f"  static constexpr int kValuesUnpacked = {self.total_in_values};")
        print()
        self.print_unpack_k(bit)
        print("};")

    def print_uint32_fallback_struct(self):
        print("template<int kBitWidth>")
        print(f"struct {self.struct_specialization('kBitWidth')} {{")
        print()

        print(
            "  static constexpr int kValuesUnpacked = "
            f"{self.struct_name}<uint32_t, kBitWidth>::kValuesUnpacked;"
        )
        print()

        print(
            "  static const uint8_t* unpack"
            f"(const uint8_t* in, {self.out_type}* out) {{"
        )
        print("    uint32_t buffer[kValuesUnpacked] = {};")
        print(f"    in = {self.struct_name}<uint32_t, kBitWidth>::unpack(in, buffer);")
        print("    for(int k = 0; k< kValuesUnpacked; ++k) {")
        print(f"      out[k] = static_cast<{self.out_type}>(buffer[k]);")
        print("    }")
        print("    return in;")
        print("  }")

        print("};")

    def print_structs(self):
        # The algorithm works for uint16_t (for simd width <=256) but is slower
        # than using uint32_t + static_cast loop
        if self.out_type == "bool" or self.out_bit_width <= 16:
            self.print_uint32_fallback_struct()
            return
        for bit in range(1, self.out_bit_width):
            self.print_struct_k(bit)
            print()


def print_note():
    print("// WARNING: this file is generated, DO NOT EDIT.")
    print("// Usage:")
    print(f"//   python {' '.join(sys.orig_argv[1:])}")


if __name__ == "__main__":
    print(LICENSE)
    print_note()
    print(HEADER)

    gen = ScalarUnpackGenerator("", smart_halve=False)
    gen.print_struct_declaration()

    gen = ScalarUnpackGenerator("bool", smart_halve=False)
    gen.print_structs()
    print()

    gen = ScalarUnpackGenerator("uint8_t", smart_halve=False)
    gen.print_structs()
    print()

    gen = ScalarUnpackGenerator("uint16_t", smart_halve=False)
    gen.print_structs()
    print()

    gen = ScalarUnpackGenerator("uint32_t", smart_halve=False)
    gen.print_structs()
    print()

    # Enable "smart_halve" because we want to decode 32 values at a time,
    # which is half the (uint64_t) word size.
    gen = ScalarUnpackGenerator("uint64_t", smart_halve=True)
    gen.print_structs()

    print(FOOTER)
