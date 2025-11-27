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


@dataclasses.dataclass
class UnpackStructGenerator:
    out_type: str
    simd_bit_width: int

    @property
    def out_bit_width(self) -> int:
        if self.out_type == "bool":
            return 8
        elif self.out_type.startswith("uint"):
            return int(self.out_type.removeprefix("uint").removesuffix("_t"))

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
    def struct_name(self) -> str:
        return f"Simd{self.simd_bit_width}UnpackerForWidth"

    def struct_specialization(self, bit: int | str) -> str:
        return f"{self.struct_name}<{self.out_type}, {bit}>"

    def print_struct_declaration(self):
        print("template<typename Uint, int BitWidth>")
        print(f"struct {self.struct_name};")

    def print_unpack_bit_func(self, bit: int):
        def p(code, level=2):
            print(indent(code, prefix="  " * level))

        p(
            f"static const uint8_t* unpack(const uint8_t* in, {self.out_type}* out) {{",
            level=1,
        )

        mask = (1 << bit) - 1
        p(f"constexpr {self.out_type} kMask = 0x{mask:0x};")
        print()
        p("simd_batch masks(kMask);")
        p("simd_batch words, shifts;")
        p("simd_batch results;")

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
                p(f"{word_part},", level=3)
            p("};")
            p(
                one_word_template.format(
                    shifts=", ".join(map(str, shifts[start:stop])),
                    words_per_batch=self.simd_value_count,
                )
            )

        p(f"in += {bit} * {self.out_byte_width};")
        p("return in;")
        p("}", level=1)

    def print_struct_k(self, bit: int):
        print("template<>")
        print(f"struct {self.struct_specialization(bit)} {{")
        print()
        print(
            "  using simd_batch = xsimd::make_sized_batch_t<"
            f"{self.out_type}, {self.simd_value_count}>;"
        )
        print(f"  static constexpr int kValuesUnpacked = {self.out_bit_width};")
        print()
        self.print_unpack_bit_func(bit)
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
        if self.out_bit_width <= 16:
            self.print_uint32_fallback_struct()
            return
        for bit in range(1, self.out_bit_width):
            self.print_struct_k(bit)
            print()


@dataclasses.dataclass
class UnpackFileGenerator:
    generators: list[UnpackStructGenerator]

    def print_license(self):
        print(LICENSE)

    def print_note(self):
        print("// WARNING: this file is generated, DO NOT EDIT.")
        print("// Usage:")
        print(f"//   python {' '.join(sys.orig_argv[1:])}")

    def print_headers(self):
        print("#include <cstdint>")
        print("#include <cstring>")
        print()
        print("#include <xsimd/xsimd.hpp>")
        print()
        print('#include "arrow/util/ubsan.h"')

    def print_file_top(self):
        print("#pragma once")
        print()
        self.print_headers()
        print()
        print("namespace arrow::internal {")
        print("namespace {")
        print()
        print("using ::arrow::util::SafeLoadAs;")

    def print_file_bottom(self):
        print("}  // namespace")
        print("}  // namespace arrow::internal")

    def print_structs(self):
        delclared = set()

        for gen in self.generators:
            if gen.simd_bit_width not in delclared:
                gen.print_struct_declaration()
                print()
                delclared.add(gen.simd_bit_width)

            gen.print_structs()
            print()

    def print_file(self):
        self.print_license()
        self.print_note()
        print()
        self.print_file_top()
        print()
        self.print_structs()
        self.print_file_bottom()


def main(simd_width, outputs):
    generators = []
    for out_width in outputs:
        try:
            generators.append(UnpackStructGenerator(out_width, simd_width))
        except Exception as e:
            print(f"WARNING skipping bit width {out_width}: {e}", file=sys.stderr)

    gen = UnpackFileGenerator(generators)
    gen.print_file()


if __name__ == "__main__":
    usage = f"""Usage: {__file__} <SIMD bit-width>"""
    if len(sys.argv) != 2:
        raise ValueError(usage)
    try:
        simd_width = int(sys.argv[1])
    except ValueError:
        raise ValueError(usage)

    main(simd_width, ["bool", "uint8_t", "uint16_t", "uint32_t", "uint64_t"])
