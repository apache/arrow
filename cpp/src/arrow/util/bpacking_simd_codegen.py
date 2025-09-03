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

import sys
from textwrap import dedent, indent


class UnpackGenerator:
    def __init__(self, simd_width, out_width, out_type):
        self.simd_width = simd_width
        self.out_width = out_width
        if simd_width % out_width != 0:
            raise ("SIMD bit width should be a multiple of output width")
        self.simd_byte_width = simd_width // 8
        self.out_byte_width = out_width // 8
        self.out_type = out_type

    def print_unpack_bit0_func(self):
        ty = self.out_type
        print(
            f"inline static const {ty}* unpack0_{self.out_width}(const {ty}* in, {ty}* out) {{"
        )
        print(f"  std::memset(out, 0x0, {self.out_width} * sizeof(*out));")
        print(f"  out += {self.out_width};")
        print("")
        print("  return in;")
        print("}")

    def print_unpack_bitmax_func(self):
        ty = self.out_type
        print(
            f"inline static const {ty}* unpack{self.out_width}_{self.out_width}(const {ty}* in, {ty}* out) {{"
        )
        print(f"  std::memcpy(out, in, {self.out_width} * sizeof(*out));")
        print(f"  in += {self.out_width};")
        print(f"  out += {self.out_width};")
        print("")
        print("  return in;")
        print("}")

    def print_unpack_bit_func(self, bit):
        def p(code, level=1):
            print(indent(code, prefix="  " * level))

        mask = (1 << bit) - 1
        ty = self.out_type
        bytes_per_batch = self.simd_byte_width
        words_per_batch = bytes_per_batch // self.out_byte_width

        print(
            f"inline static const {ty}* unpack{bit}_{self.out_width}(const {ty}* in, {ty}* out) {{"
        )
        p(
            dedent(f"""\
            using simd_batch = xsimd::make_sized_batch_t<{ty}, {self.simd_width // self.out_width}>;

            {ty} mask = 0x{mask:0x};

            simd_batch masks(mask);
            simd_batch words, shifts;
            simd_batch results;
            """)
        )

        def safe_load(index):
            return f"SafeLoad<{ty}>(in + {index})"

        def static_cast_as_needed(str):
            if self.out_width < 32:
                return f"static_cast<{ty}>({str})"
            return str

        shift = 0
        shifts = []
        in_index = 0
        inls = []

        for i in range(self.out_width):
            if shift + bit == self.out_width:
                shifts.append(shift)
                inls.append(safe_load(in_index))
                in_index += 1
                shift = 0
            elif shift + bit > self.out_width:  # cross the boundary
                inls.append(
                    static_cast_as_needed(
                        f"{safe_load(in_index)} >> {shift} "
                        f"| {safe_load(in_index + 1)} << {self.out_width - shift}"
                    )
                )
                in_index += 1
                shift = bit - (self.out_width - shift)
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

        for start in range(0, self.out_width, words_per_batch):
            stop = start + words_per_batch
            p(f"""// extract {bit}-bit bundles {start} to {stop - 1}""")
            p("words = simd_batch{")
            for word_part in inls[start:stop]:
                p(f"{word_part},", level=2)
            p("};")
            p(
                one_word_template.format(
                    shifts=", ".join(map(str, shifts[start:stop])),
                    words_per_batch=words_per_batch,
                )
            )

        p(
            dedent(f"""\
            in += {bit};
            return in;""")
        )
        print("}")


def print_copyright():
    print(
        dedent("""\
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
        """)
    )


def print_note():
    print("// Automatically generated file; DO NOT EDIT.")
    print()


def main(simd_width, outputs):
    print_copyright()
    print_note()

    struct_name = f"UnpackBits{simd_width}"

    # NOTE: templating the UnpackBits struct on the dispatch level avoids
    # potential name collisions if there are several UnpackBits generations
    # with the same SIMD width on a given architecture.

    print(
        dedent(f"""\
        #pragma once

        #include <cstdint>
        #include <cstring>

        #include <xsimd/xsimd.hpp>

        #include "arrow/util/dispatch_internal.h"
        #include "arrow/util/ubsan.h"

        namespace arrow::internal {{
        namespace {{

        using ::arrow::util::SafeLoad;

        template <DispatchLevel level>
        struct {struct_name} {{
        """)
    )

    for out_width, out_type in outputs:
        gen = UnpackGenerator(simd_width, out_width, out_type)
        gen.print_unpack_bit0_func()
        print()
        for i in range(1, out_width):
            gen.print_unpack_bit_func(i)
            print()
        gen.print_unpack_bitmax_func()
        print()

    print(
        dedent(f"""\
        }};  // struct {struct_name}

        }}  // namespace
        }}  // namespace arrow::internal
        """)
    )


if __name__ == "__main__":
    usage = f"""Usage: {__file__} <SIMD bit-width>"""
    if len(sys.argv) != 2:
        raise ValueError(usage)
    try:
        simd_width = int(sys.argv[1])
    except ValueError:
        raise ValueError(usage)

    outputs = [(16, "uint16_t"), (32, "uint32_t")]
    main(simd_width, outputs)
