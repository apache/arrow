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

# Generate the mask of lookup table for spaced compress and expand
# Modified from original source:
# https://github.com/lemire/simdprune/blob/master/scripts/epi32.py
# https://github.com/lemire/simdprune/blob/master/scripts/epi8.py
# The original copyright notice follows.

# This code is released under the
# Apache License Version 2.0 http://www.apache.org/licenses/.
# (c) Daniel Lemire

# Usage: python3 spaced_sse_table_codegen.py > spaced_sse_table_generated.cc


def print_mask_expand_table(pack_width, mask_length):
    """
    Generate the lookup mask table for SSE expand shuffle control mask.
    Ex, for epi32 full table(pack_width = 4(32/8), mask_length = 16(128/8)), the available mask
    should be in [0, 1 << 4], for mask 0b0101(each bit represent if one epi32 is valid),
    the target mask128 lookup item is:
    [0x00, 0x01, 0x02, 0x03, 0x80, 0x80, 0x80, 0x80, 0x04, 0x05, 0x06, 0x07, 0x80, 0x80, 0x80, 0x80]
    """
    loop = int(mask_length / pack_width)
    null_mask = 0x80
    for index in range(1 << loop):
        maps = []
        lastbit = 0x00

        for bit in range(loop):
            if (index & (1 << bit)):
                for n in range(pack_width):
                    maps.append(pack_width * lastbit + n)
                lastbit += 1
            else:
                for n in range(pack_width):
                    maps.append(null_mask)

        out = ""
        for item in range(mask_length):
            out += " 0x" + format(maps[item], '02x') + ","
        print(" " + out)


def print_mask_expand_epi32():
    """Generate the mask table for sse epi32 spaced expand"""
    print("extern const uint8_t kMask128SseExpandEpi32[] = {")
    print_mask_expand_table(4, 16)
    print("};")


def print_mask_expand_epi64():
    """Generate the mask table for sse epi64 spaced expand"""
    print("extern const uint8_t kMask128SseExpandEpi64[] = {")
    print_mask_expand_table(8, 16)
    print("};")


def print_mask_expand_epi8():
    """Generate the mask table for sse epi8 spaced expand"""
    print("extern const uint8_t kMask64SseExpandEpi8Thin[] = {")
    print_mask_expand_table(1, 8)
    print("};")


def print_mask_compress_table(pack_width, mask_length):
    """
    Generate the lookup mask table for SSE compress shuffle control mask.
    Ex, for epi32 full table(pack_width = 4(32/8), mask_length = 16(128/8)), the available mask
    should be in [0, 1 << 4], for mask 0b0101(each bit represent if one epi32 is valid),
    the target mask128 lookup item is:
    [0x00, 0x01, 0x02, 0x03, 0x08, 0x09, 0x0a, 0x0b, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]
    """
    loop = int(mask_length / pack_width)
    null_mask = 0x80
    for index in range(1 << loop):
        maps = []

        for bit in range(loop):
            if (index & (1 << bit)):
                for n in range(pack_width):
                    maps.append(pack_width * bit + n)
        while(len(maps) < mask_length):
            maps.append(null_mask)

        out = ""
        for item in range(mask_length):
            out += " 0x" + format(maps[item], '02x') + ","
        print(" " + out)


def print_mask_compress_epi32():
    """Generate the mask table for sse epi32 spaced compress"""
    print("extern const uint8_t kMask128SseCompressEpi32[] = {")
    print_mask_compress_table(4, 16)
    print("};")


def print_mask_compress_epi64():
    """Generate the mask table for sse epi64 spaced compress"""
    print("extern const uint8_t kMask128SseCompressEpi64[] = {")
    print_mask_compress_table(8, 16)
    print("};")


def print_mask_compress_epi8():
    """Generate the mask table for sse epi8 spaced compress"""
    print("extern const uint8_t kMask64SseCompressEpi8Thin[] = {")
    print_mask_compress_table(1, 8)
    print("};")


def print_epi8_thin_compact():
    """
    Generate the lookup mask table for SSE epi8 thin compact shuffle control mask.
    The available mask should be in [0, 9], for mask 5, the target mask128 lookup item is 5 byte from
    low part, 8 byte from high part, remaining set to null(0x80).
    [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x80, 0x80]
    """
    print("extern const uint8_t kMask128SseEpi8ThinCompact[] = {")

    null_mask = 0x80
    for i in range(9):
        maps = []
        for j in range(i):
            maps.append(j)
        for j in range(8):
            maps.append(j + 8)
        while(len(maps) < 16):
            maps.append(null_mask)
        out = ""
        for s in range(16):
            out += " 0x" + format(maps[s], '02x') + ","
        print(" " + out)

    print("};")


def print_copyright():
    print(
        """// Licensed to the Apache Software Foundation (ASF) under one
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
// under the License.""")


def print_note():
    print("//")
    print("// Automatically generated file; DO NOT EDIT.")


if __name__ == '__main__':
    print_copyright()
    print_note()
    print("")
    print("#include <cstdint>")
    print('#include "arrow/util/spaced_sse.h"')
    print("")
    print("namespace arrow {")
    print("namespace internal {")
    print("")
    print("#if defined(ARROW_HAVE_SSE4_2)")
    print_mask_compress_epi32()
    print("")
    print_mask_compress_epi64()
    print("")
    print_mask_compress_epi8()
    print("")
    print_mask_expand_epi32()
    print("")
    print_mask_expand_epi64()
    print("")
    print_mask_expand_epi8()
    print("")
    print_epi8_thin_compact()
    print("#endif // ARROW_HAVE_SSE4_2")
    print("")
    print("}  // namespace internal")
    print("}  // namespace arrow")
