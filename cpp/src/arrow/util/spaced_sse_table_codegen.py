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
# We use below API to achieve the expand/compress function, and a generated
# lookup table to avoid the slow loop over the valit bits mask.
#
#   __m128i _mm_shuffle_epi8 (__m128i a, __m128i b)
#   Description
#   Shuffle packed 8-bit integers in a according to shuffle control mask in
#     the corresponding 8-bit element of b, and store the results in dst.
#   Operation
#   FOR j := 0 to 15
#     i := j*8
#     IF b[i+7] == 1
#       dst[i+7:i] := 0
#     ELSE
#       index[3:0] := b[i+3:i]
#       dst[i+7:i] := a[index*8+7:index*8]
#     FI
#   ENDFOR

NullMask = 0x80  # Top significant bit of a byte mask


def print_mask_expand_table(dt_byte_width, item_byte_width):
    """
    Generate the lookup table for SSE expand shuffle control mask.

    Parameters
    ----------
    dt_byte_width : int
        Byte width of the data type
    item_byte_width:
        Byte width of each shuffle control item in the table

    Notes
    -----
    Ex, for epi32 full table, the data type is 32 bits(4 bytes), the shuffle control item in the table is 128 bits(16 bytes).
    The available input valid bits mask of table is in [0, 1 << (item_byte_width / dt_byte_width)].
    Then for mask 0b0101(each bit represent if each epi32 is valid), the target mask128 shuffle control lookup item is:
    [0x00, 0x01, 0x02, 0x03, 0x80, 0x80, 0x80, 0x80, 0x04, 0x05, 0x06, 0x07, 0x80, 0x80, 0x80, 0x80]
    """

    loop = int(item_byte_width / dt_byte_width)
    for index in range(1 << loop):
        maps = []
        lastbit = 0x00

        for bit in range(loop):
            if (index & (1 << bit)):
                for n in range(dt_byte_width):
                    maps.append(dt_byte_width * lastbit + n)
                lastbit += 1
            else:
                for n in range(dt_byte_width):
                    maps.append(NullMask)

        out = ""
        for item in range(item_byte_width):
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


def print_mask_compress_table(dt_byte_width, item_byte_width):
    """
    Generate the lookup table for SSE compress shuffle control mask.

    Parameters
    ----------
    dt_byte_width : int
        Byte width of the data type
    item_byte_width:
        Byte width of each shuffle control item in the table

    Notes
    -----
    Ex, for epi32 full table, the data type is 32 bits(4 bytes), the shuffle control item in the table is 128 bits(16 bytes).
    The available input valid bits mask of table is in [0, 1 << (item_byte_width / dt_byte_width)].
    Then for mask 0b0101(each bit represent if each epi32 is valid), the target mask128 shuffle control lookup item is:
    [0x00, 0x01, 0x02, 0x03, 0x08, 0x09, 0x0a, 0x0b, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]
    """

    loop = int(item_byte_width / dt_byte_width)
    for index in range(1 << loop):
        maps = []

        for bit in range(loop):
            if (index & (1 << bit)):
                for n in range(dt_byte_width):
                    maps.append(dt_byte_width * bit + n)
        while(len(maps) < item_byte_width):
            maps.append(NullMask)

        out = ""
        for item in range(item_byte_width):
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
    Generate the lookup table for SSE epi8 thin compact shuffle control mask.

    Notes
    -----
    The available input number of table is in [0, 9], for input 5, the target mask128 lookup item is
        5 byte from low part, 8 byte from high part, remaining set to null(0x80).
    [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x80, 0x80]
    """

    print("extern const uint8_t kMask128SseEpi8ThinCompact[] = {")

    for i in range(9):
        maps = []
        for j in range(i):
            maps.append(j)
        for j in range(8):
            maps.append(j + 8)
        while(len(maps) < 16):
            maps.append(NullMask)
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
