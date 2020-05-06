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

# Usage: python3 spaced_sse_codegen.py > spaced_sse_generated.h


def print_mask_expand_bitmap(width, length):
    loop = int(length / width)
    null_mask = 0x80
    for index in range(1 << loop):
        maps = []
        lastbit = 0x00

        for bit in range(loop):
            if (index & (1 << bit)):
                for n in range(width):
                    maps.append(width * lastbit + n)
                lastbit += 1
            else:
                for n in range(width):
                    maps.append(null_mask)

        out = ""
        for item in range(length):
            out += " 0x" + format(maps[item], '02x') + ","
        print(" " + out)


def print_mask_expand_epi32():
    """Generate the mask bit map for sse epi32 spaced expand"""
    print("static constexpr uint8_t kMask128SseExpandEpi32[] = {")
    print_mask_expand_bitmap(4, 16)
    print("};")


def print_mask_expand_epi64():
    """Generate the mask bit map for sse epi64 spaced expand"""
    print("static constexpr uint8_t kMask128SseExpandEpi64[] = {")
    print_mask_expand_bitmap(8, 16)
    print("};")


def print_mask_expand_epi8():
    """Generate the mask bit map for sse epi8 spaced expand"""
    print("static constexpr uint8_t kMask128SseExpandEpi8Thin[] = {")
    print_mask_expand_bitmap(1, 8)
    print("};")


def print_mask_compress_bitmap(width, length):
    loop = int(length / width)
    null_mask = 0x80
    for index in range(1 << loop):
        maps = []

        for bit in range(loop):
            if (index & (1 << bit)):
                for n in range(width):
                    maps.append(width * bit + n)
        while(len(maps) < length):
            maps.append(null_mask)

        out = ""
        for item in range(length):
            out += " 0x" + format(maps[item], '02x') + ","
        print(" " + out)


def print_mask_compress_epi32():
    """Generate the mask bit map for sse epi32 spaced compress"""
    print("static constexpr uint8_t kMask128SseCompressEpi32[] = {")
    print_mask_compress_bitmap(4, 16)
    print("};")


def print_mask_compress_epi64():
    """Generate the mask bit map for sse epi64 spaced compress"""
    print("static constexpr uint8_t kMask128SseCompressEpi64[] = {")
    print_mask_compress_bitmap(8, 16)
    print("};")


def print_mask_compress_epi8():
    """Generate the mask bit map for sse epi8 spaced compress"""
    print("static constexpr uint8_t kMask128SseCompressEpi8Thin[] = {")
    print_mask_compress_bitmap(1, 8)
    print("};")


def print_epi8_thin_compact():
    print("static constexpr uint8_t kMask128SseEpi8ThinCompact[] = {")

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
    print("#pragma once")
    print("")
    print("namespace arrow {")
    print("namespace internal {")
    print("")
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
    print("")
    print("}  // namespace internal")
    print("}  // namespace arrow")
