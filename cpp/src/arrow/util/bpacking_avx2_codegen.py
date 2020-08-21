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

# Usage: python bpacking_avx2_codegen.py > bpacking_avx2_generated.h


def print_unpack_bit_func(bit):
    shift = 0
    shifts = []
    in_index = 0
    inls = []
    mask = (1 << bit) - 1
    bracket = "{"

    print(
        f"inline static const uint32_t* unpack{bit}_32_avx2(const uint32_t* in, uint32_t* out) {bracket}")
    print("  uint32_t mask = 0x%x;" % mask)
    print("  __m256i reg_shifts, reg_inls, reg_masks;")
    print("  __m256i results;")

    print("")
    for i in range(32):
        if shift + bit == 32:
            shifts.append(shift)
            inls.append(f"in[{in_index}]")
            in_index += 1
            shift = 0
        elif shift + bit > 32:  # cross the boundary
            inls.append(
                f"in[{in_index}] >> {shift} | in[{in_index + 1}] << {32 - shift}")
            in_index += 1
            shift = bit - (32 - shift)
            shifts.append(0)  # zero shift
        else:
            shifts.append(shift)
            inls.append(f"in[{in_index}]")
            shift += bit

    print("  reg_masks = _mm256_set1_epi32(mask);")
    print("")

    print("  // shift the first 8 outs")
    print(
        f"  reg_shifts = _mm256_set_epi32({shifts[7]}, {shifts[6]}, {shifts[5]}, {shifts[4]},")
    print(
        f"                               {shifts[3]}, {shifts[2]}, {shifts[1]}, {shifts[0]});")
    print(f"  reg_inls = _mm256_set_epi32({inls[7]}, {inls[6]},")
    print(f"                             {inls[5]}, {inls[4]},")
    print(f"                             {inls[3]}, {inls[2]},")
    print(f"                             {inls[1]}, {inls[0]});")
    print(
        "  results = _mm256_and_si256(_mm256_srlv_epi32(reg_inls, reg_shifts), reg_masks);")
    print("  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), results);")
    print("  out += 8;")
    print("")

    print("  // shift the second 8 outs")
    print(
        f"  reg_shifts = _mm256_set_epi32({shifts[15]}, {shifts[14]}, {shifts[13]}, {shifts[12]},")
    print(
        f"                                {shifts[11]}, {shifts[10]}, {shifts[9]}, {shifts[8]});")
    print(f"  reg_inls = _mm256_set_epi32({inls[15]}, {inls[14]},")
    print(f"                              {inls[13]}, {inls[12]},")
    print(f"                              {inls[11]}, {inls[10]},")
    print(f"                              {inls[9]}, {inls[8]});")
    print(
        "  results = _mm256_and_si256(_mm256_srlv_epi32(reg_inls, reg_shifts), reg_masks);")
    print("  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), results);")
    print("  out += 8;")
    print("")

    print("  // shift the third 8 outs")
    print(
        f"  reg_shifts = _mm256_set_epi32({shifts[23]}, {shifts[22]}, {shifts[21]}, {shifts[20]},")
    print(
        f"                                {shifts[19]}, {shifts[18]}, {shifts[17]}, {shifts[16]});")
    print(f"  reg_inls = _mm256_set_epi32({inls[23]}, {inls[22]},")
    print(f"                              {inls[21]}, {inls[20]},")
    print(f"                              {inls[19]}, {inls[18]},")
    print(f"                              {inls[17]}, {inls[16]});")
    print(
        "  results = _mm256_and_si256(_mm256_srlv_epi32(reg_inls, reg_shifts), reg_masks);")
    print("  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), results);")
    print("  out += 8;")
    print("")

    print("  // shift the last 8 outs")
    print(
        f"  reg_shifts = _mm256_set_epi32({shifts[31]}, {shifts[30]}, {shifts[29]}, {shifts[28]},")
    print(
        f"                                {shifts[27]}, {shifts[26]}, {shifts[25]}, {shifts[24]});")
    print(f"  reg_inls = _mm256_set_epi32({inls[31]}, {inls[30]},")
    print(f"                              {inls[29]}, {inls[28]},")
    print(f"                              {inls[27]}, {inls[26]},")
    print(f"                              {inls[25]}, {inls[24]});")
    print(
        "  results = _mm256_and_si256(_mm256_srlv_epi32(reg_inls, reg_shifts), reg_masks);")
    print("  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), results);")
    print("  out += 8;")

    print("")
    print(f"  in += {bit};")
    print("")
    print("  return in;")
    print("}")


def print_unpack_bit0_func():
    print(
        "inline static const uint32_t* unpack0_32_avx2(const uint32_t* in, uint32_t* out) {")
    print("  memset(out, 0x0, 32 * sizeof(*out));")
    print("  out += 32;")
    print("")
    print("  return in;")
    print("}")


def print_unpack_bit32_func():
    print(
        "inline static const uint32_t* unpack32_32_avx2(const uint32_t* in, uint32_t* out) {")
    print("  memcpy(out, in, 32 * sizeof(*out));")
    print("  in += 32;")
    print("  out += 32;")
    print("")
    print("  return in;")
    print("}")


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


def main():
    print_copyright()
    print_note()
    print("")
    print("#pragma once")
    print("")
    print("#include <stdint.h>")
    print("#include <string.h>")
    print("")
    print("#ifdef _MSC_VER")
    print("#include <intrin.h>")
    print("#else")
    print("#include <immintrin.h>")
    print("#endif")
    print("")
    print("namespace arrow {")
    print("namespace internal {")
    print("")
    print_unpack_bit0_func()
    print("")
    for i in range(1, 32):
        print_unpack_bit_func(i)
        print("")
    print_unpack_bit32_func()
    print("")
    print("}  // namespace internal")
    print("}  // namespace arrow")


if __name__ == '__main__':
    main()
