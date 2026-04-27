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

// Generates ALP reference blobs for cross-implementation testing.
// Usage: compile, run, pipe output into Java test file.

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "arrow/util/alp/alp_wrapper.h"

using namespace arrow::util::alp;

static void printHex(const std::string& name, const char* data, size_t len) {
  std::cout << "  // " << name << " (" << len << " bytes)" << std::endl;
  std::cout << "  private static final byte[] " << name << " = {" << std::endl;
  for (size_t i = 0; i < len; i++) {
    if (i % 16 == 0) std::cout << "      ";
    char buf[16];
    snprintf(buf, sizeof(buf), "(byte)0x%02X", (unsigned char)data[i]);
    std::cout << buf;
    if (i < len - 1) std::cout << ", ";
    if (i % 16 == 15 || i == len - 1) std::cout << std::endl;
  }
  std::cout << "  };" << std::endl << std::endl;
}

static void printFloatArray(const std::string& name, const float* data, size_t len) {
  std::cout << "  private static final float[] " << name << " = {" << std::endl;
  for (size_t i = 0; i < len; i++) {
    if (i % 4 == 0) std::cout << "      ";
    uint32_t bits;
    memcpy(&bits, &data[i], 4);
    char buf[48];
    snprintf(buf, sizeof(buf), "Float.intBitsToFloat(0x%08X)", bits);
    std::cout << buf;
    if (i < len - 1) std::cout << ", ";
    if (i % 4 == 3 || i == len - 1) std::cout << std::endl;
  }
  std::cout << "  };" << std::endl << std::endl;
}

static void printDoubleArray(const std::string& name, const double* data, size_t len) {
  std::cout << "  private static final double[] " << name << " = {" << std::endl;
  for (size_t i = 0; i < len; i++) {
    if (i % 2 == 0) std::cout << "      ";
    uint64_t bits;
    memcpy(&bits, &data[i], 8);
    char buf[64];
    snprintf(buf, sizeof(buf), "Double.longBitsToDouble(0x%016lXL)", (unsigned long)bits);
    std::cout << buf;
    if (i < len - 1) std::cout << ", ";
    if (i % 2 == 1 || i == len - 1) std::cout << std::endl;
  }
  std::cout << "  };" << std::endl << std::endl;
}

template <typename T>
static void generateBlob(const std::string& prefix, const T* input, size_t count) {
  size_t decomp_size = count * sizeof(T);
  size_t max_size = AlpCodec<T>::GetMaxCompressedSize(decomp_size);
  std::vector<char> compressed(max_size);
  size_t comp_size = 0;
  AlpCodec<T>::Encode(input, decomp_size, compressed.data(), &comp_size);
  printHex(prefix + "_COMPRESSED", compressed.data(), comp_size);
}

int main() {
  // === Test case 1: Float decimal values (16 elements) ===
  {
    constexpr size_t N = 16;
    float input[N];
    for (size_t i = 0; i < N; i++) input[i] = static_cast<float>(i) * 0.1f;
    std::cout << "  // === Float decimal values (16 elements) ===" << std::endl;
    printFloatArray("FLOAT_DECIMAL_INPUT", input, N);
    generateBlob<float>("FLOAT_DECIMAL", input, N);
  }

  // === Test case 2: Float integer values (16 elements) ===
  {
    constexpr size_t N = 16;
    float input[N];
    for (size_t i = 0; i < N; i++) input[i] = static_cast<float>(i * 10);
    std::cout << "  // === Float integer values (16 elements) ===" << std::endl;
    printFloatArray("FLOAT_INTEGER_INPUT", input, N);
    generateBlob<float>("FLOAT_INTEGER", input, N);
  }

  // === Test case 3: Float with special values (16 elements) ===
  {
    constexpr size_t N = 16;
    float input[N];
    for (size_t i = 0; i < N; i++) input[i] = static_cast<float>(i) * 1.5f;
    input[3] = std::numeric_limits<float>::quiet_NaN();
    input[7] = std::numeric_limits<float>::infinity();
    input[11] = -std::numeric_limits<float>::infinity();
    input[14] = -0.0f;
    std::cout << "  // === Float with special values (16 elements) ===" << std::endl;
    printFloatArray("FLOAT_SPECIAL_INPUT", input, N);
    generateBlob<float>("FLOAT_SPECIAL", input, N);
  }

  // === Test case 4: Double decimal values (16 elements) ===
  {
    constexpr size_t N = 16;
    double input[N];
    for (size_t i = 0; i < N; i++) input[i] = static_cast<double>(i) * 0.01;
    std::cout << "  // === Double decimal values (16 elements) ===" << std::endl;
    printDoubleArray("DOUBLE_DECIMAL_INPUT", input, N);
    generateBlob<double>("DOUBLE_DECIMAL", input, N);
  }

  // === Test case 5: Double integer values (16 elements) ===
  {
    constexpr size_t N = 16;
    double input[N];
    for (size_t i = 0; i < N; i++) input[i] = static_cast<double>(i * 10);
    std::cout << "  // === Double integer values (16 elements) ===" << std::endl;
    printDoubleArray("DOUBLE_INTEGER_INPUT", input, N);
    generateBlob<double>("DOUBLE_INTEGER", input, N);
  }

  // === Test case 6: Float constant (16 elements) ===
  {
    constexpr size_t N = 16;
    float input[N];
    for (size_t i = 0; i < N; i++) input[i] = 3.14f;
    std::cout << "  // === Float constant (16 elements) ===" << std::endl;
    printFloatArray("FLOAT_CONSTANT_INPUT", input, N);
    generateBlob<float>("FLOAT_CONSTANT", input, N);
  }

  // === Test case 7: Double constant (16 elements) ===
  {
    constexpr size_t N = 16;
    double input[N];
    for (size_t i = 0; i < N; i++) input[i] = 3.14;
    std::cout << "  // === Double constant (16 elements) ===" << std::endl;
    printDoubleArray("DOUBLE_CONSTANT_INPUT", input, N);
    generateBlob<double>("DOUBLE_CONSTANT", input, N);
  }

  return 0;
}
