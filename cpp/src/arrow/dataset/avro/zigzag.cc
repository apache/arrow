/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "arrow/dataset/avro/zigzag.h"

namespace arrow {
namespace avro {

uint64_t EncodeZigzag64(int64_t input) { return ((input << 1) ^ (input >> 63)); }

int64_t DecodeZigzag64(uint64_t input) {
  return static_cast<int64_t>(((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

uint32_t EncodeZigzag32(int32_t input) { return ((input << 1) ^ (input >> 31)); }

int32_t DecodeZigzag32(uint32_t input) {
  return static_cast<int32_t>(((input >> 1) ^ -(static_cast<int64_t>(input) & 1)));
}

size_t EncodeInt64(int64_t input, std::array<uint8_t, 10>* output) {
  // get the zigzag encoding
  uint64_t val = EncodeZigzag64(input);

  // put values in an array of bytes with variable length encoding
  const int mask = 0x7F;
  *output[0] = val & mask;
  size_t bytesOut = 1;
  while (val >>= 7) {
    *output[bytesOut - 1] |= 0x80;
    *output[bytesOut++] = (val & mask);
  }

  return bytesOut;
}

size_t EncodeInt32(int32_t input, std::array<uint8_t, 5>* output) {
  // get the zigzag encoding
  uint32_t val = EncodeZigzag32(input);

  // put values in an array of bytes with variable length encoding
  const int mask = 0x7F;
  *output[0] = val & mask;
  size_t bytesOut = 1;
  while (val >>= 7) {
    *output[bytesOut - 1] |= 0x80;
    *output[bytesOut++] = (val & mask);
  }

  return bytesOut;
}

}  // namespace avro
}  // namespace arrow
