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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/array_primitive.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/align_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace BitUtil {
namespace {

void FillBitsFromBytes(const std::vector<uint8_t>& bytes, uint8_t* bits) {
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (bytes[i] > 0) {
      SetBit(bits, i);
    }
  }
}

}  // namespace

void SetBitsTo(uint8_t* bits, int64_t start_offset, int64_t length, bool bits_are_set) {
  if (length == 0) {
    return;
  }

  const int64_t i_begin = start_offset;
  const int64_t i_end = start_offset + length;
  const uint8_t fill_byte = static_cast<uint8_t>(-static_cast<uint8_t>(bits_are_set));

  const int64_t bytes_begin = i_begin / 8;
  const int64_t bytes_end = i_end / 8 + 1;

  const uint8_t first_byte_mask = kPrecedingBitmask[i_begin % 8];
  const uint8_t last_byte_mask = kTrailingBitmask[i_end % 8];

  if (bytes_end == bytes_begin + 1) {
    // set bits within a single byte
    const uint8_t only_byte_mask =
        i_end % 8 == 0 ? first_byte_mask
                       : static_cast<uint8_t>(first_byte_mask | last_byte_mask);
    bits[bytes_begin] &= only_byte_mask;
    bits[bytes_begin] |= static_cast<uint8_t>(fill_byte & ~only_byte_mask);
    return;
  }

  // set/clear trailing bits of first byte
  bits[bytes_begin] &= first_byte_mask;
  bits[bytes_begin] |= static_cast<uint8_t>(fill_byte & ~first_byte_mask);

  if (bytes_end - bytes_begin > 2) {
    // set/clear whole bytes
    std::memset(bits + bytes_begin + 1, fill_byte,
                static_cast<size_t>(bytes_end - bytes_begin - 2));
  }

  if (i_end % 8 == 0) {
    return;
  }

  // set/clear leading bits of last byte
  bits[bytes_end - 1] &= last_byte_mask;
  bits[bytes_end - 1] |= static_cast<uint8_t>(fill_byte & ~last_byte_mask);
}

Result<std::shared_ptr<Buffer>> BytesToBits(const std::vector<uint8_t>& bytes,
                                            MemoryPool* pool) {
  int64_t bit_length = BytesForBits(bytes.size());

  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(bit_length, pool));
  uint8_t* out_buf = buffer->mutable_data();
  memset(out_buf, 0, static_cast<size_t>(buffer->capacity()));
  FillBitsFromBytes(bytes, out_buf);
  return std::move(buffer);
}

}  // namespace BitUtil
}  // namespace arrow
