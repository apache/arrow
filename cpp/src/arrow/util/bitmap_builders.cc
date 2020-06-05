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

#include "arrow/util/bitmap_builders.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace internal {

namespace {

void FillBitsFromBytes(const std::vector<uint8_t>& bytes, uint8_t* bits) {
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (bytes[i] > 0) {
      BitUtil::SetBit(bits, i);
    }
  }
}

}  // namespace

Result<std::shared_ptr<Buffer>> BytesToBits(const std::vector<uint8_t>& bytes,
                                            MemoryPool* pool) {
  int64_t bit_length = BitUtil::BytesForBits(bytes.size());

  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(bit_length, pool));
  uint8_t* out_buf = buffer->mutable_data();
  memset(out_buf, 0, static_cast<size_t>(buffer->capacity()));
  FillBitsFromBytes(bytes, out_buf);
  return std::move(buffer);
}

Result<std::shared_ptr<Buffer>> BitmapAllButOne(MemoryPool* pool, int64_t length,
                                                int64_t straggler_pos, bool value) {
  if (straggler_pos < 0 || straggler_pos >= length) {
    return Status::Invalid("invalid straggler_pos ", straggler_pos);
  }

  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(BitUtil::BytesForBits(length), pool));

  auto bitmap_data = buffer->mutable_data();
  BitUtil::SetBitsTo(bitmap_data, 0, length, value);
  BitUtil::SetBitTo(bitmap_data, straggler_pos, !value);
  return std::move(buffer);
}

}  // namespace internal
}  // namespace arrow
