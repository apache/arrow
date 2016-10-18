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

#include <cstring>
#include <vector>

#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

void BitUtil::BytesToBits(const std::vector<uint8_t>& bytes, uint8_t* bits) {
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (bytes[i] > 0) { SetBit(bits, i); }
  }
}

Status BitUtil::BytesToBits(
    const std::vector<uint8_t>& bytes, std::shared_ptr<Buffer>* out) {
  int bit_length = BitUtil::BytesForBits(bytes.size());

  auto buffer = std::make_shared<PoolBuffer>();
  RETURN_NOT_OK(buffer->Resize(bit_length));
  memset(buffer->mutable_data(), 0, bit_length);
  BytesToBits(bytes, buffer->mutable_data());

  *out = buffer;
  return Status::OK();
}

}  // namespace arrow
