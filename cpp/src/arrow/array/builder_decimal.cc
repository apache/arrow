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

#include "arrow/array/builder_decimal.h"

#include <cstdint>
#include <memory>

#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

namespace arrow {

class Buffer;
class MemoryPool;

// ----------------------------------------------------------------------
// BaseDecimalBuilder

template<uint32_t width>
BaseDecimalBuilder<width>::BaseDecimalBuilder(const std::shared_ptr<DataType>& type,
                                     MemoryPool* pool)
    : FixedSizeBinaryBuilder(type, pool),
      decimal_type_(internal::checked_pointer_cast<TypeClass>(type)) {}

template<uint32_t width>
Status BaseDecimalBuilder<width>::Append(ValueType value) {
  RETURN_NOT_OK(FixedSizeBinaryBuilder::Reserve(1));
  UnsafeAppend(value);
  return Status::OK();
}

template<uint32_t width>
void BaseDecimalBuilder<width>::UnsafeAppend(ValueType value) {
  value.ToBytes(GetMutableValue(length()));
  byte_builder_.UnsafeAdvance((width >> 3));
  UnsafeAppendToBitmap(true);
}

template<uint32_t width>
void BaseDecimalBuilder<width>::UnsafeAppend(util::string_view value) {
  FixedSizeBinaryBuilder::UnsafeAppend(value);
}

template<uint32_t width>
Status BaseDecimalBuilder<width>::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));
  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  *out = ArrayData::Make(type(), length_, {null_bitmap, data}, null_count_);
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

template class BaseDecimalBuilder<16>;
template class BaseDecimalBuilder<32>;
template class BaseDecimalBuilder<64>;
template class BaseDecimalBuilder<128>;
template class BaseDecimalBuilder<256>;

}  // namespace arrow
