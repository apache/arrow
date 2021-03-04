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

#include "arrow/array/array_primitive.h"

#include <cstdint>
#include <memory>

#include "arrow/array/array_base.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// Primitive array base

PrimitiveArray::PrimitiveArray(const std::shared_ptr<DataType>& type, int64_t length,
                               const std::shared_ptr<Buffer>& data,
                               const std::shared_ptr<Buffer>& null_bitmap,
                               int64_t null_count, int64_t offset) {
  SetData(ArrayData::Make(type, length, {null_bitmap, data}, null_count, offset));
}

// ----------------------------------------------------------------------
// BooleanArray

BooleanArray::BooleanArray(const std::shared_ptr<ArrayData>& data)
    : PrimitiveArray(data) {
  ARROW_CHECK_EQ(data->type->id(), Type::BOOL);
}

BooleanArray::BooleanArray(int64_t length, const std::shared_ptr<Buffer>& data,
                           const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                           int64_t offset)
    : PrimitiveArray(boolean(), length, data, null_bitmap, null_count, offset) {}

int64_t BooleanArray::false_count() const {
  return this->length() - this->null_count() - this->true_count();
}

int64_t BooleanArray::true_count() const {
  if (data_->null_count.load() != 0) {
    DCHECK(data_->buffers[0]);
    internal::BinaryBitBlockCounter bit_counter(data_->buffers[0]->data(), data_->offset,
                                                data_->buffers[1]->data(), data_->offset,
                                                data_->length);
    int64_t count = 0;
    while (true) {
      internal::BitBlockCount block = bit_counter.NextAndWord();
      if (block.length == 0) {
        break;
      }
      count += block.popcount;
    }
    return count;
  } else {
    return internal::CountSetBits(data_->buffers[1]->data(), data_->offset,
                                  data_->length);
  }
}

// ----------------------------------------------------------------------
// Day time interval

DayTimeIntervalArray::DayTimeIntervalArray(const std::shared_ptr<ArrayData>& data) {
  SetData(data);
}

DayTimeIntervalArray::DayTimeIntervalArray(const std::shared_ptr<DataType>& type,
                                           int64_t length,
                                           const std::shared_ptr<Buffer>& data,
                                           const std::shared_ptr<Buffer>& null_bitmap,
                                           int64_t null_count, int64_t offset)
    : PrimitiveArray(type, length, data, null_bitmap, null_count, offset) {}

DayTimeIntervalType::DayMilliseconds DayTimeIntervalArray::GetValue(int64_t i) const {
  DCHECK(i < length());
  return *reinterpret_cast<const DayTimeIntervalType::DayMilliseconds*>(
      raw_values_ + (i + data_->offset) * byte_width());
}

}  // namespace arrow
