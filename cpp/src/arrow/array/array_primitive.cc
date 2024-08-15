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
// BooleanArray

BooleanArray::BooleanArray(const std::shared_ptr<ArrayData>& data,
                           const std::shared_ptr<ArrayStatistics>& statistics) {
  ARROW_CHECK_EQ(data->type->id(), Type::BOOL);
  Init(data, statistics);
}

int64_t BooleanArray::false_count() const {
  return this->length() - this->null_count() - this->true_count();
}

int64_t BooleanArray::true_count() const {
  if (data_->MayHaveNulls()) {
    DCHECK(data_->buffers[0]);
    return internal::CountAndSetBits(data_->buffers[0]->data(), data_->offset,
                                     data_->buffers[1]->data(), data_->offset,
                                     data_->length);
  } else {
    return internal::CountSetBits(data_->buffers[1]->data(), data_->offset,
                                  data_->length);
  }
}

// ----------------------------------------------------------------------
// Day time interval

DayTimeIntervalType::DayMilliseconds DayTimeIntervalArray::GetValue(int64_t i) const {
  DCHECK(i < length());
  return *reinterpret_cast<const DayTimeIntervalType::DayMilliseconds*>(
      raw_values_ + (i + data_->offset) * byte_width());
}

// ----------------------------------------------------------------------
// Month, day and Nanos interval

MonthDayNanoIntervalType::MonthDayNanos MonthDayNanoIntervalArray::GetValue(
    int64_t i) const {
  DCHECK(i < length());
  return *reinterpret_cast<const MonthDayNanoIntervalType::MonthDayNanos*>(
      raw_values_ + (i + data_->offset) * byte_width());
}

}  // namespace arrow
