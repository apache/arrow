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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// String and binary

BinaryBuilder::BinaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
    : ArrayBuilder(type, pool), offsets_builder_(pool), value_data_builder_(pool) {}

BinaryBuilder::BinaryBuilder(MemoryPool* pool) : BinaryBuilder(binary(), pool) {}

Status BinaryBuilder::Resize(int64_t capacity) {
  DCHECK_LE(capacity, kListMaximumElements);
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));

  // one more then requested for offsets
  RETURN_NOT_OK(offsets_builder_.Resize((capacity + 1) * sizeof(int32_t)));
  return ArrayBuilder::Resize(capacity);
}

Status BinaryBuilder::ReserveData(int64_t elements) {
  if (value_data_length() + elements > value_data_capacity()) {
    if (value_data_length() + elements > kBinaryMemoryLimit) {
      return Status::CapacityError(
          "Cannot reserve capacity larger than 2^31 - 1 for binary");
    }
    RETURN_NOT_OK(value_data_builder_.Reserve(elements));
  }
  return Status::OK();
}

Status BinaryBuilder::AppendNextOffset() {
  const int64_t num_bytes = value_data_builder_.length();
  if (ARROW_PREDICT_FALSE(num_bytes > kBinaryMemoryLimit)) {
    std::stringstream ss;
    ss << "BinaryArray cannot contain more than " << kBinaryMemoryLimit << " bytes, have "
       << num_bytes;
    return Status::CapacityError(ss.str());
  }
  return offsets_builder_.Append(static_cast<int32_t>(num_bytes));
}

Status BinaryBuilder::Append(const uint8_t* value, int32_t length) {
  RETURN_NOT_OK(Reserve(1));
  RETURN_NOT_OK(AppendNextOffset());
  RETURN_NOT_OK(value_data_builder_.Append(value, length));

  UnsafeAppendToBitmap(true);
  return Status::OK();
}

Status BinaryBuilder::AppendNull() {
  RETURN_NOT_OK(AppendNextOffset());
  RETURN_NOT_OK(Reserve(1));

  UnsafeAppendToBitmap(false);
  return Status::OK();
}

Status BinaryBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  // Write final offset (values length)
  RETURN_NOT_OK(AppendNextOffset());

  // These buffers' padding zeroed by BufferBuilder
  std::shared_ptr<Buffer> offsets, value_data;
  RETURN_NOT_OK(offsets_builder_.Finish(&offsets));
  RETURN_NOT_OK(value_data_builder_.Finish(&value_data));

  *out = ArrayData::Make(type_, length_, {null_bitmap_, offsets, value_data}, null_count_,
                         0);
  Reset();
  return Status::OK();
}

void BinaryBuilder::Reset() {
  ArrayBuilder::Reset();
  offsets_builder_.Reset();
  value_data_builder_.Reset();
}

const uint8_t* BinaryBuilder::GetValue(int64_t i, int32_t* out_length) const {
  const int32_t* offsets = offsets_builder_.data();
  int32_t offset = offsets[i];
  if (i == (length_ - 1)) {
    *out_length = static_cast<int32_t>(value_data_builder_.length()) - offset;
  } else {
    *out_length = offsets[i + 1] - offset;
  }
  return value_data_builder_.data() + offset;
}

util::string_view BinaryBuilder::GetView(int64_t i) const {
  const int32_t* offsets = offsets_builder_.data();
  int32_t offset = offsets[i];
  int32_t value_length;
  if (i == (length_ - 1)) {
    value_length = static_cast<int32_t>(value_data_builder_.length()) - offset;
  } else {
    value_length = offsets[i + 1] - offset;
  }
  return util::string_view(
      reinterpret_cast<const char*>(value_data_builder_.data() + offset), value_length);
}

StringBuilder::StringBuilder(MemoryPool* pool) : BinaryBuilder(utf8(), pool) {}

Status StringBuilder::AppendValues(const std::vector<std::string>& values,
                                   const uint8_t* valid_bytes) {
  std::size_t total_length = std::accumulate(
      values.begin(), values.end(), 0ULL,
      [](uint64_t sum, const std::string& str) { return sum + str.size(); });
  RETURN_NOT_OK(Reserve(values.size()));
  RETURN_NOT_OK(value_data_builder_.Reserve(total_length));
  RETURN_NOT_OK(offsets_builder_.Reserve(values.size()));

  if (valid_bytes) {
    for (std::size_t i = 0; i < values.size(); ++i) {
      RETURN_NOT_OK(AppendNextOffset());
      if (valid_bytes[i]) {
        RETURN_NOT_OK(value_data_builder_.Append(
            reinterpret_cast<const uint8_t*>(values[i].data()), values[i].size()));
      }
    }
  } else {
    for (std::size_t i = 0; i < values.size(); ++i) {
      RETURN_NOT_OK(AppendNextOffset());
      RETURN_NOT_OK(value_data_builder_.Append(
          reinterpret_cast<const uint8_t*>(values[i].data()), values[i].size()));
    }
  }

  UnsafeAppendToBitmap(valid_bytes, values.size());
  return Status::OK();
}

Status StringBuilder::AppendValues(const char** values, int64_t length,
                                   const uint8_t* valid_bytes) {
  std::size_t total_length = 0;
  std::vector<std::size_t> value_lengths(length);
  bool have_null_value = false;
  for (int64_t i = 0; i < length; ++i) {
    if (values[i]) {
      auto value_length = strlen(values[i]);
      value_lengths[i] = value_length;
      total_length += value_length;
    } else {
      have_null_value = true;
    }
  }
  RETURN_NOT_OK(Reserve(length));
  RETURN_NOT_OK(value_data_builder_.Reserve(total_length));
  RETURN_NOT_OK(offsets_builder_.Reserve(length));

  if (valid_bytes) {
    int64_t valid_bytes_offset = 0;
    for (int64_t i = 0; i < length; ++i) {
      RETURN_NOT_OK(AppendNextOffset());
      if (valid_bytes[i]) {
        if (values[i]) {
          RETURN_NOT_OK(value_data_builder_.Append(
              reinterpret_cast<const uint8_t*>(values[i]), value_lengths[i]));
        } else {
          UnsafeAppendToBitmap(valid_bytes + valid_bytes_offset, i - valid_bytes_offset);
          UnsafeAppendToBitmap(false);
          valid_bytes_offset = i + 1;
        }
      }
    }
    UnsafeAppendToBitmap(valid_bytes + valid_bytes_offset, length - valid_bytes_offset);
  } else {
    if (have_null_value) {
      std::vector<uint8_t> valid_vector(length, 0);
      for (int64_t i = 0; i < length; ++i) {
        RETURN_NOT_OK(AppendNextOffset());
        if (values[i]) {
          RETURN_NOT_OK(value_data_builder_.Append(
              reinterpret_cast<const uint8_t*>(values[i]), value_lengths[i]));
          valid_vector[i] = 1;
        }
      }
      UnsafeAppendToBitmap(valid_vector.data(), length);
    } else {
      for (int64_t i = 0; i < length; ++i) {
        RETURN_NOT_OK(AppendNextOffset());
        RETURN_NOT_OK(value_data_builder_.Append(
            reinterpret_cast<const uint8_t*>(values[i]), value_lengths[i]));
      }
      UnsafeAppendToBitmap(nullptr, length);
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Fixed width binary

FixedSizeBinaryBuilder::FixedSizeBinaryBuilder(const std::shared_ptr<DataType>& type,
                                               MemoryPool* pool)
    : ArrayBuilder(type, pool),
      byte_width_(checked_cast<const FixedSizeBinaryType&>(*type).byte_width()),
      byte_builder_(pool) {}

#ifndef NDEBUG
void FixedSizeBinaryBuilder::CheckValueSize(int64_t size) {
  DCHECK_EQ(size, byte_width_) << "Appending wrong size to FixedSizeBinaryBuilder";
}
#endif

Status FixedSizeBinaryBuilder::AppendValues(const uint8_t* data, int64_t length,
                                            const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  return byte_builder_.Append(data, length * byte_width_);
}

Status FixedSizeBinaryBuilder::AppendNull() {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(false);
  return byte_builder_.Advance(byte_width_);
}

void FixedSizeBinaryBuilder::Reset() {
  ArrayBuilder::Reset();
  byte_builder_.Reset();
}

Status FixedSizeBinaryBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
  RETURN_NOT_OK(byte_builder_.Resize(capacity * byte_width_));
  return ArrayBuilder::Resize(capacity);
}

Status FixedSizeBinaryBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));

  *out = ArrayData::Make(type_, length_, {null_bitmap_, data}, null_count_);

  null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

const uint8_t* FixedSizeBinaryBuilder::GetValue(int64_t i) const {
  const uint8_t* data_ptr = byte_builder_.data();
  return data_ptr + i * byte_width_;
}

util::string_view FixedSizeBinaryBuilder::GetView(int64_t i) const {
  const uint8_t* data_ptr = byte_builder_.data();
  return util::string_view(reinterpret_cast<const char*>(data_ptr + i * byte_width_),
                           byte_width_);
}

// ----------------------------------------------------------------------
// Decimal128Builder

Decimal128Builder::Decimal128Builder(const std::shared_ptr<DataType>& type,
                                     MemoryPool* pool)
    : FixedSizeBinaryBuilder(type, pool) {}

Status Decimal128Builder::Append(const Decimal128& value) {
  RETURN_NOT_OK(FixedSizeBinaryBuilder::Reserve(1));
  return FixedSizeBinaryBuilder::Append(value.ToBytes());
}

Status Decimal128Builder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));

  *out = ArrayData::Make(type_, length_, {null_bitmap_, data}, null_count_);

  return Status::OK();
}

}  // namespace arrow
