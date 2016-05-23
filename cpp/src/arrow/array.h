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

#ifndef ARROW_ARRAY_H
#define ARROW_ARRAY_H

#include <cstdint>
#include <memory>

#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"

namespace arrow {

class Buffer;
class Status;

// Immutable data array with some logical type and some length. Any memory is
// owned by the respective Buffer instance (or its parents).
//
// The base class is only required to have a null bitmap buffer if the null
// count is greater than 0
class Array {
 public:
  Array(const std::shared_ptr<DataType>& type, int32_t length, int32_t null_count = 0,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  virtual ~Array() = default;

  // Determine if a slot is null. For inner loops. Does *not* boundscheck
  bool IsNull(int i) const {
    return null_count_ > 0 && util::bit_not_set(null_bitmap_data_, i);
  }

  int32_t length() const { return length_; }
  int32_t null_count() const { return null_count_; }

  const std::shared_ptr<DataType>& type() const { return type_; }
  Type::type type_enum() const { return type_->type; }

  const std::shared_ptr<Buffer>& null_bitmap() const { return null_bitmap_; }

  const uint8_t* null_bitmap_data() const { return null_bitmap_data_; }

  bool EqualsExact(const Array& arr) const;
  virtual bool Equals(const std::shared_ptr<Array>& arr) const = 0;

  // Compare if the range of slots specified are equal for the given array and
  // this array.  end_idx exclusive.  This methods does not bounds check.
  virtual bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const std::shared_ptr<Array>& arr) const = 0;

  // Determines if the array is internally consistent.  Defaults to always
  // returning Status::OK.  This can be an expensive check.
  virtual Status Validate() const;

 protected:
  std::shared_ptr<DataType> type_;
  int32_t null_count_;
  int32_t length_;

  std::shared_ptr<Buffer> null_bitmap_;
  const uint8_t* null_bitmap_data_;

 private:
  Array() {}
  DISALLOW_COPY_AND_ASSIGN(Array);
};

// Degenerate null type Array
class NullArray : public Array {
 public:
  NullArray(const std::shared_ptr<DataType>& type, int32_t length)
      : Array(type, length, length, nullptr) {}

  explicit NullArray(int32_t length) : NullArray(std::make_shared<NullType>(), length) {}

  bool Equals(const std::shared_ptr<Array>& arr) const override;
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_index,
      const std::shared_ptr<Array>& arr) const override;
};

typedef std::shared_ptr<Array> ArrayPtr;
}  // namespace arrow

#endif
