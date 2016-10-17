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

#ifndef ARROW_TYPES_STRING_H
#define ARROW_TYPES_STRING_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/util/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;

class ARROW_EXPORT BinaryArray : public Array {
 public:
  BinaryArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
      const std::shared_ptr<Buffer>& data, int32_t null_count = 0,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  // Constructor that allows sub-classes/builders to propagate there logical type up the
  // class hierarchy.
  BinaryArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& offsets,
      const std::shared_ptr<Buffer>& data, int32_t null_count = 0,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  // Return the pointer to the given elements bytes
  // TODO(emkornfield) introduce a StringPiece or something similar to capture zero-copy
  // pointer + offset
  const uint8_t* GetValue(int i, int32_t* out_length) const {
    DCHECK(out_length);
    const int32_t pos = offsets_[i];
    *out_length = offsets_[i + 1] - pos;
    return data_ + pos;
  }

  std::shared_ptr<Buffer> data() const { return data_buffer_; }
  std::shared_ptr<Buffer> offsets() const { return offset_buffer_; }

  int32_t offset(int i) const { return offsets_[i]; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int i) const { return offsets_[i]; }
  int32_t value_length(int i) const { return offsets_[i + 1] - offsets_[i]; }

  bool EqualsExact(const BinaryArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const ArrayPtr& arr) const override;

  Status Validate() const override;

 private:
  std::shared_ptr<Buffer> offset_buffer_;
  const int32_t* offsets_;

  std::shared_ptr<Buffer> data_buffer_;
  const uint8_t* data_;
};

class ARROW_EXPORT StringArray : public BinaryArray {
 public:
  StringArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
      const std::shared_ptr<Buffer>& data, int32_t null_count = 0,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  // Construct a std::string
  // TODO: std::bad_alloc possibility
  std::string GetString(int i) const {
    int32_t nchars;
    const uint8_t* str = GetValue(i, &nchars);
    return std::string(reinterpret_cast<const char*>(str), nchars);
  }

  Status Validate() const override;
};

// BinaryBuilder : public ListBuilder
class ARROW_EXPORT BinaryBuilder : public ListBuilder {
 public:
  explicit BinaryBuilder(MemoryPool* pool, const TypePtr& type);
  virtual ~BinaryBuilder() {}

  Status Append(const uint8_t* value, int32_t length) {
    RETURN_NOT_OK(ListBuilder::Append());
    return byte_builder_->Append(value, length);
  }

  Status Finish(std::shared_ptr<Array>* out) override;

 protected:
  UInt8Builder* byte_builder_;
};

// String builder
class ARROW_EXPORT StringBuilder : public BinaryBuilder {
 public:
  explicit StringBuilder(MemoryPool* pool, const TypePtr& type)
      : BinaryBuilder(pool, type) {}

  Status Finish(std::shared_ptr<Array>* out) override;

  Status Append(const std::string& value) { return Append(value.c_str(), value.size()); }

  Status Append(const char* value, int32_t length) {
    return BinaryBuilder::Append(reinterpret_cast<const uint8_t*>(value), length);
  }

  Status Append(const std::vector<std::string>& values, uint8_t* null_bytes);
};

}  // namespace arrow

#endif  // ARROW_TYPES_STRING_H
