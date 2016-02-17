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
#include "arrow/types/integer.h"
#include "arrow/types/list.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

class ArrayBuilder;

struct CharType : public DataType {
  int size;

  BytesType physical_type;

  explicit CharType(int size, bool nullable = true)
      : DataType(TypeEnum::CHAR, nullable),
        size(size),
        physical_type(BytesType(size)) {}

  CharType(const CharType& other)
      : CharType(other.size, other.nullable) {}

  virtual std::string ToString() const;
};


// Variable-length, null-terminated strings, up to a certain length
struct VarcharType : public DataType {
  int size;

  BytesType physical_type;

  explicit VarcharType(int size, bool nullable = true)
      : DataType(TypeEnum::VARCHAR, nullable),
        size(size),
        physical_type(BytesType(size + 1)) {}
  VarcharType(const VarcharType& other)
      : VarcharType(other.size, other.nullable) {}

  virtual std::string ToString() const;
};

static const LayoutPtr byte1(new BytesType(1));
static const LayoutPtr physical_string = LayoutPtr(new ListLayoutType(byte1));

// String is a logical type consisting of a physical list of 1-byte values
struct StringType : public DataType {
  explicit StringType(bool nullable = true)
      : DataType(TypeEnum::STRING, nullable) {}

  StringType(const StringType& other)
      : StringType(other.nullable) {}

  const LayoutPtr& physical_type() {
    return physical_string;
  }

  static char const *name() {
    return "string";
  }

  virtual std::string ToString() const {
    return name();
  }
};


// TODO: add a BinaryArray layer in between
class StringArray : public ListArray {
 public:
  StringArray() : ListArray(), bytes_(nullptr), raw_bytes_(nullptr) {}

  StringArray(int64_t length, const std::shared_ptr<Buffer>& offsets,
      const ArrayPtr& values,
      const std::shared_ptr<Buffer>& nulls = nullptr) {
    Init(length, offsets, values, nulls);
  }

  void Init(const TypePtr& type, int64_t length,
      const std::shared_ptr<Buffer>& offsets,
      const ArrayPtr& values,
      const std::shared_ptr<Buffer>& nulls = nullptr) {
    ListArray::Init(type, length, offsets, values, nulls);

    // TODO: type validation for values array

    // For convenience
    bytes_ = static_cast<UInt8Array*>(values.get());
    raw_bytes_ = bytes_->raw_data();
  }

  void Init(int64_t length, const std::shared_ptr<Buffer>& offsets,
      const ArrayPtr& values,
      const std::shared_ptr<Buffer>& nulls = nullptr) {
    TypePtr type(new StringType(nulls != nullptr));
    Init(type, length, offsets, values, nulls);
  }

  // Compute the pointer t
  const uint8_t* GetValue(int64_t i, int64_t* out_length) const {
    int32_t pos = offsets_[i];
    *out_length = offsets_[i + 1] - pos;
    return raw_bytes_ + pos;
  }

  // Construct a std::string
  std::string GetString(int64_t i) const {
    int64_t nchars;
    const uint8_t* str = GetValue(i, &nchars);
    return std::string(reinterpret_cast<const char*>(str), nchars);
  }

 private:
  UInt8Array* bytes_;
  const uint8_t* raw_bytes_;
};

// Array builder



class StringBuilder : public ListBuilder {
 public:
  explicit StringBuilder(const TypePtr& type) :
      ListBuilder(type, static_cast<ArrayBuilder*>(new UInt8Builder(value_type_))) {
    byte_builder_ = static_cast<UInt8Builder*>(value_builder_.get());
  }

  Status Append(const std::string& value) {
    RETURN_NOT_OK(ListBuilder::Append());
    return byte_builder_->Append(reinterpret_cast<const uint8_t*>(value.c_str()),
        value.size());
  }

  Status Append(const uint8_t* value, int64_t length);
  Status Append(const std::vector<std::string>& values,
                uint8_t* null_bytes);

  virtual Status ToArray(Array** out) {
    StringArray* result = new StringArray();
    RETURN_NOT_OK(ListBuilder::Transfer(result));
    *out = static_cast<Array*>(result);
    return Status::OK();
  }

 protected:
  UInt8Builder* byte_builder_;

  static TypePtr value_type_;
};

} // namespace arrow

#endif // ARROW_TYPES_STRING_H
