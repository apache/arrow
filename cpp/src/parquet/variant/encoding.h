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

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "parquet/platform.h"

namespace parquet::variant {

enum class VariantBasicType : uint8_t {
  kPrimitive = 0,
  kShortString = 1,
  kObject = 2,
  kArray = 3,
};

enum class VariantPrimitiveType : uint8_t {
  kNull = 0,
  kBooleanTrue = 1,
  kBooleanFalse = 2,
  kInt8 = 3,
  kInt16 = 4,
  kInt32 = 5,
  kInt64 = 6,
  kDouble = 7,
  kDecimal4 = 8,
  kDecimal8 = 9,
  kDecimal16 = 10,
  kDate = 11,
  kTimestampMicros = 12,
  kTimestampNTZMicros = 13,
  kFloat = 14,
  kBinary = 15,
  kString = 16,
  kTimeNTZMicros = 17,
  kTimestampNanos = 18,
  kTimestampNTZNanos = 19,
  kUuid = 20,
};

struct PARQUET_EXPORT VariantObjectField {
  std::string_view name;
  uint32_t field_id = 0;
  std::string_view value;
};

class PARQUET_EXPORT VariantMetadataView {
 public:
  /// Parse Variant metadata bytes without copying them.
  ///
  /// The returned view and all string views borrowed from it are valid only while the
  /// input metadata bytes remain alive and unchanged.
  static VariantMetadataView Make(std::string_view metadata);

  std::string_view metadata() const { return metadata_; }
  bool sorted_strings() const { return sorted_strings_; }
  uint8_t offset_size() const { return offset_size_; }
  uint32_t dictionary_size() const { return static_cast<uint32_t>(strings_.size()); }

  std::string_view string(uint32_t id) const;
  std::optional<uint32_t> FindString(std::string_view value) const;

 private:
  std::string_view metadata_;
  bool sorted_strings_ = false;
  uint8_t offset_size_ = 0;
  std::vector<std::string_view> strings_;
};

class PARQUET_EXPORT VariantPrimitiveView {
 public:
  VariantPrimitiveView(VariantPrimitiveType type, std::string_view payload)
      : type_(type), payload_(payload) {}

  VariantPrimitiveType type() const { return type_; }
  std::string_view payload() const { return payload_; }

 private:
  VariantPrimitiveType type_ = VariantPrimitiveType::kNull;
  std::string_view payload_;
};

class PARQUET_EXPORT VariantShortStringView {
 public:
  explicit VariantShortStringView(std::string_view string) : string_(string) {}

  std::string_view string() const { return string_; }

 private:
  std::string_view string_;
};

class PARQUET_EXPORT VariantObjectView {
 public:
  explicit VariantObjectView(std::vector<VariantObjectField> fields)
      : fields_(std::move(fields)) {}

  const std::vector<VariantObjectField>& fields() const { return fields_; }
  const VariantObjectField* FindField(std::string_view name) const;
  bool ContainsField(std::string_view name) const;

 private:
  std::vector<VariantObjectField> fields_;
};

class PARQUET_EXPORT VariantArrayView {
 public:
  explicit VariantArrayView(std::vector<std::string_view> elements)
      : elements_(std::move(elements)) {}

  const std::vector<std::string_view>& elements() const { return elements_; }

 private:
  std::vector<std::string_view> elements_;
};

class PARQUET_EXPORT VariantValueView {
 public:
  using Data = std::variant<VariantPrimitiveView, VariantShortStringView,
                            VariantObjectView, VariantArrayView>;

  VariantValueView(std::string_view value, VariantBasicType basic_type, Data data)
      : value_(value), basic_type_(basic_type), data_(std::move(data)) {}

  /// Parse Variant value bytes without copying them.
  ///
  /// The returned view and any nested object/list/string views are valid only while the
  /// input value bytes remain alive and unchanged. The metadata view must also remain
  /// valid while object field names are accessed.
  static VariantValueView Make(std::string_view value,
                               const VariantMetadataView& metadata);
  static void Validate(std::string_view value, const VariantMetadataView& metadata);

  std::string_view value() const { return value_; }
  VariantBasicType basic_type() const { return basic_type_; }
  const Data& data() const { return data_; }

 private:
  std::string_view value_;
  VariantBasicType basic_type_;
  Data data_;
};

}  // namespace parquet::variant
