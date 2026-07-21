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

#include <functional>
#include <optional>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "parquet/platform.h"
#include "parquet/variant/format.h"

namespace parquet::variant {

namespace internal {

class ViewAccess;

}

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

  /// Parse a Variant metadata prefix without copying it and return bytes consumed.
  /// The returned view contains exactly the consumed metadata bytes.
  ///
  /// \param[out] consumed receives the metadata size when non-null
  static VariantMetadataView ParsePrefix(std::string_view data,
                                         size_t* consumed = nullptr);

  std::string_view metadata() const { return metadata_; }
  bool sorted_strings() const { return sorted_strings_; }
  uint8_t offset_size() const { return offset_size_; }
  /// Number of strings in the metadata dictionary used by object field ids.
  uint32_t dictionary_size() const { return static_cast<uint32_t>(strings_.size()); }

  /// Return the metadata dictionary string for an object field id.
  std::string_view string(uint32_t field_id) const;
  /// Return the metadata dictionary id for a string, if present.
  std::optional<uint32_t> FindString(std::string_view value) const;

 private:
  std::string_view metadata_;
  bool sorted_strings_ = false;
  uint8_t offset_size_ = 0;
  std::vector<std::string_view> strings_;
};

class PARQUET_EXPORT VariantPrimitiveView {
 public:
  static constexpr VariantBasicType kBasicType = VariantBasicType::kPrimitive;

  VariantPrimitiveType type() const { return type_; }
  std::string_view payload() const { return payload_; }

 private:
  friend class internal::ViewAccess;

  VariantPrimitiveView(VariantPrimitiveType type, std::string_view payload)
      : type_(type), payload_(payload) {}

  VariantPrimitiveType type_ = VariantPrimitiveType::kNull;
  std::string_view payload_;
};

class PARQUET_EXPORT VariantShortStringView {
 public:
  static constexpr VariantBasicType kBasicType = VariantBasicType::kShortString;

  std::string_view string() const { return string_; }

 private:
  friend class internal::ViewAccess;

  explicit VariantShortStringView(std::string_view string) : string_(string) {}

  std::string_view string_;
};

class VariantValueView;

class PARQUET_EXPORT VariantObjectView {
 public:
  static constexpr VariantBasicType kBasicType = VariantBasicType::kObject;

  const std::vector<VariantObjectField>& fields() const { return fields_; }
  bool ContainsField(std::string_view name) const;
  /// Return a field by name, or nullopt if it does not exist.
  std::optional<VariantValueView> GetField(std::string_view name) const;
  /// Return a field by index, or nullopt if the index is out of bounds.
  std::optional<VariantValueView> GetField(size_t index) const;

 private:
  friend class internal::ViewAccess;

  VariantObjectView(std::vector<VariantObjectField> fields,
                    const VariantMetadataView& metadata)
      : fields_(std::move(fields)), metadata_(metadata) {}

  std::vector<VariantObjectField> fields_;
  std::reference_wrapper<const VariantMetadataView> metadata_;
};

class PARQUET_EXPORT VariantArrayView {
 public:
  static constexpr VariantBasicType kBasicType = VariantBasicType::kArray;

  const std::vector<std::string_view>& elements() const { return elements_; }
  /// Return an element by index, or nullopt if the index is out of bounds.
  std::optional<VariantValueView> GetElement(size_t index) const;

 private:
  friend class internal::ViewAccess;

  VariantArrayView(std::vector<std::string_view> elements,
                   const VariantMetadataView& metadata)
      : elements_(std::move(elements)), metadata_(metadata) {}

  std::vector<std::string_view> elements_;
  std::reference_wrapper<const VariantMetadataView> metadata_;
};

class PARQUET_EXPORT VariantValueView {
 public:
  using Data = std::variant<VariantPrimitiveView, VariantShortStringView,
                            VariantObjectView, VariantArrayView>;

  /// Parse Variant value bytes without copying them.
  ///
  /// The returned view and any nested object/list/string views are valid only while the
  /// input value bytes remain alive and unchanged. The metadata view object and its
  /// underlying bytes must also remain alive and unchanged.
  static VariantValueView Make(std::string_view value,
                               const VariantMetadataView& metadata);
  static VariantValueView Make(std::string_view value,
                               VariantMetadataView&& metadata) = delete;

  /// Read the basic type from the value header without validating the remaining bytes.
  static VariantBasicType PeekBasicType(std::string_view value);

  static void Validate(std::string_view value, const VariantMetadataView& metadata);

  std::string_view value() const { return value_; }
  VariantBasicType basic_type() const {
    return std::visit([](const auto& view) { return view.kBasicType; }, data_);
  }
  const Data& data() const { return data_; }

 private:
  friend class internal::ViewAccess;
  friend class VariantArrayView;
  friend class VariantObjectView;

  VariantValueView(std::string_view value, Data data)
      : value_(value), data_(std::move(data)) {}

  static VariantValueView MakeShallow(std::string_view value,
                                      const VariantMetadataView& metadata);

  std::string_view value_;
  Data data_;
};

}  // namespace parquet::variant
