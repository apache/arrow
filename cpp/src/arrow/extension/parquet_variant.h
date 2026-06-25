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

#include <string>
#include <string_view>

#include "arrow/extension_type.h"
#include "arrow/util/visibility.h"

namespace arrow::extension {

/// \brief The extension name for the Variant extension type.
inline constexpr std::string_view kVariantExtensionName = "arrow.parquet.variant";

class ARROW_EXPORT VariantArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

/// EXPERIMENTAL: Variant is not yet fully supported.
///
/// Variant supports semi-structured objects that can be composed of
/// primitives, arrays, and objects, which can be queried by path.
///
/// Unshredded variant representation:
/// optional group variant_name (VARIANT) {
///   required binary metadata;
///   required binary value;
/// }
///
/// To read more about variant encoding, see the variant encoding spec at
/// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
///
/// Shredded variant representation:
/// optional group shredded_variant_name (VARIANT) {
///   required binary metadata;
///   optional binary value;
///   optional <type> typed_value;
/// }
///
/// The value and typed_value fields are optional in the schema, but at least one
/// must be present.
///
/// To read more about variant shredding, see the variant shredding spec at
/// https://github.com/apache/parquet-format/blob/master/VariantShredding.md
class ARROW_EXPORT VariantExtensionType : public ExtensionType {
 public:
  explicit VariantExtensionType(const std::shared_ptr<DataType>& storage_type);

  std::string extension_name() const override {
    return std::string(kVariantExtensionName);
  }

  bool ExtensionEquals(const ExtensionType& other) const override;

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized_data) const override;

  std::string Serialize() const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  static Result<std::shared_ptr<DataType>> Make(std::shared_ptr<DataType> storage_type);

  static bool IsSupportedStorageType(const std::shared_ptr<DataType>& storage_type);

  std::shared_ptr<Field> metadata() const { return metadata_; }

  std::shared_ptr<Field> value() const { return value_; }

  std::shared_ptr<Field> typed_value() const { return typed_value_; }

 private:
  std::shared_ptr<Field> metadata_;
  std::shared_ptr<Field> value_;
  std::shared_ptr<Field> typed_value_;
};

/// \brief Return a VariantExtensionType instance.
ARROW_EXPORT std::shared_ptr<DataType> variant(std::shared_ptr<DataType> storage_type);

}  // namespace arrow::extension
