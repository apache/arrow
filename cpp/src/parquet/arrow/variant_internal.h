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

#include <stdexcept>
#include <string>

#include "arrow/extension_type.h"
#include "parquet/platform.h"

namespace parquet::arrow {

class PARQUET_EXPORT VariantArray : public ::arrow::ExtensionArray {
 public:
  using ::arrow::ExtensionArray::ExtensionArray;
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
/// To read more about variant shredding, see the variant shredding spec at
/// https://github.com/apache/parquet-format/blob/master/VariantShredding.md
class PARQUET_EXPORT VariantExtensionType : public ::arrow::ExtensionType {
 public:
  explicit VariantExtensionType(const std::shared_ptr<::arrow::DataType>& storage_type);

  std::string extension_name() const override { return "parquet.variant"; }

  bool ExtensionEquals(const ::arrow::ExtensionType& other) const override;

  ::arrow::Result<std::shared_ptr<::arrow::DataType>> Deserialize(
      std::shared_ptr<::arrow::DataType> storage_type,
      const std::string& serialized_data) const override;

  std::string Serialize() const override;

  std::shared_ptr<::arrow::Array> MakeArray(
      std::shared_ptr<::arrow::ArrayData> data) const override;

  static ::arrow::Result<std::shared_ptr<::arrow::DataType>> Make(
      std::shared_ptr<::arrow::DataType> storage_type);

  static bool IsSupportedStorageType(
      const std::shared_ptr<::arrow::DataType>& storage_type);

  std::shared_ptr<::arrow::Field> metadata() const { return metadata_; }

  std::shared_ptr<::arrow::Field> value() const { return value_; }

 private:
  // TODO GH-45948 added shredded_value
  std::shared_ptr<::arrow::Field> metadata_;
  std::shared_ptr<::arrow::Field> value_;
};

/// \brief Return a VariantExtensionType instance.
PARQUET_EXPORT std::shared_ptr<::arrow::DataType> variant(
    std::shared_ptr<::arrow::DataType> storage_type);

}  // namespace parquet::arrow
