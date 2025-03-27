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

/// EXPERIMENTAL: Variant is not yet fully supported.
class PARQUET_EXPORT VariantExtensionType : public ::arrow::ExtensionType {
 public:
  explicit VariantExtensionType(const std::shared_ptr<::arrow::DataType>& storage_type)
      : ::arrow::ExtensionType(std::move(storage_type)) {}

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

  std::shared_ptr<::arrow::Field> metadata_field() const { return children_.at(0); }

  std::shared_ptr<::arrow::Field> value_field() const { return children_.at(1); }
};

/// \brief Return a VariantExtensionType instance.
PARQUET_EXPORT std::shared_ptr<::arrow::DataType> variant(
    std::shared_ptr<::arrow::DataType> storage_type);

}  // namespace parquet::arrow
