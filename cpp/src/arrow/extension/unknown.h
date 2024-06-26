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

#include "arrow/extension_type.h"
#include "arrow/type.h"

namespace arrow::extension {

/// \brief
class ARROW_EXPORT UnknownType : public ExtensionType {
 public:
  explicit UnknownType(std::shared_ptr<DataType> storage_type, std::string type_name,
                       std::string vendor_name)
      : ExtensionType(std::move(storage_type)),
        type_name_(std::move(type_name)),
        vendor_name_(std::move(vendor_name)) {}

  std::string extension_name() const override { return "arrow.unknown"; }
  std::string ToString(bool show_metadata) const override;
  bool ExtensionEquals(const ExtensionType& other) const override;
  std::string Serialize() const override;
  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized_data) const override;
  /// Create a UnknownArray from ArrayData
  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  std::string_view type_name() const { return type_name_; }
  std::string_view vendor_name() const { return vendor_name_; }

 private:
  std::string type_name_;
  std::string vendor_name_;
};

/// \brief
class ARROW_EXPORT UnknownArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

}  // namespace arrow::extension
