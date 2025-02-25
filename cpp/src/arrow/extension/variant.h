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
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow::extension {

class ARROW_EXPORT VariantExtensionType : public ExtensionType {
 public:
  explicit VariantExtensionType(const std::shared_ptr<DataType>& storage_type)
      : ExtensionType(storage_type), storage_type_(storage_type) {}

  std::string extension_name() const override { return "variant.json"; }

  bool ExtensionEquals(const ExtensionType& other) const override;

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized_data) const override;

  std::string Serialize() const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  static Result<std::shared_ptr<DataType>> Make(std::shared_ptr<DataType> storage_type);

  static bool IsSupportedStorageType(std::shared_ptr<DataType> storage_type);

 private:
  std::shared_ptr<DataType> storage_type_;
};

/// \brief Return a VariantExtensionType instance.
ARROW_EXPORT std::shared_ptr<DataType> variant(std::shared_ptr<DataType> storage_type);

}  // namespace arrow::extension
