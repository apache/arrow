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

#include <memory>
#include <string>

#include "arrow/extension_type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class ARROW_EXPORT UUIDArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class ARROW_EXPORT UUIDType : public ExtensionType {
 public:
  UUIDType() : ExtensionType(fixed_size_binary(16)) {}

  std::string extension_name() const override { return "uuid"; }

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override;

  std::string Serialize() const override { return "uuid-type-unique-code"; }
};

class ARROW_EXPORT SmallintArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class ARROW_EXPORT SmallintType : public ExtensionType {
 public:
  SmallintType() : ExtensionType(int16()) {}

  std::string extension_name() const override { return "smallint"; }

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override;

  std::string Serialize() const override { return "smallint"; }
};

ARROW_EXPORT
std::shared_ptr<DataType> uuid();

ARROW_EXPORT
std::shared_ptr<DataType> smallint();

ARROW_EXPORT
std::shared_ptr<Array> ExampleUUID();

ARROW_EXPORT
std::shared_ptr<Array> ExampleSmallint();

}  // namespace arrow
