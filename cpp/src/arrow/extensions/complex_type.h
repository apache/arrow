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

// Complex Number Extension Type

#include "arrow/extension_type.h"

namespace arrow {

class ComplexArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class ComplexType : public ExtensionType {
 private:
  std::shared_ptr<FloatingPointType> subtype_;

  static std::shared_ptr<DataType> MakeType(std::shared_ptr<DataType> subtype);
  static std::shared_ptr<FloatingPointType> FloatCast(std::shared_ptr<DataType> subtype);

 public:
  explicit ComplexType(std::shared_ptr<DataType> subtype)
      : ExtensionType(MakeType(subtype)), subtype_(FloatCast(subtype)) {}

  std::shared_ptr<FloatingPointType> subtype() const { return subtype_; }
  std::string name() const override;
  std::string extension_name() const override;

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<ComplexArray>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override;

  std::string Serialize() const override;
};

std::shared_ptr<DataType> complex(std::shared_ptr<DataType> subtype);
std::shared_ptr<DataType> complex64();
std::shared_ptr<DataType> complex128();

};  // namespace arrow
