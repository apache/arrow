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
#pragma once

#include <complex>

#include "arrow/extension_type.h"

namespace arrow {


std::shared_ptr<DataType> complex64();
std::shared_ptr<DataType> complex128();


class ComplexFloatArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class ComplexFloatType : public ExtensionType {
 public:
  using c_type = std::complex<float>;

  explicit ComplexFloatType()
      : ExtensionType(fixed_size_list(float32(), 2)) {}

  std::string name() const override {
    return "complex64";
  }

  std::string extension_name() const override {
    return "arrow.complex64";
  }

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<ComplexFloatArray>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    return complex64();
  };

  std::string Serialize() const override {
    return "";
  }
};


class ComplexDoubleArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class ComplexDoubleType : public ExtensionType {
 public:
  using c_type = std::complex<double>;

  explicit ComplexDoubleType()
      : ExtensionType(fixed_size_list(float64(), 2)) {}

  std::string name() const override {
    return "complex128";
  }

  std::string extension_name() const override {
    return "arrow.complex128";
  }

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<ComplexFloatArray>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override {
    return complex128();
  };

  std::string Serialize() const override {
    return "";
  }
};

};  // namespace arrow
