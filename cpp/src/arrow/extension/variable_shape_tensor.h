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

#include "arrow/extension_type.h"

namespace arrow {
namespace extension {

class ARROW_EXPORT VariableShapeTensorArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;

  /// \brief Get a Tensor of VariableShapeTensorArray at i
  ///
  /// This method will return a Tensor from VariableShapeTensorArray with strides
  /// derived from shape and permutation of VariableShapeTensorType. Shape and
  /// dim_names will be permuted according to permutation stored in the
  /// VariableShapeTensorType metadata.
  const Result<std::shared_ptr<Tensor>> GetTensor(const int64_t i) const;
};

/// \brief Concrete type class for variable-shape Tensor data.
/// This is a canonical arrow extension type.
/// See: https://arrow.apache.org/docs/format/CanonicalExtensions.html
class ARROW_EXPORT VariableShapeTensorType : public ExtensionType {
 public:
  VariableShapeTensorType(const std::shared_ptr<DataType>& value_type,
                          const uint32_t& ndim,
                          const std::vector<int64_t>& permutation = {},
                          const std::vector<std::string>& dim_names = {})
      : ExtensionType(struct_({::arrow::field("shape", fixed_size_list(uint32(), ndim)),
                               ::arrow::field("data", list(value_type))})),
        value_type_(value_type),
        permutation_(permutation),
        dim_names_(dim_names) {}

  std::string extension_name() const override { return "arrow.variable_shape_tensor"; }

  /// Number of dimensions of tensor elements
  uint32_t ndim() const {
    std::shared_ptr<DataType> storage_type = this->storage_type()->field(0)->type();
    return std::static_pointer_cast<FixedSizeListType>(storage_type)->list_size();
  }

  /// Value type of tensor elements
  const std::shared_ptr<DataType> value_type() const { return value_type_; }

  /// Permutation mapping from logical to physical memory layout of tensor elements
  const std::vector<int64_t>& permutation() const { return permutation_; }

  /// Dimension names of tensor elements. Dimensions are ordered physically.
  const std::vector<std::string>& dim_names() const { return dim_names_; }

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::string Serialize() const override;

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized_data) const override;

  /// Create a VariableShapeTensorArray from ArrayData
  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  /// \brief Create a VariableShapeTensorType instance
  static Result<std::shared_ptr<DataType>> Make(
      const std::shared_ptr<DataType>& value_type, const uint32_t& ndim,
      const std::vector<int64_t>& permutation = {},
      const std::vector<std::string>& dim_names = {});

 private:
  std::shared_ptr<DataType> storage_type_;
  std::shared_ptr<DataType> value_type_;
  std::vector<int64_t> permutation_;
  std::vector<std::string> dim_names_;
};

/// \brief Return a VariableShapeTensorType instance.
ARROW_EXPORT std::shared_ptr<DataType> variable_shape_tensor(
    const std::shared_ptr<DataType>& value_type, const uint32_t& ndim,
    const std::vector<int64_t>& permutation = {},
    const std::vector<std::string>& dim_names = {});

}  // namespace extension
}  // namespace arrow
