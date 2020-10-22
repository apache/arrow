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

#include <numeric>
#include <sstream>

#include "arrow/extension_type.h"

namespace arrow {
namespace extension {

class ARROW_EXPORT FixedShapeTensor : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

/// \brief Concrete type class for constant-size Tensor data.
class ARROW_EXPORT FixedShapeTensorType : public ExtensionType {
 public:
  FixedShapeTensorType(const std::shared_ptr<DataType>& value_type,
                       const std::vector<int64_t>& shape,
                       const std::vector<int64_t>& permutation = {},
                       const std::vector<std::string>& dim_names = {})
      : ExtensionType(get_storage_type(value_type, shape)),
        value_type_(value_type),
        shape_(shape),
        permutation_(permutation),
        dim_names_(dim_names) {}

  std::string extension_name() const override;

  size_t ndim() const;

  std::vector<int64_t> shape() const;

  std::vector<int64_t> strides() const;

  std::vector<int64_t> permutation() const;

  std::vector<std::string> dim_names() const;

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::string Serialize() const override;

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized_data) const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override;

  Result<std::shared_ptr<Array>> MakeArray(std::shared_ptr<Tensor> tensor) const;

  Result<std::shared_ptr<Tensor>> ToTensor(std::shared_ptr<Array> arr) const;

 private:
  std::shared_ptr<DataType> get_storage_type(const std::shared_ptr<DataType>& value_type,
                                             const std::vector<int64_t>& shape) const;
  std::shared_ptr<DataType> storage_type_;
  std::shared_ptr<DataType> value_type_;
  std::vector<int64_t> shape_;
  std::vector<int64_t> permutation_;
  std::vector<std::string> dim_names_;
};

/// \brief Return a TensorArrayType instance.
ARROW_EXPORT std::shared_ptr<FixedShapeTensorType> tensor_array(
    const std::shared_ptr<DataType>& storage_type, const std::vector<int64_t>& shape,
    const std::vector<int64_t>& permutation = {},
    const std::vector<std::string>& dim_names = {});

}  // namespace extension
}  // namespace arrow
