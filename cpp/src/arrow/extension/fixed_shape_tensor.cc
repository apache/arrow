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

#include "arrow/extension/fixed_shape_tensor.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep
#include "arrow/tensor.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/sort.h"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>

namespace rj = arrow::rapidjson;

namespace arrow {
namespace extension {

bool FixedShapeTensorType::ExtensionEquals(const ExtensionType& other) const {
  if (extension_name() != other.extension_name()) {
    return false;
  }
  const auto& other_ext = static_cast<const FixedShapeTensorType&>(other);

  auto is_permutation_trivial = [](const std::vector<int64_t>& permutation) {
    for (size_t i = 1; i < permutation.size(); ++i) {
      if (permutation[i - 1] + 1 != permutation[i]) {
        return false;
      }
    }
    return true;
  };
  const bool permutation_equivalent =
      ((permutation_ == other_ext.permutation()) ||
       (permutation_.empty() && is_permutation_trivial(other_ext.permutation())) ||
       (is_permutation_trivial(permutation_) && other_ext.permutation().empty()));

  return (storage_type()->Equals(other_ext.storage_type())) &&
         (shape_ == other_ext.shape()) && (dim_names_ == other_ext.dim_names()) &&
         permutation_equivalent;
}

std::string FixedShapeTensorType::Serialize() const {
  rj::Document document;
  document.SetObject();
  rj::Document::AllocatorType& allocator = document.GetAllocator();

  rj::Value shape(rj::kArrayType);
  for (auto v : shape_) {
    shape.PushBack(v, allocator);
  }
  document.AddMember(rj::Value("shape", allocator), shape, allocator);

  if (!permutation_.empty()) {
    rj::Value permutation(rj::kArrayType);
    for (auto v : permutation_) {
      permutation.PushBack(v, allocator);
    }
    document.AddMember(rj::Value("permutation", allocator), permutation, allocator);
  }

  if (!dim_names_.empty()) {
    rj::Value dim_names(rj::kArrayType);
    for (std::string v : dim_names_) {
      dim_names.PushBack(rj::Value{}.SetString(v.c_str(), allocator), allocator);
    }
    document.AddMember(rj::Value("dim_names", allocator), dim_names, allocator);
  }

  rj::StringBuffer buffer;
  rj::Writer<rj::StringBuffer> writer(buffer);
  document.Accept(writer);
  return buffer.GetString();
}

Result<std::shared_ptr<DataType>> FixedShapeTensorType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  if (storage_type->id() != Type::FIXED_SIZE_LIST) {
    return Status::Invalid("Expected FixedSizeList storage type, got ",
                           storage_type->ToString());
  }
  auto value_type =
      internal::checked_pointer_cast<FixedSizeListType>(storage_type)->value_type();
  rj::Document document;
  if (document.Parse(serialized_data.data(), serialized_data.length()).HasParseError() ||
      !document.HasMember("shape") || !document["shape"].IsArray()) {
    return Status::Invalid("Invalid serialized JSON data: ", serialized_data);
  }

  std::vector<int64_t> shape;
  for (auto& x : document["shape"].GetArray()) {
    shape.emplace_back(x.GetInt64());
  }
  std::vector<int64_t> permutation;
  if (document.HasMember("permutation")) {
    for (auto& x : document["permutation"].GetArray()) {
      permutation.emplace_back(x.GetInt64());
    }
    if (shape.size() != permutation.size()) {
      return Status::Invalid("Invalid permutation");
    }
  }
  std::vector<std::string> dim_names;
  if (document.HasMember("dim_names")) {
    for (auto& x : document["dim_names"].GetArray()) {
      dim_names.emplace_back(x.GetString());
    }
    if (shape.size() != dim_names.size()) {
      return Status::Invalid("Invalid dim_names");
    }
  }

  return fixed_shape_tensor(value_type, shape, permutation, dim_names);
}

std::shared_ptr<Array> FixedShapeTensorType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  return std::make_shared<ExtensionArray>(data);
}

Result<std::shared_ptr<Array>> FixedShapeTensorType::MakeArray(
    std::shared_ptr<Tensor> tensor) const {
  auto permutation = internal::ArgSort(tensor->strides());
  std::reverse(permutation.begin(), permutation.end());
  if (permutation[0] != 0) {
    return Status::Invalid(
        "Only first-major tensors can be zero-copy converted to arrays");
  }

  auto cell_shape = tensor->shape();
  cell_shape.erase(cell_shape.begin());
  if (cell_shape != shape_) {
    return Status::Invalid("Expected cell shape does not match input tensor shape");
  }

  permutation.erase(permutation.begin());
  for (auto& x : permutation) {
    x--;
  }

  auto ext_type = internal::checked_pointer_cast<ExtensionType>(
      fixed_shape_tensor(tensor->type(), cell_shape, permutation, dim_names()));

  std::shared_ptr<FixedSizeListArray> arr;
  std::shared_ptr<Array> value_array;
  switch (tensor->type_id()) {
    case Type::UINT8: {
      value_array = std::make_shared<UInt8Array>(tensor->size(), tensor->data());
      break;
    }
    case Type::INT8: {
      value_array = std::make_shared<Int8Array>(tensor->size(), tensor->data());
      break;
    }
    case Type::UINT16: {
      value_array = std::make_shared<UInt16Array>(tensor->size(), tensor->data());
      break;
    }
    case Type::INT16: {
      value_array = std::make_shared<Int16Array>(tensor->size(), tensor->data());
      break;
    }
    case Type::UINT32: {
      value_array = std::make_shared<UInt32Array>(tensor->size(), tensor->data());
      break;
    }
    case Type::INT32: {
      value_array = std::make_shared<Int32Array>(tensor->size(), tensor->data());
      break;
    }
    case Type::UINT64: {
      value_array = std::make_shared<Int64Array>(tensor->size(), tensor->data());
      break;
    }
    case Type::INT64: {
      value_array = std::make_shared<Int64Array>(tensor->size(), tensor->data());
      break;
    }
    case Type::HALF_FLOAT: {
      value_array = std::make_shared<HalfFloatArray>(tensor->size(), tensor->data());
      break;
    }
    case Type::FLOAT: {
      value_array = std::make_shared<FloatArray>(tensor->size(), tensor->data());
      break;
    }
    case Type::DOUBLE: {
      value_array = std::make_shared<DoubleArray>(tensor->size(), tensor->data());
      break;
    }
    default: {
      return Status::NotImplemented("Unsupported tensor type: ",
                                    tensor->type()->ToString());
    }
  }
  arr = std::make_shared<FixedSizeListArray>(ext_type->storage_type(), tensor->shape()[0],
                                             value_array);
  auto ext_data = arr->data();
  ext_data->type = ext_type;
  return MakeArray(ext_data);
}

Result<std::shared_ptr<Tensor>> FixedShapeTensorType::ToTensor(
    std::shared_ptr<Array> arr) {
  // To convert an array of n dimensional tensors to a n+1 dimensional tensor we
  // interpret the array's length as the first dimension the new tensor. Further, we
  // define n+1 dimensional tensor's strides by front appending a new stride to the n
  // dimensional tensor's strides.

  ARROW_RETURN_IF(arr->null_count() > 0,
                  Status::Invalid("Null values not supported in tensors."));
  auto ext_arr = internal::checked_pointer_cast<FixedSizeListArray>(
      internal::checked_pointer_cast<ExtensionArray>(arr)->storage());

  std::vector<int64_t> shape = this->shape();
  shape.insert(shape.begin(), 1, arr->length());

  int64_t major_stride;
  std::vector<int64_t> tensor_strides = this->strides();
  if (internal::MultiplyWithOverflow(arr->length(), tensor_strides[0], &major_stride)) {
    return Status::Invalid("Overflow in tensor strides");
  }
  tensor_strides.insert(tensor_strides.begin(), 1, major_stride);

  std::vector<std::string> dim_names;
  if (!this->dim_names().empty()) {
    dim_names = this->dim_names();
    dim_names.insert(dim_names.begin(), 1, "");
  } else {
    dim_names = {};
  }

  std::shared_ptr<Buffer> buffer = ext_arr->values()->data()->buffers[1];
  ARROW_ASSIGN_OR_RAISE(auto tensor, Tensor::Make(ext_arr->value_type(), buffer, shape,
                                                  tensor_strides, dim_names));
  return tensor;
}

Result<std::shared_ptr<DataType>> FixedShapeTensorType::Make(
    const std::shared_ptr<DataType>& value_type, const std::vector<int64_t>& shape,
    const std::vector<int64_t>& permutation, const std::vector<std::string>& dim_names) {
  if (!permutation.empty() && shape.size() != permutation.size()) {
    return Status::Invalid("permutation size must match shape size. Expected: ",
                           shape.size(), " Got: ", permutation.size());
  }
  if (!dim_names.empty() && shape.size() != dim_names.size()) {
    return Status::Invalid("dim_names size must match shape size. Expected: ",
                           shape.size(), " Got: ", dim_names.size());
  }
  return std::make_shared<FixedShapeTensorType>(value_type, shape, permutation,
                                                dim_names);
}

Result<std::vector<int64_t>> FixedShapeTensorType::ComputeStrides(
    const FixedShapeTensorType& type) {
  std::vector<int64_t> strides;
  auto element_type =
      internal::checked_pointer_cast<FixedSizeListType>(type.storage_type());
  auto value_type =
      internal::checked_pointer_cast<FixedWidthType>(element_type->value_type());

  ARROW_RETURN_NOT_OK(
      internal::ComputeRowMajorStrides(*value_type.get(), type.shape(), &strides));
  if (!type.permutation().empty()) {
    internal::Permute(type.permutation(), &strides);
  }
  return strides;
}

const std::vector<int64_t>& FixedShapeTensorType::strides() {
  if (strides_.empty()) {
    auto maybe_strides = ComputeStrides(*this);
    ARROW_CHECK_OK(maybe_strides.status());
    strides_ = maybe_strides.MoveValueUnsafe();
  }
  return strides_;
}

std::shared_ptr<DataType> fixed_shape_tensor(const std::shared_ptr<DataType>& value_type,
                                             const std::vector<int64_t>& shape,
                                             const std::vector<int64_t>& permutation,
                                             const std::vector<std::string>& dim_names) {
  auto maybe_type = FixedShapeTensorType::Make(value_type, shape, permutation, dim_names);
  ARROW_DCHECK_OK(maybe_type.status());
  return maybe_type.MoveValueUnsafe();
}

const std::shared_ptr<DataType> GetStorageType(
    const std::shared_ptr<DataType>& value_type, const std::vector<int64_t>& shape) {
  const auto size = std::accumulate(shape.begin(), shape.end(), static_cast<int64_t>(1),
                                    std::multiplies<>());
  return fixed_size_list(value_type, static_cast<int32_t>(size));
}

}  // namespace extension
}  // namespace arrow
