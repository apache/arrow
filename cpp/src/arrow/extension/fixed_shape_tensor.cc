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

namespace {

Status ComputeStrides(const FixedWidthType& type, const std::vector<int64_t>& shape,
                      const std::vector<int64_t>& permutation,
                      std::vector<int64_t>* strides) {
  if (permutation.empty()) {
    return internal::ComputeRowMajorStrides(type, shape, strides);
  }

  const int byte_width = type.byte_width();

  int64_t remaining = 0;
  if (!shape.empty() && shape.front() > 0) {
    remaining = byte_width;
    for (auto i : permutation) {
      if (i > 0) {
        if (internal::MultiplyWithOverflow(remaining, shape[i], &remaining)) {
          return Status::Invalid(
              "Strides computed from shape would not fit in 64-bit integer");
        }
      }
    }
  }

  if (remaining == 0) {
    strides->assign(shape.size(), byte_width);
    return Status::OK();
  }

  strides->push_back(remaining);
  for (auto i : permutation) {
    if (i > 0) {
      remaining /= shape[i];
      strides->push_back(remaining);
    }
  }
  internal::Permute(permutation, strides);

  return Status::OK();
}

}  // namespace

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
         (this->shape() == other_ext.shape()) && (dim_names_ == other_ext.dim_names()) &&
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
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.fixed_shape_tensor",
            static_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<ExtensionArray>(data);
}

Result<std::shared_ptr<FixedShapeTensorArray>> FixedShapeTensorArray::FromTensor(
    const std::shared_ptr<Tensor>& tensor) {
  auto permutation = internal::ArgSort(tensor->strides(), std::greater<>());
  if (permutation[0] != 0) {
    return Status::Invalid(
        "Only first-major tensors can be zero-copy converted to arrays");
  }
  permutation.erase(permutation.begin());

  std::vector<int64_t> cell_shape;
  for (auto i : permutation) {
    cell_shape.emplace_back(tensor->shape()[i]);
  }

  std::vector<std::string> dim_names;
  if (!tensor->dim_names().empty()) {
    for (auto i : permutation) {
      dim_names.emplace_back(tensor->dim_names()[i]);
    }
  }

  for (int64_t& i : permutation) {
    --i;
  }

  auto ext_type = internal::checked_pointer_cast<ExtensionType>(
      fixed_shape_tensor(tensor->type(), cell_shape, permutation, dim_names));

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
  auto cell_size = static_cast<int32_t>(tensor->size() / tensor->shape()[0]);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> arr,
                        FixedSizeListArray::FromArrays(value_array, cell_size));
  std::shared_ptr<Array> ext_arr = ExtensionType::WrapArray(ext_type, arr);
  return std::static_pointer_cast<FixedShapeTensorArray>(ext_arr);
}

const Result<std::shared_ptr<Tensor>> FixedShapeTensorArray::ToTensor() const {
  // To convert an array of n dimensional tensors to a n+1 dimensional tensor we
  // interpret the array's length as the first dimension the new tensor.

  auto ext_arr = std::static_pointer_cast<FixedSizeListArray>(this->storage());
  auto ext_type = internal::checked_pointer_cast<FixedShapeTensorType>(this->type());
  ARROW_RETURN_IF(!is_fixed_width(*ext_arr->value_type()),
                  Status::Invalid(ext_arr->value_type()->ToString(),
                                  " is not valid data type for a tensor"));
  auto permutation = ext_type->permutation();

  std::vector<std::string> dim_names;
  if (!ext_type->dim_names().empty()) {
    for (auto i : permutation) {
      dim_names.emplace_back(ext_type->dim_names()[i]);
    }
    dim_names.insert(dim_names.begin(), 1, "");
  } else {
    dim_names = {};
  }

  std::vector<int64_t> shape;
  for (int64_t& i : permutation) {
    shape.emplace_back(ext_type->shape()[i]);
    ++i;
  }
  shape.insert(shape.begin(), 1, this->length());
  permutation.insert(permutation.begin(), 1, 0);

  std::vector<int64_t> tensor_strides;
  auto value_type = internal::checked_pointer_cast<FixedWidthType>(ext_arr->value_type());
  ARROW_RETURN_NOT_OK(
      ComputeStrides(*value_type.get(), shape, permutation, &tensor_strides));
  ARROW_ASSIGN_OR_RAISE(auto buffers, ext_arr->Flatten());
  ARROW_ASSIGN_OR_RAISE(
      auto tensor, Tensor::Make(ext_arr->value_type(), buffers->data()->buffers[1], shape,
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
  const auto size = std::accumulate(shape.begin(), shape.end(), static_cast<int64_t>(1),
                                    std::multiplies<>());
  return std::make_shared<FixedShapeTensorType>(value_type, static_cast<int32_t>(size),
                                                shape, permutation, dim_names);
}

const std::vector<int64_t>& FixedShapeTensorType::strides() {
  if (strides_.empty()) {
    auto value_type = internal::checked_pointer_cast<FixedWidthType>(this->value_type_);
    std::vector<int64_t> tensor_strides;
    ARROW_CHECK_OK(ComputeStrides(*value_type.get(), this->shape(), this->permutation(),
                                  &tensor_strides));
    strides_ = tensor_strides;
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

}  // namespace extension
}  // namespace arrow
