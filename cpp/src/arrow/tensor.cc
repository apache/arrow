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

#include "arrow/tensor.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"

namespace arrow {

static void ComputeRowMajorStrides(const FixedWidthType& type,
    const std::vector<int64_t>& shape, std::vector<int64_t>* strides) {
  int64_t remaining = type.bit_width() / 8;
  for (int64_t dimsize : shape) {
    remaining *= dimsize;
  }

  for (int64_t dimsize : shape) {
    remaining /= dimsize;
    strides->push_back(remaining);
  }
}

static void ComputeColumnMajorStrides(const FixedWidthType& type,
    const std::vector<int64_t>& shape, std::vector<int64_t>* strides) {
  int64_t total = type.bit_width() / 8;
  for (int64_t dimsize : shape) {
    strides->push_back(total);
    total *= dimsize;
  }
}

/// Constructor with strides and dimension names
Tensor::Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
    const std::vector<int64_t>& shape, const std::vector<int64_t>& strides,
    const std::vector<std::string>& dim_names)
    : type_(type), data_(data), shape_(shape), strides_(strides), dim_names_(dim_names) {
  DCHECK(is_tensor_supported(type->type));
  if (shape.size() > 0 && strides.size() == 0) {
    ComputeRowMajorStrides(static_cast<const FixedWidthType&>(*type_), shape, &strides_);
  }
}

Tensor::Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
    const std::vector<int64_t>& shape, const std::vector<int64_t>& strides)
    : Tensor(type, data, shape, strides, {}) {}

Tensor::Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
    const std::vector<int64_t>& shape)
    : Tensor(type, data, shape, {}, {}) {}

const std::string& Tensor::dim_name(int i) const {
  static const std::string kEmpty = "";
  if (dim_names_.size() == 0) {
    return kEmpty;
  } else {
    DCHECK_LT(i, static_cast<int>(dim_names_.size()));
    return dim_names_[i];
  }
}

int64_t Tensor::size() const {
  return std::accumulate(shape_.begin(), shape_.end(), 1, std::multiplies<int64_t>());
}

bool Tensor::is_contiguous() const {
  return is_row_major() || is_column_major();
}

bool Tensor::is_row_major() const {
  std::vector<int64_t> c_strides;
  const auto& fw_type = static_cast<const FixedWidthType&>(*type_);
  ComputeRowMajorStrides(fw_type, shape_, &c_strides);
  return strides_ == c_strides;
}

bool Tensor::is_column_major() const {
  std::vector<int64_t> f_strides;
  const auto& fw_type = static_cast<const FixedWidthType&>(*type_);
  ComputeColumnMajorStrides(fw_type, shape_, &f_strides);
  return strides_ == f_strides;
}

bool Tensor::Equals(const Tensor& other) const {
  bool are_equal = false;
  Status error = TensorEquals(*this, other, &are_equal);
  if (!error.ok()) { DCHECK(false) << "Tensors not comparable: " << error.ToString(); }
  return are_equal;
}

template <typename T>
NumericTensor<T>::NumericTensor(const std::shared_ptr<Buffer>& data,
    const std::vector<int64_t>& shape, const std::vector<int64_t>& strides,
    const std::vector<std::string>& dim_names)
    : Tensor(TypeTraits<T>::type_singleton(), data, shape, strides, dim_names),
      raw_data_(nullptr),
      mutable_raw_data_(nullptr) {
  if (data_) {
    raw_data_ = reinterpret_cast<const value_type*>(data_->data());
    if (data_->is_mutable()) {
      auto mut_buf = static_cast<MutableBuffer*>(data_.get());
      mutable_raw_data_ = reinterpret_cast<value_type*>(mut_buf->mutable_data());
    }
  }
}

template <typename T>
NumericTensor<T>::NumericTensor(
    const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape)
    : NumericTensor(data, shape, {}, {}) {}

template <typename T>
NumericTensor<T>::NumericTensor(const std::shared_ptr<Buffer>& data,
    const std::vector<int64_t>& shape, const std::vector<int64_t>& strides)
    : NumericTensor(data, shape, strides, {}) {}

template class ARROW_TEMPLATE_EXPORT NumericTensor<Int8Type>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<UInt8Type>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<Int16Type>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<UInt16Type>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<Int32Type>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<UInt32Type>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<Int64Type>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<UInt64Type>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<HalfFloatType>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<FloatType>;
template class ARROW_TEMPLATE_EXPORT NumericTensor<DoubleType>;

#define TENSOR_CASE(TYPE, TENSOR_TYPE)                                        \
  case Type::TYPE:                                                            \
    *tensor = std::make_shared<TENSOR_TYPE>(data, shape, strides, dim_names); \
    break;

Status ARROW_EXPORT MakeTensor(const std::shared_ptr<DataType>& type,
    const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape,
    const std::vector<int64_t>& strides, const std::vector<std::string>& dim_names,
    std::shared_ptr<Tensor>* tensor) {
  switch (type->type) {
    TENSOR_CASE(INT8, Int8Tensor);
    TENSOR_CASE(INT16, Int16Tensor);
    TENSOR_CASE(INT32, Int32Tensor);
    TENSOR_CASE(INT64, Int64Tensor);
    TENSOR_CASE(UINT8, UInt8Tensor);
    TENSOR_CASE(UINT16, UInt16Tensor);
    TENSOR_CASE(UINT32, UInt32Tensor);
    TENSOR_CASE(UINT64, UInt64Tensor);
    TENSOR_CASE(HALF_FLOAT, HalfFloatTensor);
    TENSOR_CASE(FLOAT, FloatTensor);
    TENSOR_CASE(DOUBLE, DoubleTensor);
    default:
      return Status::NotImplemented(type->ToString());
  }
  return Status::OK();
}

}  // namespace arrow
