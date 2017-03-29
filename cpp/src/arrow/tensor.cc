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
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"

namespace arrow {

void ComputeRowMajorStrides(const FixedWidthType& type, const std::vector<int64_t>& shape,
    std::vector<int64_t>* strides) {
  int64_t remaining = type.bit_width() / 8;
  for (int64_t dimsize : shape) {
    remaining *= dimsize;
  }

  for (int64_t dimsize : shape) {
    remaining /= dimsize;
    strides->push_back(remaining);
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
  DCHECK_LT(i, static_cast<int>(dim_names_.size()));
  return dim_names_[i];
}

int64_t Tensor::size() const {
  return std::accumulate(shape_.begin(), shape_.end(), 1, std::multiplies<int64_t>());
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

}  // namespace arrow
