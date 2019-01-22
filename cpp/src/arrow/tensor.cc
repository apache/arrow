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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "arrow/compare.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

static void ComputeRowMajorStrides(const FixedWidthType& type,
                                   const std::vector<int64_t>& shape,
                                   std::vector<int64_t>* strides) {
  int64_t remaining = type.bit_width() / 8;
  for (int64_t dimsize : shape) {
    remaining *= dimsize;
  }

  if (remaining == 0) {
    strides->assign(shape.size(), type.bit_width() / 8);
    return;
  }

  for (int64_t dimsize : shape) {
    remaining /= dimsize;
    strides->push_back(remaining);
  }
}

static void ComputeColumnMajorStrides(const FixedWidthType& type,
                                      const std::vector<int64_t>& shape,
                                      std::vector<int64_t>* strides) {
  int64_t total = type.bit_width() / 8;
  for (int64_t dimsize : shape) {
    if (dimsize == 0) {
      strides->assign(shape.size(), type.bit_width() / 8);
      return;
    }
  }
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
  DCHECK(is_tensor_supported(type->id()));
  if (shape.size() > 0 && strides.size() == 0) {
    ComputeRowMajorStrides(checked_cast<const FixedWidthType&>(*type_), shape, &strides_);
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
  return std::accumulate(shape_.begin(), shape_.end(), 1LL, std::multiplies<int64_t>());
}

bool Tensor::is_contiguous() const { return is_row_major() || is_column_major(); }

bool Tensor::is_row_major() const {
  std::vector<int64_t> c_strides;
  const auto& fw_type = checked_cast<const FixedWidthType&>(*type_);
  ComputeRowMajorStrides(fw_type, shape_, &c_strides);
  return strides_ == c_strides;
}

bool Tensor::is_column_major() const {
  std::vector<int64_t> f_strides;
  const auto& fw_type = checked_cast<const FixedWidthType&>(*type_);
  ComputeColumnMajorStrides(fw_type, shape_, &f_strides);
  return strides_ == f_strides;
}

Type::type Tensor::type_id() const { return type_->id(); }

bool Tensor::Equals(const Tensor& other) const { return TensorEquals(*this, other); }

namespace internal {

template <typename TYPE>
size_t StridedTensorCountNonZero(int dim_index, int64_t offset, const Tensor& tensor) {
  using c_type = typename TYPE::c_type;
  c_type const zero = c_type(0);
  size_t nnz = 0;
  if (dim_index == tensor.ndim() - 1) {
    for (int64_t i = 0; i < tensor.shape()[dim_index]; ++i) {
      auto const* ptr = tensor.raw_data() + offset + i * tensor.strides()[dim_index];
      auto& elem = *reinterpret_cast<c_type const*>(ptr);
      if (elem != zero) ++nnz;
    }
    return nnz;
  }
  for (int64_t i = 0; i < tensor.shape()[dim_index]; ++i) {
    nnz += StridedTensorCountNonZero<TYPE>(dim_index + 1, offset, tensor);
    offset += tensor.strides()[dim_index];
  }
  return nnz;
}

template <typename TYPE>
size_t ContiguousTensorCountNonZero(const Tensor& tensor) {
  using c_type = typename TYPE::c_type;
  auto* data = reinterpret_cast<c_type const*>(tensor.raw_data());
  return std::count_if(data, data + tensor.size(),
                       [](c_type const& x) { return x != 0; });
}

}  // namespace internal

size_t Tensor::CountNonZero() const {
  if (size() == 0) {
    return 0;
  }

  if (is_contiguous()) {
    switch (type()->id()) {
      case Type::UINT8:
        return internal::ContiguousTensorCountNonZero<UInt8Type>(*this);
      case Type::INT8:
        return internal::ContiguousTensorCountNonZero<Int8Type>(*this);
      case Type::UINT16:
        return internal::ContiguousTensorCountNonZero<UInt16Type>(*this);
      case Type::INT16:
        return internal::ContiguousTensorCountNonZero<Int16Type>(*this);
      case Type::UINT32:
        return internal::ContiguousTensorCountNonZero<UInt32Type>(*this);
      case Type::INT32:
        return internal::ContiguousTensorCountNonZero<Int32Type>(*this);
      case Type::UINT64:
        return internal::ContiguousTensorCountNonZero<UInt64Type>(*this);
      case Type::INT64:
        return internal::ContiguousTensorCountNonZero<Int64Type>(*this);
      case Type::HALF_FLOAT:
        return internal::ContiguousTensorCountNonZero<HalfFloatType>(*this);
      case Type::FLOAT:
        return internal::ContiguousTensorCountNonZero<FloatType>(*this);
      case Type::DOUBLE:
        return internal::ContiguousTensorCountNonZero<DoubleType>(*this);
      default:
        return 0;  // This shouldn't be unreachable
    }
  } else {
    switch (type()->id()) {
      case Type::UINT8:
        return internal::StridedTensorCountNonZero<UInt8Type>(0, 0, *this);
      case Type::INT8:
        return internal::StridedTensorCountNonZero<Int8Type>(0, 0, *this);
      case Type::UINT16:
        return internal::StridedTensorCountNonZero<UInt16Type>(0, 0, *this);
      case Type::INT16:
        return internal::StridedTensorCountNonZero<Int16Type>(0, 0, *this);
      case Type::UINT32:
        return internal::StridedTensorCountNonZero<UInt32Type>(0, 0, *this);
      case Type::INT32:
        return internal::StridedTensorCountNonZero<Int32Type>(0, 0, *this);
      case Type::UINT64:
        return internal::StridedTensorCountNonZero<UInt64Type>(0, 0, *this);
      case Type::INT64:
        return internal::StridedTensorCountNonZero<Int64Type>(0, 0, *this);
      case Type::HALF_FLOAT:
        return internal::StridedTensorCountNonZero<HalfFloatType>(0, 0, *this);
      case Type::FLOAT:
        return internal::StridedTensorCountNonZero<FloatType>(0, 0, *this);
      case Type::DOUBLE:
        return internal::StridedTensorCountNonZero<DoubleType>(0, 0, *this);
      default:
        return 0;  // This shouldn't be unreachable
    }
  }
}

}  // namespace arrow
