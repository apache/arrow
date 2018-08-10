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

#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "arrow/compare.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

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

}  // namespace arrow
