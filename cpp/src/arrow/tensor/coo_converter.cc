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

#include "arrow/tensor/converter.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;

namespace internal {
namespace {

inline void IncrementIndex(std::vector<int64_t>& coord,
                           const std::vector<int64_t>& shape) {
  const int64_t ndim = shape.size();
  ++coord[ndim - 1];
  if (coord[ndim - 1] == shape[ndim - 1]) {
    int64_t d = ndim - 1;
    while (d > 0 && coord[d] == shape[d]) {
      coord[d] = 0;
      ++coord[d - 1];
      --d;
    }
  }
}

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCOOIndex

template <typename TYPE>
class SparseCOOTensorConverter {
 public:
  using NumericTensorType = NumericTensor<TYPE>;
  using value_type = typename NumericTensorType::value_type;

  SparseCOOTensorConverter(const NumericTensorType& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  template <typename IndexValueType>
  Status Convert() {
    using c_index_value_type = typename IndexValueType::c_type;
    const int64_t indices_elsize = sizeof(c_index_value_type);

    const int64_t ndim = tensor_.ndim();
    int64_t nonzero_count = -1;
    RETURN_NOT_OK(tensor_.CountNonZero(&nonzero_count));

    ARROW_ASSIGN_OR_RAISE(auto indices_buffer,
                          AllocateBuffer(indices_elsize * ndim * nonzero_count, pool_));
    c_index_value_type* indices =
        reinterpret_cast<c_index_value_type*>(indices_buffer->mutable_data());

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(sizeof(value_type) * nonzero_count, pool_));
    value_type* values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

    if (ndim <= 1) {
      const value_type* data = reinterpret_cast<const value_type*>(tensor_.raw_data());
      const int64_t count = ndim == 0 ? 1 : tensor_.shape()[0];
      for (int64_t i = 0; i < count; ++i, ++data) {
        if (*data != 0) {
          *indices++ = static_cast<c_index_value_type>(i);
          *values++ = *data;
        }
      }
    } else {
      const std::vector<int64_t>& shape = tensor_.shape();
      std::vector<int64_t> coord(ndim, 0);  // The current logical coordinates

      for (int64_t n = tensor_.size(); n > 0; n--) {
        const value_type x = tensor_.Value(coord);
        if (tensor_.Value(coord) != 0) {
          *values++ = x;
          // Write indices in row-major order.
          for (int64_t i = 0; i < ndim; ++i) {
            *indices++ = static_cast<c_index_value_type>(coord[i]);
          }
        }
        IncrementIndex(coord, shape);
      }
    }

    // make results
    const std::vector<int64_t> indices_shape = {nonzero_count, ndim};
    const std::vector<int64_t> indices_strides = {indices_elsize * ndim, indices_elsize};
    sparse_index = std::make_shared<SparseCOOIndex>(std::make_shared<Tensor>(
        index_value_type_, std::move(indices_buffer), indices_shape, indices_strides));
    data = std::move(values_buffer);

    return Status::OK();
  }

#define CALL_TYPE_SPECIFIC_CONVERT(TYPE_CLASS) \
  case TYPE_CLASS##Type::type_id:              \
    return Convert<TYPE_CLASS##Type>();

  Status Convert() {
    switch (index_value_type_->id()) {
      ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(CALL_TYPE_SPECIFIC_CONVERT);
      // LCOV_EXCL_START: The following invalid causes program failure.
      default:
        return Status::TypeError("Unsupported SparseTensor index value type");
        // LCOV_EXCL_STOP
    }
  }

#undef CALL_TYPE_SPECIFIC_CONVERT

  std::shared_ptr<SparseCOOIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  const NumericTensorType& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
  MemoryPool* pool_;
};

template <typename TYPE>
Status MakeSparseCOOTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  NumericTensor<TYPE> numeric_tensor(tensor.data(), tensor.shape(), tensor.strides());
  SparseCOOTensorConverter<TYPE> converter(numeric_tensor, index_value_type, pool);
  RETURN_NOT_OK(converter.Convert());

  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

}  // namespace

#define MAKE_SPARSE_TENSOR_FROM_TENSOR(TYPE_CLASS)          \
  case TYPE_CLASS##Type::type_id:                           \
    return MakeSparseCOOTensorFromTensor<TYPE_CLASS##Type>( \
        tensor, index_value_type, pool, out_sparse_index, out_data);

Status MakeSparseCOOTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  switch (tensor.type()->id()) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(MAKE_SPARSE_TENSOR_FROM_TENSOR);
      // LCOV_EXCL_START: ignore program failure
    default:
      return Status::TypeError("Unsupported Tensor value type");
      // LCOV_EXCL_STOP
  }
}

#undef MAKE_SPARSE_TENSOR_FROM_TENSOR

}  // namespace internal
}  // namespace arrow
