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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/sort.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;

namespace internal {
namespace {

inline void IncrementIndex(std::vector<int64_t>& coord, const std::vector<int64_t>& shape,
                           const std::vector<int64_t>& axis_order) {
  const int64_t ndim = shape.size();
  const int64_t last_axis = axis_order[ndim - 1];
  ++coord[last_axis];
  if (coord[last_axis] == shape[last_axis]) {
    int64_t d = ndim - 1;
    while (d > 0 && coord[axis_order[d]] == shape[axis_order[d]]) {
      coord[axis_order[d]] = 0;
      ++coord[axis_order[d - 1]];
      --d;
    }
  }
}

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCSFIndex

template <typename TYPE>
class SparseCSFTensorConverter {
 public:
  using NumericTensorType = NumericTensor<TYPE>;
  using value_type = typename NumericTensorType::value_type;

  SparseCSFTensorConverter(const NumericTensorType& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  template <typename IndexValueType>
  Status Convert() {
    using c_index_value_type = typename IndexValueType::c_type;
    RETURN_NOT_OK(CheckMaximumValue(std::numeric_limits<c_index_value_type>::max()));

    const int64_t ndim = tensor_.ndim();
    // Axis order as ascending order of dimension size is a good heuristic but is not
    // necessarily optimal.
    std::vector<int64_t> axis_order = internal::ArgSort(tensor_.shape());
    int64_t nonzero_count = -1;
    RETURN_NOT_OK(tensor_.CountNonZero(&nonzero_count));

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(sizeof(value_type) * nonzero_count, pool_));
    value_type* values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

    std::vector<int64_t> counts(ndim, 0);
    std::vector<int64_t> coord(ndim, 0);
    std::vector<int64_t> previous_coord(ndim, -1);
    std::vector<TypedBufferBuilder<c_index_value_type>> indptr_buffer_builders(ndim - 1);
    std::vector<TypedBufferBuilder<c_index_value_type>> indices_buffer_builders(ndim);

    if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    } else {
      const std::vector<int64_t>& shape = tensor_.shape();
      for (int64_t n = tensor_.size(); n > 0; n--) {
        const value_type x = tensor_.Value(coord);

        if (x != 0) {
          bool tree_split = false;
          *values++ = x;

          for (int64_t i = 0; i < ndim; ++i) {
            int64_t dimension = axis_order[i];

            tree_split = tree_split || (coord[dimension] != previous_coord[dimension]);
            if (tree_split) {
              if (i < ndim - 1) {
                RETURN_NOT_OK(indptr_buffer_builders[i].Append(
                    static_cast<c_index_value_type>(counts[i + 1])));
              }
              RETURN_NOT_OK(indices_buffer_builders[i].Append(
                  static_cast<c_index_value_type>(coord[dimension])));
              ++counts[i];
            }
          }
          previous_coord = coord;
        }
        IncrementIndex(coord, shape, axis_order);
      }
    }

    for (int64_t column = 0; column < ndim - 1; ++column) {
      RETURN_NOT_OK(indptr_buffer_builders[column].Append(
          static_cast<c_index_value_type>(counts[column + 1])));
    }

    // make results
    data = std::move(values_buffer);

    std::vector<std::shared_ptr<Buffer>> indptr_buffers(ndim - 1);
    std::vector<std::shared_ptr<Buffer>> indices_buffers(ndim);
    std::vector<int64_t> indptr_shapes(counts.begin(), counts.end() - 1);
    std::vector<int64_t> indices_shapes = counts;

    for (int64_t column = 0; column < ndim; ++column) {
      RETURN_NOT_OK(
          indices_buffer_builders[column].Finish(&indices_buffers[column], true));
    }
    for (int64_t column = 0; column < ndim - 1; ++column) {
      RETURN_NOT_OK(indptr_buffer_builders[column].Finish(&indptr_buffers[column], true));
    }

    ARROW_ASSIGN_OR_RAISE(
        sparse_index, SparseCSFIndex::Make(index_value_type_, indices_shapes, axis_order,
                                           indptr_buffers, indices_buffers));
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

  std::shared_ptr<SparseCSFIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  const NumericTensorType& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
  MemoryPool* pool_;

  template <typename c_value_type>
  inline Status CheckMaximumValue(const c_value_type type_max) const {
    auto max_dimension =
        *std::max_element(tensor_.shape().begin(), tensor_.shape().end());
    if (static_cast<int64_t>(type_max) < max_dimension) {
      // LCOV_EXCL_START: The following invalid causes program failure.
      return Status::Invalid("The bit width of the index value type is too small");
      // LCOV_EXCL_STOP
    }
    return Status::OK();
  }

  inline Status CheckMaximumValue(const int64_t) const { return Status::OK(); }

  inline Status CheckMaximumValue(const uint64_t) const { return Status::OK(); }
};

template <typename TYPE>
Status MakeSparseCSFTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  NumericTensor<TYPE> numeric_tensor(tensor.data(), tensor.shape(), tensor.strides());
  SparseCSFTensorConverter<TYPE> converter(numeric_tensor, index_value_type, pool);
  RETURN_NOT_OK(converter.Convert());

  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

}  // namespace

#define MAKE_SPARSE_CSF_TENSOR_FROM_TENSOR(TYPE_CLASS)      \
  case TYPE_CLASS##Type::type_id:                           \
    return MakeSparseCSFTensorFromTensor<TYPE_CLASS##Type>( \
        tensor, index_value_type, pool, out_sparse_index, out_data);

Status MakeSparseCSFTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  switch (tensor.type()->id()) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(MAKE_SPARSE_CSF_TENSOR_FROM_TENSOR);
      // LCOV_EXCL_START: ignore program failure
    default:
      return Status::TypeError("Unsupported Tensor value type");
      // LCOV_EXCL_STOP
  }
}

#undef MAKE_SPARSE_CSF_TENSOR_FROM_TENSOR

}  // namespace internal
}  // namespace arrow
