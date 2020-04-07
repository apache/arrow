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
#include <limits>
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

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCSCIndex

template <typename TYPE>
class SparseCSCMatrixConverter {
 public:
  using NumericTensorType = NumericTensor<TYPE>;
  using value_type = typename NumericTensorType::value_type;

  SparseCSCMatrixConverter(const NumericTensorType& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  template <typename IndexValueType>
  Status Convert() {
    using c_index_value_type = typename IndexValueType::c_type;
    RETURN_NOT_OK(CheckMaximumValue(std::numeric_limits<c_index_value_type>::max()));
    const int64_t indices_elsize = sizeof(c_index_value_type);

    const int64_t ndim = tensor_.ndim();
    if (ndim > 2) {
      // LCOV_EXCL_START: The following invalid causes program failure.
      return Status::Invalid("Invalid tensor dimension");
      // LCOV_EXCL_STOP
    }

    const int64_t nr = tensor_.shape()[0];
    const int64_t nc = tensor_.shape()[1];
    int64_t nonzero_count = -1;
    RETURN_NOT_OK(tensor_.CountNonZero(&nonzero_count));

    std::shared_ptr<Buffer> indptr_buffer;
    std::shared_ptr<Buffer> indices_buffer;

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(sizeof(value_type) * nonzero_count, pool_));
    value_type* values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

    if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    } else {
      ARROW_ASSIGN_OR_RAISE(indptr_buffer,
                            AllocateBuffer(indices_elsize * (nc + 1), pool_));
      auto* indptr = reinterpret_cast<c_index_value_type*>(indptr_buffer->mutable_data());

      ARROW_ASSIGN_OR_RAISE(indices_buffer,
                            AllocateBuffer(indices_elsize * nonzero_count, pool_));
      auto* indices =
          reinterpret_cast<c_index_value_type*>(indices_buffer->mutable_data());

      c_index_value_type k = 0;
      *indptr++ = 0;
      for (int64_t j = 0; j < nc; ++j) {
        for (int64_t i = 0; i < nr; ++i) {
          const value_type x = tensor_.Value({i, j});
          if (x != 0) {
            *values++ = x;
            *indices++ = static_cast<c_index_value_type>(i);
            k++;
          }
        }
        *indptr++ = k;
      }
    }

    std::vector<int64_t> indptr_shape({nc + 1});
    std::shared_ptr<Tensor> indptr_tensor =
        std::make_shared<Tensor>(index_value_type_, indptr_buffer, indptr_shape);

    std::vector<int64_t> indices_shape({nonzero_count});
    std::shared_ptr<Tensor> indices_tensor =
        std::make_shared<Tensor>(index_value_type_, indices_buffer, indices_shape);

    sparse_index = std::make_shared<SparseCSCIndex>(indptr_tensor, indices_tensor);
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

  std::shared_ptr<SparseCSCIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  const NumericTensorType& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
  MemoryPool* pool_;

  template <typename c_value_type>
  inline Status CheckMaximumValue(const c_value_type type_max) const {
    if (static_cast<int64_t>(type_max) < tensor_.shape()[1]) {
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
Status MakeSparseCSCMatrixFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  NumericTensor<TYPE> numeric_tensor(tensor.data(), tensor.shape(), tensor.strides());
  SparseCSCMatrixConverter<TYPE> converter(numeric_tensor, index_value_type, pool);
  RETURN_NOT_OK(converter.Convert());

  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

}  // namespace

#define MAKE_SPARSE_CSC_MATRIX_FROM_TENSOR(TYPE_CLASS)      \
  case TYPE_CLASS##Type::type_id:                           \
    return MakeSparseCSCMatrixFromTensor<TYPE_CLASS##Type>( \
        tensor, index_value_type, pool, out_sparse_index, out_data);

Status MakeSparseCSCMatrixFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  switch (tensor.type()->id()) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(MAKE_SPARSE_CSC_MATRIX_FROM_TENSOR);
      // LCOV_EXCL_START: ignore program failure
    default:
      return Status::TypeError("Unsupported Tensor value type");
      // LCOV_EXCL_STOP
  }
}

#undef MAKE_SPARSE_CSC_MATRIX_FROM_TENSOR

}  // namespace internal
}  // namespace arrow
