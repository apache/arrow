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

#include "arrow/tensor/converter_internal.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/sort_internal.h"
#include "arrow/visit_type_inline.h"

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

template <typename ValueType, typename IndexType>
Status CheckValues(const SparseCSFIndex& sparse_csf_index,
                   const typename ValueType::c_type* values, const Tensor& tensor,
                   const int64_t dim, const int64_t dim_offset, const int64_t start,
                   const int64_t stop) {
  using ValueCType = typename ValueType::c_type;
  using IndexCType = typename IndexType::c_type;

  const auto& indices = sparse_csf_index.indices();
  const auto& indptr = sparse_csf_index.indptr();
  const auto& axis_order = sparse_csf_index.axis_order();
  auto ndim = indices.size();
  auto strides = tensor.strides();

  const auto& cur_indices = indices[dim];
  const auto* indices_data = cur_indices->data()->data_as<IndexCType>() + start;

  if (dim == static_cast<int64_t>(ndim) - 1) {
    for (auto i = start; i < stop; ++i) {
      auto index = static_cast<int64_t>(*indices_data);
      const int64_t offset = dim_offset + index * strides[axis_order[dim]];

      auto sparse_value = values[i];
      auto tensor_value =
          *reinterpret_cast<const ValueCType*>(tensor.raw_data() + offset);
      if (!is_not_zero<ValueType>(sparse_value)) {
        return Status::Invalid("Sparse tensor values must be non-zero");
      } else if (sparse_value != tensor_value) {
        if constexpr (is_floating_type<ValueType>::value) {
          if (!std::isnan(tensor_value) || !std::isnan(sparse_value)) {
            return Status::Invalid(
                "Inconsistent values between sparse tensor and dense tensor");
          }
        } else {
          return Status::Invalid(
              "Inconsistent values between sparse tensor and dense tensor");
        }
      }
      ++indices_data;
    }
  } else {
    const auto& cur_indptr = indptr[dim];
    const auto* indptr_data = cur_indptr->data()->data_as<IndexCType>() + start;

    for (int64_t i = start; i < stop; ++i) {
      const int64_t index = *indices_data;
      int64_t offset = dim_offset + index * strides[axis_order[dim]];
      auto next_start = static_cast<int64_t>(*indptr_data);
      auto next_stop = static_cast<int64_t>(*(indptr_data + 1));

      ARROW_RETURN_NOT_OK((CheckValues<ValueType, IndexType>(
          sparse_csf_index, values, tensor, dim + 1, offset, next_start, next_stop)));

      ++indices_data;
      ++indptr_data;
    }
  }
  return Status::OK();
}

template <typename ValueType, typename IndexType>
Status ValidateSparseTensorCSFCreation(const SparseIndex& sparse_index,
                                       const Buffer& values_buffer,
                                       const Tensor& tensor) {
  auto sparse_csf_index = checked_cast<const SparseCSFIndex&>(sparse_index);
  const auto* values = values_buffer.data_as<typename ValueType::c_type>();
  const auto& indices = sparse_csf_index.indices();

  ARROW_ASSIGN_OR_RAISE(auto non_zero_count, tensor.CountNonZero());
  if (indices.back()->size() != non_zero_count) {
    return Status::Invalid("Mismatch between non-zero count in sparse tensor (",
                           indices.back()->size(), ") and dense tensor (", non_zero_count,
                           ")");
  } else if (indices.size() != tensor.shape().size()) {
    return Status::Invalid("Mismatch between coordinate dimension in sparse tensor (",
                           indices.size(), ") and tensor shape (", tensor.shape().size(),
                           ")");
  } else {
    return CheckValues<ValueType, IndexType>(sparse_csf_index, values, tensor, 0, 0, 0,
                                             sparse_csf_index.indptr()[0]->size() - 1);
  }
}

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCSFIndex

class SparseCSFTensorConverter {
 public:
  SparseCSFTensorConverter(const Tensor& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  template <typename ValueType, typename IndexType>
  Status Convert(const ValueType&, const IndexType&) {
    using ValueCType = typename ValueType::c_type;
    using IndexCType = typename IndexType::c_type;
    RETURN_NOT_OK(::arrow::internal::CheckSparseIndexMaximumValue(index_value_type_,
                                                                  tensor_.shape()));
    const int64_t ndim = tensor_.ndim();
    if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    }

    const int value_elsize = tensor_.type()->byte_width();
    // Axis order as ascending order of dimension size is a good heuristic but is not
    // necessarily optimal.
    std::vector<int64_t> axis_order = internal::ArgSort(tensor_.shape());
    ARROW_ASSIGN_OR_RAISE(int64_t nonzero_count, tensor_.CountNonZero());

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(value_elsize * nonzero_count, pool_));

    std::vector<int64_t> counts(ndim, 0);
    std::vector<int64_t> coord(ndim, 0);
    std::vector<int64_t> previous_coord(ndim, -1);

    std::vector<TypedBufferBuilder<IndexCType>> indptr_buffer_builders(
        ndim - 1, TypedBufferBuilder<IndexCType>(pool_));
    std::vector<TypedBufferBuilder<IndexCType>> indices_buffer_builders(
        ndim, TypedBufferBuilder<IndexCType>(pool_));

    auto* values = values_buffer->mutable_data_as<ValueCType>();

    const auto& shape = tensor_.shape();
    for (int64_t n = tensor_.size(); n > 0; n--) {
      const auto value = tensor_.Value<ValueType>(coord);

      if (is_not_zero<ValueType>(value)) {
        bool tree_split = false;
        *values++ = value;
        for (int64_t i = 0; i < ndim; ++i) {
          int64_t dimension = axis_order[i];

          tree_split = tree_split || (coord[dimension] != previous_coord[dimension]);
          if (tree_split) {
            if (i < ndim - 1) {
              RETURN_NOT_OK(indptr_buffer_builders[i].Append(
                  static_cast<IndexCType>(counts[i + 1])));
            }
            RETURN_NOT_OK(indices_buffer_builders[i].Append(
                static_cast<IndexCType>(coord[dimension])));

            ++counts[i];
          }
        }

        previous_coord = coord;
      }

      IncrementIndex(coord, shape, axis_order);
    }

    for (int64_t column = 0; column < ndim - 1; ++column) {
      RETURN_NOT_OK(indptr_buffer_builders[column].Append(
          static_cast<IndexCType>(counts[column + 1])));
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
    DCHECK_OK((ValidateSparseTensorCSFCreation<ValueType, IndexType>(*sparse_index, *data,
                                                                     tensor_)));
    return Status::OK();
  }

  std::shared_ptr<SparseCSFIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  const Tensor& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
  MemoryPool* pool_;
};

class TensorBuilderFromSparseCSFTensor : private SparseTensorConverterMixin {
  using SparseTensorConverterMixin::GetIndexValue;

  MemoryPool* pool_;
  const SparseCSFTensor* sparse_tensor_;
  const SparseCSFIndex* sparse_index_;
  const std::vector<std::shared_ptr<Tensor>>& indptr_;
  const std::vector<std::shared_ptr<Tensor>>& indices_;
  const std::vector<int64_t>& axis_order_;
  const std::vector<int64_t>& shape_;
  const int64_t non_zero_length_;
  const int ndim_;
  const int64_t tensor_size_;
  const FixedWidthType& value_type_;
  const int value_elsize_;
  const uint8_t* raw_data_;
  std::vector<int64_t> strides_;
  std::shared_ptr<Buffer> values_buffer_;
  uint8_t* values_;

 public:
  TensorBuilderFromSparseCSFTensor(const SparseCSFTensor* sparse_tensor, MemoryPool* pool)
      : pool_(pool),
        sparse_tensor_(sparse_tensor),
        sparse_index_(
            checked_cast<const SparseCSFIndex*>(sparse_tensor->sparse_index().get())),
        indptr_(sparse_index_->indptr()),
        indices_(sparse_index_->indices()),
        axis_order_(sparse_index_->axis_order()),
        shape_(sparse_tensor->shape()),
        non_zero_length_(sparse_tensor->non_zero_length()),
        ndim_(sparse_tensor->ndim()),
        tensor_size_(sparse_tensor->size()),
        value_type_(checked_cast<const FixedWidthType&>(*sparse_tensor->type())),
        value_elsize_(value_type_.byte_width()),
        raw_data_(sparse_tensor->raw_data()) {}

  int ElementSize(const std::shared_ptr<Tensor>& tensor) const {
    return tensor->type()->byte_width();
  }

  Result<std::shared_ptr<Tensor>> Build() {
    RETURN_NOT_OK(internal::ComputeRowMajorStrides(value_type_, shape_, &strides_));

    ARROW_ASSIGN_OR_RAISE(values_buffer_,
                          AllocateBuffer(value_elsize_ * tensor_size_, pool_));
    values_ = values_buffer_->mutable_data();
    std::fill_n(values_, value_elsize_ * tensor_size_, 0);

    const int64_t start = 0;
    const int64_t stop = indptr_[0]->size() - 1;
    ExpandValues(0, 0, start, stop);

    return std::make_shared<Tensor>(sparse_tensor_->type(), std::move(values_buffer_),
                                    shape_, strides_, sparse_tensor_->dim_names());
  }

  void ExpandValues(const int64_t dim, const int64_t dim_offset, const int64_t start,
                    const int64_t stop) {
    const auto& cur_indices = indices_[dim];
    const int indices_elsize = ElementSize(cur_indices);
    const auto* indices_data = cur_indices->raw_data() + start * indices_elsize;

    if (dim == ndim_ - 1) {
      for (auto i = start; i < stop; ++i) {
        const int64_t index =
            SparseTensorConverterMixin::GetIndexValue(indices_data, indices_elsize);
        const int64_t offset = dim_offset + index * strides_[axis_order_[dim]];

        std::copy_n(raw_data_ + i * value_elsize_, value_elsize_, values_ + offset);

        indices_data += indices_elsize;
      }
    } else {
      const auto& cur_indptr = indptr_[dim];
      const int indptr_elsize = ElementSize(cur_indptr);
      const auto* indptr_data = cur_indptr->raw_data() + start * indptr_elsize;

      for (int64_t i = start; i < stop; ++i) {
        const int64_t index =
            SparseTensorConverterMixin::GetIndexValue(indices_data, indices_elsize);
        const int64_t offset = dim_offset + index * strides_[axis_order_[dim]];
        const int64_t next_start = GetIndexValue(indptr_data, indptr_elsize);
        const int64_t next_stop =
            GetIndexValue(indptr_data + indptr_elsize, indptr_elsize);

        ExpandValues(dim + 1, offset, next_start, next_stop);

        indices_data += indices_elsize;
        indptr_data += indptr_elsize;
      }
    }
  }
};

}  // namespace

Status MakeSparseCSFTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  SparseCSFTensorConverter converter(tensor, index_value_type, pool);
  ConverterVisitor visitor{converter};
  ARROW_RETURN_NOT_OK(VisitValueAndIndexType(*tensor.type(), *index_value_type, visitor));
  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCSFTensor(
    MemoryPool* pool, const SparseCSFTensor* sparse_tensor) {
  TensorBuilderFromSparseCSFTensor builder(sparse_tensor, pool);
  return builder.Build();
}

}  // namespace internal
}  // namespace arrow
