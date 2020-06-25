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

class SparseCSFTensorConverter : private SparseTensorConverterMixin {
  using SparseTensorConverterMixin::AssignIndex;
  using SparseTensorConverterMixin::CheckSparseIndexMaximumValue;
  using SparseTensorConverterMixin::IsNonZero;

 public:
  SparseCSFTensorConverter(const Tensor& tensor,
                           const std::shared_ptr<DataType>& index_value_type,
                           MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  Status Convert() {
    RETURN_NOT_OK(CheckSparseIndexMaximumValue(index_value_type_, tensor_.shape()));

    const int index_elsize =
        checked_cast<const IntegerType&>(*index_value_type_).bit_width() / CHAR_BIT;
    const int value_elsize =
        checked_cast<const FixedWidthType&>(*tensor_.type()).bit_width() / CHAR_BIT;

    const int64_t ndim = tensor_.ndim();
    // Axis order as ascending order of dimension size is a good heuristic but is not
    // necessarily optimal.
    std::vector<int64_t> axis_order = internal::ArgSort(tensor_.shape());
    ARROW_ASSIGN_OR_RAISE(int64_t nonzero_count, tensor_.CountNonZero());

    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(value_elsize * nonzero_count, pool_));
    auto* values = values_buffer->mutable_data();

    std::vector<int64_t> counts(ndim, 0);
    std::vector<int64_t> coord(ndim, 0);
    std::vector<int64_t> previous_coord(ndim, -1);
    std::vector<BufferBuilder> indptr_buffer_builders(ndim - 1);
    std::vector<BufferBuilder> indices_buffer_builders(ndim);

    const auto* tensor_data = tensor_.raw_data();
    uint8_t index_buffer[sizeof(int64_t)];

    if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    } else {
      const auto& shape = tensor_.shape();
      for (int64_t n = tensor_.size(); n > 0; n--) {
        const auto offset = tensor_.CalculateValueOffset(coord);
        const auto xp = tensor_data + offset;

        if (std::any_of(xp, xp + value_elsize, IsNonZero)) {
          bool tree_split = false;

          std::copy_n(xp, value_elsize, values);
          values += value_elsize;

          for (int64_t i = 0; i < ndim; ++i) {
            int64_t dimension = axis_order[i];

            tree_split = tree_split || (coord[dimension] != previous_coord[dimension]);
            if (tree_split) {
              if (i < ndim - 1) {
                AssignIndex(index_buffer, counts[i + 1], index_elsize);
                RETURN_NOT_OK(
                    indptr_buffer_builders[i].Append(index_buffer, index_elsize));
              }

              AssignIndex(index_buffer, coord[dimension], index_elsize);
              RETURN_NOT_OK(
                  indices_buffer_builders[i].Append(index_buffer, index_elsize));

              ++counts[i];
            }
          }

          previous_coord = coord;
        }

        IncrementIndex(coord, shape, axis_order);
      }
    }

    for (int64_t column = 0; column < ndim - 1; ++column) {
      AssignIndex(index_buffer, counts[column + 1], index_elsize);
      RETURN_NOT_OK(indptr_buffer_builders[column].Append(index_buffer, index_elsize));
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

  std::shared_ptr<SparseCSFIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  const Tensor& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
  MemoryPool* pool_;
};

}  // namespace

Status MakeSparseCSFTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data) {
  SparseCSFTensorConverter converter(tensor, index_value_type, pool);
  RETURN_NOT_OK(converter.Convert());

  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

}  // namespace internal
}  // namespace arrow
