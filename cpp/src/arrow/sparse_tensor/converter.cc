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

#include "arrow/sparse_tensor.h"

#include "arrow/buffer_builder.h"
#include "arrow/util/sort.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace internal {
namespace {

// ----------------------------------------------------------------------
// IncrementIndex for SparseCOOIndex and SparseCSFIndex

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
// SparseTensorConverter

template <typename TYPE, typename SparseIndexType>
class SparseTensorConverter {
 public:
  SparseTensorConverter(const NumericTensor<TYPE>&, const std::shared_ptr<DataType>&,
                        MemoryPool*) {}

  Status Convert() { return Status::Invalid("Unsupported sparse index"); }
};

template <typename TYPE>
struct SparseTensorConverterBase {
  using NumericTensorType = NumericTensor<TYPE>;
  using value_type = typename NumericTensorType::value_type;

  SparseTensorConverterBase(const NumericTensorType& tensor,
                            const std::shared_ptr<DataType>& index_value_type,
                            MemoryPool* pool)
      : tensor_(tensor), index_value_type_(index_value_type), pool_(pool) {}

  const NumericTensorType& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
  MemoryPool* pool_;
};

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCOOIndex

template <typename TYPE>
class SparseTensorConverter<TYPE, SparseCOOIndex>
    : private SparseTensorConverterBase<TYPE> {
 public:
  using BaseClass = SparseTensorConverterBase<TYPE>;
  using typename BaseClass::NumericTensorType;
  using typename BaseClass::value_type;

  SparseTensorConverter(const NumericTensorType& tensor,
                        const std::shared_ptr<DataType>& index_value_type,
                        MemoryPool* pool)
      : BaseClass(tensor, index_value_type, pool) {}

  template <typename IndexValueType>
  Status Convert() {
    using c_index_value_type = typename IndexValueType::c_type;
    const int64_t indices_elsize = sizeof(c_index_value_type);

    const int64_t ndim = tensor_.ndim();
    int64_t nonzero_count = -1;
    RETURN_NOT_OK(tensor_.CountNonZero(&nonzero_count));

    std::shared_ptr<Buffer> indices_buffer;
    RETURN_NOT_OK(
        AllocateBuffer(pool_, indices_elsize * ndim * nonzero_count, &indices_buffer));
    c_index_value_type* indices =
        reinterpret_cast<c_index_value_type*>(indices_buffer->mutable_data());

    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(
        AllocateBuffer(pool_, sizeof(value_type) * nonzero_count, &values_buffer));
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
        index_value_type_, indices_buffer, indices_shape, indices_strides));
    data = values_buffer;

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
  using BaseClass::index_value_type_;
  using BaseClass::pool_;
  using BaseClass::tensor_;
};

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCSRIndex

template <typename TYPE>
class SparseTensorConverter<TYPE, SparseCSRIndex>
    : private SparseTensorConverterBase<TYPE> {
 public:
  using BaseClass = SparseTensorConverterBase<TYPE>;
  using NumericTensorType = typename BaseClass::NumericTensorType;
  using value_type = typename BaseClass::value_type;

  SparseTensorConverter(const NumericTensorType& tensor,
                        const std::shared_ptr<DataType>& index_value_type,
                        MemoryPool* pool)
      : BaseClass(tensor, index_value_type, pool) {}

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

    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(
        AllocateBuffer(pool_, sizeof(value_type) * nonzero_count, &values_buffer));
    value_type* values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

    if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    } else {
      RETURN_NOT_OK(AllocateBuffer(pool_, indices_elsize * (nr + 1), &indptr_buffer));
      auto* indptr = reinterpret_cast<c_index_value_type*>(indptr_buffer->mutable_data());

      RETURN_NOT_OK(
          AllocateBuffer(pool_, indices_elsize * nonzero_count, &indices_buffer));
      auto* indices =
          reinterpret_cast<c_index_value_type*>(indices_buffer->mutable_data());

      c_index_value_type k = 0;
      *indptr++ = 0;
      for (int64_t i = 0; i < nr; ++i) {
        for (int64_t j = 0; j < nc; ++j) {
          const value_type x = tensor_.Value({i, j});
          if (x != 0) {
            *values++ = x;
            *indices++ = static_cast<c_index_value_type>(j);
            k++;
          }
        }
        *indptr++ = k;
      }
    }

    std::vector<int64_t> indptr_shape({nr + 1});
    std::shared_ptr<Tensor> indptr_tensor =
        std::make_shared<Tensor>(index_value_type_, indptr_buffer, indptr_shape);

    std::vector<int64_t> indices_shape({nonzero_count});
    std::shared_ptr<Tensor> indices_tensor =
        std::make_shared<Tensor>(index_value_type_, indices_buffer, indices_shape);

    sparse_index = std::make_shared<SparseCSRIndex>(indptr_tensor, indices_tensor);
    data = values_buffer;

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

  std::shared_ptr<SparseCSRIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  using BaseClass::index_value_type_;
  using BaseClass::pool_;
  using BaseClass::tensor_;

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

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCSCIndex

template <typename TYPE>
class SparseTensorConverter<TYPE, SparseCSCIndex>
    : private SparseTensorConverterBase<TYPE> {
 public:
  using BaseClass = SparseTensorConverterBase<TYPE>;
  using NumericTensorType = typename BaseClass::NumericTensorType;
  using value_type = typename BaseClass::value_type;

  SparseTensorConverter(const NumericTensorType& tensor,
                        const std::shared_ptr<DataType>& index_value_type,
                        MemoryPool* pool)
      : BaseClass(tensor, index_value_type, pool) {}

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

    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(
        AllocateBuffer(pool_, sizeof(value_type) * nonzero_count, &values_buffer));
    value_type* values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

    if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    } else {
      RETURN_NOT_OK(AllocateBuffer(pool_, indices_elsize * (nc + 1), &indptr_buffer));
      auto* indptr = reinterpret_cast<c_index_value_type*>(indptr_buffer->mutable_data());

      RETURN_NOT_OK(
          AllocateBuffer(pool_, indices_elsize * nonzero_count, &indices_buffer));
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
    data = values_buffer;

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
  using BaseClass::index_value_type_;
  using BaseClass::pool_;
  using BaseClass::tensor_;

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

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCSFIndex

template <typename TYPE>
class SparseTensorConverter<TYPE, SparseCSFIndex>
    : private SparseTensorConverterBase<TYPE> {
 public:
  using BaseClass = SparseTensorConverterBase<TYPE>;
  using typename BaseClass::NumericTensorType;
  using typename BaseClass::value_type;

  SparseTensorConverter(const NumericTensorType& tensor,
                        const std::shared_ptr<DataType>& index_value_type,
                        MemoryPool* pool)
      : BaseClass(tensor, index_value_type, pool) {}

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

    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(
        AllocateBuffer(pool_, sizeof(value_type) * nonzero_count, &values_buffer));
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
    data = values_buffer;

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
  using BaseClass::index_value_type_;
  using BaseClass::pool_;
  using BaseClass::tensor_;

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

// ----------------------------------------------------------------------
// Instantiate templates

#define INSTANTIATE_SPARSE_TENSOR_CONVERTER(IndexType)            \
  template class SparseTensorConverter<UInt8Type, IndexType>;     \
  template class SparseTensorConverter<UInt16Type, IndexType>;    \
  template class SparseTensorConverter<UInt32Type, IndexType>;    \
  template class SparseTensorConverter<UInt64Type, IndexType>;    \
  template class SparseTensorConverter<Int8Type, IndexType>;      \
  template class SparseTensorConverter<Int16Type, IndexType>;     \
  template class SparseTensorConverter<Int32Type, IndexType>;     \
  template class SparseTensorConverter<Int64Type, IndexType>;     \
  template class SparseTensorConverter<HalfFloatType, IndexType>; \
  template class SparseTensorConverter<FloatType, IndexType>;     \
  template class SparseTensorConverter<DoubleType, IndexType>

INSTANTIATE_SPARSE_TENSOR_CONVERTER(SparseCOOIndex);
INSTANTIATE_SPARSE_TENSOR_CONVERTER(SparseCSRIndex);
INSTANTIATE_SPARSE_TENSOR_CONVERTER(SparseCSCIndex);
INSTANTIATE_SPARSE_TENSOR_CONVERTER(SparseCSFIndex);

// ----------------------------------------------------------------------

template <typename TYPE, typename SparseIndexType>
Status MakeSparseTensorFromTensor(const Tensor& tensor,
                                  const std::shared_ptr<DataType>& index_value_type,
                                  MemoryPool* pool,
                                  std::shared_ptr<SparseIndex>* out_sparse_index,
                                  std::shared_ptr<Buffer>* out_data) {
  NumericTensor<TYPE> numeric_tensor(tensor.data(), tensor.shape(), tensor.strides());
  SparseTensorConverter<TYPE, SparseIndexType> converter(numeric_tensor, index_value_type,
                                                         pool);
  RETURN_NOT_OK(converter.Convert());

  *out_sparse_index = checked_pointer_cast<SparseIndex>(converter.sparse_index);
  *out_data = converter.data;
  return Status::OK();
}

#define MAKE_SPARSE_TENSOR_FROM_TENSOR(TYPE_CLASS)                        \
  case TYPE_CLASS##Type::type_id:                                         \
    return MakeSparseTensorFromTensor<TYPE_CLASS##Type, SparseIndexType>( \
        tensor, index_value_type, pool, out_sparse_index, out_data);

template <typename SparseIndexType>
inline Status MakeSparseTensorFromTensor(
    const Tensor& tensor, const std::shared_ptr<DataType>& index_value_type,
    MemoryPool* pool, std::shared_ptr<SparseIndex>* out_sparse_index,
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

}  // namespace

Status MakeSparseTensorFromTensor(const Tensor& tensor,
                                  SparseTensorFormat::type sparse_format_id,
                                  const std::shared_ptr<DataType>& index_value_type,
                                  MemoryPool* pool,
                                  std::shared_ptr<SparseIndex>* out_sparse_index,
                                  std::shared_ptr<Buffer>* out_data) {
  switch (sparse_format_id) {
    case SparseTensorFormat::COO:
      return MakeSparseTensorFromTensor<SparseCOOIndex>(tensor, index_value_type, pool,
                                                        out_sparse_index, out_data);
    case SparseTensorFormat::CSR:
      return MakeSparseTensorFromTensor<SparseCSRIndex>(tensor, index_value_type, pool,
                                                        out_sparse_index, out_data);
    case SparseTensorFormat::CSC:
      return MakeSparseTensorFromTensor<SparseCSCIndex>(tensor, index_value_type, pool,
                                                        out_sparse_index, out_data);
    case SparseTensorFormat::CSF:
      return MakeSparseTensorFromTensor<SparseCSFIndex>(tensor, index_value_type, pool,
                                                        out_sparse_index, out_data);

    // LCOV_EXCL_START: ignore program failure
    default:
      return Status::Invalid("Invalid sparse tensor format");
      // LCOV_EXCL_STOP
  }
}

}  // namespace internal
}  // namespace arrow
