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

#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>

#include "arrow/compare.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

// ----------------------------------------------------------------------
// SparseIndex

Status SparseIndex::ValidateShape(const std::vector<int64_t>& shape) const {
  if (!std::all_of(shape.begin(), shape.end(), [](int64_t x) { return x >= 0; })) {
    return Status::Invalid("Shape elements must be positive");
  }

  return Status::OK();
}

namespace {

// ----------------------------------------------------------------------
// SparseTensorConverter

template <typename TYPE, typename SparseIndexType>
class SparseTensorConverter {
 public:
  SparseTensorConverter(const NumericTensor<TYPE>&, const std::shared_ptr<DataType>&,
                        MemoryPool*) {}

  Status Convert() { return Status::Invalid("Unsupported sparse index"); }
};

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCOOIndex

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

        // increment index
        ++coord[ndim - 1];
        if (n > 1 && coord[ndim - 1] == shape[ndim - 1]) {
          int64_t d = ndim - 1;
          while (d > 0 && coord[d] == shape[d]) {
            coord[d] = 0;
            ++coord[d - 1];
            --d;
          }
        }
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

}  // namespace

namespace internal {

namespace {

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
    // LCOV_EXCL_START: ignore program failure
    default:
      return Status::Invalid("Invalid sparse tensor format");
      // LCOV_EXCL_STOP
  }
}

template <typename TYPE, typename IndexValueType>
Status MakeTensorFromSparseTensor(MemoryPool* pool, const SparseTensor* sparse_tensor,
                                  std::shared_ptr<Tensor>* out) {
  using c_index_value_type = typename IndexValueType::c_type;
  using NumericTensorType = NumericTensor<TYPE>;
  using value_type = typename NumericTensorType::value_type;

  std::shared_ptr<Buffer> values_buffer;
  RETURN_NOT_OK(
      AllocateBuffer(pool, sizeof(value_type) * sparse_tensor->size(), &values_buffer));
  auto values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

  std::fill_n(values, sparse_tensor->size(), static_cast<value_type>(0));

  switch (sparse_tensor->format_id()) {
    case SparseTensorFormat::COO: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCOOIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> coords = sparse_index.indices();
      const auto raw_data =
          reinterpret_cast<const value_type*>(sparse_tensor->raw_data());
      std::vector<int64_t> strides(sparse_tensor->ndim(), 1);

      for (int i = sparse_tensor->ndim() - 1; i > 0; --i) {
        strides[i - 1] *= strides[i] * sparse_tensor->shape()[i];
      }
      for (int64_t i = 0; i < sparse_tensor->non_zero_length(); ++i) {
        std::vector<c_index_value_type> coord(sparse_tensor->ndim());
        int64_t offset = 0;
        for (int64_t j = 0; j < static_cast<int>(coord.size()); ++j) {
          coord[j] = coords->Value<IndexValueType>({i, j});
          offset += coord[j] * strides[j];
        }
        values[offset] = raw_data[i];
      }
      *out = std::make_shared<Tensor>(sparse_tensor->type(), values_buffer,
                                      sparse_tensor->shape());
      return Status::OK();
    }

    case SparseTensorFormat::CSR: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSRIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indptr = sparse_index.indptr();
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();
      const auto raw_data =
          reinterpret_cast<const value_type*>(sparse_tensor->raw_data());

      int64_t offset;
      for (int64_t i = 0; i < indptr->size() - 1; ++i) {
        const int64_t start = indptr->Value<IndexValueType>({i});
        const int64_t stop = indptr->Value<IndexValueType>({i + 1});
        for (int64_t j = start; j < stop; ++j) {
          offset = indices->Value<IndexValueType>({j}) + i * sparse_tensor->shape()[1];
          values[offset] = raw_data[j];
        }
      }
      *out = std::make_shared<Tensor>(sparse_tensor->type(), values_buffer,
                                      sparse_tensor->shape());
      return Status::OK();
    }

    case SparseTensorFormat::CSC: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSCIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indptr = sparse_index.indptr();
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();
      const auto raw_data =
          reinterpret_cast<const value_type*>(sparse_tensor->raw_data());

      int64_t offset;
      for (int64_t j = 0; j < indptr->size() - 1; ++j) {
        const int64_t start = indptr->Value<IndexValueType>({j});
        const int64_t stop = indptr->Value<IndexValueType>({j + 1});
        for (int64_t i = start; i < stop; ++i) {
          offset = j + indices->Value<IndexValueType>({i}) * sparse_tensor->shape()[1];
          values[offset] = raw_data[i];
        }
      }
      *out = std::make_shared<Tensor>(sparse_tensor->type(), values_buffer,
                                      sparse_tensor->shape());
      return Status::OK();
    }
  }
  return Status::NotImplemented("Unsupported SparseIndex format type");
}

#define MAKE_TENSOR_FROM_SPARSE_TENSOR_INDEX_TYPE(IndexValueType)                      \
  case IndexValueType##Type::type_id:                                                  \
    return MakeTensorFromSparseTensor<TYPE, IndexValueType##Type>(pool, sparse_tensor, \
                                                                  out);                \
    break;

template <typename TYPE>
Status MakeTensorFromSparseTensor(MemoryPool* pool, const SparseTensor* sparse_tensor,
                                  std::shared_ptr<Tensor>* out) {
  std::shared_ptr<DataType> type;
  switch (sparse_tensor->format_id()) {
    case SparseTensorFormat::COO: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCOOIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();
      type = indices->type();
      break;
    }
    case SparseTensorFormat::CSR: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSRIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();
      type = indices->type();
      break;
    }
    case SparseTensorFormat::CSC: {
      const auto& sparse_index =
          internal::checked_cast<const SparseCSCIndex&>(*sparse_tensor->sparse_index());
      const std::shared_ptr<const Tensor> indices = sparse_index.indices();
      type = indices->type();
      break;
    }
      // LCOV_EXCL_START: ignore program failure
    default:
      ARROW_LOG(FATAL) << "Unsupported SparseIndex format";
      break;
      // LCOV_EXCL_STOP
  }

  switch (type->id()) {
    ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(MAKE_TENSOR_FROM_SPARSE_TENSOR_INDEX_TYPE);
      // LCOV_EXCL_START: ignore program failure
    default:
      ARROW_LOG(FATAL) << "Unsupported SparseIndex value type";
      return Status::NotImplemented("Unsupported SparseIndex value type");
      // LCOV_EXCL_STOP
  }
}
#undef MAKE_TENSOR_FROM_SPARSE_TENSOR_INDEX_TYPE

#define MAKE_TENSOR_FROM_SPARSE_TENSOR_VALUE_TYPE(TYPE) \
  case TYPE##Type::type_id:                             \
    return MakeTensorFromSparseTensor<TYPE##Type>(pool, sparse_tensor, out);

Status MakeTensorFromSparseTensor(MemoryPool* pool, const SparseTensor* sparse_tensor,
                                  std::shared_ptr<Tensor>* out) {
  switch (sparse_tensor->type()->id()) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(MAKE_TENSOR_FROM_SPARSE_TENSOR_VALUE_TYPE);
    // LCOV_EXCL_START: ignore program failure
    default:
      ARROW_LOG(FATAL) << "Unsupported SparseTensor value type";
      return Status::NotImplemented("Unsupported SparseTensor data value type");
      // LCOV_EXCL_STOP
  }
}
#undef MAKE_TENSOR_FROM_SPARSE_TENSOR_VALUE_TYPE

}  // namespace internal

// ----------------------------------------------------------------------
// SparseCOOIndex

namespace {

inline Status CheckSparseCOOIndexValidity(const std::shared_ptr<DataType>& type,
                                          const std::vector<int64_t>& shape,
                                          const std::vector<int64_t>& strides) {
  if (!is_integer(type->id())) {
    return Status::TypeError("Type of SparseCOOIndex indices must be integer");
  }
  if (shape.size() != 2) {
    return Status::Invalid("SparseCOOIndex indices must be a matrix");
  }
  if (!internal::IsTensorStridesContiguous(type, shape, strides)) {
    return Status::Invalid("SparseCOOIndex indices must be contiguous");
  }
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<DataType>& indices_type,
    const std::vector<int64_t>& indices_shape,
    const std::vector<int64_t>& indices_strides, std::shared_ptr<Buffer> indices_data) {
  RETURN_NOT_OK(
      CheckSparseCOOIndexValidity(indices_type, indices_shape, indices_strides));
  return std::make_shared<SparseCOOIndex>(std::make_shared<Tensor>(
      indices_type, indices_data, indices_shape, indices_strides));
}

Result<std::shared_ptr<SparseCOOIndex>> SparseCOOIndex::Make(
    const std::shared_ptr<DataType>& indices_type, const std::vector<int64_t>& shape,
    int64_t non_zero_length, std::shared_ptr<Buffer> indices_data) {
  auto ndim = static_cast<int64_t>(shape.size());
  if (!is_integer(indices_type->id())) {
    return Status::TypeError("Type of SparseCOOIndex indices must be integer");
  }
  const int64_t elsize =
      internal::checked_cast<const IntegerType&>(*indices_type).bit_width() / 8;
  std::vector<int64_t> indices_shape({non_zero_length, ndim});
  std::vector<int64_t> indices_strides({elsize * ndim, elsize});
  return Make(indices_type, indices_shape, indices_strides, indices_data);
}

// Constructor with a contiguous NumericTensor
SparseCOOIndex::SparseCOOIndex(const std::shared_ptr<Tensor>& coords)
    : SparseIndexBase(coords->shape()[0]), coords_(coords) {
  ARROW_CHECK_OK(
      CheckSparseCOOIndexValidity(coords_->type(), coords_->shape(), coords_->strides()));
}

std::string SparseCOOIndex::ToString() const { return std::string("SparseCOOIndex"); }

// ----------------------------------------------------------------------
// SparseCSXIndex

namespace internal {

Status ValidateSparseCSXIndex(const std::shared_ptr<DataType>& indptr_type,
                              const std::shared_ptr<DataType>& indices_type,
                              const std::vector<int64_t>& indptr_shape,
                              const std::vector<int64_t>& indices_shape,
                              char const* type_name) {
  if (!is_integer(indptr_type->id())) {
    return Status::TypeError("Type of ", type_name, " indptr must be integer");
  }
  if (indptr_shape.size() != 1) {
    return Status::Invalid(type_name, " indptr must be a vector");
  }
  if (!is_integer(indices_type->id())) {
    return Status::Invalid("Type of ", type_name, " indices must be integer");
  }
  if (indices_shape.size() != 1) {
    return Status::Invalid(type_name, " indices must be a vector");
  }
  return Status::OK();
}

void CheckSparseCSXIndexValidity(const std::shared_ptr<DataType>& indptr_type,
                                 const std::shared_ptr<DataType>& indices_type,
                                 const std::vector<int64_t>& indptr_shape,
                                 const std::vector<int64_t>& indices_shape,
                                 char const* type_name) {
  ARROW_CHECK_OK(ValidateSparseCSXIndex(indptr_type, indices_type, indptr_shape,
                                        indices_shape, type_name));
}

}  // namespace internal

// ----------------------------------------------------------------------
// SparseTensor

// Constructor with all attributes
SparseTensor::SparseTensor(const std::shared_ptr<DataType>& type,
                           const std::shared_ptr<Buffer>& data,
                           const std::vector<int64_t>& shape,
                           const std::shared_ptr<SparseIndex>& sparse_index,
                           const std::vector<std::string>& dim_names)
    : type_(type),
      data_(data),
      shape_(shape),
      sparse_index_(sparse_index),
      dim_names_(dim_names) {
  ARROW_CHECK(is_tensor_supported(type->id()));
}

const std::string& SparseTensor::dim_name(int i) const {
  static const std::string kEmpty = "";
  if (dim_names_.size() == 0) {
    return kEmpty;
  } else {
    ARROW_CHECK_LT(i, static_cast<int>(dim_names_.size()));
    return dim_names_[i];
  }
}

int64_t SparseTensor::size() const {
  return std::accumulate(shape_.begin(), shape_.end(), 1LL, std::multiplies<int64_t>());
}

bool SparseTensor::Equals(const SparseTensor& other) const {
  return SparseTensorEquals(*this, other);
}

Status SparseTensor::ToTensor(MemoryPool* pool, std::shared_ptr<Tensor>* out) const {
  return internal::MakeTensorFromSparseTensor(pool, this, out);
}

}  // namespace arrow
