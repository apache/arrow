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

#include <functional>
#include <limits>
#include <memory>
#include <numeric>

#include "arrow/compare.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

namespace {

// ----------------------------------------------------------------------
// SparseTensorConverter

template <typename TYPE, typename SparseIndexType>
class SparseTensorConverter {
 public:
  explicit SparseTensorConverter(const NumericTensor<TYPE>&,
                                 const std::shared_ptr<DataType>&) {}

  Status Convert() { return Status::Invalid("Unsupported sparse index"); }
};

// ----------------------------------------------------------------------
// SparseTensorConverter for SparseCOOIndex

template <typename TYPE>
struct SparseTensorConverterBase {
  using NumericTensorType = NumericTensor<TYPE>;
  using value_type = typename NumericTensorType::value_type;

  explicit SparseTensorConverterBase(const NumericTensorType& tensor,
                                     const std::shared_ptr<DataType>& index_value_type)
      : tensor_(tensor), index_value_type_(index_value_type) {}

  const NumericTensorType& tensor_;
  const std::shared_ptr<DataType>& index_value_type_;
};

template <typename TYPE>
class SparseTensorConverter<TYPE, SparseCOOIndex>
    : private SparseTensorConverterBase<TYPE> {
 public:
  using BaseClass = SparseTensorConverterBase<TYPE>;
  using typename BaseClass::NumericTensorType;
  using typename BaseClass::value_type;

  explicit SparseTensorConverter(const NumericTensorType& tensor,
                                 const std::shared_ptr<DataType>& index_value_type)
      : BaseClass(tensor, index_value_type) {}

  template <typename IndexValueType>
  Status Convert() {
    using c_index_value_type = typename IndexValueType::c_type;
    const int64_t indices_elsize = sizeof(c_index_value_type);

    const int64_t ndim = tensor_.ndim();
    int64_t nonzero_count = -1;
    RETURN_NOT_OK(tensor_.CountNonZero(&nonzero_count));

    std::shared_ptr<Buffer> indices_buffer;
    RETURN_NOT_OK(AllocateBuffer(indices_elsize * ndim * nonzero_count, &indices_buffer));
    c_index_value_type* indices =
        reinterpret_cast<c_index_value_type*>(indices_buffer->mutable_data());

    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(AllocateBuffer(sizeof(value_type) * nonzero_count, &values_buffer));
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
      std::vector<int64_t> coord(ndim, 0);

      for (int64_t n = tensor_.size(); n > 0; n--) {
        const value_type x = tensor_.Value(coord);
        if (tensor_.Value(coord) != 0) {
          *values++ = x;

          c_index_value_type* indp = indices;
          for (int64_t i = 0; i < ndim; ++i) {
            *indp = static_cast<c_index_value_type>(coord[i]);
            indp += nonzero_count;
          }
          indices++;
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
    const std::vector<int64_t> indices_strides = {indices_elsize,
                                                  indices_elsize * nonzero_count};
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
        return Status::Invalid("Unsupported SparseTensor index value type");
        // LCOV_EXCL_STOP
    }
  }

#undef CALL_TYPE_SPECIFIC_CONVERT

  std::shared_ptr<SparseCOOIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  using BaseClass::index_value_type_;
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

  explicit SparseTensorConverter(const NumericTensorType& tensor,
                                 const std::shared_ptr<DataType>& index_value_type)
      : BaseClass(tensor, index_value_type) {}

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
    RETURN_NOT_OK(AllocateBuffer(sizeof(value_type) * nonzero_count, &values_buffer));
    value_type* values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

    if (ndim <= 1) {
      return Status::NotImplemented("TODO for ndim <= 1");
    } else {
      RETURN_NOT_OK(AllocateBuffer(indices_elsize * (nr + 1), &indptr_buffer));
      auto* indptr = reinterpret_cast<c_index_value_type*>(indptr_buffer->mutable_data());

      RETURN_NOT_OK(AllocateBuffer(indices_elsize * nonzero_count, &indices_buffer));
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
        return Status::Invalid("Unsupported SparseTensor index value type");
        // LCOV_EXCL_STOP
    }
  }

#undef CALL_TYPE_SPECIFIC_CONVERT

  std::shared_ptr<SparseCSRIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 private:
  using BaseClass::index_value_type_;
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

}  // namespace

namespace internal {

namespace {

template <typename TYPE, typename SparseIndexType>
void MakeSparseTensorFromTensor(const Tensor& tensor,
                                const std::shared_ptr<DataType>& index_value_type,
                                std::shared_ptr<SparseIndex>* sparse_index,
                                std::shared_ptr<Buffer>* data) {
  NumericTensor<TYPE> numeric_tensor(tensor.data(), tensor.shape(), tensor.strides());
  SparseTensorConverter<TYPE, SparseIndexType> converter(numeric_tensor,
                                                         index_value_type);
  ARROW_CHECK_OK(converter.Convert());
  *sparse_index = converter.sparse_index;
  *data = converter.data;
}

#define MAKE_SPARSE_TENSOR_FROM_TENSOR(TYPE_CLASS)                 \
  case TYPE_CLASS##Type::type_id:                                  \
    MakeSparseTensorFromTensor<TYPE_CLASS##Type, SparseIndexType>( \
        tensor, index_value_type, sparse_index, data);             \
    break;

template <typename SparseIndexType>
inline void MakeSparseTensorFromTensor(const Tensor& tensor,
                                       const std::shared_ptr<DataType>& index_value_type,
                                       std::shared_ptr<SparseIndex>* sparse_index,
                                       std::shared_ptr<Buffer>* data) {
  switch (tensor.type()->id()) {
    ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(MAKE_SPARSE_TENSOR_FROM_TENSOR);
    // LCOV_EXCL_START: ignore program failure
    default:
      ARROW_LOG(FATAL) << "Unsupported Tensor value type";
      break;
      // LCOV_EXCL_STOP
  }
}

#undef MAKE_SPARSE_TENSOR_FROM_TENSOR

}  // namespace

void MakeSparseTensorFromTensor(const Tensor& tensor,
                                SparseTensorFormat::type sparse_format_id,
                                const std::shared_ptr<DataType>& index_value_type,
                                std::shared_ptr<SparseIndex>* sparse_index,
                                std::shared_ptr<Buffer>* data) {
  switch (sparse_format_id) {
    case SparseTensorFormat::COO:
      MakeSparseTensorFromTensor<SparseCOOIndex>(tensor, index_value_type, sparse_index,
                                                 data);
      break;
    case SparseTensorFormat::CSR:
      MakeSparseTensorFromTensor<SparseCSRIndex>(tensor, index_value_type, sparse_index,
                                                 data);
      break;
    // LCOV_EXCL_START: ignore program failure
    default:
      ARROW_LOG(FATAL) << "Invalid sparse tensor format ID";
      break;
      // LCOV_EXCL_STOP
  }
}

}  // namespace internal

// ----------------------------------------------------------------------
// SparseCOOIndex

// Constructor with a contiguous NumericTensor
SparseCOOIndex::SparseCOOIndex(const std::shared_ptr<Tensor>& coords)
    : SparseIndexBase(coords->shape()[0]), coords_(coords) {
  ARROW_CHECK(is_integer(coords_->type_id()));
  ARROW_CHECK(coords_->is_contiguous());
  ARROW_CHECK_EQ(2, coords_->ndim());
}

std::string SparseCOOIndex::ToString() const { return std::string("SparseCOOIndex"); }

// ----------------------------------------------------------------------
// SparseCSRIndex

// Constructor with two index vectors
SparseCSRIndex::SparseCSRIndex(const std::shared_ptr<Tensor>& indptr,
                               const std::shared_ptr<Tensor>& indices)
    : SparseIndexBase(indices->shape()[0]), indptr_(indptr), indices_(indices) {
  ARROW_CHECK(is_integer(indptr_->type_id()));
  ARROW_CHECK_EQ(1, indptr_->ndim());
  ARROW_CHECK(is_integer(indices_->type_id()));
  ARROW_CHECK_EQ(1, indices_->ndim());
}

std::string SparseCSRIndex::ToString() const { return std::string("SparseCSRIndex"); }

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

}  // namespace arrow
