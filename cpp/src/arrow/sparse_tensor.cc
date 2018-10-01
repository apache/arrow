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
#include <memory>
#include <numeric>

#include "arrow/util/logging.h"

namespace arrow {

namespace {

template <typename T>
struct SparseIndexTraits {};

template <>
struct SparseIndexTraits<SparseCOOIndex> {
  static inline const char* name() { return "SparseCOOIndex"; }
};

template <typename TYPE, typename SparseIndexType>
class SparseTensorConverter {
 public:
  explicit SparseTensorConverter(const NumericTensor<TYPE>&) {}

  Status Convert() {
    std::string sparse_index_name(SparseIndexTraits<SparseIndexType>::name());
    return Status::NotImplemented(sparse_index_name +
                                  std::string(" is not supported yet."));
  }
};

template <typename TYPE>
class SparseTensorConverter<TYPE, SparseCOOIndex> {
 public:
  using NumericTensorType = NumericTensor<TYPE>;
  using value_type = typename NumericTensorType::value_type;

  explicit SparseTensorConverter(const NumericTensor<TYPE>& tensor) : tensor_(tensor) {}

  Status Convert() {
    const int64_t ndim = tensor_.ndim();
    const int64_t nonzero_count = static_cast<int64_t>(CountNonZero());

    std::shared_ptr<Buffer> indices_buffer;
    RETURN_NOT_OK(AllocateBuffer(sizeof(int64_t) * ndim * nonzero_count, &indices_buffer));
    int64_t* indices = reinterpret_cast<int64_t*>(indices_buffer->mutable_data());

    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(AllocateBuffer(sizeof(value_type) * nonzero_count, &values_buffer));
    value_type* values = reinterpret_cast<value_type*>(values_buffer->mutable_data());

    if (ndim <= 1) {
      const value_type* data = reinterpret_cast<const value_type*>(tensor_.raw_data());
      const int64_t count = ndim == 0 ? 1 : tensor_.shape()[0];
      for (int64_t i = 0; i < count; ++i, ++data) {
        if (*data != 0) {
          *indices++ = i;
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

          int64_t *indp = indices;
          for (int64_t i = 0; i < ndim; ++i) {
            *indp = coord[i];
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
    const int64_t indices_elsize = sizeof(int64_t);
    const std::vector<int64_t> indices_strides = {indices_elsize, indices_elsize * nonzero_count};
    sparse_index = std::make_shared<SparseCOOIndex>(
        std::make_shared<SparseCOOIndex::CoordsTensor>(indices_buffer,
                                                       indices_shape,
                                                       indices_strides));
    data = values_buffer;

    return Status::OK();
  }

  std::shared_ptr<SparseCOOIndex> sparse_index;
  std::shared_ptr<Buffer> data;

 protected:
  bool TensorIsTriviallyIterable() const {
    return tensor_.ndim() <= 1 || tensor_.is_contiguous();
  }

  size_t CountNonZero() const {
    if (tensor_.size() == 0) {
      return 0;
    }

    if (TensorIsTriviallyIterable()) {
      const value_type* data = reinterpret_cast<const value_type*>(tensor_.raw_data());
      return std::count_if(data, data + tensor_.size(), [](value_type x) { return x != 0; });
    }

    const std::vector<int64_t>& shape = tensor_.shape();
    const int64_t ndim = tensor_.ndim();

    size_t count = 0;
    std::vector<int64_t> coord(ndim, 0);
    for (int64_t n = tensor_.size(); n > 0; n--) {
      if (tensor_.Value(coord) != 0) {
        ++count;
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
    return count;
  }

 private:
  const NumericTensor<TYPE>& tensor_;
};

template <typename TYPE, typename SparseIndexType>
void MakeSparseCOOTensorFromTensor(const Tensor& tensor,
                                   std::shared_ptr<SparseIndexType>* sparse_index,
                                   std::shared_ptr<Buffer>* data) {
  NumericTensor<TYPE> numeric_tensor(tensor.data(), tensor.shape(), tensor.strides());
  SparseTensorConverter<TYPE, SparseIndexType> converter(numeric_tensor);
  DCHECK_OK(converter.Convert());
  *sparse_index = converter.sparse_index;
  *data = converter.data;
}

// ----------------------------------------------------------------------
// Instantiate templates

template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<UInt8Type, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<UInt16Type, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<UInt32Type, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<UInt64Type, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<Int8Type, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<Int16Type, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<Int32Type, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<Int64Type, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<HalfFloatType, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<FloatType, SparseCOOIndex>;
template class ARROW_TEMPLATE_EXPORT SparseTensorConverter<DoubleType, SparseCOOIndex>;

}  // namespace

// Constructor with a column-major NumericTensor
SparseCOOIndex::SparseCOOIndex(const std::shared_ptr<CoordsTensor>& coords)
    : SparseIndex(coords->shape()[0]), coords_(coords) {
  DCHECK(coords_->is_column_major());
}

// Constructor with all attributes
template <typename SparseIndexType>
SparseTensor<SparseIndexType>::SparseTensor(
    const std::shared_ptr<SparseIndexType>& sparse_index,
    const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
    const std::vector<int64_t>& shape, const std::vector<std::string>& dim_names)
    : type_(type),
      data_(data),
      shape_(shape),
      sparse_index_(sparse_index),
      dim_names_(dim_names) {
  DCHECK(is_tensor_supported(type->id()));
}

// Constructor with a dense tensor
template <typename SparseIndexType>
SparseTensor<SparseIndexType>::SparseTensor(const std::shared_ptr<DataType>& type,
                                            const std::vector<int64_t>& shape,
                                            const std::vector<std::string>& dim_names)
    : SparseTensor(nullptr, type, nullptr, shape, dim_names) {}

// Constructor with a dense tensor
template <typename SparseIndexType>
template <typename TYPE>
SparseTensor<SparseIndexType>::SparseTensor(const NumericTensor<TYPE>& tensor)
    : SparseTensor(nullptr, tensor.type(), nullptr, tensor.shape(), tensor.dim_names_) {
  SparseTensorConverter<TYPE, SparseIndexType> converter(tensor);
  DCHECK_OK(converter.Convert());
  sparse_index_ = converter.sparse_index;
  data_ = converter.data;
}

// Constructor with a dense tensor
template <typename SparseIndexType>
SparseTensor<SparseIndexType>::SparseTensor(const Tensor& tensor)
    : SparseTensor(nullptr, tensor.type(), nullptr, tensor.shape(), tensor.dim_names_) {
  switch (tensor.type()->id()) {
    case Type::UINT8:
      MakeSparseCOOTensorFromTensor<UInt8Type, SparseIndexType>(tensor, &sparse_index_,
                                                                &data_);
      return;
    case Type::INT8:
      MakeSparseCOOTensorFromTensor<Int8Type, SparseIndexType>(tensor, &sparse_index_,
                                                               &data_);
      return;
    case Type::UINT16:
      MakeSparseCOOTensorFromTensor<UInt16Type, SparseIndexType>(tensor, &sparse_index_,
                                                                 &data_);
      return;
    case Type::INT16:
      MakeSparseCOOTensorFromTensor<Int16Type, SparseIndexType>(tensor, &sparse_index_,
                                                                &data_);
      return;
    case Type::UINT32:
      MakeSparseCOOTensorFromTensor<UInt32Type, SparseIndexType>(tensor, &sparse_index_,
                                                                 &data_);
      return;
    case Type::INT32:
      MakeSparseCOOTensorFromTensor<Int32Type, SparseIndexType>(tensor, &sparse_index_,
                                                                &data_);
      return;
    case Type::UINT64:
      MakeSparseCOOTensorFromTensor<UInt64Type, SparseIndexType>(tensor, &sparse_index_,
                                                                 &data_);
      return;
    case Type::INT64:
      MakeSparseCOOTensorFromTensor<Int64Type, SparseIndexType>(tensor, &sparse_index_,
                                                                &data_);
      return;
    case Type::HALF_FLOAT:
      MakeSparseCOOTensorFromTensor<HalfFloatType, SparseIndexType>(
          tensor, &sparse_index_, &data_);
      return;
    case Type::FLOAT:
      MakeSparseCOOTensorFromTensor<FloatType, SparseIndexType>(tensor, &sparse_index_,
                                                                &data_);
      return;
    case Type::DOUBLE:
      MakeSparseCOOTensorFromTensor<DoubleType, SparseIndexType>(tensor, &sparse_index_,
                                                                 &data_);
      return;
    default:
      break;
  }
}

template <typename SparseIndexType>
const std::string& SparseTensor<SparseIndexType>::dim_name(int i) const {
  static const std::string kEmpty = "";
  if (dim_names_.size() == 0) {
    return kEmpty;
  } else {
    DCHECK_LT(i, static_cast<int>(dim_names_.size()));
    return dim_names_[i];
  }
}

template <typename SparseIndexType>
int64_t SparseTensor<SparseIndexType>::size() const {
  return std::accumulate(shape_.begin(), shape_.end(), 1LL, std::multiplies<int64_t>());
}

// ----------------------------------------------------------------------
// Instantiate templates

template class ARROW_TEMPLATE_EXPORT SparseTensor<SparseCOOIndex>;

template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<UInt8Type>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<UInt16Type>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<UInt32Type>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<UInt64Type>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<Int8Type>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<Int16Type>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<Int32Type>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<Int64Type>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<HalfFloatType>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<FloatType>&);
template SparseTensor<SparseCOOIndex>::SparseTensor(const NumericTensor<DoubleType>&);

}  // namespace arrow
