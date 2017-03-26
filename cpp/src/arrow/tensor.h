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

#ifndef ARROW_TENSOR_H
#define ARROW_TENSOR_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class MutableBuffer;
class Status;

static inline bool is_tensor_supported(Type::type type_id) {
  switch (type_id) {
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::UINT64:
    case Type::INT64:
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
      return true;
    default:
      break;
  }
  return false;
}

class ARROW_EXPORT Tensor {
 public:
  virtual ~Tensor() = default;

  /// Constructor with no dimension names or strides, data assumed to be row-major
  Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
      const std::vector<int64_t>& shape);

  /// Constructor with non-negative strides
  Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
      const std::vector<int64_t>& shape, const std::vector<int64_t>& strides);

  /// Constructor with strides and dimension names
  Tensor(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
      const std::vector<int64_t>& shape, const std::vector<int64_t>& strides,
      const std::vector<std::string>& dim_names);

  std::shared_ptr<Buffer> data() const { return data_; }
  const std::vector<int64_t>& shape() const { return shape_; }
  const std::vector<int64_t>& strides() const { return strides_; }

  const std::string& dim_name(int i) const;
  bool has_dim_names() const { return shape_.size() > 0 && dim_names_.size() > 0; }

  /// Total number of value cells in the tensor
  int64_t size() const;

  /// Return true if the underlying data buffer is mutable
  bool is_mutable() const { return data_->is_mutable(); }

 protected:
  Tensor() {}

  std::shared_ptr<DataType> type_;

  std::shared_ptr<Buffer> data_;

  std::vector<int64_t> shape_;
  std::vector<int64_t> strides_;

  /// These names are optional
  std::vector<std::string> dim_names_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Tensor);
};

template <typename T>
class ARROW_EXPORT NumericTensor : public Tensor {
 public:
  using value_type = typename T::c_type;

  NumericTensor(const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape);

  /// Constructor with non-negative strides
  NumericTensor(const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape,
      const std::vector<int64_t>& strides);

  /// Constructor with strides and dimension names
  NumericTensor(const std::shared_ptr<Buffer>& data, const std::vector<int64_t>& shape,
      const std::vector<int64_t>& strides, const std::vector<std::string>& dim_names);

  const value_type* raw_data() const { return raw_data_; }
  value_type* raw_data() { return mutable_raw_data_; }

 private:
  const value_type* raw_data_;
  value_type* mutable_raw_data_;
};

// ----------------------------------------------------------------------
// extern templates and other details

// gcc and clang disagree about how to handle template visibility when you have
// explicit specializations https://llvm.org/bugs/show_bug.cgi?id=24815
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif

// Only instantiate these templates once
extern template class ARROW_EXPORT NumericTensor<Int8Type>;
extern template class ARROW_EXPORT NumericTensor<UInt8Type>;
extern template class ARROW_EXPORT NumericTensor<Int16Type>;
extern template class ARROW_EXPORT NumericTensor<UInt16Type>;
extern template class ARROW_EXPORT NumericTensor<Int32Type>;
extern template class ARROW_EXPORT NumericTensor<UInt32Type>;
extern template class ARROW_EXPORT NumericTensor<Int64Type>;
extern template class ARROW_EXPORT NumericTensor<UInt64Type>;
extern template class ARROW_EXPORT NumericTensor<HalfFloatType>;
extern template class ARROW_EXPORT NumericTensor<FloatType>;
extern template class ARROW_EXPORT NumericTensor<DoubleType>;

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

}  // namespace arrow

#endif  // ARROW_TENSOR_H
