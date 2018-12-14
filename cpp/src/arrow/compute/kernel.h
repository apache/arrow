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

#ifndef ARROW_COMPUTE_KERNEL_H
#define ARROW_COMPUTE_KERNEL_H

#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/macros.h"
#include "arrow/util/variant.h"  // IWYU pragma: export
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class FunctionContext;

/// \class OpKernel
/// \brief Base class for operator kernels
class ARROW_EXPORT OpKernel {
 public:
  virtual ~OpKernel() = default;
};

/// \brief Placeholder for Scalar values until we implement these
struct ARROW_EXPORT Scalar {
  ~Scalar() {}

  ARROW_DISALLOW_COPY_AND_ASSIGN(Scalar);
};

/// \class Datum
/// \brief Variant type for various Arrow C++ data structures
struct ARROW_EXPORT Datum {
  enum type { NONE, SCALAR, ARRAY, CHUNKED_ARRAY, RECORD_BATCH, TABLE, COLLECTION };

  util::variant<decltype(NULLPTR), std::shared_ptr<Scalar>, std::shared_ptr<ArrayData>,
                std::shared_ptr<ChunkedArray>, std::shared_ptr<RecordBatch>,
                std::shared_ptr<Table>, std::vector<Datum>>
      value;

  /// \brief Empty datum, to be populated elsewhere
  Datum() : value(NULLPTR) {}

  Datum(const std::shared_ptr<Scalar>& value)  // NOLINT implicit conversion
      : value(value) {}
  Datum(const std::shared_ptr<ArrayData>& value)  // NOLINT implicit conversion
      : value(value) {}

  Datum(const std::shared_ptr<Array>& value)  // NOLINT implicit conversion
      : Datum(value ? value->data() : NULLPTR) {}

  Datum(const std::shared_ptr<ChunkedArray>& value)  // NOLINT implicit conversion
      : value(value) {}
  Datum(const std::shared_ptr<RecordBatch>& value)  // NOLINT implicit conversion
      : value(value) {}
  Datum(const std::shared_ptr<Table>& value)  // NOLINT implicit conversion
      : value(value) {}
  Datum(const std::vector<Datum>& value)  // NOLINT implicit conversion
      : value(value) {}

  // Cast from subtypes of Array to Datum
  template <typename T,
            typename = typename std::enable_if<std::is_base_of<Array, T>::value>::type>
  Datum(const std::shared_ptr<T>& value)  // NOLINT implicit conversion
      : Datum(std::shared_ptr<Array>(value)) {}

  ~Datum() {}

  Datum(const Datum& other) noexcept { this->value = other.value; }

  // Define move constructor and move assignment, for better performance
  Datum(Datum&& other) noexcept : value(std::move(other.value)) {}

  Datum& operator=(Datum&& other) noexcept {
    value = std::move(other.value);
    return *this;
  }

  Datum::type kind() const {
    switch (this->value.which()) {
      case 0:
        return Datum::NONE;
      case 1:
        return Datum::SCALAR;
      case 2:
        return Datum::ARRAY;
      case 3:
        return Datum::CHUNKED_ARRAY;
      case 4:
        return Datum::RECORD_BATCH;
      case 5:
        return Datum::TABLE;
      case 6:
        return Datum::COLLECTION;
      default:
        return Datum::NONE;
    }
  }

  std::shared_ptr<ArrayData> array() const {
    return util::get<std::shared_ptr<ArrayData>>(this->value);
  }

  std::shared_ptr<Array> make_array() const {
    return MakeArray(util::get<std::shared_ptr<ArrayData>>(this->value));
  }

  std::shared_ptr<ChunkedArray> chunked_array() const {
    return util::get<std::shared_ptr<ChunkedArray>>(this->value);
  }

  const std::vector<Datum> collection() const {
    return util::get<std::vector<Datum>>(this->value);
  }

  bool is_arraylike() const {
    return this->kind() == Datum::ARRAY || this->kind() == Datum::CHUNKED_ARRAY;
  }

  /// \brief The value type of the variant, if any
  ///
  /// \return nullptr if no type
  std::shared_ptr<DataType> type() const {
    if (this->kind() == Datum::ARRAY) {
      return util::get<std::shared_ptr<ArrayData>>(this->value)->type;
    } else if (this->kind() == Datum::CHUNKED_ARRAY) {
      return util::get<std::shared_ptr<ChunkedArray>>(this->value)->type();
    }
    return NULLPTR;
  }
};

/// \class UnaryKernel
/// \brief An array-valued function of a single input argument
class ARROW_EXPORT UnaryKernel : public OpKernel {
 public:
  virtual Status Call(FunctionContext* ctx, const Datum& input, Datum* out) = 0;
};

/// \class BinaryKernel
/// \brief An array-valued function of a two input arguments
class ARROW_EXPORT BinaryKernel : public OpKernel {
 public:
  virtual Status Call(FunctionContext* ctx, const Datum& left, const Datum& right,
                      Datum* out) = 0;
};

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNEL_H
