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
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/util/macros.h"
#include "arrow/util/variant.h"  // IWYU pragma: export
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class FunctionContext;

/// \class OpKernel
/// \brief Base class for operator kernels
///
/// Note to implementors:
/// Operator kernels are intended to be the lowest level of an analytics/compute
/// engine.  They will generally not be exposed directly to end-users.  Instead
/// they will be wrapped by higher level constructs (e.g. top-level functions
/// or physical execution plan nodes).  These higher level constructs are
/// responsible for user input validation and returning the appropriate
/// error Status.
///
/// Due to this design, implementations of Call (the execution
/// method on subclasses) should use assertions (i.e. DCHECK) to double-check
/// parameter arguments when in higher level components returning an
/// InvalidArgument error might be more appropriate.
///
class ARROW_EXPORT OpKernel {
 public:
  virtual ~OpKernel() = default;
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

  std::shared_ptr<Scalar> scalar() const {
    return util::get<std::shared_ptr<Scalar>>(this->value);
  }

  bool is_array() const { return this->kind() == Datum::ARRAY; }

  bool is_arraylike() const {
    return this->kind() == Datum::ARRAY || this->kind() == Datum::CHUNKED_ARRAY;
  }

  bool is_scalar() const { return this->kind() == Datum::SCALAR; }

  /// \brief The value type of the variant, if any
  ///
  /// \return nullptr if no type
  std::shared_ptr<DataType> type() const {
    if (this->kind() == Datum::ARRAY) {
      return util::get<std::shared_ptr<ArrayData>>(this->value)->type;
    } else if (this->kind() == Datum::CHUNKED_ARRAY) {
      return util::get<std::shared_ptr<ChunkedArray>>(this->value)->type();
    } else if (this->kind() == Datum::SCALAR) {
      return util::get<std::shared_ptr<Scalar>>(this->value)->type;
    }
    return NULLPTR;
  }
};

/// \class UnaryKernel
/// \brief An array-valued function of a single input argument.
///
/// Note to implementors:  Try to avoid making kernels that allocate memory if
/// the output size is a deterministic function of the Input Datum's metadata.
/// Instead separate the logic of the kernel and allocations necessary into
/// two different kernels.  Some reusable kernels that allocate buffers
/// and delegate computation to another kernel are available in util-internal.h.
class ARROW_EXPORT UnaryKernel : public OpKernel {
 public:
  /// \brief Executes the kernel.
  ///
  /// \param[in] ctx The function context for the kernel
  /// \param[in] input The kernel input data
  /// \param[out] out The output of the function. Each implementation of this
  /// function might assume different things about the existing contents of out
  /// (e.g. which buffers are preallocated).  In the future it is expected that
  /// there will be a more generic mechansim for understanding the necessary
  /// contracts.
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
