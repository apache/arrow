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

// Object model for scalar (non-Array) values. Not intended for use with large
// amounts of data
//
// NOTE: This API is experimental as of the 0.13 version and subject to change
// without deprecation warnings

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

/// \brief Base class for scalar values, representing a single value occupying
/// an array "slot"
struct ARROW_EXPORT Scalar {
  virtual ~Scalar() = default;

  /// \brief The type of the scalar value
  std::shared_ptr<DataType> type;

  /// \brief Whether the value is valid (not null) or not
  bool is_valid;

  bool Equals(const Scalar& other) const;
  bool Equals(const std::shared_ptr<Scalar>& other) const {
    if (other) return Equals(*other);
    return false;
  }

  static Status Parse(const std::shared_ptr<DataType>& type, util::string_view s,
                      std::shared_ptr<Scalar>* out);

 protected:
  Scalar(const std::shared_ptr<DataType>& type, bool is_valid)
      : type(type), is_valid(is_valid) {}
};

/// \brief A scalar value for NullType. Never valid
struct ARROW_EXPORT NullScalar : public Scalar {
 public:
  NullScalar() : Scalar{null(), false} {}
};

namespace internal {

template <typename T, typename Enable = void>
struct is_simple_scalar : std::false_type {};

template <typename T>
struct is_simple_scalar<
    T,
    typename std::enable_if<
        // scalar has a single extra data member named "value" with type "ValueType"
        std::is_same<decltype(std::declval<T>().value), typename T::ValueType>::value &&
        // scalar is constructible from (value, type, is_valid)
        std::is_constructible<T, typename T::ValueType, std::shared_ptr<DataType>,
                              bool>::value>::type> : std::true_type {};

template <typename T, typename R = void>
using enable_if_simple_scalar = std::enable_if<is_simple_scalar<T>::value, R>;

struct ARROW_EXPORT PrimitiveScalar : public Scalar {
  using Scalar::Scalar;
};

}  // namespace internal

struct ARROW_EXPORT BooleanScalar : public internal::PrimitiveScalar {
  using ValueType = bool;

  bool value;

  explicit BooleanScalar(bool value, bool is_valid = true)
      : internal::PrimitiveScalar{boolean(), is_valid}, value(value) {}

  BooleanScalar() : BooleanScalar(false, false) {}

  BooleanScalar(bool value, const std::shared_ptr<DataType>& type, bool is_valid = true)
      : BooleanScalar(value, is_valid) {
    ARROW_CHECK_EQ(type->id(), Type::BOOL);
  }
};

template <typename T>
struct NumericScalar : public internal::PrimitiveScalar {
  using ValueType = typename T::c_type;

  ValueType value;

  explicit NumericScalar(ValueType value, bool is_valid = true)
      : internal::PrimitiveScalar(TypeTraits<T>::type_singleton(), is_valid),
        value(value) {}

  NumericScalar() : NumericScalar(0, false) {}

  NumericScalar(ValueType value, const std::shared_ptr<DataType>& type,
                bool is_valid = true)
      : NumericScalar(value, is_valid) {
    ARROW_CHECK_EQ(type->id(), T::type_id);
  }
};

template <typename T>
struct BaseBinaryScalar : public Scalar {
  using ValueType = std::shared_ptr<Buffer>;

  std::shared_ptr<Buffer> value;

 protected:
  BaseBinaryScalar(const std::shared_ptr<Buffer>& value,
                   const std::shared_ptr<DataType>& type, bool is_valid = true)
      : Scalar{type, is_valid}, value(value) {}
};

struct ARROW_EXPORT BinaryScalar : public BaseBinaryScalar<BinaryType> {
  BinaryScalar(const std::shared_ptr<Buffer>& value,
               const std::shared_ptr<DataType>& type, bool is_valid = true)
      : BaseBinaryScalar(value, type, is_valid) {}

  explicit BinaryScalar(const std::shared_ptr<Buffer>& value, bool is_valid = true)
      : BinaryScalar(value, binary(), is_valid) {}

  BinaryScalar() : BinaryScalar(NULLPTR, false) {}
};

struct ARROW_EXPORT StringScalar : public BinaryScalar {
  StringScalar(const std::shared_ptr<Buffer>& value,
               const std::shared_ptr<DataType>& type, bool is_valid = true)
      : BinaryScalar(value, type, is_valid) {}

  explicit StringScalar(const std::shared_ptr<Buffer>& value, bool is_valid = true)
      : StringScalar(value, utf8(), is_valid) {}

  explicit StringScalar(std::string s);

  StringScalar() : StringScalar(NULLPTR, false) {}
};

struct ARROW_EXPORT LargeBinaryScalar : public BaseBinaryScalar<LargeBinaryType> {
  LargeBinaryScalar(const std::shared_ptr<Buffer>& value,
                    const std::shared_ptr<DataType>& type, bool is_valid = true)
      : BaseBinaryScalar(value, type, is_valid) {}

  explicit LargeBinaryScalar(const std::shared_ptr<Buffer>& value, bool is_valid = true)
      : LargeBinaryScalar(value, large_binary(), is_valid) {}

  LargeBinaryScalar() : LargeBinaryScalar(NULLPTR, false) {}
};

struct ARROW_EXPORT LargeStringScalar : public LargeBinaryScalar {
  LargeStringScalar(const std::shared_ptr<Buffer>& value,
                    const std::shared_ptr<DataType>& type, bool is_valid = true)
      : LargeBinaryScalar(value, type, is_valid) {}

  explicit LargeStringScalar(const std::shared_ptr<Buffer>& value, bool is_valid = true)
      : LargeStringScalar(value, large_utf8(), is_valid) {}

  LargeStringScalar() : LargeStringScalar(NULLPTR, false) {}
};

struct ARROW_EXPORT FixedSizeBinaryScalar : public BinaryScalar {
  FixedSizeBinaryScalar(const std::shared_ptr<Buffer>& value,
                        const std::shared_ptr<DataType>& type, bool is_valid = true);
};

class ARROW_EXPORT Date32Scalar : public NumericScalar<Date32Type> {
 public:
  using NumericScalar<Date32Type>::NumericScalar;
};

class ARROW_EXPORT Date64Scalar : public NumericScalar<Date64Type> {
 public:
  using NumericScalar<Date64Type>::NumericScalar;
};

class ARROW_EXPORT Time32Scalar : public internal::PrimitiveScalar {
 public:
  using ValueType = int32_t;

  Time32Scalar(int32_t value, const std::shared_ptr<DataType>& type,
               bool is_valid = true);

  int32_t value;
};

class ARROW_EXPORT Time64Scalar : public internal::PrimitiveScalar {
 public:
  using ValueType = int64_t;

  Time64Scalar(int64_t value, const std::shared_ptr<DataType>& type,
               bool is_valid = true);

  int64_t value;
};

class ARROW_EXPORT TimestampScalar : public internal::PrimitiveScalar {
 public:
  using ValueType = int64_t;

  TimestampScalar(int64_t value, const std::shared_ptr<DataType>& type,
                  bool is_valid = true);

  int64_t value;
};

class ARROW_EXPORT DurationScalar : public internal::PrimitiveScalar {
 public:
  using ValueType = int64_t;

  DurationScalar(int64_t value, const std::shared_ptr<DataType>& type,
                 bool is_valid = true);

  int64_t value;
};

class ARROW_EXPORT MonthIntervalScalar : public internal::PrimitiveScalar {
 public:
  using ValueType = int32_t;

  explicit MonthIntervalScalar(int32_t value, bool is_valid = true);
  MonthIntervalScalar(int32_t value, const std::shared_ptr<DataType>& type,
                      bool is_valid = true);

  int32_t value;
};

class ARROW_EXPORT DayTimeIntervalScalar : public internal::PrimitiveScalar {
 public:
  using ValueType = DayTimeIntervalType::DayMilliseconds;

  explicit DayTimeIntervalScalar(DayTimeIntervalType::DayMilliseconds value,
                                 bool is_valid = true);

  DayTimeIntervalScalar(DayTimeIntervalType::DayMilliseconds value,
                        const std::shared_ptr<DataType>& type, bool is_valid = true);

  DayTimeIntervalType::DayMilliseconds value;
};

struct ARROW_EXPORT Decimal128Scalar : public Scalar {
  using ValueType = Decimal128;

  Decimal128Scalar(const Decimal128& value, const std::shared_ptr<DataType>& type,
                   bool is_valid = true);

  Decimal128 value;
};

struct ARROW_EXPORT BaseListScalar : public Scalar {
  using ValueType = std::shared_ptr<Array>;

  BaseListScalar(const std::shared_ptr<Array>& value,
                 const std::shared_ptr<DataType>& type, bool is_valid = true);

  BaseListScalar(const std::shared_ptr<Array>& value, bool is_valid);

  std::shared_ptr<Array> value;
};

struct ARROW_EXPORT ListScalar : public BaseListScalar {
  using BaseListScalar::BaseListScalar;
};

struct ARROW_EXPORT LargeListScalar : public BaseListScalar {
  using BaseListScalar::BaseListScalar;
};

struct ARROW_EXPORT MapScalar : public BaseListScalar {
  using BaseListScalar::BaseListScalar;
};

struct ARROW_EXPORT FixedSizeListScalar : public BaseListScalar {
  FixedSizeListScalar(const std::shared_ptr<Array>& value,
                      const std::shared_ptr<DataType>& type, bool is_valid = true);

  using BaseListScalar::BaseListScalar;
};

struct ARROW_EXPORT StructScalar : public Scalar {
  using ValueType = std::vector<std::shared_ptr<Scalar>>;
  std::vector<std::shared_ptr<Scalar>> value;
};

class ARROW_EXPORT UnionScalar : public Scalar {};
class ARROW_EXPORT DictionaryScalar : public Scalar {};
class ARROW_EXPORT ExtensionScalar : public Scalar {};

/// \param[in] type the type of scalar to produce
/// \param[out] null output scalar with is_valid=false
/// \return Status
ARROW_EXPORT
Status MakeNullScalar(const std::shared_ptr<DataType>& type,
                      std::shared_ptr<Scalar>* null);

namespace internal {

inline Status CheckBufferLength(...) { return Status::OK(); }

ARROW_EXPORT Status CheckBufferLength(const FixedSizeBinaryType* t,
                                      const std::shared_ptr<Buffer>* b);

};  // namespace internal

template <typename ValueRef>
struct MakeScalarImpl {
  template <
      typename T, typename ScalarType = typename TypeTraits<T>::ScalarType,
      typename ValueType = typename ScalarType::ValueType,
      typename Enable = typename std::enable_if<
          internal::is_simple_scalar<ScalarType>::value &&
          std::is_same<ValueType, typename std::decay<ValueRef>::type>::value>::type>
  Status Visit(const T& t) {
    ARROW_RETURN_NOT_OK(internal::CheckBufferLength(&t, &value_));
    *out_ = std::make_shared<ScalarType>(ValueType(static_cast<ValueRef>(value_)), type_,
                                         true);
    return Status::OK();
  }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("constructing scalars of type ", t, " from ", value_);
  }

  const std::shared_ptr<DataType>& type_;
  ValueRef value_;
  std::shared_ptr<Scalar>* out_;
};

template <typename Value>
Status MakeScalar(const std::shared_ptr<DataType>& type, Value&& value,
                  std::shared_ptr<Scalar>* out) {
  MakeScalarImpl<Value&&> impl = {type, std::forward<Value>(value), out};
  return VisitTypeInline(*type, &impl);
}

/// \brief type inferring scalar factory
template <typename Value, typename Traits = CTypeTraits<typename std::decay<Value>::type>,
          typename ScalarType = typename Traits::ScalarType,
          typename Enable = decltype(ScalarType(std::declval<Value>(),
                                                Traits::type_singleton(), true))>
std::shared_ptr<Scalar> MakeScalar(Value value) {
  return std::make_shared<ScalarType>(std::move(value), Traits::type_singleton(), true);
}

template <size_t N>
std::shared_ptr<Scalar> MakeScalar(const char (&value)[N]) {
  return std::make_shared<StringScalar>(value);
}

}  // namespace arrow
