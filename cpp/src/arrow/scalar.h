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

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/compare.h"
#include "arrow/util/decimal.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

/// \brief Base class for scalar values
///
/// A Scalar represents a single value with a specific DataType.
/// Scalars are useful for passing single value inputs to compute functions,
/// or for representing individual array elements (with a non-trivial
/// wrapping cost, though).
struct ARROW_EXPORT Scalar : public util::EqualityComparable<Scalar> {
  virtual ~Scalar() = default;

  explicit Scalar(std::shared_ptr<DataType> type) : type(std::move(type)) {}

  /// \brief The type of the scalar value
  std::shared_ptr<DataType> type;

  /// \brief Whether the value is valid (not null) or not
  bool is_valid = false;

  using util::EqualityComparable<Scalar>::operator==;
  using util::EqualityComparable<Scalar>::Equals;
  bool Equals(const Scalar& other) const;

  struct ARROW_EXPORT Hash {
    size_t operator()(const Scalar& scalar) const { return hash(scalar); }

    size_t operator()(const std::shared_ptr<Scalar>& scalar) const {
      return hash(*scalar);
    }

    static size_t hash(const Scalar& scalar);
  };

  std::string ToString() const;

  static Result<std::shared_ptr<Scalar>> Parse(const std::shared_ptr<DataType>& type,
                                               util::string_view repr);

  // TODO(bkietz) add compute::CastOptions
  Result<std::shared_ptr<Scalar>> CastTo(std::shared_ptr<DataType> to) const;

 protected:
  Scalar(std::shared_ptr<DataType> type, bool is_valid)
      : type(std::move(type)), is_valid(is_valid) {}
};

/// \defgroup concrete-scalar-classes Concrete Scalar subclasses
///
/// @{

/// \brief A scalar value for NullType. Never valid
struct ARROW_EXPORT NullScalar : public Scalar {
 public:
  using TypeClass = NullType;

  NullScalar() : Scalar{null(), false} {}
};

/// @}

namespace internal {

struct ARROW_EXPORT PrimitiveScalarBase : public Scalar {
  using Scalar::Scalar;
  virtual void* mutable_data() = 0;
  virtual const void* data() const = 0;
};

template <typename T, typename CType = typename T::c_type>
struct ARROW_EXPORT PrimitiveScalar : public PrimitiveScalarBase {
  using PrimitiveScalarBase::PrimitiveScalarBase;
  using TypeClass = T;
  using ValueType = CType;

  // Non-null constructor.
  PrimitiveScalar(ValueType value, std::shared_ptr<DataType> type)
      : PrimitiveScalarBase(std::move(type), true), value(value) {}

  explicit PrimitiveScalar(std::shared_ptr<DataType> type)
      : PrimitiveScalarBase(std::move(type), false) {}

  ValueType value{};

  void* mutable_data() override { return &value; }
  const void* data() const override { return &value; }
};

}  // namespace internal

/// \addtogroup concrete-scalar-classes Concrete Scalar subclasses
///
/// @{

struct ARROW_EXPORT BooleanScalar : public internal::PrimitiveScalar<BooleanType, bool> {
  using Base = internal::PrimitiveScalar<BooleanType, bool>;
  using Base::Base;

  explicit BooleanScalar(bool value) : Base(value, boolean()) {}

  BooleanScalar() : Base(boolean()) {}
};

template <typename T>
struct NumericScalar : public internal::PrimitiveScalar<T> {
  using Base = typename internal::PrimitiveScalar<T>;
  using Base::Base;
  using TypeClass = typename Base::TypeClass;
  using ValueType = typename Base::ValueType;

  explicit NumericScalar(ValueType value)
      : Base(value, TypeTraits<T>::type_singleton()) {}

  NumericScalar() : Base(TypeTraits<T>::type_singleton()) {}
};

struct ARROW_EXPORT Int8Scalar : public NumericScalar<Int8Type> {
  using NumericScalar<Int8Type>::NumericScalar;
};

struct ARROW_EXPORT Int16Scalar : public NumericScalar<Int16Type> {
  using NumericScalar<Int16Type>::NumericScalar;
};

struct ARROW_EXPORT Int32Scalar : public NumericScalar<Int32Type> {
  using NumericScalar<Int32Type>::NumericScalar;
};

struct ARROW_EXPORT Int64Scalar : public NumericScalar<Int64Type> {
  using NumericScalar<Int64Type>::NumericScalar;
};

struct ARROW_EXPORT UInt8Scalar : public NumericScalar<UInt8Type> {
  using NumericScalar<UInt8Type>::NumericScalar;
};

struct ARROW_EXPORT UInt16Scalar : public NumericScalar<UInt16Type> {
  using NumericScalar<UInt16Type>::NumericScalar;
};

struct ARROW_EXPORT UInt32Scalar : public NumericScalar<UInt32Type> {
  using NumericScalar<UInt32Type>::NumericScalar;
};

struct ARROW_EXPORT UInt64Scalar : public NumericScalar<UInt64Type> {
  using NumericScalar<UInt64Type>::NumericScalar;
};

struct ARROW_EXPORT HalfFloatScalar : public NumericScalar<HalfFloatType> {
  using NumericScalar<HalfFloatType>::NumericScalar;
};

struct ARROW_EXPORT FloatScalar : public NumericScalar<FloatType> {
  using NumericScalar<FloatType>::NumericScalar;
};

struct ARROW_EXPORT DoubleScalar : public NumericScalar<DoubleType> {
  using NumericScalar<DoubleType>::NumericScalar;
};

struct ARROW_EXPORT BaseBinaryScalar : public Scalar {
  using Scalar::Scalar;
  using ValueType = std::shared_ptr<Buffer>;

  std::shared_ptr<Buffer> value;

 protected:
  BaseBinaryScalar(std::shared_ptr<Buffer> value, std::shared_ptr<DataType> type)
      : Scalar{std::move(type), true}, value(std::move(value)) {}
};

struct ARROW_EXPORT BinaryScalar : public BaseBinaryScalar {
  using BaseBinaryScalar::BaseBinaryScalar;
  using TypeClass = BinaryScalar;

  BinaryScalar(std::shared_ptr<Buffer> value, std::shared_ptr<DataType> type)
      : BaseBinaryScalar(std::move(value), std::move(type)) {}

  explicit BinaryScalar(std::shared_ptr<Buffer> value)
      : BinaryScalar(std::move(value), binary()) {}

  BinaryScalar() : BinaryScalar(binary()) {}
};

struct ARROW_EXPORT StringScalar : public BinaryScalar {
  using BinaryScalar::BinaryScalar;
  using TypeClass = StringType;

  explicit StringScalar(std::shared_ptr<Buffer> value)
      : StringScalar(std::move(value), utf8()) {}

  explicit StringScalar(std::string s);

  StringScalar() : StringScalar(utf8()) {}
};

struct ARROW_EXPORT LargeBinaryScalar : public BaseBinaryScalar {
  using BaseBinaryScalar::BaseBinaryScalar;
  using TypeClass = LargeBinaryScalar;

  LargeBinaryScalar(std::shared_ptr<Buffer> value, std::shared_ptr<DataType> type)
      : BaseBinaryScalar(std::move(value), std::move(type)) {}

  explicit LargeBinaryScalar(std::shared_ptr<Buffer> value)
      : LargeBinaryScalar(std::move(value), large_binary()) {}

  LargeBinaryScalar() : LargeBinaryScalar(large_binary()) {}
};

struct ARROW_EXPORT LargeStringScalar : public LargeBinaryScalar {
  using LargeBinaryScalar::LargeBinaryScalar;
  using TypeClass = LargeStringType;

  explicit LargeStringScalar(std::shared_ptr<Buffer> value)
      : LargeStringScalar(std::move(value), large_utf8()) {}

  explicit LargeStringScalar(std::string s);

  LargeStringScalar() : LargeStringScalar(large_utf8()) {}
};

struct ARROW_EXPORT FixedSizeBinaryScalar : public BinaryScalar {
  using TypeClass = FixedSizeBinaryType;

  FixedSizeBinaryScalar(std::shared_ptr<Buffer> value, std::shared_ptr<DataType> type);

  explicit FixedSizeBinaryScalar(std::shared_ptr<DataType> type) : BinaryScalar(type) {}
};

template <typename T>
struct ARROW_EXPORT TemporalScalar : internal::PrimitiveScalar<T> {
  using internal::PrimitiveScalar<T>::PrimitiveScalar;
  using ValueType = typename TemporalScalar<T>::ValueType;

  explicit TemporalScalar(ValueType value, std::shared_ptr<DataType> type)
      : internal::PrimitiveScalar<T>(std::move(value), type) {}
};

template <typename T>
struct ARROW_EXPORT DateScalar : public TemporalScalar<T> {
  using TemporalScalar<T>::TemporalScalar;
  using ValueType = typename TemporalScalar<T>::ValueType;

  explicit DateScalar(ValueType value)
      : TemporalScalar<T>(std::move(value), TypeTraits<T>::type_singleton()) {}
  DateScalar() : TemporalScalar<T>(TypeTraits<T>::type_singleton()) {}
};

struct ARROW_EXPORT Date32Scalar : public DateScalar<Date32Type> {
  using DateScalar<Date32Type>::DateScalar;
};

struct ARROW_EXPORT Date64Scalar : public DateScalar<Date64Type> {
  using DateScalar<Date64Type>::DateScalar;
};

template <typename T>
struct ARROW_EXPORT TimeScalar : public TemporalScalar<T> {
  using TemporalScalar<T>::TemporalScalar;
};

struct ARROW_EXPORT Time32Scalar : public TimeScalar<Time32Type> {
  using TimeScalar<Time32Type>::TimeScalar;
};

struct ARROW_EXPORT Time64Scalar : public TimeScalar<Time64Type> {
  using TimeScalar<Time64Type>::TimeScalar;
};

struct ARROW_EXPORT TimestampScalar : public TemporalScalar<TimestampType> {
  using TemporalScalar<TimestampType>::TemporalScalar;
};

template <typename T>
struct ARROW_EXPORT IntervalScalar : public TemporalScalar<T> {
  using TemporalScalar<T>::TemporalScalar;
  using ValueType = typename TemporalScalar<T>::ValueType;

  explicit IntervalScalar(ValueType value)
      : TemporalScalar<T>(value, TypeTraits<T>::type_singleton()) {}
  IntervalScalar() : TemporalScalar<T>(TypeTraits<T>::type_singleton()) {}
};

struct ARROW_EXPORT MonthIntervalScalar : public IntervalScalar<MonthIntervalType> {
  using IntervalScalar<MonthIntervalType>::IntervalScalar;
};

struct ARROW_EXPORT DayTimeIntervalScalar : public IntervalScalar<DayTimeIntervalType> {
  using IntervalScalar<DayTimeIntervalType>::IntervalScalar;
};

struct ARROW_EXPORT DurationScalar : public TemporalScalar<DurationType> {
  using TemporalScalar<DurationType>::TemporalScalar;
};

struct ARROW_EXPORT Decimal128Scalar : public Scalar {
  using Scalar::Scalar;
  using TypeClass = Decimal128Type;
  using ValueType = Decimal128;

  Decimal128Scalar(Decimal128 value, std::shared_ptr<DataType> type)
      : Scalar(std::move(type), true), value(value) {}

  Decimal128 value;
};

struct ARROW_EXPORT BaseListScalar : public Scalar {
  using Scalar::Scalar;
  using ValueType = std::shared_ptr<Array>;

  BaseListScalar(std::shared_ptr<Array> value, std::shared_ptr<DataType> type);

  std::shared_ptr<Array> value;
};

struct ARROW_EXPORT ListScalar : public BaseListScalar {
  using TypeClass = ListType;
  using BaseListScalar::BaseListScalar;

  explicit ListScalar(std::shared_ptr<Array> value);
};

struct ARROW_EXPORT LargeListScalar : public BaseListScalar {
  using TypeClass = LargeListType;
  using BaseListScalar::BaseListScalar;

  explicit LargeListScalar(std::shared_ptr<Array> value);
};

struct ARROW_EXPORT MapScalar : public BaseListScalar {
  using TypeClass = MapType;
  using BaseListScalar::BaseListScalar;

  explicit MapScalar(std::shared_ptr<Array> value);
};

struct ARROW_EXPORT FixedSizeListScalar : public BaseListScalar {
  using TypeClass = FixedSizeListType;
  using BaseListScalar::BaseListScalar;

  FixedSizeListScalar(std::shared_ptr<Array> value, std::shared_ptr<DataType> type);

  explicit FixedSizeListScalar(std::shared_ptr<Array> value);
};

struct ARROW_EXPORT StructScalar : public Scalar {
  using TypeClass = StructType;
  using ValueType = std::vector<std::shared_ptr<Scalar>>;

  ScalarVector value;

  Result<std::shared_ptr<Scalar>> field(FieldRef ref) const;

  StructScalar(ValueType value, std::shared_ptr<DataType> type)
      : Scalar(std::move(type), true), value(std::move(value)) {}

  explicit StructScalar(std::shared_ptr<DataType> type) : Scalar(std::move(type)) {}
};

struct ARROW_EXPORT UnionScalar : public Scalar {
  using Scalar::Scalar;
  using ValueType = std::shared_ptr<Scalar>;
  ValueType value;

  UnionScalar(ValueType value, std::shared_ptr<DataType> type)
      : Scalar(std::move(type), true), value(std::move(value)) {}
};

struct ARROW_EXPORT SparseUnionScalar : public UnionScalar {
  using UnionScalar::UnionScalar;
  using TypeClass = SparseUnionType;
};

struct ARROW_EXPORT DenseUnionScalar : public UnionScalar {
  using UnionScalar::UnionScalar;
  using TypeClass = DenseUnionType;
};

struct ARROW_EXPORT DictionaryScalar : public Scalar {
  using TypeClass = DictionaryType;
  struct ValueType {
    std::shared_ptr<Scalar> index;
    std::shared_ptr<Array> dictionary;
  } value;

  explicit DictionaryScalar(std::shared_ptr<DataType> type);

  DictionaryScalar(ValueType value, std::shared_ptr<DataType> type)
      : Scalar(std::move(type), true), value(std::move(value)) {}

  Result<std::shared_ptr<Scalar>> GetEncodedValue() const;
};

struct ARROW_EXPORT ExtensionScalar : public Scalar {
  using Scalar::Scalar;
  using TypeClass = ExtensionType;
};

/// @}

/// \defgroup scalar-factories Scalar factory functions
///
/// @{

/// \brief Scalar factory for null scalars
ARROW_EXPORT
std::shared_ptr<Scalar> MakeNullScalar(std::shared_ptr<DataType> type);

/// @}

namespace internal {

inline Status CheckBufferLength(...) { return Status::OK(); }

ARROW_EXPORT Status CheckBufferLength(const FixedSizeBinaryType* t,
                                      const std::shared_ptr<Buffer>* b);

}  // namespace internal

template <typename ValueRef>
struct MakeScalarImpl {
  template <typename T, typename ScalarType = typename TypeTraits<T>::ScalarType,
            typename ValueType = typename ScalarType::ValueType,
            typename Enable = typename std::enable_if<
                std::is_constructible<ScalarType, ValueType,
                                      std::shared_ptr<DataType>>::value &&
                std::is_convertible<ValueRef, ValueType>::value>::type>
  Status Visit(const T& t) {
    ARROW_RETURN_NOT_OK(internal::CheckBufferLength(&t, &value_));
    out_ = std::make_shared<ScalarType>(
        static_cast<ValueType>(static_cast<ValueRef>(value_)), std::move(type_));
    return Status::OK();
  }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("constructing scalars of type ", t,
                                  " from unboxed values");
  }

  Result<std::shared_ptr<Scalar>> Finish() && {
    ARROW_RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return std::move(out_);
  }

  std::shared_ptr<DataType> type_;
  ValueRef value_;
  std::shared_ptr<Scalar> out_;
};

/// \addtogroup scalar-factories
///
/// @{

/// \brief Scalar factory for non-null scalars
template <typename Value>
Result<std::shared_ptr<Scalar>> MakeScalar(std::shared_ptr<DataType> type,
                                           Value&& value) {
  return MakeScalarImpl<Value&&>{type, std::forward<Value>(value), NULLPTR}.Finish();
}

/// \brief Type-inferring scalar factory for non-null scalars
///
/// Construct a Scalar instance with a DataType determined by the input C++ type.
/// (for example Int8Scalar for a int8_t input).
/// Only non-parametric primitive types and String are supported.
template <typename Value, typename Traits = CTypeTraits<typename std::decay<Value>::type>,
          typename ScalarType = typename Traits::ScalarType,
          typename Enable = decltype(ScalarType(std::declval<Value>(),
                                                Traits::type_singleton()))>
std::shared_ptr<Scalar> MakeScalar(Value value) {
  return std::make_shared<ScalarType>(std::move(value), Traits::type_singleton());
}

inline std::shared_ptr<Scalar> MakeScalar(std::string value) {
  return std::make_shared<StringScalar>(std::move(value));
}

/// @}

}  // namespace arrow
