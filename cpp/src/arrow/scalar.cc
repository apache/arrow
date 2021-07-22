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

#include "arrow/scalar.h"

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/formatting.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/time.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

bool Scalar::Equals(const Scalar& other, const EqualOptions& options) const {
  return ScalarEquals(*this, other, options);
}

bool Scalar::ApproxEquals(const Scalar& other, const EqualOptions& options) const {
  return ScalarApproxEquals(*this, other, options);
}

struct ScalarHashImpl {
  static std::hash<std::string> string_hash;

  Status Visit(const NullScalar& s) { return Status::OK(); }

  template <typename T>
  Status Visit(const internal::PrimitiveScalar<T>& s) {
    return ValueHash(s);
  }

  Status Visit(const BaseBinaryScalar& s) { return BufferHash(*s.value); }

  template <typename T>
  Status Visit(const TemporalScalar<T>& s) {
    return ValueHash(s);
  }

  Status Visit(const DayTimeIntervalScalar& s) {
    return StdHash(s.value.days) & StdHash(s.value.days);
  }

  Status Visit(const Decimal128Scalar& s) {
    return StdHash(s.value.low_bits()) & StdHash(s.value.high_bits());
  }

  Status Visit(const Decimal256Scalar& s) {
    Status status = Status::OK();
    // endianness doesn't affect result
    for (uint64_t elem : s.value.native_endian_array()) {
      status &= StdHash(elem);
    }
    return status;
  }

  Status Visit(const BaseListScalar& s) { return ArrayHash(*s.value); }

  Status Visit(const StructScalar& s) {
    for (const auto& child : s.value) {
      AccumulateHashFrom(*child);
    }
    return Status::OK();
  }

  Status Visit(const DictionaryScalar& s) {
    AccumulateHashFrom(*s.value.index);
    return Status::OK();
  }

  // TODO(bkietz) implement less wimpy hashing when these have ValueType
  Status Visit(const UnionScalar& s) { return Status::OK(); }
  Status Visit(const ExtensionScalar& s) { return Status::OK(); }

  template <typename T>
  Status StdHash(const T& t) {
    static std::hash<T> hash;
    hash_ ^= hash(t);
    return Status::OK();
  }

  template <typename S>
  Status ValueHash(const S& s) {
    return StdHash(s.value);
  }

  Status BufferHash(const Buffer& b) {
    hash_ ^= internal::ComputeStringHash<1>(b.data(), b.size());
    return Status::OK();
  }

  Status ArrayHash(const Array& a) { return ArrayHash(*a.data()); }

  Status ArrayHash(const ArrayData& a) {
    RETURN_NOT_OK(StdHash(a.length) & StdHash(a.GetNullCount()));
    if (a.buffers[0] != nullptr) {
      // We can't visit values without unboxing the whole array, so only hash
      // the null bitmap for now.
      RETURN_NOT_OK(BufferHash(*a.buffers[0]));
    }
    for (const auto& child : a.child_data) {
      RETURN_NOT_OK(ArrayHash(*child));
    }
    return Status::OK();
  }

  explicit ScalarHashImpl(const Scalar& scalar) : hash_(scalar.type->Hash()) {
    if (scalar.is_valid) {
      AccumulateHashFrom(scalar);
    }
  }

  void AccumulateHashFrom(const Scalar& scalar) {
    DCHECK_OK(StdHash(scalar.type->fingerprint()));
    DCHECK_OK(VisitScalarInline(scalar, this));
  }

  size_t hash_;
};

size_t Scalar::hash() const { return ScalarHashImpl(*this).hash_; }

StringScalar::StringScalar(std::string s)
    : StringScalar(Buffer::FromString(std::move(s))) {}

LargeStringScalar::LargeStringScalar(std::string s)
    : LargeStringScalar(Buffer::FromString(std::move(s))) {}

FixedSizeBinaryScalar::FixedSizeBinaryScalar(std::shared_ptr<Buffer> value,
                                             std::shared_ptr<DataType> type)
    : BinaryScalar(std::move(value), std::move(type)) {
  ARROW_CHECK_EQ(checked_cast<const FixedSizeBinaryType&>(*this->type).byte_width(),
                 this->value->size());
}

BaseListScalar::BaseListScalar(std::shared_ptr<Array> value,
                               std::shared_ptr<DataType> type)
    : Scalar{std::move(type), true}, value(std::move(value)) {
  ARROW_CHECK(this->type->field(0)->type()->Equals(this->value->type()));
}

ListScalar::ListScalar(std::shared_ptr<Array> value)
    : BaseListScalar(value, list(value->type())) {}

LargeListScalar::LargeListScalar(std::shared_ptr<Array> value)
    : BaseListScalar(value, large_list(value->type())) {}

inline std::shared_ptr<DataType> MakeMapType(const std::shared_ptr<DataType>& pair_type) {
  ARROW_CHECK_EQ(pair_type->id(), Type::STRUCT);
  ARROW_CHECK_EQ(pair_type->num_fields(), 2);
  return map(pair_type->field(0)->type(), pair_type->field(1)->type());
}

MapScalar::MapScalar(std::shared_ptr<Array> value)
    : BaseListScalar(value, MakeMapType(value->type())) {}

FixedSizeListScalar::FixedSizeListScalar(std::shared_ptr<Array> value,
                                         std::shared_ptr<DataType> type)
    : BaseListScalar(value, std::move(type)) {
  ARROW_CHECK_EQ(this->value->length(),
                 checked_cast<const FixedSizeListType&>(*this->type).list_size());
}

FixedSizeListScalar::FixedSizeListScalar(std::shared_ptr<Array> value)
    : BaseListScalar(
          value, fixed_size_list(value->type(), static_cast<int32_t>(value->length()))) {}

Result<std::shared_ptr<StructScalar>> StructScalar::Make(
    ScalarVector values, std::vector<std::string> field_names) {
  if (values.size() != field_names.size()) {
    return Status::Invalid("Mismatching number of field names and child scalars");
  }

  FieldVector fields(field_names.size());
  for (size_t i = 0; i < fields.size(); ++i) {
    fields[i] = arrow::field(std::move(field_names[i]), values[i]->type);
  }

  return std::make_shared<StructScalar>(std::move(values), struct_(std::move(fields)));
}

Result<std::shared_ptr<Scalar>> StructScalar::field(FieldRef ref) const {
  ARROW_ASSIGN_OR_RAISE(auto path, ref.FindOne(*type));
  if (path.indices().size() != 1) {
    return Status::NotImplemented("retrieval of nested fields from StructScalar");
  }
  auto index = path.indices()[0];
  if (is_valid) {
    return value[index];
  } else {
    const auto& struct_type = checked_cast<const StructType&>(*this->type);
    const auto& field_type = struct_type.field(index)->type();
    return MakeNullScalar(field_type);
  }
}

DictionaryScalar::DictionaryScalar(std::shared_ptr<DataType> type)
    : Scalar(std::move(type)),
      value{MakeNullScalar(checked_cast<const DictionaryType&>(*this->type).index_type()),
            MakeArrayOfNull(checked_cast<const DictionaryType&>(*this->type).value_type(),
                            0)
                .ValueOrDie()} {}

Result<std::shared_ptr<Scalar>> DictionaryScalar::GetEncodedValue() const {
  const auto& dict_type = checked_cast<DictionaryType&>(*type);

  if (!is_valid) {
    return MakeNullScalar(dict_type.value_type());
  }

  int64_t index_value = 0;
  switch (dict_type.index_type()->id()) {
    case Type::UINT8:
      index_value =
          static_cast<int64_t>(checked_cast<const UInt8Scalar&>(*value.index).value);
      break;
    case Type::INT8:
      index_value =
          static_cast<int64_t>(checked_cast<const Int8Scalar&>(*value.index).value);
      break;
    case Type::UINT16:
      index_value =
          static_cast<int64_t>(checked_cast<const UInt16Scalar&>(*value.index).value);
      break;
    case Type::INT16:
      index_value =
          static_cast<int64_t>(checked_cast<const Int16Scalar&>(*value.index).value);
      break;
    case Type::UINT32:
      index_value =
          static_cast<int64_t>(checked_cast<const UInt32Scalar&>(*value.index).value);
      break;
    case Type::INT32:
      index_value =
          static_cast<int64_t>(checked_cast<const Int32Scalar&>(*value.index).value);
      break;
    case Type::UINT64:
      index_value =
          static_cast<int64_t>(checked_cast<const UInt64Scalar&>(*value.index).value);
      break;
    case Type::INT64:
      index_value =
          static_cast<int64_t>(checked_cast<const Int64Scalar&>(*value.index).value);
      break;
    default:
      return Status::TypeError("Not implemented dictionary index type");
      break;
  }
  return value.dictionary->GetScalar(index_value);
}

std::shared_ptr<DictionaryScalar> DictionaryScalar::Make(std::shared_ptr<Scalar> index,
                                                         std::shared_ptr<Array> dict) {
  auto type = dictionary(index->type, dict->type());
  return std::make_shared<DictionaryScalar>(ValueType{std::move(index), std::move(dict)},
                                            std::move(type));
}

template <typename T>
using scalar_constructor_has_arrow_type =
    std::is_constructible<typename TypeTraits<T>::ScalarType, std::shared_ptr<DataType>>;

template <typename T, typename R = void>
using enable_if_scalar_constructor_has_arrow_type =
    typename std::enable_if<scalar_constructor_has_arrow_type<T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_scalar_constructor_has_no_arrow_type =
    typename std::enable_if<!scalar_constructor_has_arrow_type<T>::value, R>::type;

struct MakeNullImpl {
  template <typename T, typename ScalarType = typename TypeTraits<T>::ScalarType>
  enable_if_scalar_constructor_has_arrow_type<T, Status> Visit(const T&) {
    out_ = std::make_shared<ScalarType>(type_);
    return Status::OK();
  }

  template <typename T, typename ScalarType = typename TypeTraits<T>::ScalarType>
  enable_if_scalar_constructor_has_no_arrow_type<T, Status> Visit(const T&) {
    out_ = std::make_shared<ScalarType>();
    return Status::OK();
  }

  std::shared_ptr<Scalar> Finish() && {
    // Should not fail.
    DCHECK_OK(VisitTypeInline(*type_, this));
    return std::move(out_);
  }

  std::shared_ptr<DataType> type_;
  std::shared_ptr<Scalar> out_;
};

std::shared_ptr<Scalar> MakeNullScalar(std::shared_ptr<DataType> type) {
  return MakeNullImpl{std::move(type), nullptr}.Finish();
}

std::string Scalar::ToString() const {
  if (!this->is_valid) {
    return "null";
  }
  if (type->id() == Type::DICTIONARY) {
    auto dict_scalar = checked_cast<const DictionaryScalar*>(this);
    return dict_scalar->value.dictionary->ToString() + "[" +
           dict_scalar->value.index->ToString() + "]";
  }
  auto maybe_repr = CastTo(utf8());
  if (maybe_repr.ok()) {
    return checked_cast<const StringScalar&>(*maybe_repr.ValueOrDie()).value->ToString();
  }
  return "...";
}

struct ScalarParseImpl {
  template <typename T, typename = internal::enable_if_parseable<T>>
  Status Visit(const T& t) {
    typename internal::StringConverter<T>::value_type value;
    if (!internal::ParseValue(t, s_.data(), s_.size(), &value)) {
      return Status::Invalid("error parsing '", s_, "' as scalar of type ", t);
    }
    return Finish(value);
  }

  Status Visit(const BinaryType&) { return FinishWithBuffer(); }

  Status Visit(const LargeBinaryType&) { return FinishWithBuffer(); }

  Status Visit(const FixedSizeBinaryType&) { return FinishWithBuffer(); }

  Status Visit(const DictionaryType& t) {
    ARROW_ASSIGN_OR_RAISE(auto value, Scalar::Parse(t.value_type(), s_));
    return Finish(std::move(value));
  }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("parsing scalars of type ", t);
  }

  template <typename Arg>
  Status Finish(Arg&& arg) {
    return MakeScalar(std::move(type_), std::forward<Arg>(arg)).Value(&out_);
  }

  Status FinishWithBuffer() { return Finish(Buffer::FromString(std::string(s_))); }

  Result<std::shared_ptr<Scalar>> Finish() && {
    RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return std::move(out_);
  }

  ScalarParseImpl(std::shared_ptr<DataType> type, util::string_view s)
      : type_(std::move(type)), s_(s) {}

  std::shared_ptr<DataType> type_;
  util::string_view s_;
  std::shared_ptr<Scalar> out_;
};

Result<std::shared_ptr<Scalar>> Scalar::Parse(const std::shared_ptr<DataType>& type,
                                              util::string_view s) {
  return ScalarParseImpl{type, s}.Finish();
}

namespace internal {
Status CheckBufferLength(const FixedSizeBinaryType* t, const std::shared_ptr<Buffer>* b) {
  return t->byte_width() == (*b)->size()
             ? Status::OK()
             : Status::Invalid("buffer length ", (*b)->size(), " is not compatible with ",
                               *t);
}
}  // namespace internal

namespace {
// CastImpl(...) assumes `to` points to a non null scalar of the correct type with
// uninitialized value

// helper for StringFormatter
template <typename Formatter, typename ScalarType>
std::shared_ptr<Buffer> FormatToBuffer(Formatter&& formatter, const ScalarType& from) {
  if (!from.is_valid) {
    return Buffer::FromString("null");
  }
  return formatter(from.value, [&](util::string_view v) {
    return Buffer::FromString(std::string(v));
  });
}

// error fallback
Status CastImpl(const Scalar& from, Scalar* to) {
  return Status::NotImplemented("casting scalars of type ", *from.type, " to type ",
                                *to->type);
}

// numeric to numeric
template <typename From, typename To>
Status CastImpl(const NumericScalar<From>& from, NumericScalar<To>* to) {
  to->value = static_cast<typename To::c_type>(from.value);
  return Status::OK();
}

// numeric to boolean
template <typename T>
Status CastImpl(const NumericScalar<T>& from, BooleanScalar* to) {
  constexpr auto zero = static_cast<typename T::c_type>(0);
  to->value = from.value != zero;
  return Status::OK();
}

// boolean to numeric
template <typename T>
Status CastImpl(const BooleanScalar& from, NumericScalar<T>* to) {
  to->value = static_cast<typename T::c_type>(from.value);
  return Status::OK();
}

// numeric to temporal
template <typename From, typename To>
typename std::enable_if<std::is_base_of<TemporalType, To>::value &&
                            !std::is_same<DayTimeIntervalType, To>::value,
                        Status>::type
CastImpl(const NumericScalar<From>& from, TemporalScalar<To>* to) {
  to->value = static_cast<typename To::c_type>(from.value);
  return Status::OK();
}

// temporal to numeric
template <typename From, typename To>
typename std::enable_if<std::is_base_of<TemporalType, From>::value &&
                            !std::is_same<DayTimeIntervalType, From>::value,
                        Status>::type
CastImpl(const TemporalScalar<From>& from, NumericScalar<To>* to) {
  to->value = static_cast<typename To::c_type>(from.value);
  return Status::OK();
}

// timestamp to timestamp
Status CastImpl(const TimestampScalar& from, TimestampScalar* to) {
  return util::ConvertTimestampValue(from.type, to->type, from.value).Value(&to->value);
}

template <typename TypeWithTimeUnit>
std::shared_ptr<DataType> AsTimestampType(const std::shared_ptr<DataType>& type) {
  return timestamp(checked_cast<const TypeWithTimeUnit&>(*type).unit());
}

// duration to duration
Status CastImpl(const DurationScalar& from, DurationScalar* to) {
  return util::ConvertTimestampValue(AsTimestampType<DurationType>(from.type),
                                     AsTimestampType<DurationType>(to->type), from.value)
      .Value(&to->value);
}

// time to time
template <typename F, typename ToScalar, typename T = typename ToScalar::TypeClass>
enable_if_time<T, Status> CastImpl(const TimeScalar<F>& from, ToScalar* to) {
  return util::ConvertTimestampValue(AsTimestampType<F>(from.type),
                                     AsTimestampType<T>(to->type), from.value)
      .Value(&to->value);
}

constexpr int64_t kMillisecondsInDay = 86400000;

// date to date
Status CastImpl(const Date32Scalar& from, Date64Scalar* to) {
  to->value = from.value * kMillisecondsInDay;
  return Status::OK();
}
Status CastImpl(const Date64Scalar& from, Date32Scalar* to) {
  to->value = static_cast<int32_t>(from.value / kMillisecondsInDay);
  return Status::OK();
}

// timestamp to date
Status CastImpl(const TimestampScalar& from, Date64Scalar* to) {
  ARROW_ASSIGN_OR_RAISE(
      auto millis,
      util::ConvertTimestampValue(from.type, timestamp(TimeUnit::MILLI), from.value));
  to->value = millis - millis % kMillisecondsInDay;
  return Status::OK();
}
Status CastImpl(const TimestampScalar& from, Date32Scalar* to) {
  ARROW_ASSIGN_OR_RAISE(
      auto millis,
      util::ConvertTimestampValue(from.type, timestamp(TimeUnit::MILLI), from.value));
  to->value = static_cast<int32_t>(millis / kMillisecondsInDay);
  return Status::OK();
}

// date to timestamp
template <typename D>
Status CastImpl(const DateScalar<D>& from, TimestampScalar* to) {
  int64_t millis = from.value;
  if (std::is_same<D, Date32Type>::value) {
    millis *= kMillisecondsInDay;
  }
  return util::ConvertTimestampValue(timestamp(TimeUnit::MILLI), to->type, millis)
      .Value(&to->value);
}

// string to any
template <typename ScalarType>
Status CastImpl(const StringScalar& from, ScalarType* to) {
  ARROW_ASSIGN_OR_RAISE(auto out,
                        Scalar::Parse(to->type, util::string_view(*from.value)));
  to->value = std::move(checked_cast<ScalarType&>(*out).value);
  return Status::OK();
}

// binary to string
Status CastImpl(const BinaryScalar& from, StringScalar* to) {
  to->value = from.value;
  return Status::OK();
}

// formattable to string
template <typename ScalarType, typename T = typename ScalarType::TypeClass,
          typename Formatter = internal::StringFormatter<T>,
          // note: Value unused but necessary to trigger SFINAE if Formatter is
          // undefined
          typename Value = typename Formatter::value_type>
Status CastImpl(const ScalarType& from, StringScalar* to) {
  to->value = FormatToBuffer(Formatter{from.type}, from);
  return Status::OK();
}

Status CastImpl(const Decimal128Scalar& from, StringScalar* to) {
  auto from_type = checked_cast<const Decimal128Type*>(from.type.get());
  to->value = Buffer::FromString(from.value.ToString(from_type->scale()));
  return Status::OK();
}

Status CastImpl(const Decimal256Scalar& from, StringScalar* to) {
  auto from_type = checked_cast<const Decimal256Type*>(from.type.get());
  to->value = Buffer::FromString(from.value.ToString(from_type->scale()));
  return Status::OK();
}

Status CastImpl(const StructScalar& from, StringScalar* to) {
  std::stringstream ss;
  ss << '{';
  for (int i = 0; static_cast<size_t>(i) < from.value.size(); i++) {
    if (i > 0) ss << ", ";
    ss << from.type->field(i)->name() << ':' << from.type->field(i)->type()->ToString()
       << " = " << from.value[i]->ToString();
  }
  ss << '}';
  to->value = Buffer::FromString(ss.str());
  return Status::OK();
}

struct CastImplVisitor {
  Status NotImplemented() {
    return Status::NotImplemented("cast to ", *to_type_, " from ", *from_.type);
  }

  const Scalar& from_;
  const std::shared_ptr<DataType>& to_type_;
  Scalar* out_;
};

template <typename ToType>
struct FromTypeVisitor : CastImplVisitor {
  using ToScalar = typename TypeTraits<ToType>::ScalarType;

  FromTypeVisitor(const Scalar& from, const std::shared_ptr<DataType>& to_type,
                  Scalar* out)
      : CastImplVisitor{from, to_type, out} {}

  template <typename FromType>
  Status Visit(const FromType&) {
    return CastImpl(checked_cast<const typename TypeTraits<FromType>::ScalarType&>(from_),
                    checked_cast<ToScalar*>(out_));
  }

  // identity cast only for parameter free types
  template <typename T1 = ToType>
  typename std::enable_if<TypeTraits<T1>::is_parameter_free, Status>::type Visit(
      const ToType&) {
    checked_cast<ToScalar*>(out_)->value = checked_cast<const ToScalar&>(from_).value;
    return Status::OK();
  }

  Status Visit(const NullType&) { return NotImplemented(); }
  Status Visit(const SparseUnionType&) { return NotImplemented(); }
  Status Visit(const DenseUnionType&) { return NotImplemented(); }
  Status Visit(const DictionaryType&) { return NotImplemented(); }
  Status Visit(const ExtensionType&) { return NotImplemented(); }
};

struct ToTypeVisitor : CastImplVisitor {
  ToTypeVisitor(const Scalar& from, const std::shared_ptr<DataType>& to_type, Scalar* out)
      : CastImplVisitor{from, to_type, out} {}

  template <typename ToType>
  Status Visit(const ToType&) {
    FromTypeVisitor<ToType> unpack_from_type{from_, to_type_, out_};
    return VisitTypeInline(*from_.type, &unpack_from_type);
  }

  Status Visit(const NullType&) {
    if (from_.is_valid) {
      return Status::Invalid("attempting to cast non-null scalar to NullScalar");
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& dict_type) {
    auto& out = checked_cast<DictionaryScalar*>(out_)->value;
    ARROW_ASSIGN_OR_RAISE(auto cast_value, from_.CastTo(dict_type.value_type()));
    ARROW_ASSIGN_OR_RAISE(out.dictionary, MakeArrayFromScalar(*cast_value, 1));
    return Int32Scalar(0).CastTo(dict_type.index_type()).Value(&out.index);
  }

  Status Visit(const SparseUnionType&) { return NotImplemented(); }
  Status Visit(const DenseUnionType&) { return NotImplemented(); }
  Status Visit(const ExtensionType&) { return NotImplemented(); }
};

}  // namespace

Result<std::shared_ptr<Scalar>> Scalar::CastTo(std::shared_ptr<DataType> to) const {
  std::shared_ptr<Scalar> out = MakeNullScalar(to);
  if (is_valid) {
    out->is_valid = true;
    ToTypeVisitor unpack_to_type{*this, to, out.get()};
    RETURN_NOT_OK(VisitTypeInline(*to, &unpack_to_type));
  }
  return out;
}

}  // namespace arrow
