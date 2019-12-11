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
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/formatting.h"
#include "arrow/util/logging.h"
#include "arrow/util/parsing.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

bool Scalar::Equals(const Scalar& other) const { return ScalarEquals(*this, other); }

Time32Scalar::Time32Scalar(int32_t value, const std::shared_ptr<DataType>& type,
                           bool is_valid)
    : internal::PrimitiveScalar{type, is_valid}, value(value) {
  ARROW_CHECK_EQ(Type::TIME32, type->id());
}

Time64Scalar::Time64Scalar(int64_t value, const std::shared_ptr<DataType>& type,
                           bool is_valid)
    : internal::PrimitiveScalar{type, is_valid}, value(value) {
  ARROW_CHECK_EQ(Type::TIME64, type->id());
}

TimestampScalar::TimestampScalar(int64_t value, const std::shared_ptr<DataType>& type,
                                 bool is_valid)
    : internal::PrimitiveScalar{type, is_valid}, value(value) {
  ARROW_CHECK_EQ(Type::TIMESTAMP, type->id());
}

DurationScalar::DurationScalar(int64_t value, const std::shared_ptr<DataType>& type,
                               bool is_valid)
    : internal::PrimitiveScalar{type, is_valid}, value(value) {
  DCHECK_EQ(Type::DURATION, type->id());
}

MonthIntervalScalar::MonthIntervalScalar(int32_t value, bool is_valid)
    : internal::PrimitiveScalar{month_interval(), is_valid}, value(value) {}

MonthIntervalScalar::MonthIntervalScalar(int32_t value,
                                         const std::shared_ptr<DataType>& type,
                                         bool is_valid)
    : internal::PrimitiveScalar{type, is_valid}, value(value) {
  DCHECK_EQ(Type::INTERVAL, type->id());
  DCHECK_EQ(IntervalType::MONTHS,
            checked_cast<IntervalType*>(type.get())->interval_type());
}

DayTimeIntervalScalar::DayTimeIntervalScalar(DayTimeIntervalType::DayMilliseconds value,
                                             bool is_valid)
    : internal::PrimitiveScalar{day_time_interval(), is_valid}, value(value) {}

DayTimeIntervalScalar::DayTimeIntervalScalar(DayTimeIntervalType::DayMilliseconds value,
                                             const std::shared_ptr<DataType>& type,
                                             bool is_valid)
    : internal::PrimitiveScalar{type, is_valid}, value(value) {
  DCHECK_EQ(Type::INTERVAL, type->id());
  DCHECK_EQ(IntervalType::DAY_TIME,
            checked_cast<IntervalType*>(type.get())->interval_type());
}

StringScalar::StringScalar(std::string s)
    : StringScalar(Buffer::FromString(std::move(s)), true) {}

FixedSizeBinaryScalar::FixedSizeBinaryScalar(const std::shared_ptr<Buffer>& value,
                                             const std::shared_ptr<DataType>& type,
                                             bool is_valid)
    : BinaryScalar(value, type, is_valid) {
  ARROW_CHECK_EQ(checked_cast<const FixedSizeBinaryType&>(*type).byte_width(),
                 value->size());
}

Decimal128Scalar::Decimal128Scalar(const Decimal128& value,
                                   const std::shared_ptr<DataType>& type, bool is_valid)
    : Scalar{type, is_valid}, value(value) {}

BaseListScalar::BaseListScalar(const std::shared_ptr<Array>& value,
                               const std::shared_ptr<DataType>& type, bool is_valid)
    : Scalar{type, is_valid}, value(value) {}

BaseListScalar::BaseListScalar(const std::shared_ptr<Array>& value, bool is_valid)
    : BaseListScalar(value, value->type(), is_valid) {}

FixedSizeListScalar::FixedSizeListScalar(const std::shared_ptr<Array>& value,
                                         const std::shared_ptr<DataType>& type,
                                         bool is_valid)
    : BaseListScalar(value, type, is_valid) {
  ARROW_CHECK_EQ(value->length(),
                 checked_cast<const FixedSizeListType*>(type.get())->list_size());
}

// TODO(bkietz) This doesn't need a factory. Just rewrite all scalars to be generically
// constructible (is_simple_scalar should apply to all scalars)
struct MakeNullImpl {
  template <typename T, typename ScalarType = typename TypeTraits<T>::ScalarType,
            typename ValueType = typename ScalarType::ValueType,
            typename Enable = typename std::enable_if<
                internal::is_simple_scalar<ScalarType>::value>::type>
  Status Visit(const T&) {
    out_ = std::make_shared<ScalarType>(ValueType(), type_, false);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    out_ = std::make_shared<NullScalar>();
    return Status::OK();
  }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("construcing null scalars of type ", t);
  }

  const std::shared_ptr<DataType>& type_;
  std::shared_ptr<Scalar> out_;
};

Result<std::shared_ptr<Scalar>> MakeNullScalar(const std::shared_ptr<DataType>& type) {
  MakeNullImpl impl = {type, nullptr};
  RETURN_NOT_OK(VisitTypeInline(*type, &impl));
  return std::move(impl.out_);
}

std::string Scalar::ToString() const {
  auto maybe_repr = CastTo(utf8());
  if (maybe_repr.ok()) {
    return checked_cast<const StringScalar&>(*maybe_repr.ValueOrDie()).value->ToString();
  }
  return "...";
}

struct ScalarParseImpl {
  template <typename T, typename Converter = internal::StringConverter<T>,
            typename Value = typename Converter::value_type>
  Status Visit(const T& t) {
    Value value;
    if (!Converter{type_}(s_.data(), s_.size(), &value)) {
      return Status::Invalid("error parsing '", s_, "' as scalar of type ", t);
    }
    return Finish(std::move(value));
  }

  Status Visit(const BinaryType&) { return FinishWithBuffer(); }

  Status Visit(const LargeBinaryType&) { return FinishWithBuffer(); }

  Status Visit(const FixedSizeBinaryType& t) { return FinishWithBuffer(); }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("parsing scalars of type ", t);
  }

  template <typename Arg>
  Status Finish(Arg&& arg) {
    return MakeScalar(type_, std::forward<Arg>(arg)).Value(out_);
  }

  Status FinishWithBuffer() { return Finish(Buffer::FromString(s_.to_string())); }

  ScalarParseImpl(const std::shared_ptr<DataType>& type, util::string_view s,
                  std::shared_ptr<Scalar>* out)
      : type_(type), s_(s), out_(out) {}

  const std::shared_ptr<DataType>& type_;
  util::string_view s_;
  std::shared_ptr<Scalar>* out_;
};

Result<std::shared_ptr<Scalar>> Scalar::Parse(const std::shared_ptr<DataType>& type,
                                              util::string_view s) {
  std::shared_ptr<Scalar> out;
  ScalarParseImpl impl = {type, s, &out};
  RETURN_NOT_OK(VisitTypeInline(*type, &impl));
  return out;
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
          // note: Value unused but necessary to trigger SFINAE if Formatter is undefined
          typename Value = typename Formatter::value_type>
Status CastImpl(const ScalarType& from, StringScalar* to) {
  if (!from.is_valid) {
    to->value = Buffer::FromString("null");
    return Status::OK();
  }

  return Formatter{from.type}(from.value, [to](util::string_view v) {
    to->value = Buffer::FromString(v.to_string());
    return Status::OK();
  });
}

template <typename ToType>
struct FromTypeVisitor {
  using ToScalar = typename TypeTraits<ToType>::ScalarType;

  template <typename FromType>
  Status Visit(const FromType&) {
    return CastImpl(checked_cast<const typename TypeTraits<FromType>::ScalarType&>(from_),
                    out_);
  }

  // identity cast
  Status Visit(const ToType&) {
    out_->value = checked_cast<const ToScalar&>(from_).value;
    return Status::OK();
  }

  // null to any
  Status Visit(const NullType&) {
    return Status::Invalid("attempting to cast scalar of type null to ", *to_type_);
  }

  Status Visit(const UnionType&) { return Status::NotImplemented("cast to ", *to_type_); }
  Status Visit(const DictionaryType&) {
    return Status::NotImplemented("cast to ", *to_type_);
  }
  Status Visit(const ExtensionType&) {
    return Status::NotImplemented("cast to ", *to_type_);
  }

  const Scalar& from_;
  const std::shared_ptr<DataType>& to_type_;
  ToScalar* out_;
};

struct ToTypeVisitor {
  template <typename ToType>
  Status Visit(const ToType&) {
    using ToScalar = typename TypeTraits<ToType>::ScalarType;
    FromTypeVisitor<ToType> unpack_from_type{from_, to_type_,
                                             checked_cast<ToScalar*>(out_)};
    return VisitTypeInline(*from_.type, &unpack_from_type);
  }

  Status Visit(const NullType&) {
    if (from_.is_valid) {
      return Status::Invalid("attempting to cast non-null scalar to NullScalar");
    }
    return Status::OK();
  }

  Status Visit(const UnionType&) {
    return Status::NotImplemented("cast from ", *from_.type);
  }
  Status Visit(const DictionaryType&) {
    return Status::NotImplemented("cast from ", *from_.type);
  }
  Status Visit(const ExtensionType&) {
    return Status::NotImplemented("cast from ", *from_.type);
  }

  const Scalar& from_;
  const std::shared_ptr<DataType>& to_type_;
  Scalar* out_;
};

}  // namespace

Result<std::shared_ptr<Scalar>> Scalar::CastTo(std::shared_ptr<DataType> to) const {
  ARROW_ASSIGN_OR_RAISE(auto out, MakeNullScalar(to));
  if (is_valid) {
    out->is_valid = true;
    ToTypeVisitor unpack_to_type{*this, to, out.get()};
    RETURN_NOT_OK(VisitTypeInline(*to, &unpack_to_type));
  }
  return out;
}

}  // namespace arrow
