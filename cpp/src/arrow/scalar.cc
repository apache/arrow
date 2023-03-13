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
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/pretty_print.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/formatting.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/time.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visit_scalar_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

bool Scalar::Equals(const Scalar& other, const EqualOptions& options) const {
  return ScalarEquals(*this, other, options);
}

bool Scalar::ApproxEquals(const Scalar& other, const EqualOptions& options) const {
  return ScalarApproxEquals(*this, other, options);
}

Status Scalar::Accept(ScalarVisitor* visitor) const {
  return VisitScalarInline(*this, visitor);
}

namespace {

// Implementation of Scalar::hash()
struct ScalarHashImpl {
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
    return StdHash(s.value.days) & StdHash(s.value.milliseconds);
  }

  Status Visit(const MonthDayNanoIntervalScalar& s) {
    return StdHash(s.value.days) & StdHash(s.value.months) & StdHash(s.value.nanoseconds);
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

  Status Visit(const DenseUnionScalar& s) {
    // type_code is ignored when comparing for equality, so do not hash it either
    AccumulateHashFrom(*s.value);
    return Status::OK();
  }

  Status Visit(const SparseUnionScalar& s) {
    // type_code is ignored when comparing for equality, so do not hash it either
    AccumulateHashFrom(*s.value[s.child_id]);
    return Status::OK();
  }

  Status Visit(const RunEndEncodedScalar& s) {
    AccumulateHashFrom(*s.value);
    return Status::OK();
  }

  Status Visit(const ExtensionScalar& s) {
    AccumulateHashFrom(*s.value);
    return Status::OK();
  }

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
    AccumulateHashFrom(scalar);
  }

  void AccumulateHashFrom(const Scalar& scalar) {
    // Note we already injected the type in ScalarHashImpl::ScalarHashImpl
    if (scalar.is_valid) {
      DCHECK_OK(VisitScalarInline(scalar, this));
    }
  }

  size_t hash_;
};

struct ScalarBoundsCheckImpl {
  int64_t min_value;
  int64_t max_value;
  int64_t actual_value = -1;
  bool ok = true;

  ScalarBoundsCheckImpl(int64_t min_value, int64_t max_value)
      : min_value(min_value), max_value(max_value) {}

  Status Visit(const Scalar&) {
    Unreachable();
    return Status::NotImplemented("");
  }

  template <typename ScalarType, typename Type = typename ScalarType::TypeClass>
  enable_if_integer<Type, Status> Visit(const ScalarType& scalar) {
    actual_value = static_cast<int64_t>(scalar.value);
    ok = (actual_value >= min_value && actual_value <= max_value);
    return Status::OK();
  }
};

// Implementation of Scalar::Validate() and Scalar::ValidateFull()
struct ScalarValidateImpl {
  const bool full_validation_;

  explicit ScalarValidateImpl(bool full_validation) : full_validation_(full_validation) {
    ::arrow::util::InitializeUTF8();
  }

  Status Validate(const Scalar& scalar) {
    if (!scalar.type) {
      return Status::Invalid("scalar lacks a type");
    }
    return VisitScalarInline(scalar, this);
  }

  Status Visit(const NullScalar& s) {
    if (s.is_valid) {
      return Status::Invalid("null scalar should have is_valid = false");
    }
    return Status::OK();
  }

  template <typename T>
  Status Visit(const internal::PrimitiveScalar<T>& s) {
    return Status::OK();
  }

  Status Visit(const BaseBinaryScalar& s) { return ValidateBinaryScalar(s); }

  Status Visit(const StringScalar& s) { return ValidateStringScalar(s); }

  Status Visit(const LargeStringScalar& s) { return ValidateStringScalar(s); }

  template <typename ScalarType>
  Status CheckValueNotNull(const ScalarType& s) {
    if (!s.value) {
      return Status::Invalid(s.type->ToString(), " value is null");
    }
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryScalar& s) {
    const auto& byte_width =
        checked_cast<const FixedSizeBinaryType&>(*s.type).byte_width();
    RETURN_NOT_OK(CheckValueNotNull(s));
    if (s.value->size() != byte_width) {
      return Status::Invalid(s.type->ToString(), " scalar should have a value of size ",
                             byte_width, ", got ", s.value->size());
    }
    return Status::OK();
  }

  Status Visit(const Decimal128Scalar& s) {
    const auto& ty = checked_cast<const DecimalType&>(*s.type);
    if (!s.value.FitsInPrecision(ty.precision())) {
      return Status::Invalid("Decimal value ", s.value.ToIntegerString(),
                             " does not fit in precision of ", ty);
    }
    return Status::OK();
  }

  Status Visit(const Decimal256Scalar& s) {
    const auto& ty = checked_cast<const DecimalType&>(*s.type);
    if (!s.value.FitsInPrecision(ty.precision())) {
      return Status::Invalid("Decimal value ", s.value.ToIntegerString(),
                             " does not fit in precision of ", ty);
    }
    return Status::OK();
  }

  Status Visit(const BaseListScalar& s) {
    RETURN_NOT_OK(CheckValueNotNull(s));
    const auto st = full_validation_ ? s.value->ValidateFull() : s.value->Validate();
    if (!st.ok()) {
      return st.WithMessage(s.type->ToString(),
                            " scalar fails validation for value: ", st.message());
    }

    const auto& list_type = checked_cast<const BaseListType&>(*s.type);
    const auto& value_type = *list_type.value_type();
    if (!s.value->type()->Equals(value_type)) {
      return Status::Invalid(list_type.ToString(), " scalar should have a value of type ",
                             value_type.ToString(), ", got ",
                             s.value->type()->ToString());
    }
    return Status::OK();
  }

  Status Visit(const FixedSizeListScalar& s) {
    RETURN_NOT_OK(Visit(static_cast<const BaseListScalar&>(s)));
    const auto& list_type = checked_cast<const FixedSizeListType&>(*s.type);
    if (s.value->length() != list_type.list_size()) {
      return Status::Invalid(s.type->ToString(),
                             " scalar should have a child value of length ",
                             list_type.list_size(), ", got ", s.value->length());
    }
    return Status::OK();
  }

  Status Visit(const StructScalar& s) {
    const int num_fields = s.type->num_fields();
    const auto& fields = s.type->fields();
    if (fields.size() != s.value.size()) {
      return Status::Invalid("non-null ", s.type->ToString(), " scalar should have ",
                             num_fields, " child values, got ", s.value.size());
    }
    for (int i = 0; i < num_fields; ++i) {
      const auto st = Validate(*s.value[i]);
      if (!st.ok()) {
        return st.WithMessage(s.type->ToString(),
                              " scalar fails validation for child at index ", i, ": ",
                              st.message());
      }
      if (!s.value[i]->type->Equals(*fields[i]->type())) {
        return Status::Invalid(
            s.type->ToString(), " scalar should have a child value of type ",
            fields[i]->type()->ToString(), "at index ", i, ", got ", s.value[i]->type);
      }
    }
    return Status::OK();
  }

  Status Visit(const DictionaryScalar& s) {
    const auto& dict_type = checked_cast<const DictionaryType&>(*s.type);

    // Validate index
    if (!s.value.index) {
      return Status::Invalid(s.type->ToString(), " scalar doesn't have an index value");
    }
    {
      const auto st = Validate(*s.value.index);
      if (!st.ok()) {
        return st.WithMessage(s.type->ToString(),
                              " scalar fails validation for index value: ", st.message());
      }
    }
    if (!s.value.index->type->Equals(*dict_type.index_type())) {
      return Status::Invalid(
          s.type->ToString(), " scalar should have an index value of type ",
          dict_type.index_type()->ToString(), ", got ", s.value.index->type->ToString());
    }
    if (s.is_valid && !s.value.index->is_valid) {
      return Status::Invalid("non-null ", s.type->ToString(),
                             " scalar has null index value");
    }
    if (!s.is_valid && s.value.index->is_valid) {
      return Status::Invalid("null ", s.type->ToString(),
                             " scalar has non-null index value");
    }

    // Validate dictionary
    if (!s.value.dictionary) {
      return Status::Invalid(s.type->ToString(),
                             " scalar doesn't have a dictionary value");
    }
    {
      const auto st = full_validation_ ? s.value.dictionary->ValidateFull()
                                       : s.value.dictionary->Validate();
      if (!st.ok()) {
        return st.WithMessage(
            s.type->ToString(),
            " scalar fails validation for dictionary value: ", st.message());
      }
    }
    if (!s.value.dictionary->type()->Equals(*dict_type.value_type())) {
      return Status::Invalid(s.type->ToString(),
                             " scalar should have a dictionary value of type ",
                             dict_type.value_type()->ToString(), ", got ",
                             s.value.dictionary->type()->ToString());
    }

    // Check index is in bounds
    if (full_validation_ && s.value.index->is_valid) {
      ScalarBoundsCheckImpl bounds_checker{0, s.value.dictionary->length() - 1};
      RETURN_NOT_OK(VisitScalarInline(*s.value.index, &bounds_checker));
      if (!bounds_checker.ok) {
        return Status::Invalid(s.type->ToString(), " scalar index value out of bounds: ",
                               bounds_checker.actual_value);
      }
    }
    return Status::OK();
  }

  Status ValidateValue(const Scalar& s, const Scalar& value) {
    const auto st = Validate(value);
    if (!st.ok()) {
      return st.WithMessage(
          s.type->ToString(),
          " scalar fails validation for underlying value: ", st.message());
    }
    return Status::OK();
  }

  Status ValidateDenseUnion(const DenseUnionScalar& s, int child_id) {
    const auto& union_type = checked_cast<const DenseUnionType&>(*s.type);
    const auto& field_type = *union_type.field(child_id)->type();
    if (!field_type.Equals(*s.value->type)) {
      return Status::Invalid(s.type->ToString(), " scalar with type code ", s.type_code,
                             " should have an underlying value of type ",
                             field_type.ToString(), ", got ", s.value->type->ToString());
    }
    return ValidateValue(s, *s.value);
  }

  Status ValidateSparseUnion(const SparseUnionScalar& s) {
    const auto& union_type = checked_cast<const SparseUnionType&>(*s.type);
    if (union_type.num_fields() != static_cast<int>(s.value.size())) {
      return Status::Invalid("Sparse union scalar value had ", union_type.num_fields(),
                             " fields but type has ", s.value.size(), " fields.");
    }
    for (int j = 0; j < union_type.num_fields(); ++j) {
      const auto& field_type = *union_type.field(j)->type();
      const Scalar& field_value = *s.value[j];
      if (!field_type.Equals(*field_value.type)) {
        return Status::Invalid(s.type->ToString(), " value for field ",
                               union_type.field(j)->ToString(), " had incorrect type of ",
                               field_value.type->ToString());
      }
      RETURN_NOT_OK(ValidateValue(s, field_value));
    }
    return Status::OK();
  }

  Status Visit(const UnionScalar& s) {
    const int type_code = s.type_code;  // avoid 8-bit int types for printing
    const auto& union_type = checked_cast<const UnionType&>(*s.type);
    const auto& child_ids = union_type.child_ids();
    if (type_code < 0 || type_code >= static_cast<int64_t>(child_ids.size()) ||
        child_ids[type_code] == UnionType::kInvalidChildId) {
      return Status::Invalid(s.type->ToString(), " scalar has invalid type code ",
                             type_code);
    }
    if (union_type.id() == Type::DENSE_UNION) {
      return ValidateDenseUnion(checked_cast<const DenseUnionScalar&>(s),
                                child_ids[type_code]);
    } else {
      return ValidateSparseUnion(checked_cast<const SparseUnionScalar&>(s));
    }
  }

  Status Visit(const RunEndEncodedScalar& s) {
    const auto& ree_type = checked_cast<const RunEndEncodedType&>(*s.type);
    if (!s.value) {
      return Status::Invalid(s.type->ToString(), " scalar doesn't have storage value");
    }
    if (!s.is_valid && s.value->is_valid) {
      return Status::Invalid("null ", s.type->ToString(),
                             " scalar has non-null storage value");
    }
    if (s.is_valid && !s.value->is_valid) {
      return Status::Invalid("non-null ", s.type->ToString(),
                             " scalar has null storage value");
    }
    if (!ree_type.value_type()->Equals(*s.value->type)) {
      return Status::Invalid(
          ree_type.ToString(), " scalar should have an underlying value of type ",
          ree_type.value_type()->ToString(), ", got ", s.value->type->ToString());
    }
    return ValidateValue(s, *s.value);
  }

  Status Visit(const ExtensionScalar& s) {
    if (!s.value) {
      return Status::Invalid(s.type->ToString(), " scalar doesn't have storage value");
    }
    if (!s.is_valid && s.value->is_valid) {
      return Status::Invalid("null ", s.type->ToString(),
                             " scalar has non-null storage value");
    }
    if (s.is_valid && !s.value->is_valid) {
      return Status::Invalid("non-null ", s.type->ToString(),
                             " scalar has null storage value");
    }
    const auto st = Validate(*s.value);
    if (!st.ok()) {
      return st.WithMessage(s.type->ToString(),
                            " scalar fails validation for storage value: ", st.message());
    }
    return Status::OK();
  }

  Status ValidateStringScalar(const BaseBinaryScalar& s) {
    RETURN_NOT_OK(ValidateBinaryScalar(s));
    if (s.is_valid && full_validation_) {
      if (!::arrow::util::ValidateUTF8(s.value->data(), s.value->size())) {
        return Status::Invalid(s.type->ToString(), " scalar contains invalid UTF8 data");
      }
    }
    return Status::OK();
  }

  Status ValidateBinaryScalar(const BaseBinaryScalar& s) {
    if (s.is_valid && !s.value) {
      return Status::Invalid(s.type->ToString(),
                             " scalar is marked valid but doesn't have a value");
    }
    if (!s.is_valid && s.value) {
      return Status::Invalid(s.type->ToString(),
                             " scalar is marked null but has a value");
    }
    return Status::OK();
  }
};

}  // namespace

size_t Scalar::hash() const { return ScalarHashImpl(*this).hash_; }

Status Scalar::Validate() const {
  return ScalarValidateImpl(/*full_validation=*/false).Validate(*this);
}

Status Scalar::ValidateFull() const {
  return ScalarValidateImpl(/*full_validation=*/true).Validate(*this);
}

BinaryScalar::BinaryScalar(std::string s)
    : BinaryScalar(Buffer::FromString(std::move(s))) {}

StringScalar::StringScalar(std::string s)
    : StringScalar(Buffer::FromString(std::move(s))) {}

LargeBinaryScalar::LargeBinaryScalar(std::string s)
    : LargeBinaryScalar(Buffer::FromString(std::move(s))) {}

LargeStringScalar::LargeStringScalar(std::string s)
    : LargeStringScalar(Buffer::FromString(std::move(s))) {}

FixedSizeBinaryScalar::FixedSizeBinaryScalar(std::shared_ptr<Buffer> value,
                                             std::shared_ptr<DataType> type,
                                             bool is_valid)
    : BinaryScalar(std::move(value), std::move(type)) {
  ARROW_CHECK_EQ(checked_cast<const FixedSizeBinaryType&>(*this->type).byte_width(),
                 this->value->size());
  this->is_valid = is_valid;
}

FixedSizeBinaryScalar::FixedSizeBinaryScalar(const std::shared_ptr<Buffer>& value,
                                             bool is_valid)
    : BinaryScalar(value, fixed_size_binary(static_cast<int>(value->size()))) {
  this->is_valid = is_valid;
}

FixedSizeBinaryScalar::FixedSizeBinaryScalar(std::string s, bool is_valid)
    : FixedSizeBinaryScalar(Buffer::FromString(std::move(s)), is_valid) {}

BaseListScalar::BaseListScalar(std::shared_ptr<Array> value,
                               std::shared_ptr<DataType> type, bool is_valid)
    : Scalar{std::move(type), is_valid}, value(std::move(value)) {
  ARROW_CHECK(this->type->field(0)->type()->Equals(this->value->type()));
}

ListScalar::ListScalar(std::shared_ptr<Array> value, bool is_valid)
    : BaseListScalar(value, list(value->type()), is_valid) {}

LargeListScalar::LargeListScalar(std::shared_ptr<Array> value, bool is_valid)
    : BaseListScalar(value, large_list(value->type()), is_valid) {}

inline std::shared_ptr<DataType> MakeMapType(const std::shared_ptr<DataType>& pair_type) {
  ARROW_CHECK_EQ(pair_type->id(), Type::STRUCT);
  ARROW_CHECK_EQ(pair_type->num_fields(), 2);
  return map(pair_type->field(0)->type(), pair_type->field(1)->type());
}

MapScalar::MapScalar(std::shared_ptr<Array> value, bool is_valid)
    : BaseListScalar(value, MakeMapType(value->type()), is_valid) {}

FixedSizeListScalar::FixedSizeListScalar(std::shared_ptr<Array> value,
                                         std::shared_ptr<DataType> type, bool is_valid)
    : BaseListScalar(value, std::move(type), is_valid) {
  ARROW_CHECK_EQ(this->value->length(),
                 checked_cast<const FixedSizeListType&>(*this->type).list_size());
}

FixedSizeListScalar::FixedSizeListScalar(std::shared_ptr<Array> value, bool is_valid)
    : BaseListScalar(
          value, fixed_size_list(value->type(), static_cast<int32_t>(value->length())),
          is_valid) {}

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

RunEndEncodedScalar::RunEndEncodedScalar(std::shared_ptr<Scalar> value,
                                         std::shared_ptr<DataType> type)
    : Scalar{std::move(type), value->is_valid}, value{std::move(value)} {
  ARROW_CHECK_EQ(this->type->id(), Type::RUN_END_ENCODED);
}

RunEndEncodedScalar::RunEndEncodedScalar(const std::shared_ptr<DataType>& type)
    : RunEndEncodedScalar(
          MakeNullScalar(checked_cast<const RunEndEncodedType&>(*type).value_type()),
          type) {}

RunEndEncodedScalar::~RunEndEncodedScalar() = default;

DictionaryScalar::DictionaryScalar(std::shared_ptr<DataType> type)
    : internal::PrimitiveScalarBase(std::move(type)),
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
  auto is_valid = index->is_valid;
  return std::make_shared<DictionaryScalar>(ValueType{std::move(index), std::move(dict)},
                                            std::move(type), is_valid);
}

Result<TimestampScalar> TimestampScalar::FromISO8601(std::string_view iso8601,
                                                     TimeUnit::type unit) {
  ValueType value;
  if (internal::ParseTimestampISO8601(iso8601.data(), iso8601.size(), unit, &value)) {
    return TimestampScalar{value, timestamp(unit)};
  }
  return Status::Invalid("Couldn't parse ", iso8601, " as a timestamp");
}

SparseUnionScalar::SparseUnionScalar(ValueType value, int8_t type_code,
                                     std::shared_ptr<DataType> type)
    : UnionScalar(std::move(type), type_code, /*is_valid=*/true),
      value(std::move(value)) {
  this->child_id =
      checked_cast<const SparseUnionType&>(*this->type).child_ids()[type_code];

  // Fix nullness based on whether the selected child is null
  this->is_valid = this->value[this->child_id]->is_valid;
}

std::shared_ptr<Scalar> SparseUnionScalar::FromValue(std::shared_ptr<Scalar> value,
                                                     int field_index,
                                                     std::shared_ptr<DataType> type) {
  const auto& union_type = checked_cast<const SparseUnionType&>(*type);
  int8_t type_code = union_type.type_codes()[field_index];
  ScalarVector field_values;
  for (int i = 0; i < type->num_fields(); ++i) {
    if (i == field_index) {
      field_values.emplace_back(std::move(value));
    } else {
      field_values.emplace_back(MakeNullScalar(type->field(i)->type()));
    }
  }
  return std::make_shared<SparseUnionScalar>(field_values, type_code, std::move(type));
}

namespace {

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

  template <typename T, typename ScalarType = typename TypeTraits<T>::ScalarType>
  Status VisitListLike(const T& type, int64_t value_size = 0) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> value,
                          MakeArrayOfNull(type.value_type(), value_size));
    out_ = std::make_shared<ScalarType>(std::move(value), type_, /*is_valid=*/false);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType& type) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> value,
                          AllocateBuffer(type.byte_width()));
    // Avoid exposing past memory contents
    memset(value->mutable_data(), 0, value->size());
    out_ = std::make_shared<FixedSizeBinaryScalar>(std::move(value), type_,
                                                   /*is_valid=*/false);
    return Status::OK();
  }

  Status Visit(const ListType& type) { return VisitListLike<ListType>(type); }

  Status Visit(const MapType& type) { return VisitListLike<MapType>(type); }

  Status Visit(const LargeListType& type) { return VisitListLike<LargeListType>(type); }

  Status Visit(const FixedSizeListType& type) {
    return VisitListLike<FixedSizeListType>(type, type.list_size());
  }

  Status Visit(const StructType& type) {
    ScalarVector field_values;
    for (int i = 0; i < type.num_fields(); ++i) {
      field_values.push_back(MakeNullScalar(type.field(i)->type()));
    }
    out_ = std::make_shared<StructScalar>(std::move(field_values), type_,
                                          /*is_valid=*/false);
    return Status::OK();
  }

  Status Visit(const SparseUnionType& type) {
    if (type.num_fields() == 0) {
      return Status::Invalid("Cannot make scalar of empty union type");
    }
    ScalarVector field_values;
    for (int i = 0; i < type.num_fields(); ++i) {
      field_values.emplace_back(MakeNullScalar(type.field(i)->type()));
    }
    out_ = std::make_shared<SparseUnionScalar>(std::move(field_values),
                                               type.type_codes()[0], type_);
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) {
    if (type.num_fields() == 0) {
      return Status::Invalid("Cannot make scalar of empty union type");
    }
    out_ = std::make_shared<DenseUnionScalar>(MakeNullScalar(type.field(0)->type()),
                                              type.type_codes()[0], type_);
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    out_ = std::make_shared<RunEndEncodedScalar>(type_);
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    out_ = std::make_shared<ExtensionScalar>(MakeNullScalar(type.storage_type()), type_,
                                             /*is_valid=*/false);
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

}  // namespace

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

  std::string result;
  std::shared_ptr<Array> as_array = *MakeArrayFromScalar(*this, 1);
  DCHECK_OK(PrettyPrint(*as_array, PrettyPrintOptions::Defaults(), &result));
  return result;
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

  ScalarParseImpl(std::shared_ptr<DataType> type, std::string_view s)
      : type_(std::move(type)), s_(s) {}

  std::shared_ptr<DataType> type_;
  std::string_view s_;
  std::shared_ptr<Scalar> out_;
};

Result<std::shared_ptr<Scalar>> Scalar::Parse(const std::shared_ptr<DataType>& type,
                                              std::string_view s) {
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
  return formatter(
      from.value, [&](std::string_view v) { return Buffer::FromString(std::string(v)); });
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
                            !std::is_same<DayTimeIntervalType, To>::value &&
                            !std::is_same<MonthDayNanoIntervalType, To>::value,
                        Status>::type
CastImpl(const NumericScalar<From>& from, TemporalScalar<To>* to) {
  to->value = static_cast<typename To::c_type>(from.value);
  return Status::OK();
}

// temporal to numeric
template <typename From, typename To>
typename std::enable_if<std::is_base_of<TemporalType, From>::value &&
                            !std::is_same<DayTimeIntervalType, From>::value &&
                            !std::is_same<MonthDayNanoIntervalType, From>::value,
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
  ARROW_ASSIGN_OR_RAISE(auto out, Scalar::Parse(to->type, std::string_view(*from.value)));
  to->value = std::move(checked_cast<ScalarType&>(*out).value);
  return Status::OK();
}

// binary/large binary/large string to string
template <typename ScalarType>
enable_if_t<std::is_base_of_v<BaseBinaryScalar, ScalarType> &&
                !std::is_same<ScalarType, StringScalar>::value,
            Status>
CastImpl(const ScalarType& from, StringScalar* to) {
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
  to->value = FormatToBuffer(Formatter{from.type.get()}, from);
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

// list based types (list, large list and map (fixed sized list too)) to string
Status CastImpl(const BaseListScalar& from, StringScalar* to) {
  std::stringstream ss;
  ss << from.type->ToString() << "[";
  for (int64_t i = 0; i < from.value->length(); i++) {
    if (i > 0) ss << ", ";
    ARROW_ASSIGN_OR_RAISE(auto value, from.value->GetScalar(i));
    ss << value->ToString();
  }
  ss << ']';
  to->value = Buffer::FromString(ss.str());
  return Status::OK();
}

Status CastImpl(const UnionScalar& from, StringScalar* to) {
  const auto& union_ty = checked_cast<const UnionType&>(*from.type);
  std::stringstream ss;
  const Scalar* selected_value;
  if (from.type->id() == Type::DENSE_UNION) {
    selected_value = checked_cast<const DenseUnionScalar&>(from).value.get();
  } else {
    const auto& sparse_scalar = checked_cast<const SparseUnionScalar&>(from);
    selected_value = sparse_scalar.value[sparse_scalar.child_id].get();
  }
  ss << "union{" << union_ty.field(union_ty.child_ids()[from.type_code])->ToString()
     << " = " << selected_value->ToString() << '}';
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

void PrintTo(const Scalar& scalar, std::ostream* os) { *os << scalar.ToString(); }

}  // namespace arrow
