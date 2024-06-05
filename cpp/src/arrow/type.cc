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

#include "arrow/type.h"

#include <algorithm>
#include <climits>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/compare.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/hash_util.h"
#include "arrow/util/hashing.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"
#include "arrow/util/string.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/vector.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;

constexpr Type::type NullType::type_id;
constexpr Type::type ListType::type_id;
constexpr Type::type LargeListType::type_id;

constexpr Type::type MapType::type_id;

constexpr Type::type FixedSizeListType::type_id;

constexpr Type::type BinaryType::type_id;

constexpr Type::type BinaryViewType::type_id;

constexpr Type::type LargeBinaryType::type_id;

constexpr Type::type StringType::type_id;

constexpr Type::type StringViewType::type_id;

constexpr Type::type LargeStringType::type_id;

constexpr Type::type FixedSizeBinaryType::type_id;

constexpr Type::type StructType::type_id;

constexpr Type::type Decimal128Type::type_id;

constexpr Type::type Decimal256Type::type_id;

constexpr Type::type SparseUnionType::type_id;

constexpr Type::type DenseUnionType::type_id;

constexpr Type::type Date32Type::type_id;

constexpr Type::type Date64Type::type_id;

constexpr Type::type Time32Type::type_id;

constexpr Type::type Time64Type::type_id;

constexpr Type::type TimestampType::type_id;

constexpr Type::type MonthIntervalType::type_id;

constexpr Type::type DayTimeIntervalType::type_id;

constexpr Type::type MonthDayNanoIntervalType::type_id;

constexpr Type::type DurationType::type_id;

constexpr Type::type DictionaryType::type_id;

std::vector<Type::type> AllTypeIds() {
  return {Type::NA,
          Type::BOOL,
          Type::INT8,
          Type::INT16,
          Type::INT32,
          Type::INT64,
          Type::UINT8,
          Type::UINT16,
          Type::UINT32,
          Type::UINT64,
          Type::HALF_FLOAT,
          Type::FLOAT,
          Type::DOUBLE,
          Type::DECIMAL128,
          Type::DECIMAL256,
          Type::DATE32,
          Type::DATE64,
          Type::TIME32,
          Type::TIME64,
          Type::TIMESTAMP,
          Type::INTERVAL_DAY_TIME,
          Type::INTERVAL_MONTHS,
          Type::DURATION,
          Type::STRING,
          Type::BINARY,
          Type::LARGE_STRING,
          Type::LARGE_BINARY,
          Type::STRING_VIEW,
          Type::BINARY_VIEW,
          Type::FIXED_SIZE_BINARY,
          Type::STRUCT,
          Type::LIST,
          Type::LARGE_LIST,
          Type::LIST_VIEW,
          Type::LARGE_LIST_VIEW,
          Type::FIXED_SIZE_LIST,
          Type::MAP,
          Type::DENSE_UNION,
          Type::SPARSE_UNION,
          Type::DICTIONARY,
          Type::EXTENSION,
          Type::INTERVAL_MONTH_DAY_NANO,
          Type::RUN_END_ENCODED};
}

namespace internal {

struct TypeIdToTypeNameVisitor {
  std::string out;

  template <typename ArrowType>
  Status Visit(const ArrowType*) {
    out = ArrowType::type_name();
    return Status::OK();
  }
};

std::string ToTypeName(Type::type id) {
  TypeIdToTypeNameVisitor visitor;

  ARROW_CHECK_OK(VisitTypeIdInline(id, &visitor));
  return std::move(visitor.out);
}

std::string ToString(Type::type id) {
  switch (id) {
#define TO_STRING_CASE(_id) \
  case Type::_id:           \
    return ARROW_STRINGIFY(_id);

    TO_STRING_CASE(NA)
    TO_STRING_CASE(BOOL)
    TO_STRING_CASE(INT8)
    TO_STRING_CASE(INT16)
    TO_STRING_CASE(INT32)
    TO_STRING_CASE(INT64)
    TO_STRING_CASE(UINT8)
    TO_STRING_CASE(UINT16)
    TO_STRING_CASE(UINT32)
    TO_STRING_CASE(UINT64)
    TO_STRING_CASE(HALF_FLOAT)
    TO_STRING_CASE(FLOAT)
    TO_STRING_CASE(DOUBLE)
    TO_STRING_CASE(DECIMAL128)
    TO_STRING_CASE(DECIMAL256)
    TO_STRING_CASE(DATE32)
    TO_STRING_CASE(DATE64)
    TO_STRING_CASE(TIME32)
    TO_STRING_CASE(TIME64)
    TO_STRING_CASE(TIMESTAMP)
    TO_STRING_CASE(INTERVAL_DAY_TIME)
    TO_STRING_CASE(INTERVAL_MONTH_DAY_NANO)
    TO_STRING_CASE(INTERVAL_MONTHS)
    TO_STRING_CASE(DURATION)
    TO_STRING_CASE(STRING)
    TO_STRING_CASE(STRING_VIEW)
    TO_STRING_CASE(BINARY)
    TO_STRING_CASE(BINARY_VIEW)
    TO_STRING_CASE(LARGE_STRING)
    TO_STRING_CASE(LARGE_BINARY)
    TO_STRING_CASE(FIXED_SIZE_BINARY)
    TO_STRING_CASE(STRUCT)
    TO_STRING_CASE(LIST)
    TO_STRING_CASE(LARGE_LIST)
    TO_STRING_CASE(LIST_VIEW)
    TO_STRING_CASE(LARGE_LIST_VIEW)
    TO_STRING_CASE(FIXED_SIZE_LIST)
    TO_STRING_CASE(MAP)
    TO_STRING_CASE(DENSE_UNION)
    TO_STRING_CASE(SPARSE_UNION)
    TO_STRING_CASE(DICTIONARY)
    TO_STRING_CASE(RUN_END_ENCODED)
    TO_STRING_CASE(EXTENSION)

#undef TO_STRING_CASE

    default:
      ARROW_LOG(FATAL) << "Unhandled type id: " << id;
      return "";
  }
}

std::string ToString(TimeUnit::type unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      return "s";
    case TimeUnit::MILLI:
      return "ms";
    case TimeUnit::MICRO:
      return "us";
    case TimeUnit::NANO:
      return "ns";
    default:
      DCHECK(false);
      return "";
  }
}

}  // namespace internal

namespace {

struct PhysicalTypeVisitor {
  const std::shared_ptr<DataType>& real_type;
  std::shared_ptr<DataType> result;

  Status Visit(const DataType&) {
    result = real_type;
    return Status::OK();
  }

  template <typename Type, typename PhysicalType = typename Type::PhysicalType>
  Status Visit(const Type& type) {
    result = TypeTraits<PhysicalType>::type_singleton();
    return Status::OK();
  }
};

}  // namespace

std::shared_ptr<DataType> GetPhysicalType(const std::shared_ptr<DataType>& real_type) {
  PhysicalTypeVisitor visitor{real_type, {}};
  ARROW_CHECK_OK(VisitTypeInline(*real_type, &visitor));
  return std::move(visitor.result);
}

namespace {

using internal::checked_cast;

FieldVector MakeFields(
    std::initializer_list<std::pair<std::string, std::shared_ptr<DataType>>> init_list) {
  FieldVector fields;
  fields.reserve(init_list.size());
  for (const auto& [name, type] : init_list) {
    fields.push_back(field(name, type));
  }
  return fields;
}

}  // namespace

Field::~Field() {}

bool Field::HasMetadata() const {
  return (metadata_ != nullptr) && (metadata_->size() > 0);
}

std::shared_ptr<Field> Field::WithMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) const {
  return std::make_shared<Field>(name_, type_, nullable_, metadata);
}

std::shared_ptr<Field> Field::WithMergedMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) const {
  std::shared_ptr<const KeyValueMetadata> merged_metadata;
  if (metadata_) {
    merged_metadata = metadata_->Merge(*metadata);
  } else {
    merged_metadata = metadata;
  }
  return std::make_shared<Field>(name_, type_, nullable_, merged_metadata);
}

std::shared_ptr<Field> Field::RemoveMetadata() const {
  return std::make_shared<Field>(name_, type_, nullable_);
}

std::shared_ptr<Field> Field::WithType(const std::shared_ptr<DataType>& type) const {
  return std::make_shared<Field>(name_, type, nullable_, metadata_);
}

std::shared_ptr<Field> Field::WithName(const std::string& name) const {
  return std::make_shared<Field>(name, type_, nullable_, metadata_);
}

std::shared_ptr<Field> Field::WithNullable(const bool nullable) const {
  return std::make_shared<Field>(name_, type_, nullable, metadata_);
}

Field::MergeOptions Field::MergeOptions::Permissive() {
  MergeOptions options = Defaults();
  options.promote_nullability = true;
  options.promote_decimal = true;
  options.promote_decimal_to_float = true;
  options.promote_integer_to_decimal = true;
  options.promote_integer_to_float = true;
  options.promote_integer_sign = true;
  options.promote_numeric_width = true;
  options.promote_binary = true;
  options.promote_temporal_unit = true;
  options.promote_list = true;
  options.promote_dictionary = true;
  options.promote_dictionary_ordered = false;
  return options;
}

std::string Field::MergeOptions::ToString() const {
  std::stringstream ss;
  ss << "MergeOptions{";
  ss << "promote_nullability=" << (promote_nullability ? "true" : "false");
  ss << ", promote_decimal=" << (promote_decimal ? "true" : "false");
  ss << ", promote_decimal_to_float=" << (promote_decimal_to_float ? "true" : "false");
  ss << ", promote_integer_to_decimal="
     << (promote_integer_to_decimal ? "true" : "false");
  ss << ", promote_integer_to_float=" << (promote_integer_to_float ? "true" : "false");
  ss << ", promote_integer_sign=" << (promote_integer_sign ? "true" : "false");
  ss << ", promote_numeric_width=" << (promote_numeric_width ? "true" : "false");
  ss << ", promote_binary=" << (promote_binary ? "true" : "false");
  ss << ", promote_temporal_unit=" << (promote_temporal_unit ? "true" : "false");
  ss << ", promote_list=" << (promote_list ? "true" : "false");
  ss << ", promote_dictionary=" << (promote_dictionary ? "true" : "false");
  ss << ", promote_dictionary_ordered="
     << (promote_dictionary_ordered ? "true" : "false");
  ss << '}';
  return ss.str();
}

namespace {
// Utilities for Field::MergeWith

std::shared_ptr<DataType> MakeBinary(const DataType& type) {
  switch (type.id()) {
    case Type::BINARY:
    case Type::STRING:
      return binary();
    case Type::LARGE_BINARY:
    case Type::LARGE_STRING:
      return large_binary();
    default:
      Unreachable("Hit an unknown type");
  }
  return nullptr;
}

Result<std::shared_ptr<DataType>> WidenDecimals(
    const std::shared_ptr<DataType>& promoted_type,
    const std::shared_ptr<DataType>& other_type, const Field::MergeOptions& options) {
  const auto& left = checked_cast<const DecimalType&>(*promoted_type);
  const auto& right = checked_cast<const DecimalType&>(*other_type);
  if (!options.promote_numeric_width && left.bit_width() != right.bit_width()) {
    return Status::TypeError(
        "Cannot promote decimal128 to decimal256 without promote_numeric_width=true");
  }
  const int32_t max_scale = std::max<int32_t>(left.scale(), right.scale());
  const int32_t common_precision =
      std::max<int32_t>(left.precision() + max_scale - left.scale(),
                        right.precision() + max_scale - right.scale());
  if (left.id() == Type::DECIMAL256 || right.id() == Type::DECIMAL256 ||
      common_precision > BasicDecimal128::kMaxPrecision) {
    return DecimalType::Make(Type::DECIMAL256, common_precision, max_scale);
  }
  return DecimalType::Make(Type::DECIMAL128, common_precision, max_scale);
}

Result<std::shared_ptr<DataType>> MergeTypes(std::shared_ptr<DataType> promoted_type,
                                             std::shared_ptr<DataType> other_type,
                                             const Field::MergeOptions& options);

// Merge temporal types based on options. Returns nullptr for non-temporal types.
Result<std::shared_ptr<DataType>> MaybeMergeTemporalTypes(
    const std::shared_ptr<DataType>& promoted_type,
    const std::shared_ptr<DataType>& other_type, const Field::MergeOptions& options) {
  if (options.promote_temporal_unit) {
    if (promoted_type->id() == Type::DATE32 && other_type->id() == Type::DATE64) {
      return date64();
    }
    if (promoted_type->id() == Type::DATE64 && other_type->id() == Type::DATE32) {
      return date64();
    }

    if (promoted_type->id() == Type::DURATION && other_type->id() == Type::DURATION) {
      const auto& left = checked_cast<const DurationType&>(*promoted_type);
      const auto& right = checked_cast<const DurationType&>(*other_type);
      return duration(std::max(left.unit(), right.unit()));
    }

    if (is_time(promoted_type->id()) && is_time(other_type->id())) {
      const auto& left = checked_cast<const TimeType&>(*promoted_type);
      const auto& right = checked_cast<const TimeType&>(*other_type);
      const auto unit = std::max(left.unit(), right.unit());
      if (unit == TimeUnit::MICRO || unit == TimeUnit::NANO) {
        return time64(unit);
      }
      return time32(unit);
    }
  }

  if (promoted_type->id() == Type::TIMESTAMP && other_type->id() == Type::TIMESTAMP) {
    const auto& left = checked_cast<const TimestampType&>(*promoted_type);
    const auto& right = checked_cast<const TimestampType&>(*other_type);
    if (left.timezone().empty() ^ right.timezone().empty()) {
      return Status::TypeError(
          "Cannot merge timestamp with timezone and timestamp without timezone");
    }
    if (left.timezone() != right.timezone()) {
      return Status::TypeError("Cannot merge timestamps with differing timezones");
    }
    if (options.promote_temporal_unit) {
      return timestamp(std::max(left.unit(), right.unit()), left.timezone());
    }
  }

  return nullptr;
}

// Merge numeric types based on options. Returns nullptr for non-numeric types.
Result<std::shared_ptr<DataType>> MaybeMergeNumericTypes(
    std::shared_ptr<DataType> promoted_type, std::shared_ptr<DataType> other_type,
    const Field::MergeOptions& options) {
  bool promoted = false;
  if (options.promote_decimal_to_float) {
    if (is_decimal(promoted_type->id()) && is_floating(other_type->id())) {
      promoted_type = other_type;
      promoted = true;
    } else if (is_floating(promoted_type->id()) && is_decimal(other_type->id())) {
      other_type = promoted_type;
      promoted = true;
    }
  }

  if (options.promote_integer_to_decimal &&
      ((is_decimal(promoted_type->id()) && is_integer(other_type->id())) ||
       (is_decimal(other_type->id()) && is_integer(promoted_type->id())))) {
    if (is_integer(promoted_type->id()) && is_decimal(other_type->id())) {
      // Other type is always the int
      promoted_type.swap(other_type);
    }
    ARROW_ASSIGN_OR_RAISE(const int32_t precision,
                          MaxDecimalDigitsForInteger(other_type->id()));
    ARROW_ASSIGN_OR_RAISE(const auto promoted_decimal,
                          DecimalType::Make(promoted_type->id(), precision, 0));
    ARROW_ASSIGN_OR_RAISE(promoted_type,
                          WidenDecimals(promoted_type, promoted_decimal, options));
    return promoted_type;
  }

  if (options.promote_decimal && is_decimal(promoted_type->id()) &&
      is_decimal(other_type->id())) {
    ARROW_ASSIGN_OR_RAISE(promoted_type,
                          WidenDecimals(promoted_type, other_type, options));
    return promoted_type;
  }

  if (options.promote_integer_sign && ((is_unsigned_integer(promoted_type->id()) &&
                                        is_signed_integer(other_type->id())) ||
                                       (is_signed_integer(promoted_type->id()) &&
                                        is_unsigned_integer(other_type->id())))) {
    if (is_signed_integer(promoted_type->id()) && is_unsigned_integer(other_type->id())) {
      // Other type is always the signed int
      promoted_type.swap(other_type);
    }

    if (!options.promote_numeric_width &&
        bit_width(promoted_type->id()) < bit_width(other_type->id())) {
      return Status::TypeError(
          "Cannot widen signed integers without promote_numeric_width=true");
    }
    int max_width =
        std::max<int>(bit_width(promoted_type->id()), bit_width(other_type->id()));

    // If the unsigned one is bigger or equal to the signed one, we need another bit
    if (bit_width(promoted_type->id()) >= bit_width(other_type->id())) {
      ++max_width;
    }

    if (max_width > 32) {
      promoted_type = int64();
    } else if (max_width > 16) {
      promoted_type = int32();
    } else if (max_width > 8) {
      promoted_type = int16();
    } else {
      promoted_type = int8();
    }
    return promoted_type;
  }

  if (options.promote_integer_to_float &&
      ((is_floating(promoted_type->id()) && is_integer(other_type->id())) ||
       (is_integer(promoted_type->id()) && is_floating(other_type->id())))) {
    if (is_integer(promoted_type->id()) && is_floating(other_type->id())) {
      // Other type is always the int
      promoted_type.swap(other_type);
    }

    const int int_width = bit_width(other_type->id());
    promoted = true;
    if (int_width <= 8) {
      other_type = float16();
    } else if (int_width <= 16) {
      other_type = float32();
    } else {
      other_type = float64();
    }

    if (!options.promote_numeric_width &&
        bit_width(promoted_type->id()) != bit_width(other_type->id())) {
      return Status::TypeError("Cannot widen float without promote_numeric_width=true");
    }
  }

  if (options.promote_numeric_width) {
    const int max_width =
        std::max<int>(bit_width(promoted_type->id()), bit_width(other_type->id()));
    if (is_floating(promoted_type->id()) && is_floating(other_type->id())) {
      promoted = true;
      if (max_width >= 64) {
        promoted_type = float64();
      } else if (max_width >= 32) {
        promoted_type = float32();
      } else {
        promoted_type = float16();
      }
    } else if (is_signed_integer(promoted_type->id()) &&
               is_signed_integer(other_type->id())) {
      promoted = true;
      if (max_width >= 64) {
        promoted_type = int64();
      } else if (max_width >= 32) {
        promoted_type = int32();
      } else if (max_width >= 16) {
        promoted_type = int16();
      } else {
        promoted_type = int8();
      }
    } else if (is_unsigned_integer(promoted_type->id()) &&
               is_unsigned_integer(other_type->id())) {
      promoted = true;
      if (max_width >= 64) {
        promoted_type = uint64();
      } else if (max_width >= 32) {
        promoted_type = uint32();
      } else if (max_width >= 16) {
        promoted_type = uint16();
      } else {
        promoted_type = uint8();
      }
    }
  }

  return promoted ? promoted_type : nullptr;
}

// Merge two dictionary types, or else give an error.
Result<std::shared_ptr<DataType>> MergeDictionaryTypes(
    const std::shared_ptr<DataType>& promoted_type,
    const std::shared_ptr<DataType>& other_type, const Field::MergeOptions& options) {
  const auto& left = checked_cast<const DictionaryType&>(*promoted_type);
  const auto& right = checked_cast<const DictionaryType&>(*other_type);
  if (!options.promote_dictionary_ordered && left.ordered() != right.ordered()) {
    return Status::TypeError(
        "Cannot merge ordered and unordered dictionary unless "
        "promote_dictionary_ordered=true");
  }
  Field::MergeOptions index_options = options;
  index_options.promote_integer_sign = true;
  index_options.promote_numeric_width = true;
  ARROW_ASSIGN_OR_RAISE(
      auto indices,
      MaybeMergeNumericTypes(left.index_type(), right.index_type(), index_options));
  ARROW_ASSIGN_OR_RAISE(auto values,
                        MergeTypes(left.value_type(), right.value_type(), options));
  auto ordered = left.ordered() && right.ordered();
  if (indices && values) {
    return dictionary(indices, values, ordered);
  } else if (values) {
    return Status::TypeError("Could not merge dictionary index types");
  }
  return Status::TypeError("Could not merge dictionary value types");
}

// Merge temporal types based on options. Returns nullptr for non-binary types.
Result<std::shared_ptr<DataType>> MaybeMergeBinaryTypes(
    std::shared_ptr<DataType>& promoted_type, std::shared_ptr<DataType>& other_type,
    const Field::MergeOptions& options) {
  if (options.promote_binary) {
    if (other_type->id() == Type::FIXED_SIZE_BINARY &&
        is_base_binary_like(promoted_type->id())) {
      return MakeBinary(*promoted_type);
    } else if (promoted_type->id() == Type::FIXED_SIZE_BINARY &&
               is_base_binary_like(other_type->id())) {
      return MakeBinary(*other_type);
    } else if (promoted_type->id() == Type::FIXED_SIZE_BINARY &&
               other_type->id() == Type::FIXED_SIZE_BINARY) {
      return binary();
    }

    if ((other_type->id() == Type::LARGE_STRING ||
         other_type->id() == Type::LARGE_BINARY) &&
        (promoted_type->id() == Type::STRING || promoted_type->id() == Type::BINARY)

    ) {
      // Promoted type is always large in case there are regular and large types
      promoted_type.swap(other_type);
    }

    // When one field is binary and the other a string
    if (is_string(promoted_type->id()) && is_binary(other_type->id())) {
      return MakeBinary(*promoted_type);
    } else if (is_binary(promoted_type->id()) && is_string(other_type->id())) {
      return MakeBinary(*promoted_type);
    }

    // When the types are the same, but one is large
    if ((promoted_type->id() == Type::STRING && other_type->id() == Type::LARGE_STRING) ||
        (promoted_type->id() == Type::LARGE_STRING && other_type->id() == Type::STRING)) {
      return large_utf8();
    } else if ((promoted_type->id() == Type::BINARY &&
                other_type->id() == Type::LARGE_BINARY) ||
               (promoted_type->id() == Type::LARGE_BINARY &&
                other_type->id() == Type::BINARY)) {
      return large_binary();
    }
  }

  return nullptr;
}

// Merge list types based on options. Returns nullptr for non-list types.
Result<std::shared_ptr<DataType>> MergeStructs(
    const std::shared_ptr<DataType>& promoted_type,
    const std::shared_ptr<DataType>& other_type, const Field::MergeOptions& options) {
  SchemaBuilder builder(SchemaBuilder::CONFLICT_APPEND, options);
  // Add the LHS fields. Duplicates will be preserved.
  RETURN_NOT_OK(builder.AddFields(promoted_type->fields()));

  // Add the RHS fields. Duplicates will be merged, unless the field was
  // already a duplicate, in which case we error (since we don't know which
  // field to merge with).
  builder.SetPolicy(SchemaBuilder::CONFLICT_MERGE);
  RETURN_NOT_OK(builder.AddFields(other_type->fields()));

  ARROW_ASSIGN_OR_RAISE(auto schema, builder.Finish());
  return struct_(schema->fields());
}

// Merge list types based on options. Returns nullptr for non-list types.
Result<std::shared_ptr<DataType>> MaybeMergeListTypes(
    const std::shared_ptr<DataType>& promoted_type,
    const std::shared_ptr<DataType>& other_type, const Field::MergeOptions& options) {
  if (promoted_type->id() == Type::FIXED_SIZE_LIST &&
      other_type->id() == Type::FIXED_SIZE_LIST) {
    const auto& left = checked_cast<const FixedSizeListType&>(*promoted_type);
    const auto& right = checked_cast<const FixedSizeListType&>(*other_type);
    ARROW_ASSIGN_OR_RAISE(
        auto value_field,
        left.value_field()->MergeWith(
            *right.value_field()->WithName(left.value_field()->name()), options));
    if (left.list_size() == right.list_size()) {
      return fixed_size_list(std::move(value_field), left.list_size());
    } else {
      return list(std::move(value_field));
    }
  } else if (is_list(promoted_type->id()) && is_list(other_type->id())) {
    const auto& left = checked_cast<const BaseListType&>(*promoted_type);
    const auto& right = checked_cast<const BaseListType&>(*other_type);
    ARROW_ASSIGN_OR_RAISE(
        auto value_field,
        left.value_field()->MergeWith(
            *right.value_field()->WithName(left.value_field()->name()), options));

    if (!options.promote_list && promoted_type->id() != other_type->id()) {
      return Status::TypeError("Cannot merge lists unless promote_list=true");
    }

    if (promoted_type->id() == Type::LARGE_LIST || other_type->id() == Type::LARGE_LIST) {
      return large_list(std::move(value_field));
    } else {
      return list(std::move(value_field));
    }
  } else if (promoted_type->id() == Type::MAP && other_type->id() == Type::MAP) {
    const auto& left = checked_cast<const MapType&>(*promoted_type);
    const auto& right = checked_cast<const MapType&>(*other_type);
    ARROW_ASSIGN_OR_RAISE(
        auto key_field,
        left.key_field()->MergeWith(
            *right.key_field()->WithName(left.key_field()->name()), options));
    ARROW_ASSIGN_OR_RAISE(
        auto item_field,
        left.item_field()->MergeWith(
            *right.item_field()->WithName(left.item_field()->name()), options));
    return map(key_field->type(), std::move(item_field),
               /*keys_sorted=*/left.keys_sorted() && right.keys_sorted());
  } else if (promoted_type->id() == Type::STRUCT && other_type->id() == Type::STRUCT) {
    return MergeStructs(promoted_type, other_type, options);
  }

  return nullptr;
}

Result<std::shared_ptr<DataType>> MergeTypes(std::shared_ptr<DataType> promoted_type,
                                             std::shared_ptr<DataType> other_type,
                                             const Field::MergeOptions& options) {
  if (promoted_type->Equals(*other_type)) return promoted_type;

  bool promoted = false;
  if (options.promote_nullability) {
    if (promoted_type->id() == Type::NA) {
      return other_type;
    } else if (other_type->id() == Type::NA) {
      return promoted_type;
    }
  } else if (promoted_type->id() == Type::NA || other_type->id() == Type::NA) {
    return Status::TypeError(
        "Cannot merge type with null unless promote_nullability=true");
  }

  if (options.promote_dictionary && is_dictionary(promoted_type->id()) &&
      is_dictionary(other_type->id())) {
    return MergeDictionaryTypes(promoted_type, other_type, options);
  }

  ARROW_ASSIGN_OR_RAISE(auto maybe_promoted,
                        MaybeMergeTemporalTypes(promoted_type, other_type, options));
  if (maybe_promoted) return maybe_promoted;

  ARROW_ASSIGN_OR_RAISE(maybe_promoted,
                        MaybeMergeNumericTypes(promoted_type, other_type, options));
  if (maybe_promoted) return maybe_promoted;

  ARROW_ASSIGN_OR_RAISE(maybe_promoted,
                        MaybeMergeBinaryTypes(promoted_type, other_type, options));
  if (maybe_promoted) return maybe_promoted;

  ARROW_ASSIGN_OR_RAISE(maybe_promoted,
                        MaybeMergeListTypes(promoted_type, other_type, options));
  if (maybe_promoted) return maybe_promoted;

  return promoted ? promoted_type : nullptr;
}
}  // namespace

Result<std::shared_ptr<Field>> Field::MergeWith(const Field& other,
                                                MergeOptions options) const {
  if (name() != other.name()) {
    return Status::Invalid("Field ", name(), " doesn't have the same name as ",
                           other.name());
  }

  if (Equals(other, /*check_metadata=*/false)) {
    return Copy();
  }

  auto maybe_promoted_type = MergeTypes(type_, other.type(), options);
  if (!maybe_promoted_type.ok()) {
    return maybe_promoted_type.status().WithMessage(
        "Unable to merge: Field ", name(),
        " has incompatible types: ", type()->ToString(), " vs ", other.type()->ToString(),
        ": ", maybe_promoted_type.status().message());
  }
  auto promoted_type = *std::move(maybe_promoted_type);
  if (promoted_type) {
    bool nullable = nullable_;
    if (options.promote_nullability) {
      nullable = nullable || other.nullable() || type_->id() == Type::NA ||
                 other.type()->id() == Type::NA;
    } else if (nullable_ != other.nullable()) {
      return Status::TypeError("Unable to merge: Field ", name(),
                               " has incompatible nullability: ", nullable_, " vs ",
                               other.nullable());
    }

    return std::make_shared<Field>(name_, promoted_type, nullable, metadata_);
  }
  return Status::TypeError("Unable to merge: Field ", name(),
                           " has incompatible types: ", type()->ToString(), " vs ",
                           other.type()->ToString());
}

Result<std::shared_ptr<Field>> Field::MergeWith(const std::shared_ptr<Field>& other,
                                                MergeOptions options) const {
  DCHECK_NE(other, nullptr);
  return MergeWith(*other, options);
}

FieldVector Field::Flatten() const {
  FieldVector flattened;
  if (type_->id() == Type::STRUCT) {
    for (const auto& child : type_->fields()) {
      auto flattened_child = child->Copy();
      flattened.push_back(flattened_child);
      flattened_child->name_.insert(0, name() + ".");
      flattened_child->nullable_ |= nullable_;
    }
  } else {
    flattened.push_back(this->Copy());
  }
  return flattened;
}

std::shared_ptr<Field> Field::Copy() const {
  return ::arrow::field(name_, type_, nullable_, metadata_);
}

bool Field::Equals(const Field& other, bool check_metadata) const {
  if (this == &other) {
    return true;
  }
  if (this->name_ == other.name_ && this->nullable_ == other.nullable_ &&
      this->type_->Equals(*other.type_.get(), check_metadata)) {
    if (!check_metadata) {
      return true;
    } else if (this->HasMetadata() && other.HasMetadata()) {
      return metadata_->Equals(*other.metadata_);
    } else if (!this->HasMetadata() && !other.HasMetadata()) {
      return true;
    } else {
      return false;
    }
  }
  return false;
}

bool Field::Equals(const std::shared_ptr<Field>& other, bool check_metadata) const {
  return Equals(*other.get(), check_metadata);
}

bool Field::IsCompatibleWith(const Field& other) const { return MergeWith(other).ok(); }

bool Field::IsCompatibleWith(const std::shared_ptr<Field>& other) const {
  DCHECK_NE(other, nullptr);
  return IsCompatibleWith(*other);
}

std::string Field::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << name_ << ": " << type_->ToString(show_metadata);
  if (!nullable_) {
    ss << " not null";
  }
  if (show_metadata && metadata_) {
    ss << metadata_->ToString();
  }
  return ss.str();
}

void PrintTo(const Field& field, std::ostream* os) { *os << field.ToString(); }

DataType::~DataType() {}

bool DataType::Equals(const DataType& other, bool check_metadata) const {
  return TypeEquals(*this, other, check_metadata);
}

bool DataType::Equals(const std::shared_ptr<DataType>& other, bool check_metadata) const {
  if (!other) {
    return false;
  }
  return Equals(*other.get(), check_metadata);
}

size_t DataType::Hash() const {
  static constexpr size_t kHashSeed = 0;
  size_t result = kHashSeed;
  internal::hash_combine(result, this->fingerprint());
  return result;
}

std::ostream& operator<<(std::ostream& os, const DataType& type) {
  os << type.ToString();
  return os;
}

std::ostream& operator<<(std::ostream& os, const TypeHolder& type) {
  os << type.ToString();
  return os;
}

// ----------------------------------------------------------------------
// TypeHolder

std::string TypeHolder::ToString(const std::vector<TypeHolder>& types,
                                 bool show_metadata) {
  std::stringstream ss;
  ss << "(";
  for (size_t i = 0; i < types.size(); ++i) {
    if (i > 0) {
      ss << ", ";
    }
    ss << types[i].type->ToString(show_metadata);
  }
  ss << ")";
  return ss.str();
}

std::vector<TypeHolder> TypeHolder::FromTypes(
    const std::vector<std::shared_ptr<DataType>>& types) {
  std::vector<TypeHolder> type_holders;
  type_holders.reserve(types.size());
  for (const auto& type : types) {
    type_holders.emplace_back(type);
  }
  return type_holders;
}

// ----------------------------------------------------------------------

FixedWidthType::~FixedWidthType() {}

PrimitiveCType::~PrimitiveCType() {}

NumberType::~NumberType() {}

IntegerType::~IntegerType() {}

FloatingPointType::~FloatingPointType() {}

FloatingPointType::Precision HalfFloatType::precision() const {
  return FloatingPointType::HALF;
}

FloatingPointType::Precision FloatType::precision() const {
  return FloatingPointType::SINGLE;
}

FloatingPointType::Precision DoubleType::precision() const {
  return FloatingPointType::DOUBLE;
}

std::ostream& operator<<(std::ostream& os,
                         DayTimeIntervalType::DayMilliseconds interval) {
  os << interval.days << "d" << interval.milliseconds << "ms";
  return os;
}

std::ostream& operator<<(std::ostream& os,
                         MonthDayNanoIntervalType::MonthDayNanos interval) {
  os << interval.months << "M" << interval.days << "d" << interval.nanoseconds << "ns";
  return os;
}

NestedType::~NestedType() {}

BaseBinaryType::~BaseBinaryType() {}

BaseListType::~BaseListType() {}

std::string ListType::ToString(bool show_metadata) const {
  std::stringstream s;
  s << "list<" << value_field()->ToString(show_metadata) << ">";
  return s.str();
}

std::string LargeListType::ToString(bool show_metadata) const {
  std::stringstream s;
  s << "large_list<" << value_field()->ToString(show_metadata) << ">";
  return s.str();
}

std::string ListViewType::ToString(bool show_metadata) const {
  std::stringstream s;
  s << "list_view<" << value_field()->ToString(show_metadata) << ">";
  return s.str();
}

std::string LargeListViewType::ToString(bool show_metadata) const {
  std::stringstream s;
  s << "large_list_view<" << value_field()->ToString(show_metadata) << ">";
  return s.str();
}

MapType::MapType(std::shared_ptr<DataType> key_type, std::shared_ptr<DataType> item_type,
                 bool keys_sorted)
    : MapType(::arrow::field("key", std::move(key_type), false),
              ::arrow::field("value", std::move(item_type)), keys_sorted) {}

MapType::MapType(std::shared_ptr<DataType> key_type, std::shared_ptr<Field> item_field,
                 bool keys_sorted)
    : MapType(::arrow::field("key", std::move(key_type), false), std::move(item_field),
              keys_sorted) {}

MapType::MapType(std::shared_ptr<Field> key_field, std::shared_ptr<Field> item_field,
                 bool keys_sorted)
    : MapType(
          ::arrow::field("entries",
                         struct_({std::move(key_field), std::move(item_field)}), false),
          keys_sorted) {}

MapType::MapType(std::shared_ptr<Field> value_field, bool keys_sorted)
    : ListType(std::move(value_field)), keys_sorted_(keys_sorted) {
  id_ = type_id;
}

Result<std::shared_ptr<DataType>> MapType::Make(std::shared_ptr<Field> value_field,
                                                bool keys_sorted) {
  const auto& value_type = *value_field->type();
  if (value_field->nullable() || value_type.id() != Type::STRUCT) {
    return Status::TypeError("Map entry field should be non-nullable struct");
  }
  const auto& struct_type = checked_cast<const StructType&>(value_type);
  if (struct_type.num_fields() != 2) {
    return Status::TypeError("Map entry field should have two children (got ",
                             struct_type.num_fields(), ")");
  }
  if (struct_type.field(0)->nullable()) {
    return Status::TypeError("Map key field should be non-nullable");
  }
  return std::make_shared<MapType>(std::move(value_field), keys_sorted);
}

std::string MapType::ToString(bool show_metadata) const {
  std::stringstream s;

  const auto print_field_name = [](std::ostream& os, const Field& field,
                                   const char* std_name) {
    if (field.name() != std_name) {
      os << " ('" << field.name() << "')";
    }
  };
  const auto print_field = [&](std::ostream& os, const Field& field,
                               const char* std_name) {
    os << field.type()->ToString(show_metadata);
    print_field_name(os, field, std_name);
  };

  s << "map<";
  print_field(s, *key_field(), "key");
  s << ", ";
  print_field(s, *item_field(), "value");
  if (keys_sorted_) {
    s << ", keys_sorted";
  }
  print_field_name(s, *value_field(), "entries");
  s << ">";
  return s.str();
}

std::string FixedSizeListType::ToString(bool show_metadata) const {
  std::stringstream s;
  s << "fixed_size_list<" << value_field()->ToString(show_metadata) << ">[" << list_size_
    << "]";
  return s.str();
}

std::string BinaryType::ToString(bool show_metadata) const { return "binary"; }

std::string BinaryViewType::ToString(bool show_metadata) const { return "binary_view"; }

std::string LargeBinaryType::ToString(bool show_metadata) const { return "large_binary"; }

std::string StringType::ToString(bool show_metadata) const { return "string"; }

std::string StringViewType::ToString(bool show_metadata) const { return "string_view"; }

std::string LargeStringType::ToString(bool show_metadata) const { return "large_string"; }

int FixedSizeBinaryType::bit_width() const { return CHAR_BIT * byte_width(); }

Result<std::shared_ptr<DataType>> FixedSizeBinaryType::Make(int32_t byte_width) {
  if (byte_width < 0) {
    return Status::Invalid("Negative FixedSizeBinaryType byte width");
  }
  if (byte_width > std::numeric_limits<int>::max() / CHAR_BIT) {
    // bit_width() would overflow
    return Status::Invalid("byte width of FixedSizeBinaryType too large");
  }
  return std::make_shared<FixedSizeBinaryType>(byte_width);
}

std::string FixedSizeBinaryType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "fixed_size_binary[" << byte_width_ << "]";
  return ss.str();
}

TemporalType::~TemporalType() {}

// ----------------------------------------------------------------------
// Date types

DateType::DateType(Type::type type_id) : TemporalType(type_id) {}

Date32Type::Date32Type() : DateType(Type::DATE32) {}

Date64Type::Date64Type() : DateType(Type::DATE64) {}

std::string Date64Type::ToString(bool show_metadata) const {
  return std::string("date64[ms]");
}

std::string Date32Type::ToString(bool show_metadata) const {
  return std::string("date32[day]");
}

// ----------------------------------------------------------------------
// Time types

TimeType::TimeType(Type::type type_id, TimeUnit::type unit)
    : TemporalType(type_id), unit_(unit) {}

Time32Type::Time32Type(TimeUnit::type unit) : TimeType(Type::TIME32, unit) {
  ARROW_CHECK(unit == TimeUnit::SECOND || unit == TimeUnit::MILLI)
      << "Must be seconds or milliseconds";
}

std::string Time32Type::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "time32[" << this->unit_ << "]";
  return ss.str();
}

Time64Type::Time64Type(TimeUnit::type unit) : TimeType(Type::TIME64, unit) {
  ARROW_CHECK(unit == TimeUnit::MICRO || unit == TimeUnit::NANO)
      << "Must be microseconds or nanoseconds";
}

std::string Time64Type::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "time64[" << this->unit_ << "]";
  return ss.str();
}

std::ostream& operator<<(std::ostream& os, TimeUnit::type unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      os << "s";
      break;
    case TimeUnit::MILLI:
      os << "ms";
      break;
    case TimeUnit::MICRO:
      os << "us";
      break;
    case TimeUnit::NANO:
      os << "ns";
      break;
  }
  return os;
}

// ----------------------------------------------------------------------
// Timestamp types

std::string TimestampType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "timestamp[" << this->unit_;
  if (this->timezone_.size() > 0) {
    ss << ", tz=" << this->timezone_;
  }
  ss << "]";
  return ss.str();
}

// Duration types
std::string DurationType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "duration[" << this->unit_ << "]";
  return ss.str();
}

// ----------------------------------------------------------------------
// Union type

constexpr int8_t UnionType::kMaxTypeCode;
constexpr int UnionType::kInvalidChildId;

UnionMode::type UnionType::mode() const {
  return id_ == Type::SPARSE_UNION ? UnionMode::SPARSE : UnionMode::DENSE;
}

UnionType::UnionType(FieldVector fields, std::vector<int8_t> type_codes, Type::type id)
    : NestedType(id),
      type_codes_(std::move(type_codes)),
      child_ids_(kMaxTypeCode + 1, kInvalidChildId) {
  children_ = std::move(fields);
  DCHECK_OK(ValidateParameters(children_, type_codes_, mode()));
  for (int child_id = 0; child_id < static_cast<int>(type_codes_.size()); ++child_id) {
    const auto type_code = type_codes_[child_id];
    child_ids_[type_code] = child_id;
  }
}

Status UnionType::ValidateParameters(const FieldVector& fields,
                                     const std::vector<int8_t>& type_codes,
                                     UnionMode::type mode) {
  if (fields.size() != type_codes.size()) {
    return Status::Invalid("Union should get the same number of fields as type codes");
  }
  for (const auto type_code : type_codes) {
    if (type_code < 0 || type_code > kMaxTypeCode) {
      return Status::Invalid("Union type code out of bounds");
    }
  }
  return Status::OK();
}

DataTypeLayout UnionType::layout() const {
  if (mode() == UnionMode::SPARSE) {
    return DataTypeLayout(
        {DataTypeLayout::AlwaysNull(), DataTypeLayout::FixedWidth(sizeof(uint8_t))});
  } else {
    return DataTypeLayout({DataTypeLayout::AlwaysNull(),
                           DataTypeLayout::FixedWidth(sizeof(uint8_t)),
                           DataTypeLayout::FixedWidth(sizeof(int32_t))});
  }
}

uint8_t UnionType::max_type_code() const {
  return type_codes_.size() == 0
             ? 0
             : *std::max_element(type_codes_.begin(), type_codes_.end());
}

std::string UnionType::ToString(bool show_metadata) const {
  std::stringstream s;

  s << name() << "<";

  for (size_t i = 0; i < children_.size(); ++i) {
    if (i) {
      s << ", ";
    }
    s << children_[i]->ToString(show_metadata) << "=" << static_cast<int>(type_codes_[i]);
  }
  s << ">";
  return s.str();
}

SparseUnionType::SparseUnionType(FieldVector fields, std::vector<int8_t> type_codes)
    : UnionType(std::move(fields), std::move(type_codes), Type::SPARSE_UNION) {}

Result<std::shared_ptr<DataType>> SparseUnionType::Make(FieldVector fields,
                                                        std::vector<int8_t> type_codes) {
  RETURN_NOT_OK(ValidateParameters(fields, type_codes, UnionMode::SPARSE));
  return std::make_shared<SparseUnionType>(std::move(fields), std::move(type_codes));
}

DenseUnionType::DenseUnionType(FieldVector fields, std::vector<int8_t> type_codes)
    : UnionType(std::move(fields), std::move(type_codes), Type::DENSE_UNION) {}

Result<std::shared_ptr<DataType>> DenseUnionType::Make(FieldVector fields,
                                                       std::vector<int8_t> type_codes) {
  RETURN_NOT_OK(ValidateParameters(fields, type_codes, UnionMode::DENSE));
  return std::make_shared<DenseUnionType>(std::move(fields), std::move(type_codes));
}

// ----------------------------------------------------------------------
// Run-end encoded type

RunEndEncodedType::RunEndEncodedType(std::shared_ptr<DataType> run_end_type,
                                     std::shared_ptr<DataType> value_type)
    : NestedType(Type::RUN_END_ENCODED) {
  DCHECK(RunEndTypeValid(*run_end_type));
  children_ = {std::make_shared<Field>("run_ends", std::move(run_end_type), false),
               std::make_shared<Field>("values", std::move(value_type), true)};
}

RunEndEncodedType::~RunEndEncodedType() = default;

std::string RunEndEncodedType::ToString(bool show_metadata) const {
  std::stringstream s;
  s << name() << "<run_ends: " << run_end_type()->ToString(show_metadata)
    << ", values: " << value_type()->ToString(show_metadata) << ">";
  return s.str();
}

bool RunEndEncodedType::RunEndTypeValid(const DataType& run_end_type) {
  return is_run_end_type(run_end_type.id());
}

// ----------------------------------------------------------------------
// Struct type

namespace {

std::unordered_multimap<std::string, int> CreateNameToIndexMap(
    const FieldVector& fields) {
  std::unordered_multimap<std::string, int> name_to_index;
  for (size_t i = 0; i < fields.size(); ++i) {
    name_to_index.emplace(fields[i]->name(), static_cast<int>(i));
  }
  return name_to_index;
}

template <int NotFoundValue = -1, int DuplicateFoundValue = -1>
int LookupNameIndex(const std::unordered_multimap<std::string, int>& name_to_index,
                    const std::string& name) {
  auto p = name_to_index.equal_range(name);
  auto it = p.first;
  if (it == p.second) {
    // Not found
    return NotFoundValue;
  }
  auto index = it->second;
  if (++it != p.second) {
    // Duplicate field name
    return DuplicateFoundValue;
  }
  return index;
}

}  // namespace

class StructType::Impl {
 public:
  explicit Impl(const FieldVector& fields)
      : name_to_index_(CreateNameToIndexMap(fields)) {}

  const std::unordered_multimap<std::string, int> name_to_index_;
};

StructType::StructType(const FieldVector& fields)
    : NestedType(Type::STRUCT), impl_(new Impl(fields)) {
  children_ = fields;
}

StructType::~StructType() {}

std::string StructType::ToString(bool show_metadata) const {
  std::stringstream s;
  s << "struct<";
  for (int i = 0; i < this->num_fields(); ++i) {
    if (i > 0) {
      s << ", ";
    }
    std::shared_ptr<Field> field = this->field(i);
    s << field->ToString(show_metadata);
  }
  s << ">";
  return s.str();
}

std::shared_ptr<Field> StructType::GetFieldByName(const std::string& name) const {
  int i = GetFieldIndex(name);
  return i == -1 ? nullptr : children_[i];
}

int StructType::GetFieldIndex(const std::string& name) const {
  return LookupNameIndex(impl_->name_to_index_, name);
}

std::vector<int> StructType::GetAllFieldIndices(const std::string& name) const {
  std::vector<int> result;
  auto p = impl_->name_to_index_.equal_range(name);
  for (auto it = p.first; it != p.second; ++it) {
    result.push_back(it->second);
  }
  if (result.size() > 1) {
    std::sort(result.begin(), result.end());
  }
  return result;
}

FieldVector StructType::GetAllFieldsByName(const std::string& name) const {
  FieldVector result;
  auto p = impl_->name_to_index_.equal_range(name);
  for (auto it = p.first; it != p.second; ++it) {
    result.push_back(children_[it->second]);
  }
  return result;
}

Result<std::shared_ptr<StructType>> StructType::AddField(
    int i, const std::shared_ptr<Field>& field) const {
  if (i < 0 || i > this->num_fields()) {
    return Status::Invalid("Invalid column index to add field.");
  }
  return std::make_shared<StructType>(internal::AddVectorElement(children_, i, field));
}

Result<std::shared_ptr<StructType>> StructType::RemoveField(int i) const {
  if (i < 0 || i >= this->num_fields()) {
    return Status::Invalid("Invalid column index to remove field.");
  }
  return std::make_shared<StructType>(internal::DeleteVectorElement(children_, i));
}

Result<std::shared_ptr<StructType>> StructType::SetField(
    int i, const std::shared_ptr<Field>& field) const {
  if (i < 0 || i >= this->num_fields()) {
    return Status::Invalid("Invalid column index to set field.");
  }
  return std::make_shared<StructType>(
      internal::ReplaceVectorElement(children_, i, field));
}

Result<std::shared_ptr<DataType>> DecimalType::Make(Type::type type_id, int32_t precision,
                                                    int32_t scale) {
  if (type_id == Type::DECIMAL128) {
    return Decimal128Type::Make(precision, scale);
  } else if (type_id == Type::DECIMAL256) {
    return Decimal256Type::Make(precision, scale);
  } else {
    return Status::Invalid("Not a decimal type_id: ", type_id);
  }
}

// Taken from the Apache Impala codebase. The comments next
// to the return values are the maximum value that can be represented in 2's
// complement with the returned number of bytes.
int32_t DecimalType::DecimalSize(int32_t precision) {
  DCHECK_GE(precision, 1) << "decimal precision must be greater than or equal to 1, got "
                          << precision;

  // Generated in python with:
  // >>> decimal_size = lambda prec: int(math.ceil((prec * math.log2(10) + 1) / 8))
  // >>> [-1] + [decimal_size(i) for i in range(1, 77)]
  constexpr int32_t kBytes[] = {
      -1, 1,  1,  2,  2,  3,  3,  4,  4,  4,  5,  5,  6,  6,  6,  7,  7,  8,  8,  9,
      9,  9,  10, 10, 11, 11, 11, 12, 12, 13, 13, 13, 14, 14, 15, 15, 16, 16, 16, 17,
      17, 18, 18, 18, 19, 19, 20, 20, 21, 21, 21, 22, 22, 23, 23, 23, 24, 24, 25, 25,
      26, 26, 26, 27, 27, 28, 28, 28, 29, 29, 30, 30, 31, 31, 31, 32, 32};

  if (precision <= 76) {
    return kBytes[precision];
  }
  return static_cast<int32_t>(std::ceil((precision / 8.0) * std::log2(10) + 1));
}

// ----------------------------------------------------------------------
// Decimal128 type

Decimal128Type::Decimal128Type(int32_t precision, int32_t scale)
    : DecimalType(type_id, 16, precision, scale) {
  ARROW_CHECK_GE(precision, kMinPrecision);
  ARROW_CHECK_LE(precision, kMaxPrecision);
}

Result<std::shared_ptr<DataType>> Decimal128Type::Make(int32_t precision, int32_t scale) {
  if (precision < kMinPrecision || precision > kMaxPrecision) {
    return Status::Invalid("Decimal precision out of range [", int32_t(kMinPrecision),
                           ", ", int32_t(kMaxPrecision), "]: ", precision);
  }
  return std::make_shared<Decimal128Type>(precision, scale);
}

// ----------------------------------------------------------------------
// Decimal256 type

Decimal256Type::Decimal256Type(int32_t precision, int32_t scale)
    : DecimalType(type_id, 32, precision, scale) {
  ARROW_CHECK_GE(precision, kMinPrecision);
  ARROW_CHECK_LE(precision, kMaxPrecision);
}

Result<std::shared_ptr<DataType>> Decimal256Type::Make(int32_t precision, int32_t scale) {
  if (precision < kMinPrecision || precision > kMaxPrecision) {
    return Status::Invalid("Decimal precision out of range [", int32_t(kMinPrecision),
                           ", ", int32_t(kMaxPrecision), "]: ", precision);
  }
  return std::make_shared<Decimal256Type>(precision, scale);
}

// ----------------------------------------------------------------------
// Dictionary-encoded type

Status DictionaryType::ValidateParameters(const DataType& index_type,
                                          const DataType& value_type) {
  if (!is_integer(index_type.id())) {
    return Status::TypeError("Dictionary index type should be integer, got ",
                             index_type.ToString());
  }
  return Status::OK();
}

int DictionaryType::bit_width() const {
  return checked_cast<const FixedWidthType&>(*index_type_).bit_width();
}

Result<std::shared_ptr<DataType>> DictionaryType::Make(
    const std::shared_ptr<DataType>& index_type,
    const std::shared_ptr<DataType>& value_type, bool ordered) {
  RETURN_NOT_OK(ValidateParameters(*index_type, *value_type));
  return std::make_shared<DictionaryType>(index_type, value_type, ordered);
}

DictionaryType::DictionaryType(const std::shared_ptr<DataType>& index_type,
                               const std::shared_ptr<DataType>& value_type, bool ordered)
    : FixedWidthType(Type::DICTIONARY),
      index_type_(index_type),
      value_type_(value_type),
      ordered_(ordered) {
  ARROW_CHECK_OK(ValidateParameters(*index_type_, *value_type_));
}

DataTypeLayout DictionaryType::layout() const {
  auto layout = index_type_->layout();
  layout.has_dictionary = true;
  return layout;
}

std::string DictionaryType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << this->name() << "<values=" << value_type_->ToString(show_metadata)
     << ", indices=" << index_type_->ToString(show_metadata) << ", ordered=" << ordered_
     << ">";
  return ss.str();
}

// ----------------------------------------------------------------------
// Null type

std::string NullType::ToString(bool show_metadata) const { return name(); }

// ----------------------------------------------------------------------
// FieldPath

size_t FieldPath::hash() const {
  return internal::ComputeStringHash<0>(indices().data(), indices().size() * sizeof(int));
}

std::string FieldPath::ToString() const {
  if (this->indices().empty()) {
    return "FieldPath(empty)";
  }

  std::string repr = "FieldPath(";
  for (auto index : this->indices()) {
    repr += internal::ToChars(index) + " ";
  }
  repr.back() = ')';
  return repr;
}

struct NestedSelectorUtil {
  static Status NonStructError() {
    return Status::NotImplemented("Get child data of non-struct array");
  }

  template <typename T>
  static const DataType* GetType(const T& input) {
    if constexpr (std::is_same_v<T, ArrayData>) {
      return input.type.get();
    } else {
      return input.type().get();
    }
  }
};

// Utility class for retrieving a child field/column from a top-level Field, Array,
// ArrayData, or ChunkedArray. The "root" value can either be a single parent or a vector
// of its children.
template <typename T, bool IsFlattening = false>
class NestedSelector {
 public:
  using ArrowType = T;
  using Util = NestedSelectorUtil;

  explicit NestedSelector(const std::vector<std::shared_ptr<T>>& children)
      : parent_or_children_(&children) {}
  explicit NestedSelector(const T& parent) : parent_or_children_(&parent) {}
  explicit NestedSelector(std::shared_ptr<T> parent)
      : owned_parent_(std::move(parent)), parent_or_children_(owned_parent_.get()) {}
  template <typename Arg>
  NestedSelector(Arg&& arg, MemoryPool* pool) : NestedSelector(std::forward<Arg>(arg)) {
    if (pool) {
      pool_ = pool;
    }
  }

  // If the index is out of bounds, this returns an invalid selector rather than an
  // error.
  Result<NestedSelector> GetChild(int i) const {
    std::shared_ptr<T> child;
    if (auto parent = get_parent()) {
      const DataType* type = Util::GetType(*parent);
      // We avoid this check for schema fields since it's inconsequential (plus there are
      // tests elsewhere that rely on it not happening)
      if constexpr (!std::is_same_v<T, Field>) {
        if (ARROW_PREDICT_FALSE(type->id() != Type::STRUCT)) {
          return Util::NonStructError();
        }
      }
      // Bounds-check the index *once* using the parent's type
      if (ARROW_PREDICT_TRUE(i >= 0 && i < type->num_fields())) {
        ARROW_ASSIGN_OR_RAISE(child, GetChild(*parent, i, pool_));
      }
    } else if (auto children = get_children()) {
      if (ARROW_PREDICT_TRUE(i >= 0 && static_cast<size_t>(i) < children->size())) {
        child = (*children)[i];
      }
    }
    return NestedSelector(std::move(child), pool_);
  }

  Result<std::shared_ptr<T>> Finish() const {
    DCHECK(get_parent() && owned_parent_);
    return owned_parent_;
  }

  template <typename OStream, typename U = T>
  std::enable_if_t<std::is_same_v<U, Field>> Summarize(OStream* os) const {
    const FieldVector* fields = get_children();
    if (!fields && get_parent()) {
      fields = &get_parent()->type()->fields();
    }
    *os << "fields: { ";
    if (fields) {
      for (const auto& field : *fields) {
        *os << field->ToString() << ", ";
      }
    }
    *os << "}";
  }

  template <typename OStream, typename U = T>
  std::enable_if_t<!std::is_same_v<U, Field>> Summarize(OStream* os) const {
    *os << "column types: { ";
    if (auto children = get_children()) {
      for (const auto& child : *children) {
        *os << *Util::GetType(*child) << ", ";
      }
    } else if (auto parent = get_parent()) {
      for (const auto& field : Util::GetType(*parent)->fields()) {
        *os << *field->type() << ", ";
      }
    }
    *os << "}";
  }

  bool is_valid() const { return get_parent() || get_children(); }
  operator bool() const { return is_valid(); }

 private:
  // Accessors for the variant
  auto get_parent() const { return get_value<const T*>(); }
  auto get_children() const {
    return get_value<const std::vector<std::shared_ptr<T>>*>();
  }
  template <typename U>
  U get_value() const {
    auto ptr = std::get_if<U>(&parent_or_children_);
    return ptr ? *ptr : nullptr;
  }

  static Result<std::shared_ptr<Field>> GetChild(const Field& field, int i, MemoryPool*) {
    return field.type()->field(i);
  }

  static Result<std::shared_ptr<ArrayData>> GetChild(const ArrayData& data, int i,
                                                     MemoryPool* pool) {
    std::shared_ptr<ArrayData> child_data;
    if constexpr (IsFlattening) {
      // First, convert to an Array so we can use StructArray::GetFlattenedField
      auto array = MakeArray(data.Copy());
      ARROW_ASSIGN_OR_RAISE(auto child_array, GetChild(*array, i, pool));
      child_data = child_array->data();
    } else {
      // We could achieve the same result by converting to an Array (via MakeArray),
      // calling StructArray::field(i), and pulling out the new ArrayData. However, this
      // process can be very expensive when there are many columns - so we just
      // reimplement the functionality that we need
      child_data = data.child_data[i];
      if (data.offset != 0 || data.child_data[i]->length != data.length) {
        child_data = child_data->Slice(data.offset, data.length);
      }
    }

    return child_data;
  }

  static Result<std::shared_ptr<Array>> GetChild(const Array& array, int i,
                                                 MemoryPool* pool) {
    const auto& struct_array = checked_cast<const StructArray&>(array);
    if constexpr (IsFlattening) {
      return struct_array.GetFlattenedField(i, pool);
    } else {
      return struct_array.field(i);
    }
  }

  static Result<std::shared_ptr<ChunkedArray>> GetChild(const ChunkedArray& chunked_array,
                                                        int i, MemoryPool* pool) {
    const auto& type = *chunked_array.type();

    ArrayVector chunks;
    chunks.reserve(chunked_array.num_chunks());
    for (const auto& parent_chunk : chunked_array.chunks()) {
      ARROW_ASSIGN_OR_RAISE(auto chunk, GetChild(*parent_chunk, i, pool));
      chunks.push_back(std::move(chunk));
    }

    return std::make_shared<ChunkedArray>(std::move(chunks), type.field(i)->type());
  }

  std::shared_ptr<T> owned_parent_;
  std::variant<const T*, const std::vector<std::shared_ptr<T>>*> parent_or_children_;
  MemoryPool* pool_ = default_memory_pool();
};

using FieldSelector = NestedSelector<Field>;
template <typename T>
using ZeroCopySelector = NestedSelector<T, false>;
template <typename T>
using FlatteningSelector = NestedSelector<T, true>;

struct FieldPathGetImpl {
  template <typename Selector>
  static Status IndexError(const FieldPath* path, int out_of_range_depth,
                           const Selector& selector) {
    std::stringstream ss;
    ss << "index out of range. ";

    ss << "indices=[ ";
    int depth = 0;
    for (int i : path->indices()) {
      if (depth++ != out_of_range_depth) {
        ss << i << " ";
      } else {
        ss << ">" << i << "< ";
      }
    }
    ss << "] ";

    selector.Summarize(&ss);

    return Status::IndexError(ss.str());
  }

  template <typename Selector, typename T = typename Selector::ArrowType>
  static Result<std::shared_ptr<T>> Get(const FieldPath* path, Selector selector,
                                        int* out_of_range_depth = nullptr) {
    if (path->empty()) {
      return Status::Invalid("empty indices cannot be traversed");
    }

    int depth = 0;
    for (auto index : *path) {
      ARROW_ASSIGN_OR_RAISE(auto next_selector, selector.GetChild(index));

      // Handle failed bounds check
      if (!next_selector) {
        if (out_of_range_depth) {
          *out_of_range_depth = depth;
          return nullptr;
        }
        return IndexError(path, depth, selector);
      }

      selector = std::move(next_selector);
      ++depth;
    }

    return selector.Finish();
  }
};

Result<std::shared_ptr<Field>> FieldPath::Get(const Schema& schema) const {
  return Get(schema.fields());
}

Result<std::shared_ptr<Field>> FieldPath::Get(const Field& field) const {
  return Get(field.type()->fields());
}

Result<std::shared_ptr<Field>> FieldPath::Get(const DataType& type) const {
  return Get(type.fields());
}

Result<std::shared_ptr<Field>> FieldPath::Get(const FieldVector& fields) const {
  return FieldPathGetImpl::Get(this, FieldSelector(fields));
}

Result<std::shared_ptr<Schema>> FieldPath::GetAll(const Schema& schm,
                                                  const std::vector<FieldPath>& paths) {
  FieldVector fields;
  fields.reserve(paths.size());
  for (const auto& path : paths) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Field> field, path.Get(schm));
    fields.push_back(std::move(field));
  }
  return schema(std::move(fields));
}

Result<std::shared_ptr<Array>> FieldPath::Get(const RecordBatch& batch) const {
  // Deliberately calling `column_data` here because `RecordBatch::columns` is nontrivial
  ARROW_ASSIGN_OR_RAISE(
      auto data,
      FieldPathGetImpl::Get(this, ZeroCopySelector<ArrayData>(batch.column_data())));
  return MakeArray(data);
}

Result<std::shared_ptr<ChunkedArray>> FieldPath::Get(const Table& table) const {
  return FieldPathGetImpl::Get(this, ZeroCopySelector<ChunkedArray>(table.columns()));
}

Result<std::shared_ptr<Array>> FieldPath::Get(const Array& array) const {
  return FieldPathGetImpl::Get(this, ZeroCopySelector<Array>(array));
}

Result<std::shared_ptr<ArrayData>> FieldPath::Get(const ArrayData& data) const {
  return FieldPathGetImpl::Get(this, ZeroCopySelector<ArrayData>(data));
}

Result<std::shared_ptr<ChunkedArray>> FieldPath::Get(
    const ChunkedArray& chunked_array) const {
  return FieldPathGetImpl::Get(this, ZeroCopySelector<ChunkedArray>(chunked_array));
}

Result<std::shared_ptr<Array>> FieldPath::GetFlattened(const Array& array,
                                                       MemoryPool* pool) const {
  return FieldPathGetImpl::Get(this, FlatteningSelector<Array>(array, pool));
}

Result<std::shared_ptr<ArrayData>> FieldPath::GetFlattened(const ArrayData& data,
                                                           MemoryPool* pool) const {
  return FieldPathGetImpl::Get(this, FlatteningSelector<ArrayData>(data, pool));
}

Result<std::shared_ptr<ChunkedArray>> FieldPath::GetFlattened(
    const ChunkedArray& chunked_array, MemoryPool* pool) const {
  return FieldPathGetImpl::Get(this,
                               FlatteningSelector<ChunkedArray>(chunked_array, pool));
}

Result<std::shared_ptr<Array>> FieldPath::GetFlattened(const RecordBatch& batch,
                                                       MemoryPool* pool) const {
  ARROW_ASSIGN_OR_RAISE(
      auto data, FieldPathGetImpl::Get(
                     this, FlatteningSelector<ArrayData>(batch.column_data(), pool)));
  return MakeArray(data);
}

Result<std::shared_ptr<ChunkedArray>> FieldPath::GetFlattened(const Table& table,
                                                              MemoryPool* pool) const {
  return FieldPathGetImpl::Get(this,
                               FlatteningSelector<ChunkedArray>(table.columns(), pool));
}

// ----------------------------------------------------------------------
// FieldRef

FieldRef::FieldRef(FieldPath indices) : impl_(std::move(indices)) {}

void FieldRef::Flatten(std::vector<FieldRef> children) {
  ARROW_CHECK(!children.empty());

  // flatten children
  struct Visitor {
    void operator()(std::string&& name, std::vector<FieldRef>* out) {
      out->push_back(FieldRef(std::move(name)));
    }

    void operator()(FieldPath&& path, std::vector<FieldRef>* out) {
      if (path.indices().empty()) {
        return;
      }
      out->push_back(FieldRef(std::move(path)));
    }

    void operator()(std::vector<FieldRef>&& children, std::vector<FieldRef>* out) {
      if (children.empty()) {
        return;
      }
      // First flatten children into temporary result
      std::vector<FieldRef> flattened_children;
      flattened_children.reserve(children.size());
      for (auto&& child : children) {
        std::visit(std::bind(*this, std::placeholders::_1, &flattened_children),
                   std::move(child.impl_));
      }
      // If all children are FieldPaths, concatenate them into a single FieldPath
      int64_t n_indices = 0;
      for (const auto& child : flattened_children) {
        const FieldPath* path = child.field_path();
        if (!path) {
          n_indices = -1;
          break;
        }
        n_indices += static_cast<int64_t>(path->indices().size());
      }
      if (n_indices == 0) {
        return;
      } else if (n_indices > 0) {
        std::vector<int> indices(n_indices);
        auto out_indices = indices.begin();
        for (const auto& child : flattened_children) {
          for (int index : *child.field_path()) {
            *out_indices++ = index;
          }
        }
        DCHECK_EQ(out_indices, indices.end());
        out->push_back(FieldRef(std::move(indices)));
      } else {
        // ... otherwise, just transfer them to the final result
        out->insert(out->end(), std::move_iterator(flattened_children.begin()),
                    std::move_iterator(flattened_children.end()));
      }
    }
  };

  std::vector<FieldRef> out;
  Visitor visitor;
  visitor(std::move(children), &out);

  if (out.empty()) {
    impl_ = std::vector<int>();
  } else if (out.size() == 1) {
    impl_ = std::move(out[0].impl_);
  } else {
    impl_ = std::move(out);
  }
}

Result<FieldRef> FieldRef::FromDotPath(const std::string& dot_path_arg) {
  if (dot_path_arg.empty()) {
    return FieldRef();
  }

  std::vector<FieldRef> children;

  std::string_view dot_path = dot_path_arg;

  auto parse_name = [&] {
    std::string name;
    for (;;) {
      auto segment_end = dot_path.find_first_of("\\[.");
      if (segment_end == std::string_view::npos) {
        // dot_path doesn't contain any other special characters; consume all
        name.append(dot_path.data(), dot_path.length());
        dot_path = "";
        break;
      }

      if (dot_path[segment_end] != '\\') {
        // segment_end points to a subscript for a new FieldRef
        name.append(dot_path.data(), segment_end);
        dot_path = dot_path.substr(segment_end);
        break;
      }

      if (dot_path.size() == segment_end + 1) {
        // dot_path ends with backslash; consume it all
        name.append(dot_path.data(), dot_path.length());
        dot_path = "";
        break;
      }

      // append all characters before backslash, then the character which follows it
      name.append(dot_path.data(), segment_end);
      name.push_back(dot_path[segment_end + 1]);
      dot_path = dot_path.substr(segment_end + 2);
    }
    return name;
  };

  while (!dot_path.empty()) {
    auto subscript = dot_path[0];
    dot_path = dot_path.substr(1);
    switch (subscript) {
      case '.': {
        // next element is a name
        children.emplace_back(parse_name());
        continue;
      }
      case '[': {
        auto subscript_end = dot_path.find_first_not_of("0123456789");
        if (subscript_end == std::string_view::npos || dot_path[subscript_end] != ']') {
          return Status::Invalid("Dot path '", dot_path_arg,
                                 "' contained an unterminated index");
        }
        children.emplace_back(std::atoi(dot_path.data()));
        dot_path = dot_path.substr(subscript_end + 1);
        continue;
      }
      default:
        return Status::Invalid("Dot path must begin with '[' or '.', got '", dot_path_arg,
                               "'");
    }
  }

  FieldRef out;
  out.Flatten(std::move(children));
  return out;
}

std::string FieldRef::ToDotPath() const {
  struct Visitor {
    std::string operator()(const FieldPath& path) {
      std::string out;
      for (int i : path.indices()) {
        out += "[" + internal::ToChars(i) + "]";
      }
      return out;
    }

    std::string operator()(const std::string& name) { return "." + name; }

    std::string operator()(const std::vector<FieldRef>& children) {
      std::string out;
      for (const auto& child : children) {
        out += child.ToDotPath();
      }
      return out;
    }
  };

  return std::visit(Visitor{}, impl_);
}

size_t FieldRef::hash() const {
  struct Visitor : std::hash<std::string> {
    using std::hash<std::string>::operator();

    size_t operator()(const FieldPath& path) { return path.hash(); }

    size_t operator()(const std::vector<FieldRef>& children) {
      size_t hash = 0;

      for (const FieldRef& child : children) {
        hash ^= child.hash();
      }

      return hash;
    }
  };

  return std::visit(Visitor{}, impl_);
}

std::string FieldRef::ToString() const {
  struct Visitor {
    std::string operator()(const FieldPath& path) { return path.ToString(); }

    std::string operator()(const std::string& name) { return "Name(" + name + ")"; }

    std::string operator()(const std::vector<FieldRef>& children) {
      std::string repr = "Nested(";
      for (const auto& child : children) {
        repr += child.ToString() + " ";
      }
      repr.resize(repr.size() - 1);
      repr += ")";
      return repr;
    }
  };

  return "FieldRef." + std::visit(Visitor{}, impl_);
}

std::vector<FieldPath> FieldRef::FindAll(const Schema& schema) const {
  if (auto name = this->name()) {
    return internal::MapVector([](int i) { return FieldPath{i}; },
                               schema.GetAllFieldIndices(*name));
  }
  return FindAll(schema.fields());
}

std::vector<FieldPath> FieldRef::FindAll(const Field& field) const {
  return FindAll(field.type()->fields());
}

std::vector<FieldPath> FieldRef::FindAll(const DataType& type) const {
  return FindAll(type.fields());
}

std::vector<FieldPath> FieldRef::FindAll(const FieldVector& fields) const {
  struct Visitor {
    std::vector<FieldPath> operator()(const FieldPath& path) {
      // skip long IndexError construction if path is out of range
      int out_of_range_depth;
      auto maybe_field =
          FieldPathGetImpl::Get(&path, FieldSelector(fields_), &out_of_range_depth);

      DCHECK_OK(maybe_field.status());

      if (maybe_field.ValueOrDie() != nullptr) {
        return {path};
      }
      return {};
    }

    std::vector<FieldPath> operator()(const std::string& name) {
      std::vector<FieldPath> out;

      for (int i = 0; i < static_cast<int>(fields_.size()); ++i) {
        if (fields_[i]->name() == name) {
          out.push_back({i});
        }
      }

      return out;
    }

    struct Matches {
      // referents[i] is referenced by prefixes[i]
      std::vector<FieldPath> prefixes;
      FieldVector referents;

      Matches(std::vector<FieldPath> matches, const FieldVector& fields) {
        for (auto& match : matches) {
          Add({}, std::move(match), fields);
        }
      }

      Matches() = default;

      size_t size() const { return referents.size(); }

      void Add(const FieldPath& prefix, const FieldPath& suffix,
               const FieldVector& fields) {
        auto maybe_field = suffix.Get(fields);
        DCHECK_OK(maybe_field.status());
        referents.push_back(std::move(maybe_field).ValueOrDie());

        std::vector<int> concatenated_indices(prefix.indices().size() +
                                              suffix.indices().size());
        auto it = concatenated_indices.begin();
        for (auto path : {&prefix, &suffix}) {
          it = std::copy(path->indices().begin(), path->indices().end(), it);
        }
        prefixes.emplace_back(std::move(concatenated_indices));
      }
    };

    std::vector<FieldPath> operator()(const std::vector<FieldRef>& refs) {
      DCHECK_GE(refs.size(), 1);
      Matches matches(refs.front().FindAll(fields_), fields_);

      for (auto ref_it = refs.begin() + 1; ref_it != refs.end(); ++ref_it) {
        Matches next_matches;
        for (size_t i = 0; i < matches.size(); ++i) {
          const auto& referent = *matches.referents[i];

          for (const FieldPath& match : ref_it->FindAll(referent)) {
            next_matches.Add(matches.prefixes[i], match, referent.type()->fields());
          }
        }
        matches = std::move(next_matches);
      }

      return matches.prefixes;
    }

    const FieldVector& fields_;
  };

  return std::visit(Visitor{fields}, impl_);
}

std::vector<FieldPath> FieldRef::FindAll(const ArrayData& array) const {
  return FindAll(*array.type);
}

std::vector<FieldPath> FieldRef::FindAll(const Array& array) const {
  return FindAll(*array.type());
}

std::vector<FieldPath> FieldRef::FindAll(const ChunkedArray& chunked_array) const {
  return FindAll(*chunked_array.type());
}

std::vector<FieldPath> FieldRef::FindAll(const RecordBatch& batch) const {
  return FindAll(*batch.schema());
}

std::vector<FieldPath> FieldRef::FindAll(const Table& table) const {
  return FindAll(*table.schema());
}

void PrintTo(const FieldRef& ref, std::ostream* os) { *os << ref.ToString(); }

std::ostream& operator<<(std::ostream& os, const FieldRef& ref) {
  os << ref.ToString();
  return os;
}

// ----------------------------------------------------------------------
// Schema implementation

std::string EndiannessToString(Endianness endianness) {
  switch (endianness) {
    case Endianness::Little:
      return "little";
    case Endianness::Big:
      return "big";
    default:
      DCHECK(false) << "invalid endianness";
      return "???";
  }
}

class Schema::Impl {
 public:
  Impl(FieldVector fields, Endianness endianness,
       std::shared_ptr<const KeyValueMetadata> metadata)
      : fields_(std::move(fields)),
        endianness_(endianness),
        name_to_index_(CreateNameToIndexMap(fields_)),
        metadata_(std::move(metadata)) {}

  FieldVector fields_;
  Endianness endianness_;
  std::unordered_multimap<std::string, int> name_to_index_;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

Schema::Schema(FieldVector fields, Endianness endianness,
               std::shared_ptr<const KeyValueMetadata> metadata)
    : detail::Fingerprintable(),
      impl_(new Impl(std::move(fields), endianness, std::move(metadata))) {}

Schema::Schema(FieldVector fields, std::shared_ptr<const KeyValueMetadata> metadata)
    : detail::Fingerprintable(),
      impl_(new Impl(std::move(fields), Endianness::Native, std::move(metadata))) {}

Schema::Schema(const Schema& schema)
    : detail::Fingerprintable(), impl_(new Impl(*schema.impl_)) {}

Schema::~Schema() = default;

std::shared_ptr<Schema> Schema::WithEndianness(Endianness endianness) const {
  return std::make_shared<Schema>(impl_->fields_, endianness, impl_->metadata_);
}

Endianness Schema::endianness() const { return impl_->endianness_; }

bool Schema::is_native_endian() const { return impl_->endianness_ == Endianness::Native; }

int Schema::num_fields() const { return static_cast<int>(impl_->fields_.size()); }

const std::shared_ptr<Field>& Schema::field(int i) const {
  DCHECK_GE(i, 0);
  DCHECK_LT(i, num_fields());
  return impl_->fields_[i];
}

const FieldVector& Schema::fields() const { return impl_->fields_; }

bool Schema::Equals(const Schema& other, bool check_metadata) const {
  if (this == &other) {
    return true;
  }

  // checks endianness equality
  if (endianness() != other.endianness()) {
    return false;
  }

  // checks field equality
  if (num_fields() != other.num_fields()) {
    return false;
  }

  if (check_metadata) {
    const auto& metadata_fp = metadata_fingerprint();
    const auto& other_metadata_fp = other.metadata_fingerprint();
    if (metadata_fp != other_metadata_fp) {
      return false;
    }
  }

  // Fast path using fingerprints, if possible
  const auto& fp = fingerprint();
  const auto& other_fp = other.fingerprint();
  if (!fp.empty() && !other_fp.empty()) {
    return fp == other_fp;
  }

  // Fall back on field-by-field comparison
  for (int i = 0; i < num_fields(); ++i) {
    if (!field(i)->Equals(*other.field(i).get(), check_metadata)) {
      return false;
    }
  }

  return true;
}

bool Schema::Equals(const std::shared_ptr<Schema>& other, bool check_metadata) const {
  if (other == nullptr) {
    return false;
  }

  return Equals(*other, check_metadata);
}

std::shared_ptr<Field> Schema::GetFieldByName(const std::string& name) const {
  int i = GetFieldIndex(name);
  return i == -1 ? nullptr : impl_->fields_[i];
}

int Schema::GetFieldIndex(const std::string& name) const {
  return LookupNameIndex(impl_->name_to_index_, name);
}

std::vector<int> Schema::GetAllFieldIndices(const std::string& name) const {
  std::vector<int> result;
  auto p = impl_->name_to_index_.equal_range(name);
  for (auto it = p.first; it != p.second; ++it) {
    result.push_back(it->second);
  }
  if (result.size() > 1) {
    std::sort(result.begin(), result.end());
  }
  return result;
}

Status Schema::CanReferenceFieldByName(const std::string& name) const {
  if (GetFieldByName(name) == nullptr) {
    return Status::Invalid("Field named '", name,
                           "' not found or not unique in the schema.");
  }
  return Status::OK();
}

Status Schema::CanReferenceFieldsByNames(const std::vector<std::string>& names) const {
  for (const auto& name : names) {
    ARROW_RETURN_NOT_OK(CanReferenceFieldByName(name));
  }
  return Status::OK();
}

FieldVector Schema::GetAllFieldsByName(const std::string& name) const {
  FieldVector result;
  auto p = impl_->name_to_index_.equal_range(name);
  for (auto it = p.first; it != p.second; ++it) {
    result.push_back(impl_->fields_[it->second]);
  }
  return result;
}

Result<std::shared_ptr<Schema>> Schema::AddField(
    int i, const std::shared_ptr<Field>& field) const {
  if (i < 0 || i > this->num_fields()) {
    return Status::Invalid("Invalid column index to add field.");
  }

  return std::make_shared<Schema>(internal::AddVectorElement(impl_->fields_, i, field),
                                  impl_->metadata_);
}

Result<std::shared_ptr<Schema>> Schema::SetField(
    int i, const std::shared_ptr<Field>& field) const {
  if (i < 0 || i > this->num_fields()) {
    return Status::Invalid("Invalid column index to set field.");
  }

  return std::make_shared<Schema>(
      internal::ReplaceVectorElement(impl_->fields_, i, field), impl_->metadata_);
}

Result<std::shared_ptr<Schema>> Schema::RemoveField(int i) const {
  if (i < 0 || i >= this->num_fields()) {
    return Status::Invalid("Invalid column index to remove field.");
  }

  return std::make_shared<Schema>(internal::DeleteVectorElement(impl_->fields_, i),
                                  impl_->metadata_);
}

bool Schema::HasMetadata() const {
  return (impl_->metadata_ != nullptr) && (impl_->metadata_->size() > 0);
}

bool Schema::HasDistinctFieldNames() const {
  auto fields = field_names();
  std::unordered_set<std::string> names{fields.cbegin(), fields.cend()};
  return names.size() == fields.size();
}

Result<std::shared_ptr<Schema>> Schema::WithNames(
    const std::vector<std::string>& names) const {
  if (names.size() != impl_->fields_.size()) {
    return Status::Invalid("attempted to rename schema with ", impl_->fields_.size(),
                           " fields but only ", names.size(), " new names were given");
  }
  FieldVector new_fields;
  new_fields.reserve(names.size());
  auto names_itr = names.begin();
  for (const auto& field : impl_->fields_) {
    new_fields.push_back(field->WithName(*names_itr++));
  }
  return schema(std::move(new_fields));
}

std::shared_ptr<Schema> Schema::WithMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) const {
  return std::make_shared<Schema>(impl_->fields_, metadata);
}

const std::shared_ptr<const KeyValueMetadata>& Schema::metadata() const {
  return impl_->metadata_;
}

std::shared_ptr<Schema> Schema::RemoveMetadata() const {
  return std::make_shared<Schema>(impl_->fields_);
}

std::string Schema::ToString(bool show_metadata) const {
  std::stringstream buffer;

  int i = 0;
  for (const auto& field : impl_->fields_) {
    if (i > 0) {
      buffer << std::endl;
    }
    buffer << field->ToString(show_metadata);
    ++i;
  }

  if (impl_->endianness_ != Endianness::Native) {
    buffer << "\n-- endianness: " << EndiannessToString(impl_->endianness_) << " --";
  }

  if (show_metadata && HasMetadata()) {
    buffer << impl_->metadata_->ToString();
  }

  return buffer.str();
}

std::vector<std::string> Schema::field_names() const {
  std::vector<std::string> names;
  for (const auto& field : impl_->fields_) {
    names.push_back(field->name());
  }
  return names;
}

class SchemaBuilder::Impl {
 public:
  friend class SchemaBuilder;
  Impl(ConflictPolicy policy, Field::MergeOptions field_merge_options)
      : policy_(policy), field_merge_options_(field_merge_options) {}

  Impl(FieldVector fields, std::shared_ptr<const KeyValueMetadata> metadata,
       ConflictPolicy conflict_policy, Field::MergeOptions field_merge_options)
      : fields_(std::move(fields)),
        name_to_index_(CreateNameToIndexMap(fields_)),
        metadata_(std::move(metadata)),
        policy_(conflict_policy),
        field_merge_options_(field_merge_options) {}

  Status AddField(const std::shared_ptr<Field>& field) {
    DCHECK_NE(field, nullptr);

    // Short-circuit, no lookup needed.
    if (policy_ == CONFLICT_APPEND) {
      return AppendField(field);
    }

    auto name = field->name();
    constexpr int kNotFound = -1;
    constexpr int kDuplicateFound = -2;
    auto i = LookupNameIndex<kNotFound, kDuplicateFound>(name_to_index_, name);

    if (i == kNotFound) {
      return AppendField(field);
    }

    // From this point, there's one or more field in the builder that exists with
    // the same name.

    if (policy_ == CONFLICT_IGNORE) {
      // The ignore policy is more generous when there's duplicate in the builder.
      return Status::OK();
    } else if (policy_ == CONFLICT_ERROR) {
      return Status::Invalid("Duplicate found, policy dictate to treat as an error");
    }

    if (i == kDuplicateFound) {
      // Cannot merge/replace when there's more than one field in the builder
      // because we can't decide which to merge/replace.
      return Status::Invalid("Cannot merge field ", name,
                             " more than one field with same name exists");
    }

    DCHECK_GE(i, 0);

    if (policy_ == CONFLICT_REPLACE) {
      fields_[i] = field;
    } else if (policy_ == CONFLICT_MERGE) {
      ARROW_ASSIGN_OR_RAISE(fields_[i],
                            fields_[i]->MergeWith(field, field_merge_options_));
    }

    return Status::OK();
  }

  Status AppendField(const std::shared_ptr<Field>& field) {
    name_to_index_.emplace(field->name(), static_cast<int>(fields_.size()));
    fields_.push_back(field);
    return Status::OK();
  }

  void Reset() {
    fields_.clear();
    name_to_index_.clear();
    metadata_.reset();
  }

 private:
  FieldVector fields_;
  std::unordered_multimap<std::string, int> name_to_index_;
  std::shared_ptr<const KeyValueMetadata> metadata_;
  ConflictPolicy policy_;
  Field::MergeOptions field_merge_options_;
};

SchemaBuilder::SchemaBuilder(ConflictPolicy policy,
                             Field::MergeOptions field_merge_options) {
  impl_ = std::make_unique<Impl>(policy, field_merge_options);
}

SchemaBuilder::SchemaBuilder(FieldVector fields, ConflictPolicy policy,
                             Field::MergeOptions field_merge_options) {
  impl_ = std::make_unique<Impl>(std::move(fields), nullptr, policy, field_merge_options);
}

SchemaBuilder::SchemaBuilder(const std::shared_ptr<Schema>& schema, ConflictPolicy policy,
                             Field::MergeOptions field_merge_options) {
  std::shared_ptr<const KeyValueMetadata> metadata;
  if (schema->HasMetadata()) {
    metadata = schema->metadata()->Copy();
  }

  impl_ = std::make_unique<Impl>(schema->fields(), std::move(metadata), policy,
                                 field_merge_options);
}

SchemaBuilder::~SchemaBuilder() {}

SchemaBuilder::ConflictPolicy SchemaBuilder::policy() const { return impl_->policy_; }

void SchemaBuilder::SetPolicy(SchemaBuilder::ConflictPolicy resolution) {
  impl_->policy_ = resolution;
}

Status SchemaBuilder::AddField(const std::shared_ptr<Field>& field) {
  return impl_->AddField(field);
}

Status SchemaBuilder::AddFields(const FieldVector& fields) {
  for (const auto& field : fields) {
    RETURN_NOT_OK(AddField(field));
  }

  return Status::OK();
}

Status SchemaBuilder::AddSchema(const std::shared_ptr<Schema>& schema) {
  DCHECK_NE(schema, nullptr);
  return AddFields(schema->fields());
}

Status SchemaBuilder::AddSchemas(const std::vector<std::shared_ptr<Schema>>& schemas) {
  for (const auto& schema : schemas) {
    RETURN_NOT_OK(AddSchema(schema));
  }

  return Status::OK();
}

Status SchemaBuilder::AddMetadata(const KeyValueMetadata& metadata) {
  impl_->metadata_ = metadata.Copy();
  return Status::OK();
}

Result<std::shared_ptr<Schema>> SchemaBuilder::Finish() const {
  return schema(impl_->fields_, impl_->metadata_);
}

void SchemaBuilder::Reset() { impl_->Reset(); }

Result<std::shared_ptr<Schema>> SchemaBuilder::Merge(
    const std::vector<std::shared_ptr<Schema>>& schemas, ConflictPolicy policy) {
  SchemaBuilder builder{policy};
  RETURN_NOT_OK(builder.AddSchemas(schemas));
  return builder.Finish();
}

Status SchemaBuilder::AreCompatible(const std::vector<std::shared_ptr<Schema>>& schemas,
                                    ConflictPolicy policy) {
  return Merge(schemas, policy).status();
}

std::shared_ptr<Schema> schema(FieldVector fields,
                               std::shared_ptr<const KeyValueMetadata> metadata) {
  return std::make_shared<Schema>(std::move(fields), std::move(metadata));
}

std::shared_ptr<Schema> schema(
    std::initializer_list<std::pair<std::string, std::shared_ptr<DataType>>> fields,
    std::shared_ptr<const KeyValueMetadata> metadata) {
  return std::make_shared<Schema>(MakeFields(fields), std::move(metadata));
}

std::shared_ptr<Schema> schema(FieldVector fields, Endianness endianness,
                               std::shared_ptr<const KeyValueMetadata> metadata) {
  return std::make_shared<Schema>(std::move(fields), endianness, std::move(metadata));
}

std::shared_ptr<Schema> schema(
    std::initializer_list<std::pair<std::string, std::shared_ptr<DataType>>> fields,
    Endianness endianness, std::shared_ptr<const KeyValueMetadata> metadata) {
  return std::make_shared<Schema>(MakeFields(fields), endianness, std::move(metadata));
}

Result<std::shared_ptr<Schema>> UnifySchemas(
    const std::vector<std::shared_ptr<Schema>>& schemas,
    const Field::MergeOptions field_merge_options) {
  if (schemas.empty()) {
    return Status::Invalid("Must provide at least one schema to unify.");
  }

  if (!schemas[0]->HasDistinctFieldNames()) {
    return Status::Invalid("Can't unify schema with duplicate field names.");
  }

  SchemaBuilder builder{schemas[0], SchemaBuilder::CONFLICT_MERGE, field_merge_options};

  for (size_t i = 1; i < schemas.size(); i++) {
    const auto& schema = schemas[i];
    if (!schema->HasDistinctFieldNames()) {
      return Status::Invalid("Can't unify schema with duplicate field names.");
    }
    RETURN_NOT_OK(builder.AddSchema(schema));
  }

  return builder.Finish();
}

// ----------------------------------------------------------------------
// Fingerprint computations

namespace detail {

Fingerprintable::~Fingerprintable() {
  delete fingerprint_.load();
  delete metadata_fingerprint_.load();
}

template <typename ComputeFingerprint>
static const std::string& LoadFingerprint(std::atomic<std::string*>* fingerprint,
                                          ComputeFingerprint&& compute_fingerprint) {
  auto new_p = new std::string(std::forward<ComputeFingerprint>(compute_fingerprint)());
  // Since fingerprint() and metadata_fingerprint() return a *reference* to the
  // allocated string, the first allocation ever should never be replaced by another
  // one.  Hence the compare_exchange_strong() against nullptr.
  std::string* expected = nullptr;
  if (fingerprint->compare_exchange_strong(expected, new_p)) {
    return *new_p;
  } else {
    delete new_p;
    DCHECK_NE(expected, nullptr);
    return *expected;
  }
}

const std::string& Fingerprintable::LoadFingerprintSlow() const {
  return LoadFingerprint(&fingerprint_, [this]() { return ComputeFingerprint(); });
}

const std::string& Fingerprintable::LoadMetadataFingerprintSlow() const {
  return LoadFingerprint(&metadata_fingerprint_,
                         [this]() { return ComputeMetadataFingerprint(); });
}

}  // namespace detail

static inline std::string TypeIdFingerprint(const DataType& type) {
  auto c = static_cast<int>(type.id()) + 'A';
  DCHECK_GE(c, 0);
  DCHECK_LT(c, 128);  // Unlikely to happen any soon
  // Prefix with an unusual character in order to disambiguate
  std::string s{'@', static_cast<char>(c)};
  return s;
}

static char TimeUnitFingerprint(TimeUnit::type unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      return 's';
    case TimeUnit::MILLI:
      return 'm';
    case TimeUnit::MICRO:
      return 'u';
    case TimeUnit::NANO:
      return 'n';
    default:
      DCHECK(false) << "Unexpected TimeUnit";
      return '\0';
  }
}

static char IntervalTypeFingerprint(IntervalType::type unit) {
  switch (unit) {
    case IntervalType::DAY_TIME:
      return 'd';
    case IntervalType::MONTHS:
      return 'M';
    case IntervalType::MONTH_DAY_NANO:
      return 'N';
    default:
      DCHECK(false) << "Unexpected IntervalType::type";
      return '\0';
  }
}

static void AppendMetadataFingerprint(const KeyValueMetadata& metadata,
                                      std::stringstream* ss) {
  // Compute metadata fingerprint.  KeyValueMetadata is not immutable,
  // so we don't cache the result on the metadata instance.
  const auto pairs = metadata.sorted_pairs();
  if (!pairs.empty()) {
    *ss << "!{";
    for (const auto& p : pairs) {
      const auto& k = p.first;
      const auto& v = p.second;
      // Since metadata strings can contain arbitrary characters, prefix with
      // string length to disambiguate.
      *ss << k.length() << ':' << k << ':';
      *ss << v.length() << ':' << v << ';';
    }
    *ss << '}';
  }
}

std::string Field::ComputeFingerprint() const {
  const auto& type_fingerprint = type_->fingerprint();
  if (type_fingerprint.empty()) {
    // Underlying DataType doesn't support fingerprinting.
    return "";
  }
  std::stringstream ss;
  ss << 'F';
  if (nullable_) {
    ss << 'n';
  } else {
    ss << 'N';
  }
  ss << name_;
  ss << '{' << type_fingerprint << '}';
  return ss.str();
}

std::string Field::ComputeMetadataFingerprint() const {
  std::stringstream ss;
  if (metadata_) {
    AppendMetadataFingerprint(*metadata_, &ss);
  }
  const auto& type_fingerprint = type_->metadata_fingerprint();
  if (!type_fingerprint.empty()) {
    ss << "+{" << type_->metadata_fingerprint() << "}";
  }
  return ss.str();
}

std::string Schema::ComputeFingerprint() const {
  std::stringstream ss;
  ss << "S{";
  for (const auto& field : fields()) {
    const auto& field_fingerprint = field->fingerprint();
    if (field_fingerprint.empty()) {
      return "";
    }
    ss << field_fingerprint << ";";
  }
  ss << (endianness() == Endianness::Little ? "L" : "B");
  ss << "}";
  return ss.str();
}

std::string Schema::ComputeMetadataFingerprint() const {
  std::stringstream ss;
  if (HasMetadata()) {
    AppendMetadataFingerprint(*metadata(), &ss);
  }
  ss << "S{";
  for (const auto& field : fields()) {
    const auto& field_fingerprint = field->metadata_fingerprint();
    ss << field_fingerprint << ";";
  }
  ss << "}";
  return ss.str();
}

void PrintTo(const Schema& s, std::ostream* os) { *os << s; }

std::string DataType::ComputeFingerprint() const {
  // Default implementation returns empty string, signalling non-implemented
  // functionality.
  return "";
}

std::string DataType::ComputeMetadataFingerprint() const {
  // Whatever the data type, metadata can only be found on child fields
  std::string s;
  for (const auto& child : children_) {
    // Add field name to metadata fingerprint so that the field names within
    // list and map types are included as part of the metadata. They are
    // excluded from the base fingerprint.
    s += child->name() + "=";
    s += child->metadata_fingerprint() + ";";
  }
  return s;
}

#define PARAMETER_LESS_FINGERPRINT(TYPE_CLASS)               \
  std::string TYPE_CLASS##Type::ComputeFingerprint() const { \
    return TypeIdFingerprint(*this);                         \
  }

PARAMETER_LESS_FINGERPRINT(Null)
PARAMETER_LESS_FINGERPRINT(Boolean)
PARAMETER_LESS_FINGERPRINT(Int8)
PARAMETER_LESS_FINGERPRINT(Int16)
PARAMETER_LESS_FINGERPRINT(Int32)
PARAMETER_LESS_FINGERPRINT(Int64)
PARAMETER_LESS_FINGERPRINT(UInt8)
PARAMETER_LESS_FINGERPRINT(UInt16)
PARAMETER_LESS_FINGERPRINT(UInt32)
PARAMETER_LESS_FINGERPRINT(UInt64)
PARAMETER_LESS_FINGERPRINT(HalfFloat)
PARAMETER_LESS_FINGERPRINT(Float)
PARAMETER_LESS_FINGERPRINT(Double)
PARAMETER_LESS_FINGERPRINT(Binary)
PARAMETER_LESS_FINGERPRINT(BinaryView)
PARAMETER_LESS_FINGERPRINT(LargeBinary)
PARAMETER_LESS_FINGERPRINT(String)
PARAMETER_LESS_FINGERPRINT(StringView)
PARAMETER_LESS_FINGERPRINT(LargeString)
PARAMETER_LESS_FINGERPRINT(Date32)
PARAMETER_LESS_FINGERPRINT(Date64)

#undef PARAMETER_LESS_FINGERPRINT

std::string DictionaryType::ComputeFingerprint() const {
  const auto& index_fingerprint = index_type_->fingerprint();
  const auto& value_fingerprint = value_type_->fingerprint();
  std::string ordered_fingerprint = ordered_ ? "1" : "0";

  DCHECK(!index_fingerprint.empty());  // it's an integer type
  if (!value_fingerprint.empty()) {
    return TypeIdFingerprint(*this) + index_fingerprint + value_fingerprint +
           ordered_fingerprint;
  }
  return ordered_fingerprint;
}

std::string ListType::ComputeFingerprint() const {
  const auto& child_fingerprint = value_type()->fingerprint();
  if (!child_fingerprint.empty()) {
    std::stringstream ss;
    ss << TypeIdFingerprint(*this);
    if (value_field()->nullable()) {
      ss << 'n';
    } else {
      ss << 'N';
    }
    ss << '{' << child_fingerprint << '}';
    return ss.str();
  }
  return "";
}

std::string LargeListType::ComputeFingerprint() const {
  const auto& child_fingerprint = value_type()->fingerprint();
  if (!child_fingerprint.empty()) {
    std::stringstream ss;
    ss << TypeIdFingerprint(*this);
    if (value_field()->nullable()) {
      ss << 'n';
    } else {
      ss << 'N';
    }
    ss << '{' << child_fingerprint << '}';
    return ss.str();
  }
  return "";
}

std::string ListViewType::ComputeFingerprint() const {
  const auto& child_fingerprint = value_type()->fingerprint();
  if (!child_fingerprint.empty()) {
    std::stringstream ss;
    ss << TypeIdFingerprint(*this);
    if (value_field()->nullable()) {
      ss << 'n';
    } else {
      ss << 'N';
    }
    ss << '{' << child_fingerprint << '}';
    return ss.str();
  }
  return "";
}

std::string LargeListViewType::ComputeFingerprint() const {
  const auto& child_fingerprint = value_type()->fingerprint();
  if (!child_fingerprint.empty()) {
    std::stringstream ss;
    ss << TypeIdFingerprint(*this);
    if (value_field()->nullable()) {
      ss << 'n';
    } else {
      ss << 'N';
    }
    ss << '{' << child_fingerprint << '}';
    return ss.str();
  }
  return "";
}

std::string MapType::ComputeFingerprint() const {
  const auto& key_fingerprint = key_type()->fingerprint();
  const auto& item_fingerprint = item_type()->fingerprint();
  if (!key_fingerprint.empty() && !item_fingerprint.empty()) {
    std::stringstream ss;
    ss << TypeIdFingerprint(*this);
    if (keys_sorted_) {
      ss << 's';
    }
    if (item_field()->nullable()) {
      ss << 'n';
    } else {
      ss << 'N';
    }
    ss << '{' << key_fingerprint + item_fingerprint << '}';
    return ss.str();
  }
  return "";
}

std::string FixedSizeListType::ComputeFingerprint() const {
  const auto& child_fingerprint = value_type()->fingerprint();
  if (!child_fingerprint.empty()) {
    std::stringstream ss;
    ss << TypeIdFingerprint(*this);
    if (value_field()->nullable()) {
      ss << 'n';
    } else {
      ss << 'N';
    }
    ss << "[" << list_size_ << "]"
       << "{" << child_fingerprint << "}";
    return ss.str();
  }
  return "";
}

std::string FixedSizeBinaryType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this) << "[" << byte_width_ << "]";
  return ss.str();
}

std::string DecimalType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this) << "[" << byte_width_ << "," << precision_ << ","
     << scale_ << "]";
  return ss.str();
}

std::string RunEndEncodedType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this) << "{";
  ss << run_end_type()->fingerprint() << ";";
  ss << value_type()->fingerprint() << ";";
  ss << "}";
  return ss.str();
}

std::string StructType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this) << "{";
  for (const auto& child : children_) {
    const auto& child_fingerprint = child->fingerprint();
    if (child_fingerprint.empty()) {
      return "";
    }
    ss << child_fingerprint << ";";
  }
  ss << "}";
  return ss.str();
}

std::string UnionType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this);
  switch (mode()) {
    case UnionMode::SPARSE:
      ss << "[s";
      break;
    case UnionMode::DENSE:
      ss << "[d";
      break;
    default:
      DCHECK(false) << "Unexpected UnionMode";
  }
  for (const auto code : type_codes_) {
    // Represent code as integer, not raw character
    ss << ':' << static_cast<int32_t>(code);
  }
  ss << "]{";
  for (const auto& child : children_) {
    const auto& child_fingerprint = child->fingerprint();
    if (child_fingerprint.empty()) {
      return "";
    }
    ss << child_fingerprint << ";";
  }
  ss << "}";
  return ss.str();
}

std::string TimeType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this) << TimeUnitFingerprint(unit_);
  return ss.str();
}

std::string TimestampType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this) << TimeUnitFingerprint(unit_) << timezone_.length()
     << ':' << timezone_;
  return ss.str();
}

std::string IntervalType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this) << IntervalTypeFingerprint(interval_type());
  return ss.str();
}

std::string DurationType::ComputeFingerprint() const {
  std::stringstream ss;
  ss << TypeIdFingerprint(*this) << TimeUnitFingerprint(unit_);
  return ss.str();
}

// ----------------------------------------------------------------------
// Visitors and factory functions

Status DataType::Accept(TypeVisitor* visitor) const {
  return VisitTypeInline(*this, visitor);
}

#define TYPE_FACTORY(NAME, KLASS)                                        \
  const std::shared_ptr<DataType>& NAME() {                              \
    static std::shared_ptr<DataType> result = std::make_shared<KLASS>(); \
    return result;                                                       \
  }

TYPE_FACTORY(null, NullType)
TYPE_FACTORY(boolean, BooleanType)
TYPE_FACTORY(int8, Int8Type)
TYPE_FACTORY(uint8, UInt8Type)
TYPE_FACTORY(int16, Int16Type)
TYPE_FACTORY(uint16, UInt16Type)
TYPE_FACTORY(int32, Int32Type)
TYPE_FACTORY(uint32, UInt32Type)
TYPE_FACTORY(int64, Int64Type)
TYPE_FACTORY(uint64, UInt64Type)
TYPE_FACTORY(float16, HalfFloatType)
TYPE_FACTORY(float32, FloatType)
TYPE_FACTORY(float64, DoubleType)
TYPE_FACTORY(utf8, StringType)
TYPE_FACTORY(large_utf8, LargeStringType)
TYPE_FACTORY(binary, BinaryType)
TYPE_FACTORY(large_binary, LargeBinaryType)
TYPE_FACTORY(date64, Date64Type)
TYPE_FACTORY(date32, Date32Type)

const std::shared_ptr<DataType>& utf8_view() {
  static std::shared_ptr<DataType> type = std::make_shared<StringViewType>();
  return type;
}

const std::shared_ptr<DataType>& binary_view() {
  static std::shared_ptr<DataType> type = std::make_shared<BinaryViewType>();
  return type;
}

std::shared_ptr<DataType> fixed_size_binary(int32_t byte_width) {
  return std::make_shared<FixedSizeBinaryType>(byte_width);
}

std::shared_ptr<DataType> duration(TimeUnit::type unit) {
  return std::make_shared<DurationType>(unit);
}

std::shared_ptr<DataType> day_time_interval() {
  return std::make_shared<DayTimeIntervalType>();
}

std::shared_ptr<DataType> month_day_nano_interval() {
  return std::make_shared<MonthDayNanoIntervalType>();
}

std::shared_ptr<DataType> month_interval() {
  return std::make_shared<MonthIntervalType>();
}

std::shared_ptr<DataType> timestamp(TimeUnit::type unit) {
  return std::make_shared<TimestampType>(unit);
}

std::shared_ptr<DataType> timestamp(TimeUnit::type unit, const std::string& timezone) {
  return std::make_shared<TimestampType>(unit, timezone);
}

std::shared_ptr<DataType> time32(TimeUnit::type unit) {
  return std::make_shared<Time32Type>(unit);
}

std::shared_ptr<DataType> time64(TimeUnit::type unit) {
  return std::make_shared<Time64Type>(unit);
}

std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type) {
  return std::make_shared<ListType>(value_type);
}

std::shared_ptr<DataType> list(const std::shared_ptr<Field>& value_field) {
  return std::make_shared<ListType>(value_field);
}

std::shared_ptr<DataType> large_list(const std::shared_ptr<DataType>& value_type) {
  return std::make_shared<LargeListType>(value_type);
}

std::shared_ptr<DataType> large_list(const std::shared_ptr<Field>& value_field) {
  return std::make_shared<LargeListType>(value_field);
}

std::shared_ptr<DataType> map(std::shared_ptr<DataType> key_type,
                              std::shared_ptr<DataType> item_type, bool keys_sorted) {
  return std::make_shared<MapType>(std::move(key_type), std::move(item_type),
                                   keys_sorted);
}

std::shared_ptr<DataType> map(std::shared_ptr<DataType> key_type,
                              std::shared_ptr<Field> item_field, bool keys_sorted) {
  return std::make_shared<MapType>(std::move(key_type), std::move(item_field),
                                   keys_sorted);
}

std::shared_ptr<DataType> map(std::shared_ptr<Field> key_field,
                              std::shared_ptr<Field> item_field, bool keys_sorted) {
  return std::make_shared<MapType>(std::move(key_field), std::move(item_field),
                                   keys_sorted);
}

std::shared_ptr<DataType> fixed_size_list(const std::shared_ptr<DataType>& value_type,
                                          int32_t list_size) {
  return std::make_shared<FixedSizeListType>(value_type, list_size);
}

std::shared_ptr<DataType> fixed_size_list(const std::shared_ptr<Field>& value_field,
                                          int32_t list_size) {
  return std::make_shared<FixedSizeListType>(value_field, list_size);
}

std::shared_ptr<DataType> list_view(std::shared_ptr<DataType> value_type) {
  return std::make_shared<ListViewType>(std::move(value_type));
}

std::shared_ptr<DataType> list_view(std::shared_ptr<Field> value_field) {
  return std::make_shared<ListViewType>(std::move(value_field));
}

std::shared_ptr<DataType> large_list_view(std::shared_ptr<DataType> value_type) {
  return std::make_shared<LargeListViewType>(std::move(value_type));
}

std::shared_ptr<DataType> large_list_view(std::shared_ptr<Field> value_field) {
  return std::make_shared<LargeListViewType>(std::move(value_field));
}

std::shared_ptr<DataType> struct_(const FieldVector& fields) {
  return std::make_shared<StructType>(fields);
}

std::shared_ptr<DataType> struct_(
    std::initializer_list<std::pair<std::string, std::shared_ptr<DataType>>> fields) {
  return std::make_shared<StructType>(MakeFields(fields));
}

std::shared_ptr<DataType> run_end_encoded(std::shared_ptr<DataType> run_end_type,
                                          std::shared_ptr<DataType> value_type) {
  return std::make_shared<RunEndEncodedType>(std::move(run_end_type),
                                             std::move(value_type));
}

std::shared_ptr<DataType> sparse_union(FieldVector child_fields,
                                       std::vector<int8_t> type_codes) {
  if (type_codes.empty()) {
    type_codes = internal::Iota(static_cast<int8_t>(child_fields.size()));
  }
  return std::make_shared<SparseUnionType>(std::move(child_fields),
                                           std::move(type_codes));
}
std::shared_ptr<DataType> dense_union(FieldVector child_fields,
                                      std::vector<int8_t> type_codes) {
  if (type_codes.empty()) {
    type_codes = internal::Iota(static_cast<int8_t>(child_fields.size()));
  }
  return std::make_shared<DenseUnionType>(std::move(child_fields), std::move(type_codes));
}

FieldVector FieldsFromArraysAndNames(std::vector<std::string> names,
                                     const ArrayVector& arrays) {
  FieldVector fields(arrays.size());
  int i = 0;
  if (names.empty()) {
    for (const auto& array : arrays) {
      fields[i] = field(internal::ToChars(i), array->type());
      ++i;
    }
  } else {
    DCHECK_EQ(names.size(), arrays.size());
    for (const auto& array : arrays) {
      fields[i] = field(std::move(names[i]), array->type());
      ++i;
    }
  }
  return fields;
}

std::shared_ptr<DataType> sparse_union(const ArrayVector& children,
                                       std::vector<std::string> field_names,
                                       std::vector<int8_t> type_codes) {
  if (type_codes.empty()) {
    type_codes = internal::Iota(static_cast<int8_t>(children.size()));
  }
  auto fields = FieldsFromArraysAndNames(std::move(field_names), children);
  return sparse_union(std::move(fields), std::move(type_codes));
}

std::shared_ptr<DataType> dense_union(const ArrayVector& children,
                                      std::vector<std::string> field_names,
                                      std::vector<int8_t> type_codes) {
  if (type_codes.empty()) {
    type_codes = internal::Iota(static_cast<int8_t>(children.size()));
  }
  auto fields = FieldsFromArraysAndNames(std::move(field_names), children);
  return dense_union(std::move(fields), std::move(type_codes));
}

std::shared_ptr<DataType> dictionary(const std::shared_ptr<DataType>& index_type,
                                     const std::shared_ptr<DataType>& dict_type,
                                     bool ordered) {
  return std::make_shared<DictionaryType>(index_type, dict_type, ordered);
}

std::shared_ptr<Field> field(std::string name, std::shared_ptr<DataType> type,
                             bool nullable,
                             std::shared_ptr<const KeyValueMetadata> metadata) {
  return std::make_shared<Field>(std::move(name), std::move(type), nullable,
                                 std::move(metadata));
}

std::shared_ptr<Field> field(std::string name, std::shared_ptr<DataType> type,
                             std::shared_ptr<const KeyValueMetadata> metadata) {
  return std::make_shared<Field>(std::move(name), std::move(type), /*nullable=*/true,
                                 std::move(metadata));
}

std::shared_ptr<DataType> decimal(int32_t precision, int32_t scale) {
  return precision <= Decimal128Type::kMaxPrecision ? decimal128(precision, scale)
                                                    : decimal256(precision, scale);
}

std::shared_ptr<DataType> decimal128(int32_t precision, int32_t scale) {
  return std::make_shared<Decimal128Type>(precision, scale);
}

std::shared_ptr<DataType> decimal256(int32_t precision, int32_t scale) {
  return std::make_shared<Decimal256Type>(precision, scale);
}

std::string Decimal128Type::ToString(bool show_metadata) const {
  std::stringstream s;
  s << "decimal128(" << precision_ << ", " << scale_ << ")";
  return s.str();
}

std::string Decimal256Type::ToString(bool show_metadata) const {
  std::stringstream s;
  s << "decimal256(" << precision_ << ", " << scale_ << ")";
  return s.str();
}

namespace {

std::vector<std::shared_ptr<DataType>> g_signed_int_types;
std::vector<std::shared_ptr<DataType>> g_unsigned_int_types;
std::vector<std::shared_ptr<DataType>> g_int_types;
std::vector<std::shared_ptr<DataType>> g_floating_types;
std::vector<std::shared_ptr<DataType>> g_numeric_types;
std::vector<std::shared_ptr<DataType>> g_base_binary_types;
std::vector<std::shared_ptr<DataType>> g_temporal_types;
std::vector<std::shared_ptr<DataType>> g_interval_types;
std::vector<std::shared_ptr<DataType>> g_duration_types;
std::vector<std::shared_ptr<DataType>> g_primitive_types;
std::once_flag static_data_initialized;

template <typename T>
void Extend(const std::vector<T>& values, std::vector<T>* out) {
  out->insert(out->end(), values.begin(), values.end());
}

void InitStaticData() {
  // Signed int types
  g_signed_int_types = {int8(), int16(), int32(), int64()};

  // Unsigned int types
  g_unsigned_int_types = {uint8(), uint16(), uint32(), uint64()};

  // All int types
  Extend(g_unsigned_int_types, &g_int_types);
  Extend(g_signed_int_types, &g_int_types);

  // Floating point types
  g_floating_types = {float32(), float64()};

  // Numeric types
  Extend(g_int_types, &g_numeric_types);
  Extend(g_floating_types, &g_numeric_types);

  // Temporal types
  g_temporal_types = {date32(),
                      date64(),
                      time32(TimeUnit::SECOND),
                      time32(TimeUnit::MILLI),
                      time64(TimeUnit::MICRO),
                      time64(TimeUnit::NANO),
                      timestamp(TimeUnit::SECOND),
                      timestamp(TimeUnit::MILLI),
                      timestamp(TimeUnit::MICRO),
                      timestamp(TimeUnit::NANO)};

  // Interval types
  g_interval_types = {day_time_interval(), month_interval(), month_day_nano_interval()};

  // Duration types
  g_duration_types = {duration(TimeUnit::SECOND), duration(TimeUnit::MILLI),
                      duration(TimeUnit::MICRO), duration(TimeUnit::NANO)};

  // Base binary types (without FixedSizeBinary)
  g_base_binary_types = {binary(), utf8(), large_binary(), large_utf8()};

  // Non-parametric, non-nested types. This also DOES NOT include
  //
  // * Decimal
  // * Fixed Size Binary
  // * Time32
  // * Time64
  // * Timestamp
  g_primitive_types = {null(), boolean(), date32(), date64(), binary_view(), utf8_view()};
  Extend(g_numeric_types, &g_primitive_types);
  Extend(g_base_binary_types, &g_primitive_types);
}

}  // namespace

const std::vector<std::shared_ptr<DataType>>& BaseBinaryTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_base_binary_types;
}

const std::vector<std::shared_ptr<DataType>>& BinaryTypes() {
  static DataTypeVector types = {binary(), large_binary()};
  return types;
}

const std::vector<std::shared_ptr<DataType>>& StringTypes() {
  static DataTypeVector types = {utf8(), large_utf8()};
  return types;
}

const std::vector<std::shared_ptr<DataType>>& SignedIntTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_signed_int_types;
}

const std::vector<std::shared_ptr<DataType>>& UnsignedIntTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_unsigned_int_types;
}

const std::vector<std::shared_ptr<DataType>>& IntTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_int_types;
}

const std::vector<std::shared_ptr<DataType>>& FloatingPointTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_floating_types;
}

const std::vector<std::shared_ptr<DataType>>& NumericTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_numeric_types;
}

const std::vector<std::shared_ptr<DataType>>& TemporalTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_temporal_types;
}

const std::vector<std::shared_ptr<DataType>>& IntervalTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_interval_types;
}

const std::vector<std::shared_ptr<DataType>>& DurationTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_duration_types;
}

const std::vector<std::shared_ptr<DataType>>& PrimitiveTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_primitive_types;
}

const std::vector<TimeUnit::type>& TimeUnit::values() {
  static std::vector<TimeUnit::type> units = {TimeUnit::SECOND, TimeUnit::MILLI,
                                              TimeUnit::MICRO, TimeUnit::NANO};
  return units;
}

}  // namespace arrow
