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

#include "arrow/compute/kernels/codegen_internal.h"

#include <cmath>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "arrow/type_fwd.h"

namespace arrow {
namespace compute {
namespace internal {

const std::vector<std::shared_ptr<DataType>>& ExampleParametricTypes() {
  static DataTypeVector example_parametric_types = {
      decimal128(12, 2),
      duration(TimeUnit::SECOND),
      timestamp(TimeUnit::SECOND),
      time32(TimeUnit::SECOND),
      time64(TimeUnit::MICRO),
      fixed_size_binary(0),
      list(null()),
      large_list(null()),
      fixed_size_list(field("dummy", null()), 0),
      struct_({}),
      sparse_union(FieldVector{}),
      dense_union(FieldVector{}),
      dictionary(int32(), null()),
      map(null(), null())};
  return example_parametric_types;
}

Result<TypeHolder> FirstType(KernelContext*, const std::vector<TypeHolder>& types) {
  return types.front();
}

Result<TypeHolder> LastType(KernelContext*, const std::vector<TypeHolder>& types) {
  return types.back();
}

Result<TypeHolder> ListValuesType(KernelContext*, const std::vector<TypeHolder>& args) {
  const auto& list_type = checked_cast<const BaseListType&>(*args[0].type);
  return list_type.value_type().get();
}

void EnsureDictionaryDecoded(std::vector<TypeHolder>* types) {
  EnsureDictionaryDecoded(types->data(), types->size());
}

void EnsureDictionaryDecoded(TypeHolder* begin, size_t count) {
  auto* end = begin + count;
  for (auto it = begin; it != end; it++) {
    if (it->id() == Type::DICTIONARY) {
      *it = checked_cast<const DictionaryType&>(*it->type).value_type();
    }
  }
}

void ReplaceNullWithOtherType(std::vector<TypeHolder>* types) {
  ReplaceNullWithOtherType(types->data(), types->size());
}

void ReplaceNullWithOtherType(TypeHolder* first, size_t count) {
  DCHECK_EQ(count, 2);

  TypeHolder* second = first++;
  if (first->type->id() == Type::NA) {
    *first = *second;
    return;
  }

  if (second->type->id() == Type::NA) {
    *second = *first;
    return;
  }
}

void ReplaceTemporalTypes(const TimeUnit::type unit, std::vector<TypeHolder>* types) {
  auto* end = types->data() + types->size();

  for (auto* it = types->data(); it != end; it++) {
    switch (it->type->id()) {
      case Type::TIMESTAMP: {
        const auto& ty = checked_cast<const TimestampType&>(*it->type);
        *it = timestamp(unit, ty.timezone());
        continue;
      }
      case Type::TIME32:
      case Type::TIME64: {
        if (unit > TimeUnit::MILLI) {
          *it = time64(unit);
        } else {
          *it = time32(unit);
        }
        continue;
      }
      case Type::DURATION: {
        *it = duration(unit);
        continue;
      }
      case Type::DATE32:
      case Type::DATE64: {
        *it = timestamp(unit);
        continue;
      }
      default:
        continue;
    }
  }
}

void ReplaceTypes(const TypeHolder& replacement, std::vector<TypeHolder>* types) {
  ReplaceTypes(replacement, types->data(), types->size());
}

void ReplaceTypes(const TypeHolder& replacement, TypeHolder* begin, size_t count) {
  auto* end = begin + count;
  for (auto* it = begin; it != end; it++) {
    *it = replacement;
  }
}

TypeHolder CommonNumeric(const std::vector<TypeHolder>& types) {
  return CommonNumeric(types.data(), types.size());
}

TypeHolder CommonNumeric(const TypeHolder* begin, size_t count) {
  DCHECK_GT(count, 0) << "tried to find CommonNumeric type of an empty set";

  for (size_t i = 0; i < count; i++) {
    const auto& holder = *(begin + i);
    auto id = holder.id();
    if (!is_floating(id) && !is_integer(id)) {
      // a common numeric type is only possible if all types are numeric
      return nullptr;
    }
    if (id == Type::HALF_FLOAT) {
      // float16 arithmetic is not currently supported
      return nullptr;
    }
  }

  for (size_t i = 0; i < count; i++) {
    const auto& holder = *(begin + i);
    if (holder.id() == Type::DOUBLE) return float64();
  }

  for (size_t i = 0; i < count; i++) {
    const auto& holder = *(begin + i);
    if (holder.id() == Type::FLOAT) return float32();
  }

  int max_width_signed = 0, max_width_unsigned = 0;

  for (size_t i = 0; i < count; i++) {
    const auto& holder = *(begin + i);
    auto id = holder.id();
    auto max_width = &(is_signed_integer(id) ? max_width_signed : max_width_unsigned);
    *max_width = std::max(bit_width(id), *max_width);
  }

  if (max_width_signed == 0) {
    if (max_width_unsigned >= 64) return uint64();
    if (max_width_unsigned == 32) return uint32();
    if (max_width_unsigned == 16) return uint16();
    DCHECK_EQ(max_width_unsigned, 8);
    return uint8();
  }

  if (max_width_signed <= max_width_unsigned) {
    max_width_signed = static_cast<int>(bit_util::NextPower2(max_width_unsigned + 1));
  }

  if (max_width_signed >= 64) return int64();
  if (max_width_signed == 32) return int32();
  if (max_width_signed == 16) return int16();
  DCHECK_EQ(max_width_signed, 8);
  return int8();
}

bool CommonTemporalResolution(const TypeHolder* begin, size_t count,
                              TimeUnit::type* finest_unit) {
  bool is_time_unit = false;
  *finest_unit = TimeUnit::SECOND;
  const TypeHolder* end = begin + count;
  for (auto it = begin; it != end; it++) {
    auto id = it->type->id();
    switch (id) {
      case Type::DATE32: {
        // Date32's unit is days, but the coarsest we have is seconds
        is_time_unit = true;
        continue;
      }
      case Type::DATE64: {
        *finest_unit = std::max(*finest_unit, TimeUnit::MILLI);
        is_time_unit = true;
        continue;
      }
      case Type::TIMESTAMP: {
        const auto& ty = checked_cast<const TimestampType&>(*it->type);
        *finest_unit = std::max(*finest_unit, ty.unit());
        is_time_unit = true;
        continue;
      }
      case Type::DURATION: {
        const auto& ty = checked_cast<const DurationType&>(*it->type);
        *finest_unit = std::max(*finest_unit, ty.unit());
        is_time_unit = true;
        continue;
      }
      case Type::TIME32: {
        const auto& ty = checked_cast<const Time32Type&>(*it->type);
        *finest_unit = std::max(*finest_unit, ty.unit());
        is_time_unit = true;
        continue;
      }
      case Type::TIME64: {
        const auto& ty = checked_cast<const Time64Type&>(*it->type);
        *finest_unit = std::max(*finest_unit, ty.unit());
        is_time_unit = true;
        continue;
      }
      default:
        continue;
    }
  }
  return is_time_unit;
}

TypeHolder CommonTemporal(const TypeHolder* begin, size_t count) {
  TimeUnit::type finest_unit = TimeUnit::SECOND;
  const std::string* timezone = nullptr;
  bool saw_date32 = false;
  bool saw_date64 = false;

  const TypeHolder* end = begin + count;
  for (auto it = begin; it != end; it++) {
    auto id = it->type->id();
    // a common timestamp is only possible if all types are timestamp like
    switch (id) {
      case Type::DATE32:
        // Date32's unit is days, but the coarsest we have is seconds
        saw_date32 = true;
        continue;
      case Type::DATE64:
        finest_unit = std::max(finest_unit, TimeUnit::MILLI);
        saw_date64 = true;
        continue;
      case Type::TIMESTAMP: {
        const auto& ty = checked_cast<const TimestampType&>(*it->type);
        if (timezone && *timezone != ty.timezone()) return TypeHolder(nullptr);
        timezone = &ty.timezone();
        finest_unit = std::max(finest_unit, ty.unit());
        continue;
      }
      default:
        return TypeHolder(nullptr);
    }
  }

  if (timezone) {
    // At least one timestamp seen
    return timestamp(finest_unit, *timezone);
  } else if (saw_date64) {
    return date64();
  } else if (saw_date32) {
    return date32();
  }
  return TypeHolder(nullptr);
}

TypeHolder CommonBinary(const TypeHolder* begin, size_t count) {
  bool all_utf8 = true, all_offset32 = true, all_fixed_width = true;

  const TypeHolder* end = begin + count;
  for (auto it = begin; it != end; ++it) {
    auto id = it->type->id();
    // a common varbinary type is only possible if all types are binary like
    switch (id) {
      case Type::STRING:
        all_fixed_width = false;
        continue;
      case Type::BINARY:
        all_fixed_width = false;
        all_utf8 = false;
        continue;
      case Type::FIXED_SIZE_BINARY:
        all_utf8 = false;
        continue;
      case Type::LARGE_STRING:
        all_offset32 = false;
        all_fixed_width = false;
        continue;
      case Type::LARGE_BINARY:
        all_offset32 = false;
        all_fixed_width = false;
        all_utf8 = false;
        continue;
      default:
        return TypeHolder(nullptr);
    }
  }

  if (all_fixed_width) {
    // At least for the purposes of comparison, no need to cast.
    return TypeHolder(nullptr);
  }

  if (all_utf8) {
    if (all_offset32) return utf8();
    return large_utf8();
  }

  if (all_offset32) return binary();
  return large_binary();
}

Status CastBinaryDecimalArgs(DecimalPromotion promotion, std::vector<TypeHolder>* types) {
  const DataType& left_type = *(*types)[0];
  const DataType& right_type = *(*types)[1];
  DCHECK(is_decimal(left_type.id()) || is_decimal(right_type.id()));

  // decimal + float64 = float64
  // decimal + float32 is roughly float64 + float32 so we choose float64
  if (is_floating(left_type.id()) || is_floating(right_type.id())) {
    (*types)[0] = float64();
    (*types)[1] = float64();
    return Status::OK();
  }

  // precision, scale of left and right args
  int32_t p1, s1, p2, s2;

  // decimal + integer = decimal
  if (is_decimal(left_type.id())) {
    const auto& decimal = checked_cast<const DecimalType&>(left_type);
    p1 = decimal.precision();
    s1 = decimal.scale();
  } else {
    DCHECK(is_integer(left_type.id()));
    ARROW_ASSIGN_OR_RAISE(p1, MaxDecimalDigitsForInteger(left_type.id()));
    s1 = 0;
  }
  if (is_decimal(right_type.id())) {
    const auto& decimal = checked_cast<const DecimalType&>(right_type);
    p2 = decimal.precision();
    s2 = decimal.scale();
  } else {
    DCHECK(is_integer(right_type.id()));
    ARROW_ASSIGN_OR_RAISE(p2, MaxDecimalDigitsForInteger(right_type.id()));
    s2 = 0;
  }
  if (s1 < 0 || s2 < 0) {
    return Status::NotImplemented("Decimals with negative scales not supported");
  }

  // decimal128 + decimal256 = decimal256
  Type::type casted_type_id = Type::DECIMAL128;
  if (left_type.id() == Type::DECIMAL256 || right_type.id() == Type::DECIMAL256) {
    casted_type_id = Type::DECIMAL256;
  }

  // decimal promotion rules compatible with amazon redshift
  // https://docs.aws.amazon.com/redshift/latest/dg/r_numeric_computations201.html
  int32_t left_scaleup = 0;
  int32_t right_scaleup = 0;

  switch (promotion) {
    case DecimalPromotion::kAdd: {
      left_scaleup = std::max(s1, s2) - s1;
      right_scaleup = std::max(s1, s2) - s2;
      break;
    }
    case DecimalPromotion::kMultiply: {
      left_scaleup = right_scaleup = 0;
      break;
    }
    case DecimalPromotion::kDivide: {
      left_scaleup = std::max(4, s1 + p2 - s2 + 1) + s2 - s1;
      right_scaleup = 0;
      break;
    }
    default:
      DCHECK(false) << "Invalid DecimalPromotion value " << static_cast<int>(promotion);
  }
  ARROW_ASSIGN_OR_RAISE(
      auto casted_left,
      DecimalType::Make(casted_type_id, p1 + left_scaleup, s1 + left_scaleup));
  ARROW_ASSIGN_OR_RAISE(
      auto casted_right,
      DecimalType::Make(casted_type_id, p2 + right_scaleup, s2 + right_scaleup));
  (*types)[0] = casted_left;
  (*types)[1] = casted_right;
  return Status::OK();
}

Status CastDecimalArgs(TypeHolder* begin, size_t count) {
  Type::type casted_type_id = Type::DECIMAL128;
  TypeHolder* end = begin + count;

  int32_t max_scale = 0;
  bool any_floating = false;
  for (auto* it = begin; it != end; ++it) {
    const auto& ty = *it->type;
    if (is_floating(ty.id())) {
      // Decimal + float = float
      any_floating = true;
    } else if (is_integer(ty.id())) {
      // Nothing to do here
    } else if (is_decimal(ty.id())) {
      max_scale = std::max(max_scale, checked_cast<const DecimalType&>(ty).scale());
      if (ty.id() == Type::DECIMAL256) {
        casted_type_id = Type::DECIMAL256;
      }
    } else {
      // Non-numeric, can't cast
      return Status::OK();
    }
  }
  if (any_floating) {
    ReplaceTypes(float64(), begin, count);
    return Status::OK();
  }

  // All integer and decimal, rescale
  int32_t common_precision = 0;
  for (auto* it = begin; it != end; ++it) {
    const auto& ty = *it->type;
    if (is_integer(ty.id())) {
      ARROW_ASSIGN_OR_RAISE(auto precision, MaxDecimalDigitsForInteger(ty.id()));
      precision += max_scale;
      common_precision = std::max(common_precision, precision);
    } else if (is_decimal(ty.id())) {
      const auto& decimal_ty = checked_cast<const DecimalType&>(ty);
      auto precision = decimal_ty.precision();
      const auto scale = decimal_ty.scale();
      precision += max_scale - scale;
      common_precision = std::max(common_precision, precision);
    }
  }

  if (common_precision > BasicDecimal256::kMaxPrecision) {
    return Status::Invalid("Result precision (", common_precision,
                           ") exceeds max precision of Decimal256 (",
                           BasicDecimal256::kMaxPrecision, ")");
  } else if (common_precision > BasicDecimal128::kMaxPrecision) {
    casted_type_id = Type::DECIMAL256;
  }

  ARROW_ASSIGN_OR_RAISE(auto casted_ty,
                        DecimalType::Make(casted_type_id, common_precision, max_scale));
  for (auto* it = begin; it != end; ++it) {
    *it = casted_ty;
  }
  return Status::OK();
}

bool HasDecimal(const std::vector<TypeHolder>& types) {
  for (const auto& th : types) {
    if (is_decimal(th.id())) {
      return true;
    }
  }
  return false;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
