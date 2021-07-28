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

#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "arrow/type_fwd.h"

namespace arrow {
namespace compute {
namespace internal {

Status ExecFail(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  return Status::NotImplemented("This kernel is malformed");
}

ArrayKernelExec MakeFlippedBinaryExec(ArrayKernelExec exec) {
  return [exec](KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ExecBatch flipped_batch = batch;
    std::swap(flipped_batch.values[0], flipped_batch.values[1]);
    return exec(ctx, flipped_batch, out);
  };
}

std::vector<std::shared_ptr<DataType>> g_signed_int_types;
std::vector<std::shared_ptr<DataType>> g_unsigned_int_types;
std::vector<std::shared_ptr<DataType>> g_int_types;
std::vector<std::shared_ptr<DataType>> g_floating_types;
std::vector<std::shared_ptr<DataType>> g_numeric_types;
std::vector<std::shared_ptr<DataType>> g_base_binary_types;
std::vector<std::shared_ptr<DataType>> g_temporal_types;
std::vector<std::shared_ptr<DataType>> g_primitive_types;
std::vector<Type::type> g_decimal_type_ids;
static std::once_flag codegen_static_initialized;

template <typename T>
void Extend(const std::vector<T>& values, std::vector<T>* out) {
  for (const auto& t : values) {
    out->push_back(t);
  }
}

static void InitStaticData() {
  // Signed int types
  g_signed_int_types = {int8(), int16(), int32(), int64()};

  // Unsigned int types
  g_unsigned_int_types = {uint8(), uint16(), uint32(), uint64()};

  // All int types
  Extend(g_unsigned_int_types, &g_int_types);
  Extend(g_signed_int_types, &g_int_types);

  // Floating point types
  g_floating_types = {float32(), float64()};

  // Decimal types
  g_decimal_type_ids = {Type::DECIMAL128, Type::DECIMAL256};

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

  // Base binary types (without FixedSizeBinary)
  g_base_binary_types = {binary(), utf8(), large_binary(), large_utf8()};

  // Non-parametric, non-nested types. This also DOES NOT include
  //
  // * Decimal
  // * Fixed Size Binary
  // * Time32
  // * Time64
  // * Timestamp
  g_primitive_types = {null(), boolean(), date32(), date64()};
  Extend(g_numeric_types, &g_primitive_types);
  Extend(g_base_binary_types, &g_primitive_types);
}

const std::vector<std::shared_ptr<DataType>>& BaseBinaryTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_base_binary_types;
}

const std::vector<std::shared_ptr<DataType>>& StringTypes() {
  static DataTypeVector types = {utf8(), large_utf8()};
  return types;
}

const std::vector<std::shared_ptr<DataType>>& SignedIntTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_signed_int_types;
}

const std::vector<std::shared_ptr<DataType>>& UnsignedIntTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_unsigned_int_types;
}

const std::vector<std::shared_ptr<DataType>>& IntTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_int_types;
}

const std::vector<std::shared_ptr<DataType>>& FloatingPointTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_floating_types;
}

const std::vector<Type::type>& DecimalTypeIds() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_decimal_type_ids;
}

const std::vector<TimeUnit::type>& AllTimeUnits() {
  static std::vector<TimeUnit::type> units = {TimeUnit::SECOND, TimeUnit::MILLI,
                                              TimeUnit::MICRO, TimeUnit::NANO};
  return units;
}

const std::vector<std::shared_ptr<DataType>>& NumericTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_numeric_types;
}

const std::vector<std::shared_ptr<DataType>>& TemporalTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_temporal_types;
}

const std::vector<std::shared_ptr<DataType>>& PrimitiveTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_primitive_types;
}

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

// Construct dummy parametric types so that we can get VisitTypeInline to
// work above

Result<ValueDescr> FirstType(KernelContext*, const std::vector<ValueDescr>& descrs) {
  ValueDescr result = descrs.front();
  result.shape = GetBroadcastShape(descrs);
  return result;
}

void EnsureDictionaryDecoded(std::vector<ValueDescr>* descrs) {
  for (ValueDescr& descr : *descrs) {
    if (descr.type->id() == Type::DICTIONARY) {
      descr.type = checked_cast<const DictionaryType&>(*descr.type).value_type();
    }
  }
}

void ReplaceNullWithOtherType(std::vector<ValueDescr>* descrs) {
  DCHECK_EQ(descrs->size(), 2);

  if (descrs->at(0).type->id() == Type::NA) {
    descrs->at(0).type = descrs->at(1).type;
    return;
  }

  if (descrs->at(1).type->id() == Type::NA) {
    descrs->at(1).type = descrs->at(0).type;
    return;
  }
}

void ReplaceTypes(const std::shared_ptr<DataType>& type,
                  std::vector<ValueDescr>* descrs) {
  for (auto& descr : *descrs) {
    descr.type = type;
  }
}

std::shared_ptr<DataType> CommonNumeric(const std::vector<ValueDescr>& descrs) {
  return CommonNumeric(descrs.data(), descrs.size());
}

std::shared_ptr<DataType> CommonNumeric(const ValueDescr* begin, size_t count) {
  DCHECK_GT(count, 0) << "tried to find CommonNumeric type of an empty set";

  for (size_t i = 0; i < count; i++) {
    const auto& descr = *(begin + i);
    auto id = descr.type->id();
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
    const auto& descr = *(begin + i);
    if (descr.type->id() == Type::DOUBLE) return float64();
  }

  for (size_t i = 0; i < count; i++) {
    const auto& descr = *(begin + i);
    if (descr.type->id() == Type::FLOAT) return float32();
  }

  int max_width_signed = 0, max_width_unsigned = 0;

  for (size_t i = 0; i < count; i++) {
    const auto& descr = *(begin + i);
    auto id = descr.type->id();
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
    max_width_signed = static_cast<int>(BitUtil::NextPower2(max_width_unsigned + 1));
  }

  if (max_width_signed >= 64) return int64();
  if (max_width_signed == 32) return int32();
  if (max_width_signed == 16) return int16();
  DCHECK_EQ(max_width_signed, 8);
  return int8();
}

std::shared_ptr<DataType> CommonTimestamp(const std::vector<ValueDescr>& descrs) {
  TimeUnit::type finest_unit = TimeUnit::SECOND;

  for (const auto& descr : descrs) {
    auto id = descr.type->id();
    // a common timestamp is only possible if all types are timestamp like
    switch (id) {
      case Type::DATE32:
      case Type::DATE64:
        continue;
      case Type::TIMESTAMP:
        finest_unit =
            std::max(finest_unit, checked_cast<const TimestampType&>(*descr.type).unit());
        continue;
      default:
        return nullptr;
    }
  }

  return timestamp(finest_unit);
}

std::shared_ptr<DataType> CommonBinary(const std::vector<ValueDescr>& descrs) {
  bool all_utf8 = true, all_offset32 = true;

  for (const auto& descr : descrs) {
    auto id = descr.type->id();
    // a common varbinary type is only possible if all types are binary like
    switch (id) {
      case Type::STRING:
        continue;
      case Type::BINARY:
        all_utf8 = false;
        continue;
      case Type::LARGE_STRING:
        all_offset32 = false;
        continue;
      case Type::LARGE_BINARY:
        all_offset32 = false;
        all_utf8 = false;
        continue;
      default:
        return nullptr;
    }
  }

  if (all_utf8) {
    if (all_offset32) return utf8();
    return large_utf8();
  }

  if (all_offset32) return binary();
  return large_binary();
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
