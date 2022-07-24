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

#include "arrow/compute/kernels/util_internal.h"

#include <cstdint>
#include <mutex>

#include "arrow/array/data.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

ExecValue GetExecValue(const Datum& value) {
  ExecValue result;
  if (value.is_array()) {
    result.SetArray(*value.array());
  } else {
    result.SetScalar(value.scalar().get());
  }
  return result;
}

int64_t GetTrueCount(const ArraySpan& mask) {
  if (mask.buffers[0].data != nullptr) {
    return CountAndSetBits(mask.buffers[0].data, mask.offset, mask.buffers[1].data,
                           mask.offset, mask.length);
  } else {
    return CountSetBits(mask.buffers[1].data, mask.offset, mask.length);
  }
}

}  // namespace internal

namespace {

std::vector<const DataType*> g_signed_int_types;
std::vector<const DataType*> g_unsigned_int_types;
std::vector<const DataType*> g_int_types;
std::vector<const DataType*> g_floating_types;
std::vector<const DataType*> g_numeric_types;
std::vector<const DataType*> g_temporal_types;
std::vector<const DataType*> g_base_binary_types;
std::vector<const DataType*> g_interval_types;
std::vector<const DataType*> g_primitive_types;

std::once_flag static_data_initialized;

template <typename T>
void Extend(const std::vector<T>& values, std::vector<T>* out) {
  out->insert(out->end(), values.begin(), values.end());
}

void InitStaticData() {
  // Signed int types
  g_signed_int_types = {int8().get(), int16().get(), int32().get(), int64().get()};

  // Unsigned int types
  g_unsigned_int_types = {uint8().get(), uint16().get(), uint32().get(), uint64().get()};

  // All int types
  Extend(g_unsigned_int_types, &g_int_types);
  Extend(g_signed_int_types, &g_int_types);

  // Floating point types
  g_floating_types = {float32().get(), float64().get()};

  // Numeric types
  Extend(g_int_types, &g_numeric_types);
  Extend(g_floating_types, &g_numeric_types);

  // Temporal types
  g_temporal_types = {date32().get(),
                      date64().get(),
                      time32(TimeUnit::SECOND).get(),
                      time32(TimeUnit::MILLI).get(),
                      time64(TimeUnit::MICRO).get(),
                      time64(TimeUnit::NANO).get(),
                      timestamp(TimeUnit::SECOND).get(),
                      timestamp(TimeUnit::MILLI).get(),
                      timestamp(TimeUnit::MICRO).get(),
                      timestamp(TimeUnit::NANO).get()};

  // Interval types
  g_interval_types = {day_time_interval().get(), month_interval().get(),
                      month_day_nano_interval().get()};

  // Base binary types (without FixedSizeBinary)
  g_base_binary_types = {binary().get(), utf8().get(), large_binary().get(),
                         large_utf8().get()};

  // Non-parametric, non-nested types. This also DOES NOT include
  //
  // * Decimal
  // * Fixed Size Binary
  // * Time32
  // * Time64
  // * Timestamp
  g_primitive_types = {null().get(), boolean().get(), date32().get(), date64().get()};
  Extend(g_numeric_types, &g_primitive_types);
  Extend(g_base_binary_types, &g_primitive_types);
}

}  // namespace

const std::vector<const DataType*>& BaseBinaryTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_base_binary_types;
}

const std::vector<const DataType*>& BinaryTypes() {
  static std::vector<const DataType*> types = {binary().get(), large_binary().get()};
  return types;
}

const std::vector<const DataType*>& StringTypes() {
  static std::vector<const DataType*> types = {utf8().get(), large_utf8().get()};
  return types;
}

const std::vector<const DataType*>& SignedIntTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_signed_int_types;
}

const std::vector<const DataType*>& UnsignedIntTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_unsigned_int_types;
}

const std::vector<const DataType*>& IntTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_int_types;
}

const std::vector<const DataType*>& FloatingPointTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_floating_types;
}

const std::vector<const DataType*>& NumericTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_numeric_types;
}

const std::vector<const DataType*>& TemporalTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_temporal_types;
}

const std::vector<const DataType*>& IntervalTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_interval_types;
}

const std::vector<const DataType*>& PrimitiveTypes() {
  std::call_once(static_data_initialized, InitStaticData);
  return g_primitive_types;
}

namespace {

struct PhysicalTypeVisitor {
  const DataType* real_type;
  const DataType* result;

  Status Visit(const DataType&) {
    result = real_type;
    return Status::OK();
  }

  template <typename Type, typename PhysicalType = typename Type::PhysicalType>
  Status Visit(const Type&) {
    result = TypeTraits<PhysicalType>::type_singleton().get();
    return Status::OK();
  }
};

}  // namespace

const DataType* GetPhysicalType(const DataType* real_type) {
  PhysicalTypeVisitor visitor{real_type, {}};
  ARROW_CHECK_OK(VisitTypeInline(*real_type, &visitor));
  return visitor.result;
}

}  // namespace compute
}  // namespace arrow
