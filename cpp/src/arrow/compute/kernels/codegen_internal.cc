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

void ExecFail(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ctx->SetStatus(Status::NotImplemented("This kernel is malformed"));
}

ArrayKernelExec MakeFlippedBinaryExec(ArrayKernelExec exec) {
  return [exec](KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ExecBatch flipped_batch = batch;
    std::swap(flipped_batch.values[0], flipped_batch.values[1]);
    exec(ctx, flipped_batch, out);
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
      decimal(12, 2),
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
  return descrs[0];
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
