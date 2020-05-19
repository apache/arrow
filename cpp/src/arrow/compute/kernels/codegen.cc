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

#include "arrow/compute/kernels/codegen.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

void ExecFail(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ctx->SetStatus(Status::NotImplemented("This kernel is malformed"));
}

void BinaryExecFlipped(KernelContext* ctx, ArrayKernelExec exec,
                       const ExecBatch& batch, Datum* out) {
  ExecBatch flipped_batch = batch;
  Datum tmp = flipped_batch.values[0];
  flipped_batch.values[0] = flipped_batch.values[1];
  flipped_batch.values[1] = tmp;
  exec(ctx, flipped_batch, out);
}

std::vector<std::shared_ptr<DataType>> g_signed_int_types;
std::vector<std::shared_ptr<DataType>> g_unsigned_int_types;
std::vector<std::shared_ptr<DataType>> g_int_types;
std::vector<std::shared_ptr<DataType>> g_floating_types;
std::vector<std::shared_ptr<DataType>> g_numeric_types;
std::vector<std::shared_ptr<DataType>> g_base_binary_types;
std::vector<std::shared_ptr<DataType>> g_temporal_types;
std::vector<std::shared_ptr<DataType>> g_non_parametric_types;
static std::once_flag codegen_static_initialized;

static void InitStaticData() {
  // Signed int types
  g_signed_int_types.push_back(int8());
  g_signed_int_types.push_back(int16());
  g_signed_int_types.push_back(int32());
  g_signed_int_types.push_back(int64());

  // Unsigned int types
  g_unsigned_int_types.push_back(uint8());
  g_unsigned_int_types.push_back(uint16());
  g_unsigned_int_types.push_back(uint32());
  g_unsigned_int_types.push_back(uint64());

  // All int types
  Extend(g_unsigned_int_types, &g_int_types);
  Extend(g_signed_int_types, &g_int_types);

  // Floating point types
  g_floating_types.push_back(float32());
  g_floating_types.push_back(float64());

  // Numeric types
  Extend(g_int_types, &g_numeric_types);
  Extend(g_floating_types, &g_numeric_types);

  // Temporal types
  g_temporal_types.push_back(date32());
  g_temporal_types.push_back(date64());
  g_temporal_types.push_back(time32(TimeUnit::SECOND));
  g_temporal_types.push_back(time32(TimeUnit::MILLI));
  g_temporal_types.push_back(time64(TimeUnit::MICRO));
  g_temporal_types.push_back(time64(TimeUnit::NANO));
  g_temporal_types.push_back(timestamp(TimeUnit::SECOND));
  g_temporal_types.push_back(timestamp(TimeUnit::MILLI));
  g_temporal_types.push_back(timestamp(TimeUnit::MICRO));
  g_temporal_types.push_back(timestamp(TimeUnit::NANO));

  // Base binary types (without FixedSizeBinary)
  g_base_binary_types.push_back(binary());
  g_base_binary_types.push_back(utf8());
  g_base_binary_types.push_back(large_binary());
  g_base_binary_types.push_back(large_utf8());

  // Non-parametric, non-nested types
  g_non_parametric_types.push_back(boolean());
  Extend(g_numeric_types, &g_non_parametric_types);
  Extend(g_temporal_types, &g_non_parametric_types);
  Extend(g_base_binary_types, &g_non_parametric_types);
}

const std::vector<std::shared_ptr<DataType>>& BaseBinaryTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_base_binary_types;
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

const std::vector<std::shared_ptr<DataType>>& NumericTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_numeric_types;
}

const std::vector<std::shared_ptr<DataType>>& TemporalTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_temporal_types;
}

const std::vector<std::shared_ptr<DataType>>& NonParametricTypes() {
  std::call_once(codegen_static_initialized, InitStaticData);
  return g_non_parametric_types;
}

Result<ValueDescr> FirstType(const std::vector<ValueDescr>& descrs) {
  return descrs[0];
}

}  // namespace compute
}  // namespace arrow
