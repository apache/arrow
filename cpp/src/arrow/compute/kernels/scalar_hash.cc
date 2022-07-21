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

/**
 * @file  scalar_hash.cc
 * @brief Element-wise (scalar) kernels for hashing values.
 */

#include <algorithm>
#include <iostream>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/exec/key_hash.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/light_array.h"
#include "arrow/result.h"

#include "arrow/util/hashing.h"

// NOTES:
// * `KeyColumnArray` comes from light_array.h
//   * Should be replaceable with `ExecSpan`

namespace arrow {
namespace compute {
namespace internal {

// Define symbols visible within `arrow::compute::internal` in this file;
// these symbols are not visible outside of this file.
namespace {

using arrow::internal::ScalarHelper;

// ------------------------------
// Function documentation
const FunctionDoc fast_hash_32_doc{
    "Construct a hash for every element of the input argument",
    ("An element-wise function that uses an xxHash-like algorithm.\n"
     "This function is not suitable for cryptographic purposes.\n"
     "Hash results are 32-bit and emitted for each valid row.\n"
     "Null (or invalid) rows emit a null in the output."),
    {"hash_input"}};

const FunctionDoc fast_hash_64_doc{
    "Construct a hash for every element of the input argument",
    ("An element-wise function that uses an xxHash-like algorithm.\n"
     "This function is not suitable for cryptographic purposes.\n"
     "Hash results are 64-bit and emitted for each valid row.\n"
     "Null (or invalid) rows emit a null in the output."),
    {"hash_input"}};

const FunctionDoc xx_hash_doc{
    "Construct a hash for every element of the input argument",
    ("An element-wise function that uses the xxHash algorithm.\n"
     "Hash results are 64-bit and emitted for each valid row.\n"
     "Null (or invalid) rows emit a null in the output."),
    {"hash_input"}};

// ------------------------------
// Kernel implementations
struct FastHash32Scalar {
  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    if (input_arg.num_values() != 1 or !input_arg[0].is_array()) {
      return Status::Invalid("FastHash32 currently supports a single array input");
    }

    auto exec_ctx = default_exec_context();
    if (ctx && ctx->exec_context()) {
      exec_ctx = ctx->exec_context();
    }

    // Initialize stack-based memory allocator with an allocator and memory size
    util::TempVectorStack stack_memallocator;
    ARROW_RETURN_NOT_OK(
        stack_memallocator.Init(exec_ctx->memory_pool(),
                                3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

    // Prepare input data structure for propagation to hash function
    ArraySpan hash_input = input_arg[0].array;
    ARROW_ASSIGN_OR_RAISE(KeyColumnArray input_keycol,
                          ColumnArrayFromArrayData(hash_input.ToArrayData(),
                                                   default_rstart, hash_input.length));

    // Call hashing function
    LightContext hash_ctx;
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hash_buffer,
                          AllocateBuffer(hash_input.length * sizeof(uint32_t)));

    hash_ctx.hardware_flags = exec_ctx->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;
    Hashing32::HashMultiColumn({input_keycol}, &hash_ctx,
                               reinterpret_cast<uint32_t*>(hash_buffer->mutable_data()));

    auto result_buffer = std::shared_ptr<Buffer>(std::move(hash_buffer));
    ArraySpan hash_result{uint64().get(), hash_input.length};
    hash_result.SetBuffer(1, result_buffer);
    out->value = hash_result;
    return Status::OK();
  }

  static constexpr int64_t default_rstart = 0;
};

struct FastHash64Scalar {
  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    if (input_arg.num_values() != 1 or !input_arg[0].is_array()) {
      return Status::Invalid("FastHash64 currently supports a single array input");
    }

    auto exec_ctx = default_exec_context();
    if (ctx && ctx->exec_context()) {
      exec_ctx = ctx->exec_context();
    }

    // Initialize stack-based memory allocator with an allocator and memory size
    util::TempVectorStack stack_memallocator;
    ARROW_RETURN_NOT_OK(
        stack_memallocator.Init(exec_ctx->memory_pool(),
                                3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

    // Prepare input data structure for propagation to hash function
    ArraySpan hash_input = input_arg[0].array;
    ARROW_ASSIGN_OR_RAISE(KeyColumnArray input_keycol,
                          ColumnArrayFromArrayData(hash_input.ToArrayData(),
                                                   default_rstart, hash_input.length));

    // Call hashing function
    LightContext hash_ctx;
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hash_buffer,
                          AllocateBuffer(hash_input.length * sizeof(uint64_t)));

    hash_ctx.hardware_flags = exec_ctx->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;
    Hashing64::HashMultiColumn({input_keycol}, &hash_ctx,
                               reinterpret_cast<uint64_t*>(hash_buffer->mutable_data()));

    auto result_buffer = std::shared_ptr<Buffer>(std::move(hash_buffer));
    ArraySpan hash_result{uint64().get(), hash_input.length};
    hash_result.SetBuffer(1, result_buffer);
    out->value = hash_result;
    return Status::OK();
  }

  static constexpr int64_t default_rstart = 0;
};

struct XxHashScalar {
  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    if (input_arg.num_values() != 1 or !input_arg[0].is_array()) {
      return Status::Invalid("FastHash64 currently supports a single array input");
    }

    // Prepare input data structure for propagation to hash function
    const uint64_t* hash_inputs = input_arg[0].array.GetValues<uint64_t>(1, 0);
    const auto input_len = input_arg[0].array.length;

    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hash_buffer,
                          AllocateBuffer(input_len * sizeof(uint64_t)));

    // Call hashing function
    uint64_t* hash_results = reinterpret_cast<uint64_t*>(hash_buffer->mutable_data());
    for (int val_ndx = 0; val_ndx < input_len; ++val_ndx) {
      hash_results[val_ndx] = ScalarHelper<int64_t, 0>::ComputeHash(hash_inputs[val_ndx]);
      // total += ScalarHelper<int64_t, 1>::ComputeHash(v);
    }

    auto result_buffer = std::shared_ptr<Buffer>(std::move(hash_buffer));
    ArraySpan hash_result{uint64().get(), input_len};
    hash_result.SetBuffer(1, result_buffer);
    out->value = hash_result;
    return Status::OK();
  }
};

// ------------------------------
// Function construction and kernel registration
std::shared_ptr<ScalarFunction> RegisterKernelsFastHash32() {
  // Create function instance
  auto fn_fast_hash_32 =
      std::make_shared<ScalarFunction>("fast_hash_32", Arity::Unary(), fast_hash_32_doc);

  // Associate kernel with function
  for (auto& simple_inputtype : PrimitiveTypes()) {
    DCHECK_OK(fn_fast_hash_32->AddKernel({InputType(simple_inputtype)},
                                         OutputType(uint32()), FastHash32Scalar::Exec));
  }

  // Return function to be registered
  return fn_fast_hash_32;
}

std::shared_ptr<ScalarFunction> RegisterKernelsFastHash64() {
  // Create function instance
  auto fn_fast_hash_64 =
      std::make_shared<ScalarFunction>("fast_hash_64", Arity::Unary(), fast_hash_64_doc);

  // Associate kernel with function
  for (auto& simple_inputtype : PrimitiveTypes()) {
    DCHECK_OK(fn_fast_hash_64->AddKernel({InputType(simple_inputtype)},
                                         OutputType(uint64()), FastHash64Scalar::Exec));
  }

  // Return function to be registered
  return fn_fast_hash_64;
}

std::shared_ptr<ScalarFunction> RegisterKernelsXxHash() {
  // Create function instance
  auto fn_xx_hash =
      std::make_shared<ScalarFunction>("xx_hash", Arity::Unary(), xx_hash_doc);

  // Associate kernel with function
  for (auto& simple_inputtype : PrimitiveTypes()) {
    DCHECK_OK(fn_xx_hash->AddKernel({InputType(simple_inputtype)}, OutputType(uint64()),
                                    FastHash64Scalar::Exec));
  }

  // Return function to be registered
  return fn_xx_hash;
}

}  // namespace

void RegisterScalarHash(FunctionRegistry* registry) {
  auto fn_scalarhash32 = RegisterKernelsFastHash32();
  DCHECK_OK(registry->AddFunction(std::move(fn_scalarhash32)));

  auto fn_scalarhash64 = RegisterKernelsFastHash64();
  DCHECK_OK(registry->AddFunction(std::move(fn_scalarhash64)));

  auto fn_scalarhashxx = RegisterKernelsXxHash();
  DCHECK_OK(registry->AddFunction(std::move(fn_scalarhashxx)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
