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

// NOTES:
// * `KeyColumnArray` comes from light_array.h
//   * Should be replaceable with `ExecSpan`

namespace arrow {
namespace compute {
namespace internal {

// Define symbols visible within `arrow::compute::internal` in this file;
// these symbols are not visible outside of this file.
namespace {

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

// ------------------------------
// Kernel implementations
struct FastHash32Scalar {
  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    if (input_arg.num_values() != 1 or not input_arg[0].is_array()) {
      return Status::Invalid("FastHash32 currently supports a single array input");
    }

    // Initialize stack-based memory allocator with an allocator and memory size
    util::TempVectorStack stack_memallocator;
    ARROW_RETURN_NOT_OK(stack_memallocator.Init(ctx->exec_context()->memory_pool(),
                                                max_bitwidth * max_batchsize));

    // Prepare input data structure for propagation to hash function
    ArraySpan hash_input = input_arg[0].array;
    ARROW_ASSIGN_OR_RAISE(KeyColumnArray input_keycol,
                          ColumnArrayFromArrayData(hash_input.ToArrayData(),
                                                   default_rstart, hash_input.length));

    // Call hashing function
    std::vector<uint32_t> hash_results(hash_input.length);
    LightContext hash_ctx;

    hash_ctx.hardware_flags = ctx->exec_context()->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;
    Hashing32::HashMultiColumn({input_keycol}, &hash_ctx, hash_results.data());

    // Prepare results of hash function for kernel output argument
    arrow::UInt32Builder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(hash_results.size()));
    ARROW_RETURN_NOT_OK(builder.AppendValues(hash_results));
    ARROW_ASSIGN_OR_RAISE(auto result_array, builder.Finish());

    out->value = ArraySpan{*(result_array->data())};
    return Status::OK();
  }

  // This 96 represents the most bits *per row* that the Hashing32 and Hashing64
  // algorithms allocate from the provided `TempVectorStack`
  static constexpr uint32_t max_bitwidth = 96;
  static constexpr uint32_t max_batchsize = util::MiniBatch::kMiniBatchLength;
  static constexpr int64_t default_rstart = 0;
};

struct FastHash64Scalar {
  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    if (input_arg.num_values() != 1 or not input_arg[0].is_array()) {
      return Status::Invalid("FastHash64 currently supports a single array input");
    }

    // Initialize stack-based memory allocator with an allocator and memory size
    util::TempVectorStack stack_memallocator;
    ARROW_RETURN_NOT_OK(stack_memallocator.Init(ctx->exec_context()->memory_pool(),
                                                max_bitwidth * max_batchsize));

    // Prepare input data structure for propagation to hash function
    ArraySpan hash_input = input_arg[0].array;
    ARROW_ASSIGN_OR_RAISE(KeyColumnArray input_keycol,
                          ColumnArrayFromArrayData(hash_input.ToArrayData(),
                                                   default_rstart, hash_input.length));

    // Call hashing function
    std::vector<uint64_t> hash_results(hash_input.length);
    LightContext hash_ctx;

    hash_ctx.hardware_flags = ctx->exec_context()->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;
    Hashing64::HashMultiColumn({input_keycol}, &hash_ctx, hash_results.data());

    // Prepare results of hash function for kernel output argument
    arrow::UInt64Builder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(hash_results.size()));
    ARROW_RETURN_NOT_OK(builder.AppendValues(hash_results));
    ARROW_ASSIGN_OR_RAISE(auto result_array, builder.Finish());

    out->value = ArraySpan{*(result_array->data())};
    return Status::OK();
  }

  // This 96 represents the most bits *per row* that the Hashing32 and Hashing64
  // algorithms allocate from the provided `TempVectorStack`
  static constexpr uint64_t max_bitwidth = 96;
  static constexpr uint32_t max_batchsize = util::MiniBatch::kMiniBatchLength;
  static constexpr int64_t default_rstart = 0;
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

}  // namespace

void RegisterScalarHash(FunctionRegistry* registry) {
  auto fn_scalarhash32 = RegisterKernelsFastHash32();
  DCHECK_OK(registry->AddFunction(std::move(fn_scalarhash32)));

  auto fn_scalarhash64 = RegisterKernelsFastHash64();
  DCHECK_OK(registry->AddFunction(std::move(fn_scalarhash64)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
