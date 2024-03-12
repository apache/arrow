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
//#include <iostream>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/key_hash.h"
#include "arrow/compute/util.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/light_array.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {
namespace internal {

// Define symbols visible within `arrow::compute::internal` in this file;
// these symbols are not visible outside of this file.
namespace {

// Function documentation
const FunctionDoc hash_64_doc{
    "Construct a hash for every element of the input argument",
    ("An element-wise function that uses an xxHash-like algorithm.\n"
     "This function is not suitable for cryptographic purposes.\n"
     "Hash results are 64-bit and emitted for each valid row.\n"
     "Null (or invalid) rows emit a null in the output."),
    {"hash_input"}};

// ------------------------------
// Kernel implementations
// It is expected that HashArrowType is either UInt32Type or UInt64Type (default)
template <typename HashArrowType = UInt64Type>
struct FastHashScalar {
  using OutputCType = typename TypeTraits<HashArrowType>::CType;
  using KeyColumnArrayVec = std::vector<KeyColumnArray>;

  // Internal wrapper functions to resolve Hashing32 vs Hashing64 using parameter types
  static void FastHashMultiColumn(KeyColumnArrayVec& cols, LightContext* ctx,
                                  uint32_t* hashes) {
    Hashing32::HashMultiColumn(cols, ctx, hashes);
  }

  static void FastHashMultiColumn(KeyColumnArrayVec& cols, LightContext* ctx,
                                  uint64_t* hashes) {
    Hashing64::HashMultiColumn(cols, ctx, hashes);
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    if (input_arg.num_values() != 1 || !input_arg[0].is_array()) {
      return Status::Invalid("FastHash currently supports a single array input");
    }
    ArraySpan hash_input = input_arg[0].array;

    auto exec_ctx = default_exec_context();
    if (ctx && ctx->exec_context()) {
      exec_ctx = ctx->exec_context();
    }

    // Initialize stack-based memory allocator used by Hashing32 and Hashing64
    util::TempVectorStack stack_memallocator;
    ARROW_RETURN_NOT_OK(
        stack_memallocator.Init(exec_ctx->memory_pool(),
                                3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

    // Prepare context used by Hashing32 and Hashing64
    LightContext hash_ctx;
    hash_ctx.hardware_flags = exec_ctx->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;

    // Construct vector<KeyColumnArray> from input ArraySpan; this essentially
    // flattens the input array span, lifting nested Array buffers into a single level
    ARROW_ASSIGN_OR_RAISE(KeyColumnArrayVec input_keycols,
                          ColumnArraysFromArraySpan(hash_input, hash_input.length));

    // Call the hashing function, overloaded based on OutputCType
    ArraySpan* result_span = out->array_span_mutable();
    FastHashMultiColumn(input_keycols, &hash_ctx, result_span->GetValues<OutputCType>(1));

    return Status::OK();
  }
};

// ------------------------------
// Function construction and kernel registration
std::shared_ptr<ScalarFunction> RegisterKernelsFastHash64() {
  // Create function instance
  auto fn_hash_64 =
      std::make_shared<ScalarFunction>("hash_64", Arity::Unary(), hash_64_doc);

  // Associate kernel with function
  for (auto& simple_inputtype : PrimitiveTypes()) {
    DCHECK_OK(fn_hash_64->AddKernel({InputType(simple_inputtype)}, OutputType(uint64()),
                                    FastHashScalar<UInt64Type>::Exec));
  }

  for (const auto nested_type :
       {Type::STRUCT, Type::DENSE_UNION, Type::SPARSE_UNION, Type::LIST,
        Type::FIXED_SIZE_LIST, Type::MAP, Type::DICTIONARY}) {
    DCHECK_OK(fn_hash_64->AddKernel({InputType(nested_type)}, OutputType(uint64()),
                                    FastHashScalar<UInt64Type>::Exec));
  }

  // Return function to be registered
  return fn_hash_64;
}

}  // namespace

void RegisterScalarHash(FunctionRegistry* registry) {
  auto fn_scalarhash64 = RegisterKernelsFastHash64();
  DCHECK_OK(registry->AddFunction(std::move(fn_scalarhash64)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
