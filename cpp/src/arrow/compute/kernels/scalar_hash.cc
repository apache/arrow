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


namespace arrow {
namespace compute {
namespace internal {

// Define symbols visible within `arrow::compute::internal` in this file;
// these symbols are not visible outside of this file.
namespace {

using arrow::internal::ScalarHelper;
using arrow::internal::ComputeStringHash;

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
// It is expected that HashArrowType is either Uint32Type (default) or Uint64Type
template <typename HashArrowType = UInt32Type>
struct FastHashScalar {
  using OutputCType = typename TypeTraits<HashArrowType>::CType;
  using KeyColumnArrayVec = std::vector<KeyColumnArray>;

  // Internal wrapper functions to resolve Hashing32 vs Hashing64 using parameter types
  static void FastHashMultiColumn(KeyColumnArrayVec &cols, LightContext *ctx, uint32_t *hashes) {
    Hashing32::HashMultiColumn(cols, ctx, hashes);
  }

  static void FastHashMultiColumn(KeyColumnArrayVec &cols, LightContext *ctx, uint64_t *hashes) {
    Hashing64::HashMultiColumn(cols, ctx, hashes);
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    if (input_arg.num_values() != 1 or !input_arg[0].is_array()) {
      return Status::Invalid("FastHash currently supports a single array input");
    }
    ArraySpan hash_input = input_arg[0].array;

    auto exec_ctx = default_exec_context();
    if (ctx && ctx->exec_context()) { exec_ctx = ctx->exec_context(); }

    // Initialize stack-based memory allocator used by Hashing32 and Hashing64
    util::TempVectorStack stack_memallocator;
    ARROW_RETURN_NOT_OK(stack_memallocator.Init(exec_ctx->memory_pool(),
                        3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

    // Prepare context used by Hashing32 and Hashing64
    LightContext hash_ctx;
    hash_ctx.hardware_flags = exec_ctx->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;

    // Allocate an output buffer; this gets moved into an ArrayData at the end
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hash_buffer,
                          AllocateBuffer(hash_input.length * sizeof(OutputCType)));

    // Construct vector<KeyColumnArray> from input ArraySpan; this essentially
    // flattens the input array span, lifting nested Array buffers into a single level
    ARROW_ASSIGN_OR_RAISE(KeyColumnArrayVec input_keycols,
                          ColumnArraysFromArraySpan(hash_input, hash_input.length));

    // Call the hashing function, overloaded based on OutputCType
    FastHashMultiColumn(input_keycols, &hash_ctx,
                        reinterpret_cast<OutputCType*>(hash_buffer->mutable_data()));

    out->value = ArrayData{TypeTraits<HashArrowType>::type_singleton(),
                           hash_input.length,
                           {hash_input.GetBuffer(0), std::move(hash_buffer)}};
    return Status::OK();
  }
};

// InputArrowType is the data type of the input values (used to get the input c type)
template <typename ValueType>
struct XxHashScalar {

  static Status ExecPrimitive(KernelContext* ctx, const ExecSpan& input_arg,
                              ExecResult* out) {
    if (input_arg.num_values() != 1 or !input_arg[0].is_array()) {
      return Status::Invalid("xxHash currently supports a single array input");
    }

    // Prepare input data structure for propagation to hash function
    const auto input_data = input_arg[0].array;
    const auto input_len = input_data.length;
    const ValueType* hash_inputs = input_arg[0].array.GetValues<ValueType>(1, 0);

    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hash_buffer,
                          AllocateBuffer(input_len * sizeof(uint64_t)));

    // Call hashing function
    uint64_t* hash_results = reinterpret_cast<uint64_t*>(hash_buffer->mutable_data());
    for (int val_ndx = 0; val_ndx < input_len; ++val_ndx) {
      if (input_data.IsValid(val_ndx)) {
        hash_results[val_ndx] = (
            ScalarHelper<ValueType, 0>::ComputeHash(hash_inputs[val_ndx])
          + ScalarHelper<ValueType, 1>::ComputeHash(hash_inputs[val_ndx])
        );
      }
    }

    out->value = ArrayData{uint64(), input_len,
                           {input_data.GetBuffer(0), std::move(hash_buffer)}};
    return Status::OK();
  }

  static Status ExecBinary(KernelContext* ctx, const ExecSpan& input_arg,
                           ExecResult* out) {
    if (input_arg.num_values() != 1 or !input_arg[0].is_array()) {
      return Status::Invalid("xxHash currently supports a single array input");
    }

    // Grab array buffers to check validity and compute element-wise hashes on binary
    const auto input_data = input_arg[0].array;
    const auto input_len = input_data.length;
    const ValueType* input_offsets = input_data.GetValues<ValueType>(1);
    const uint8_t* hash_inputs = input_data.GetValues<uint8_t>(2);

    // Prepare a place to write the results of hashing
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hash_buffer,
                          AllocateBuffer(input_len * sizeof(uint64_t)));
    uint64_t* hash_results = reinterpret_cast<uint64_t*>(hash_buffer->mutable_data());

    // Call hashing function
    for (int val_ndx = 0; val_ndx < input_len; ++val_ndx) {
      if (input_data.IsValid(val_ndx)) {
        const auto value_offset = input_offsets[val_ndx];
        const auto value_size = input_offsets[val_ndx + 1] - value_offset;

        hash_results[val_ndx] = (
            ComputeStringHash<0>(hash_inputs + value_offset, value_size)
          + ComputeStringHash<1>(hash_inputs + value_offset, value_size)
        );
      }
    }

    out->value = ArrayData{uint64(), input_len,
                           {input_data.GetBuffer(0), std::move(hash_buffer)}};
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
                                         OutputType(uint32()), FastHashScalar<>::Exec));
  }

  for (const auto nested_type : {Type::STRUCT, Type::DENSE_UNION, Type::SPARSE_UNION,
                                 Type::LIST, Type::FIXED_SIZE_LIST,
                                 Type::MAP, Type::DICTIONARY}) {
    DCHECK_OK(fn_fast_hash_32->AddKernel({InputType(nested_type)},
                                         OutputType(uint32()), FastHashScalar<UInt32Type>::Exec));
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
                                         OutputType(uint64()),
                                         FastHashScalar<UInt64Type>::Exec));
  }

  for (const auto nested_type : {Type::STRUCT, Type::DENSE_UNION, Type::SPARSE_UNION,
                                 Type::LIST, Type::FIXED_SIZE_LIST,
                                 Type::MAP, Type::DICTIONARY}) {
    DCHECK_OK(fn_fast_hash_64->AddKernel({InputType(nested_type)},
                                         OutputType(uint64()),
                                         FastHashScalar<UInt64Type>::Exec));
  }

  // Return function to be registered
  return fn_fast_hash_64;
}

std::shared_ptr<ScalarFunction> RegisterKernelsXxHash() {
  // Create function instance
  auto fn_xx_hash =
      std::make_shared<ScalarFunction>("xx_hash", Arity::Unary(), xx_hash_doc);

  // The output type for every XxHash kernel is uint64
  auto &&out_type = OutputType(uint64());

  // non null primitive types are handled by `ExecPrimitive` and template is for value
  // types
  DCHECK_OK(fn_xx_hash->AddKernel({InputType(int8())}, out_type,
                                   XxHashScalar<int8_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(int16())}, out_type,
                                   XxHashScalar<int16_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(int32())}, out_type,
                                   XxHashScalar<int32_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(int64())}, out_type,
                                   XxHashScalar<int64_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(uint8())}, out_type,
                                   XxHashScalar<uint8_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(uint16())}, out_type,
                                   XxHashScalar<uint16_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(uint32())}, out_type,
                                   XxHashScalar<uint32_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(uint64())}, out_type,
                                   XxHashScalar<uint64_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(float32())}, out_type,
                                   XxHashScalar<float>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(float64())}, out_type,
                                   XxHashScalar<double>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(boolean())}, out_type,
                                   XxHashScalar<bool>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(date32())}, out_type,
                                   XxHashScalar<int32_t>::ExecPrimitive));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(date64())}, out_type,
                                   XxHashScalar<int64_t>::ExecPrimitive));

  // binary types are handled by `ExecBinary`, and template is for offset types
  DCHECK_OK(fn_xx_hash->AddKernel({InputType(binary())}, out_type,
                                   XxHashScalar<int32_t>::ExecBinary));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(utf8())}, out_type,
                                   XxHashScalar<int32_t>::ExecBinary));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(large_binary())}, out_type,
                                   XxHashScalar<int64_t>::ExecBinary));

  DCHECK_OK(fn_xx_hash->AddKernel({InputType(large_utf8())}, out_type,
                                   XxHashScalar<int64_t>::ExecBinary));

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
