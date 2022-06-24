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

#include "arrow/result.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/light_array.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/key_hash.h"

/*
#include "arrow/array/util.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/dict_internal.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/util/make_unique.h"
*/


// NOTES:
// * `KeyColumnArray` comes from light_array.h
//   * Should be replaceable with `ExecSpan`

namespace arrow::compute::internal {

  // Define symbols visible within `arrow::compute::internal` in this file;
  // these symbols are not visible outside of this file.
  namespace {

    // Documentation for `FastHash32` function:
    //  1. Summary
    //  2. Description
    //  3. Argument Names
    const FunctionDoc fast_hash_32_doc {
       "Construct a hash for every element of the input argument"
      ,(
         "`hash_input` may be a scalar value or an array.\n"
         "A hash result is emitted for each non-null element in `hash_input`.\n"
         "A null value emits a null; null elements in an array produce null results."
       )
      ,{ "hash_input" }
    };

    // ------------------------------
    // For scalar inputs

    struct FastHash32Scalar {
      template <typename ArrayType>
      UInt32Array
      Exec(KernelContext *ctx, const Datum& input_array, ExecResult *out) {
        ARROW_DCHECK(input_array.is_array());

        LightContext    light_ctx;
        TempVectorStack mem_stack;
        std::vector<KeyColumnArray> col_arrays;
        std::vector<uint32_t>       hash_results;

        // Initialize LightContext
        light_ctx.hardware_flags = hardware_flags;
        light_ctx.stack          = mem_stack;

				// TODO: replace this with correct type inspection and make robust
				// CalculateTempStackSize<StringType, StringArray>()
				auto col_bufsize = 16384;
        auto init_status = mem_stack.Init(ctx->exec_context()->memory_pool(), col_bufsize);

        // TODO: generalize for nested arrays
        col_arrays.resize(1);
        ARROW_ASSIGN_OR_RAISE(
           col_arrays[0]
          ,ColumnArrayFromArrayData(input_array.array(), start_row, num_rows)
        );

        // Use HashMultiColumn to compute hash for each col_array and combine them
        HashMultiColumn(col_arrays, &light_ctx, hash_results.data());

        // TODO: convert to an Array
        return hash_results;
      }
    };

  } // anonymous namespace

  void
  RegisterScalarHash(FunctionRegistry* registry) {
    // >> Construct instance of compute function
    auto fn_fast_hash_32 = std::make_shared<ScalarFunction>(
       "fast_hash_32"   // function name
      ,Arity::Unary()   // Arity of function (how many parameters)
      ,fast_hash_32_doc // function documentation
    );

    // >> Register kernel implementations with compute function instance
    // TODO: we can probably add a kernel for each type, that also knows how to
    //       calculate necessary Stack-based memory needed for the type
    //  |> Input is (scalar)
    DCHECK_OK(
      fn_fast_hash_32->AddKernel(
         { InputType(Type::ARRAY) }    // Input types
        ,UInt32Array                   // Output type
        ,FastHash32Scalar<StringArray> // Exec function
        ,                              // Init function (default: NULLPTR)
      )
    );

    // >> Register compute function with FunctionRegistry
    DCHECK_OK(registry->AddFunction(std::move(fn_fast_hash_32)));
  }

}  // namespace arrow::compute::internal
