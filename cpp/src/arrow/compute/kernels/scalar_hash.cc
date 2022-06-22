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
#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/dict_internal.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/light_array.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/exec/key_hash.h"
#include "arrow/util/make_unique.h"


// NOTES:
// * `KeyColumnArray` comes from light_array.h
//   * Should be replaceable with `ExecSpan`

namespace arrow::compute::internal {

  // Define symbols visible within `arrow::compute::internal` in this file;
  // these symbols are not visible outside of this file.
  namespace {

    // Utility function to wrap a `HashEach` input for propagation to `HashBatch`
    Result<KeyColumnArray>
    ColumnArrayFromArrayData( const std::shared_ptr<ArrayData> &array_data
                             ,      int64_t                     start_row
                             ,      int64_t                     num_rows) {
      ARROW_ASSIGN_OR_RAISE(
         KeyColumnMetadata metadata
        ,ColumnMetadataFromDataType(array_data->type)
      );

      // Grab pointers to various data buffers
      const uint8_t *array_validbuf = (
        array_data->buffers[0] != NULLPTR ?  array_data->buffers[0]->data() : nullptr
      );

      const uint8_t *array_fixedbuf = array_data->buffers[1]->data();
      const uint8_t *array_varbuf   = (
        array_data->buffers.size() > 2 && array_data->buffers[2] != NULLPTR ?
            array_data->buffers[2]->data()
          : nullptr
      );

      // Construct a view into the specified rows of the ArrayData
      KeyColumnArray column_array = KeyColumnArray(
         metadata
        ,array_data->offset + start_row + num_rows
        ,array_validbuf
        ,array_fixedbuf
        ,array_varbuf
      );

      // I don't know why we need to slice it now
      return column_array.Slice(array_data->offset + start_row, num_rows);
    }

    // Documentation for `HashEach` function:
    //  1. Summary
    //  2. Description
    //  3. Argument Names
    const FunctionDoc hash_each_doc {
       "Construct a hash for every element of the input argument"
      ,(
         "`hash_input` may be a scalar value or an array.\n"
         "A hash result is emitted for each non-null element in `hash_input`.\n"
         "A null value emits a null; null elements in an array produce null results."
       )
      ,{ "hash_input" }
    };

    // ------------------------------
    // For the new `ExecSpan`

    // A function that will be registered as a `ScalarKernel` for the `HashEach` function
    Status HashEachSpan(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
      using ScalarType       = typename TypeTraits<Type>::ScalarType;
      using OffsetScalarType = typename TypeTraits<Type>::OffsetScalarType;

      if (batch[0].is_array()) {
        const ArraySpan   &arr        = batch[0].array;
              ArraySpan   *out_arr    = out->array_span();
              auto         out_values = out_arr->GetValues<offset_type>(1);
        const offset_type *offsets    = arr.GetValues<offset_type>(1);

        // Offsets are always well-defined and monotonic, even for null values
        for (int64_t offset_ndx = 0; offset_ndx < arr.length; ++offset_ndx) {
          *out_values++ = offsets[offset_ndx + 1] - offsets[offset_ndx];
        }
      }

      else {
        const auto& arg0 = batch[0].scalar_as<ScalarType>();
        if (arg0.is_valid) {
          checked_cast<OffsetScalarType*>(out->scalar().get())->value = (
              static_cast<offset_type>(arg0.value->length())
          );
        }
      }

      return Status::OK();
    }

    // ------------------------------
    // For the old `KeyColumnArray`

    // A function that will be registered as a `ScalarKernel` for the `HashEach` function
    Status HashEachArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
      using ScalarType       = typename TypeTraits<Type>::ScalarType;
      using OffsetScalarType = typename TypeTraits<Type>::OffsetScalarType;

      if (batch[0].is_array()) {
        const ArraySpan   &arr        = batch[0].array;
              ArraySpan   *out_arr    = out->array_span();
              auto         out_values = out_arr->GetValues<offset_type>(1);
        const offset_type *offsets    = arr.GetValues<offset_type>(1);

        // Offsets are always well-defined and monotonic, even for null values
        for (int64_t offset_ndx = 0; offset_ndx < arr.length; ++offset_ndx) {
          *out_values++ = offsets[offset_ndx + 1] - offsets[offset_ndx];
        }
      }

      else {
        const auto& arg0 = batch[0].scalar_as<ScalarType>();
        if (arg0.is_valid) {
          checked_cast<OffsetScalarType*>(out->scalar().get())->value = (
              static_cast<offset_type>(arg0.value->length())
          );
        }
      }

      return Status::OK();
    }
  }



  void RegisterScalarHash(FunctionRegistry* registry) {
    // >> Construct instance of compute function
    auto fn_hash_each = std::make_shared<ScalarFunction>(
       "hash_each"    // function name
      ,Arity::Unary() // Arity of function (how many parameters)
      ,hash_each_doc // function documentation
    );

    // >> Register kernel implementations with compute function instance

    //  |> Input is (scalar)
    DCHECK_OK(
      list_value_length->AddKernel(
         { InputType(Type::LIST) }
        ,int32()
        ,ListValueLength<ListType>
      )
    );

    // >> Register compute function with FunctionRegistry
    DCHECK_OK(registry->AddFunction(std::move(fn_hash_each)));
  }

}  // namespace arrow::compute::internal
