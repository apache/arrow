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

#include <algorithm>
#include <cstring>

#include "arrow/compute/kernels/common.h"
#include "arrow/scalar.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

using internal::BitBlockCount;
using internal::BitBlockCounter;

namespace compute {
namespace internal {

namespace {

template <typename Type, typename Enable = void>
struct FillNullFunctor {};

// Numeric inputs

template <typename Type>
struct FillNullFunctor<Type, enable_if_t<is_number_type<Type>::value>> {
  using T = typename TypeTraits<Type>::CType;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ArrayData& data = *batch[0].array();
    const Scalar& fill_value = *batch[1].scalar();
    ArrayData* output = out->mutable_array();

    // Ensure the kernel is configured properly to have no validity bitmap /
    // null count 0 unless we explicitly propagate it below.
    DCHECK(output->buffers[0] == nullptr);

    T value = UnboxScalar<Type>::Unbox(fill_value);
    if (data.MayHaveNulls() != 0 && fill_value.is_valid) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                            ctx->Allocate(data.length * sizeof(T)));

      const uint8_t* is_valid = data.buffers[0]->data();
      const T* in_values = data.GetValues<T>(1);
      T* out_values = reinterpret_cast<T*>(out_buf->mutable_data());
      int64_t offset = data.offset;
      BitBlockCounter bit_counter(is_valid, data.offset, data.length);
      while (offset < data.offset + data.length) {
        BitBlockCount block = bit_counter.NextWord();
        if (block.AllSet()) {
          // Block all not null
          std::memcpy(out_values, in_values, block.length * sizeof(T));
        } else if (block.NoneSet()) {
          // Block all null
          std::fill(out_values, out_values + block.length, value);
        } else {
          for (int64_t i = 0; i < block.length; ++i) {
            out_values[i] = BitUtil::GetBit(is_valid, offset + i) ? in_values[i] : value;
          }
        }
        offset += block.length;
        out_values += block.length;
        in_values += block.length;
      }
      output->buffers[1] = out_buf;
      output->null_count = 0;
    } else {
      *output = data;
    }

    return Status::OK();
  }
};

// Boolean input

template <typename Type>
struct FillNullFunctor<Type, enable_if_t<is_boolean_type<Type>::value>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ArrayData& data = *batch[0].array();
    const Scalar& fill_value = *batch[1].scalar();
    ArrayData* output = out->mutable_array();

    bool value = UnboxScalar<BooleanType>::Unbox(fill_value);
    if (data.MayHaveNulls() != 0 && fill_value.is_valid) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                            ctx->AllocateBitmap(data.length));

      const uint8_t* is_valid = data.buffers[0]->data();
      const uint8_t* data_bitmap = data.buffers[1]->data();
      uint8_t* out_bitmap = out_buf->mutable_data();

      int64_t data_offset = data.offset;
      BitBlockCounter bit_counter(is_valid, data.offset, data.length);

      int64_t out_offset = 0;
      while (out_offset < data.length) {
        BitBlockCount block = bit_counter.NextWord();
        if (block.AllSet()) {
          // Block all not null
          ::arrow::internal::CopyBitmap(data_bitmap, data_offset, block.length,
                                        out_bitmap, out_offset);
        } else if (block.NoneSet()) {
          // Block all null
          BitUtil::SetBitsTo(out_bitmap, out_offset, block.length, value);
        } else {
          for (int64_t i = 0; i < block.length; ++i) {
            BitUtil::SetBitTo(out_bitmap, out_offset + i,
                              BitUtil::GetBit(is_valid, data_offset + i)
                                  ? BitUtil::GetBit(data_bitmap, data_offset + i)
                                  : value);
          }
        }
        data_offset += block.length;
        out_offset += block.length;
      }
      output->buffers[1] = out_buf;
      output->null_count = 0;
    } else {
      *output = data;
    }

    return Status::OK();
  }
};

// Null input

template <typename Type>
struct FillNullFunctor<Type, enable_if_t<is_null_type<Type>::value>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // Nothing preallocated, so we assign into the output
    *out->mutable_array() = *batch[0].array();
    return Status::OK();
  }
};

// Binary-like input

template <typename Type>
struct FillNullFunctor<Type, enable_if_t<is_base_binary_type<Type>::value>> {
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ArrayData& input = *batch[0].array();
    const auto& fill_value_scalar =
        checked_cast<const BaseBinaryScalar&>(*batch[1].scalar());
    ArrayData* output = out->mutable_array();

    // Ensure the kernel is configured properly to have no validity bitmap /
    // null count 0 unless we explicitly propagate it below.
    DCHECK(output->buffers[0] == nullptr);

    const int64_t null_count = input.GetNullCount();

    if (null_count > 0 && fill_value_scalar.is_valid) {
      util::string_view fill_value(*fill_value_scalar.value);
      BuilderType builder(input.type, ctx->memory_pool());
      RETURN_NOT_OK(builder.ReserveData(input.buffers[2]->size() +
                                        fill_value.length() * null_count));
      RETURN_NOT_OK(builder.Resize(input.length));

      VisitArrayDataInline<Type>(
          input, [&](util::string_view s) { builder.UnsafeAppend(s); },
          [&]() { builder.UnsafeAppend(fill_value); });
      std::shared_ptr<Array> string_array;
      RETURN_NOT_OK(builder.Finish(&string_array));
      *output = *string_array->data();
      // The builder does not match the logical type, due to
      // GenerateTypeAgnosticVarBinaryBase
      output->type = input.type;
    } else {
      *output = input;
    }

    return Status::OK();
  }
};

void AddBasicFillNullKernels(ScalarKernel kernel, ScalarFunction* func) {
  auto AddKernels = [&](const std::vector<std::shared_ptr<DataType>>& types) {
    for (const std::shared_ptr<DataType>& ty : types) {
      kernel.signature =
          KernelSignature::Make({InputType::Array(ty), InputType::Scalar(ty)}, ty);
      kernel.exec = GenerateTypeAgnosticPrimitive<FillNullFunctor>(*ty);
      DCHECK_OK(func->AddKernel(kernel));
    }
  };
  AddKernels(NumericTypes());
  AddKernels(TemporalTypes());
  AddKernels({boolean(), null()});
}

void AddBinaryFillNullKernels(ScalarKernel kernel, ScalarFunction* func) {
  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    kernel.signature =
        KernelSignature::Make({InputType::Array(ty), InputType::Scalar(ty)}, ty);
    kernel.exec = GenerateTypeAgnosticVarBinaryBase<FillNullFunctor>(*ty);
    DCHECK_OK(func->AddKernel(kernel));
  }
}

const FunctionDoc fill_null_doc{
    "Replace null elements",
    ("`fill_value` must be a scalar of the same type as `values`.\n"
     "Each non-null value in `values` is emitted as-is.\n"
     "Each null value in `values` is replaced with `fill_value`."),
    {"values", "fill_value"}};

}  // namespace

void RegisterScalarFillNull(FunctionRegistry* registry) {
  {
    ScalarKernel fill_null_base;
    fill_null_base.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    fill_null_base.mem_allocation = MemAllocation::NO_PREALLOCATE;
    auto fill_null =
        std::make_shared<ScalarFunction>("fill_null", Arity::Binary(), &fill_null_doc);
    AddBasicFillNullKernels(fill_null_base, fill_null.get());
    AddBinaryFillNullKernels(fill_null_base, fill_null.get());
    DCHECK_OK(registry->AddFunction(fill_null));
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
