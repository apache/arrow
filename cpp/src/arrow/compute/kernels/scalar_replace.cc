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

#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

using internal::BitBlockCount;
using internal::BitBlockCounter;

namespace compute {
namespace internal {

namespace {

void handle_nulls(KernelContext* ctx, const ArrayData& data, const ArrayData& mask,
                  ArrayData* output) {
  if (data.MayHaveNulls()) {
    KERNEL_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_nulls, ctx,
                           ctx->AllocateBitmap(data.length));

    if (mask.MayHaveNulls()) {
      ::arrow::internal::BitmapAnd(mask.buffers[0]->data(), mask.offset,
                                   mask.buffers[1]->data(), mask.offset, mask.length,
                                   output->offset, out_nulls->mutable_data());
      ::arrow::internal::BitmapOr(data.buffers[0]->data(), data.offset, out_nulls->data(),
                                  output->offset, data.length, output->offset,
                                  out_nulls->mutable_data());
    } else {
      ::arrow::internal::BitmapOr(data.buffers[0]->data(), data.offset,
                                  mask.buffers[1]->data(), mask.offset, mask.length,
                                  output->offset, out_nulls->mutable_data());
    }

    if (::arrow::internal::CountSetBits(out_nulls->data(), output->offset, data.length) <
        data.length)
      output->buffers[0] = out_nulls;
  }
}

template <typename Type, typename Enable = void>
struct ReplaceFunctor {};

// Numeric inputs

template <typename Type>
struct ReplaceFunctor<Type, enable_if_t<is_number_type<Type>::value>> {
  using T = typename TypeTraits<Type>::CType;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ArrayData& data = *batch[0].array();
    const ArrayData& mask = *batch[1].array();
    const Scalar& replacement = *batch[2].scalar();
    ArrayData* output = out->mutable_array();

    // Ensure the kernel is configured properly to have no validity bitmap /
    // null count 0 unless we explicitly propagate it below.
    DCHECK(output->buffers[0] == nullptr);

    if (replacement.is_valid) {
      handle_nulls(ctx, data, mask, output);

      KERNEL_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf, ctx,
                             ctx->Allocate(data.length * sizeof(T)));
      T value = UnboxScalar<Type>::Unbox(replacement);
      const uint8_t* to_replace = mask.buffers[1]->data();
      const T* in_values = data.GetValues<T>(1);
      T* out_values = reinterpret_cast<T*>(out_buf->mutable_data());
      int64_t offset = data.offset;
      BitBlockCounter bit_counter(to_replace, data.offset, data.length);
      while (offset < data.offset + data.length) {
        BitBlockCount block = bit_counter.NextWord();
        if (block.NoneSet()) {
          std::memcpy(out_values, in_values, block.length * sizeof(T));
        } else if (block.AllSet()) {
          std::fill(out_values, out_values + block.length, value);
        } else {
          for (int64_t i = 0; i < block.length; ++i) {
            out_values[i] =
                BitUtil::GetBit(to_replace, offset + i) ? value : in_values[i];
          }
        }
        offset += block.length;
        out_values += block.length;
        in_values += block.length;
      }
      output->buffers[1] = out_buf;
    } else {
      *output = data;
    }
  }
};

// Boolean input

template <typename Type>
struct ReplaceFunctor<Type, enable_if_t<is_boolean_type<Type>::value>> {
  static void Exec(KernelContext* ctx, const ExecBatch batch, Datum* out) {
    const ArrayData& data = *batch[0].array();
    const ArrayData& mask = *batch[1].array();
    const Scalar& replacement = *batch[2].scalar();
    ArrayData* output = out->mutable_array();

    // Ensure the kernel is configured properly to have no validity bitmap /
    // null count 0 unless we explicitly propagate it below.
    DCHECK(output->buffers[0] == nullptr);

    bool value = UnboxScalar<BooleanType>::Unbox(replacement);
    if (replacement.is_valid) {
      handle_nulls(ctx, data, mask, output);

      KERNEL_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf, ctx,
                             ctx->AllocateBitmap(data.length));
      const uint8_t* to_replace = mask.buffers[1]->data();
      const uint8_t* data_bitmap = data.buffers[1]->data();
      uint8_t* out_bitmap = out_buf->mutable_data();

      int64_t data_offset = data.offset;
      BitBlockCounter bit_counter(to_replace, data.offset, data.length);

      int64_t out_offset = 0;
      while (out_offset < data.length) {
        BitBlockCount block = bit_counter.NextWord();
        if (block.NoneSet()) {
          ::arrow::internal::CopyBitmap(data_bitmap, data_offset, block.length,
                                        out_bitmap, out_offset);
        } else if (block.AllSet()) {
          BitUtil::SetBitsTo(out_bitmap, out_offset, block.length, value);
        } else {
          for (int64_t i = 0; i < block.length; ++i) {
            BitUtil::SetBitTo(out_bitmap, out_offset + i,
                              BitUtil::GetBit(to_replace, data_offset + i)
                                  ? value
                                  : BitUtil::GetBit(data_bitmap, data_offset + i));
          }
        }
        data_offset += block.length;
        out_offset += block.length;
      }
      output->buffers[1] = out_buf;
    } else {
      *output = data;
    }
  }
};

// Null input

template <typename Type>
struct ReplaceFunctor<Type, enable_if_t<is_null_type<Type>::value>> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // Nothing preallocated, so we assign into the output
    *out->mutable_array() = *batch[0].array();
  }
};

// Binary-like

template <typename Type>
struct ReplaceFunctor<Type, enable_if_t<is_base_binary_type<Type>::value>> {
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using OffsetType = typename Type::offset_type;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ArrayData& input = *batch[0].array();
    const ArrayData& mask = *batch[1].array();
    const auto& replacement_scalar =
        checked_cast<const BaseBinaryScalar&>(*batch[2].scalar());
    util::string_view replacement(*replacement_scalar.value);
    ArrayData* output = out->mutable_array();

    // Ensure the kernel is configured properly to have no validity bitmap /
    // null count 0 unless we explicitly propagate it below.
    DCHECK(output->buffers[0] == nullptr);

    const uint8_t* mask_validities =
        mask.buffers[0] == nullptr ? nullptr : mask.buffers[0]->data();
    const std::shared_ptr<Buffer> to_replace = mask.buffers[1];
    std::shared_ptr<Buffer> to_replace_valid = nullptr;
    uint64_t replace_count = 0;
    {
      if (mask_validities == nullptr) {
        to_replace_valid = to_replace;
      } else {
        KERNEL_ASSIGN_OR_RAISE(
            to_replace_valid, ctx,
            ::arrow::internal::BitmapAnd(ctx->memory_pool(), mask_validities, mask.offset,
                                         to_replace->data(), mask.offset, mask.length,
                                         output->offset));
      }
      BitBlockCounter bit_counter(to_replace_valid->data(), input.offset, input.length);
      int64_t i = 0;
      while (i < input.length) {
        BitBlockCount block = bit_counter.NextWord();
        replace_count += block.popcount;
        i += block.length;
      }
    }

    if (replace_count > 0 && replacement_scalar.is_valid) {
      const uint8_t* input_validities =
          input.buffers[0] == nullptr ? nullptr : input.buffers[0]->data();
      const auto input_offsets = input.GetValues<OffsetType>(1, input.offset);
      // offset is 0 otherwise GetValue() will "shift" the buffer by input.offset bytes
      // (should it rather shift by the lengths of the first input.offset string values ?)
      const auto input_values = input.GetValues<char>(2, 0);
      BuilderType builder(input.type, ctx->memory_pool());
      KERNEL_RETURN_IF_ERROR(
          ctx, builder.ReserveData(input.buffers[2]->size() - input_offsets[0] +
                                   replace_count * replacement.length()));
      KERNEL_RETURN_IF_ERROR(ctx, builder.Resize(input.length));

      BitBlockCounter bit_counter(to_replace_valid->data(), input.offset, input.length);
      int64_t j = 0;
      while (j < input.length) {
        BitBlockCount block = bit_counter.NextWord();
        for (int64_t i = 0; i < block.length; ++i) {
          if (BitUtil::GetBit(to_replace_valid->data(), input.offset + j + i)) {
            builder.UnsafeAppend(replacement);
          } else {
            if (input_validities == nullptr ||
                BitUtil::GetBit(input_validities, input.offset + j + i)) {
              auto current_offset = input_offsets[j + i];
              auto next_offset = input_offsets[j + i + 1];
              auto string_value = util::string_view(input_values + current_offset,
                                                    next_offset - current_offset);
              builder.UnsafeAppend(string_value);
            } else {
              builder.UnsafeAppendNull();
            }
          }
        }
        j += block.length;
      }
      std::shared_ptr<Array> string_array;
      KERNEL_RETURN_IF_ERROR(ctx, builder.Finish(&string_array));
      *output = *string_array->data();
      // The builder does not match the logical type, due to
      // GenerateTypeAgnosticVarBinaryBase
      output->type = input.type;
    } else {
      *output = input;
    }
  }
};

void AddBasicReplaceKernels(ScalarKernel kernel, ScalarFunction* func) {
  auto AddKernels = [&](const std::vector<std::shared_ptr<DataType>>& types) {
    for (const std::shared_ptr<DataType>& ty : types) {
      kernel.signature = KernelSignature::Make(
          {InputType::Array(ty), InputType::Array(boolean()), InputType::Scalar(ty)}, ty);
      kernel.exec = GenerateTypeAgnosticPrimitive<ReplaceFunctor>(*ty);
      DCHECK_OK(func->AddKernel(kernel));
    }
  };
  AddKernels(NumericTypes());
  AddKernels(TemporalTypes());
  AddKernels({boolean(), null()});
}

void AddBinaryReplaceKernels(ScalarKernel kernel, ScalarFunction* func) {
  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    kernel.signature = KernelSignature::Make(
        {InputType::Array(ty), InputType::Array(boolean()), InputType::Scalar(ty)}, ty);
    kernel.exec = GenerateTypeAgnosticVarBinaryBase<ReplaceFunctor>(*ty);
    DCHECK_OK(func->AddKernel(kernel));
  }
}

const FunctionDoc replace_doc{
    "Replace selected elements",
    ("`replacement` must be a scalar of the same type as `values`.\n"
     "Each unmasked value in `values` is emitted as-is.\n"
     "Each masked value in `values` is replaced with `replacement`."),
    {"values", "mask", "replacement"}};

}  // namespace

void RegisterScalarReplace(FunctionRegistry* registry) {
  ScalarKernel replace_base;
  replace_base.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  replace_base.mem_allocation = MemAllocation::NO_PREALLOCATE;
  auto replace =
      std::make_shared<ScalarFunction>("replace", Arity::Ternary(), &replace_doc);
  AddBasicReplaceKernels(replace_base, replace.get());
  AddBinaryReplaceKernels(replace_base, replace.get());
  DCHECK_OK(registry->AddFunction(replace));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
