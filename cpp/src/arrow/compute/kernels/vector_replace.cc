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

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

Status ReplacementArrayTooShort(int64_t expected, int64_t actual) {
  return Status::Invalid("Replacement array must be of appropriate length (expected ",
                         expected, " items but got ", actual, " items)");
}

// Helper to implement replace_with kernel with scalar mask for fixed-width types,
// using callbacks to handle both bool and byte-sized types
Status ReplaceWithScalarMask(KernelContext* ctx, const ArrayData& array,
                             const BooleanScalar& mask, const Datum& replacements,
                             ArrayData* output) {
  if (!mask.is_valid) {
    // Output = null
    ARROW_ASSIGN_OR_RAISE(auto replacement_array,
                          MakeArrayOfNull(array.type, array.length, ctx->memory_pool()));
    *output = *replacement_array->data();
    return Status::OK();
  }
  if (mask.value) {
    // Output = replacement
    if (replacements.is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(
          auto replacement_array,
          MakeArrayFromScalar(*replacements.scalar(), array.length, ctx->memory_pool()));
      *output = *replacement_array->data();
    } else {
      auto replacement_array = replacements.array();
      if (replacement_array->length != array.length) {
        return ReplacementArrayTooShort(array.length, replacement_array->length);
      }
      *output = *replacement_array;
    }
  } else {
    // Output = input
    *output = array;
  }
  return Status::OK();
}

// Helper to implement replace_with kernel with array mask for fixed-width types,
// using callbacks to handle both bool and byte-sized types and to handle
// scalar and array replacements
template <typename Functor>
Status ReplaceWithArrayMask(KernelContext* ctx, const ArrayData& array,
                            const ArrayData& mask, const Datum& replacements,
                            ArrayData* output) {
  ARROW_ASSIGN_OR_RAISE(output->buffers[1],
                        Functor::AllocateData(ctx, *array.type, array.length));

  uint8_t* out_bitmap = nullptr;
  uint8_t* out_values = output->buffers[1]->mutable_data();
  const uint8_t* mask_bitmap = mask.MayHaveNulls() ? mask.buffers[0]->data() : nullptr;
  const uint8_t* mask_values = mask.buffers[1]->data();
  bool replacements_bitmap;
  int64_t replacements_length;
  if (replacements.is_array()) {
    replacements_bitmap = replacements.array()->MayHaveNulls();
    replacements_length = replacements.array()->length;
  } else {
    replacements_bitmap = !replacements.scalar()->is_valid;
    replacements_length = std::numeric_limits<int64_t>::max();
  }
  if (array.MayHaveNulls() || mask.MayHaveNulls() || replacements_bitmap) {
    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(array.length));
    out_bitmap = output->buffers[0]->mutable_data();
    output->null_count = -1;
    if (array.MayHaveNulls()) {
      // Copy array's bitmap
      arrow::internal::CopyBitmap(array.buffers[0]->data(), array.offset, array.length,
                                  out_bitmap, /*dest_offset=*/0);
    } else {
      // Array has no bitmap but mask/replacements do, generate an all-valid bitmap
      std::memset(out_bitmap, 0xFF, output->buffers[0]->size());
    }
  } else {
    output->null_count = 0;
  }
  auto copy_bitmap = [&](int64_t out_offset, int64_t in_offset, int64_t length) {
    DCHECK(out_bitmap);
    if (replacements.is_array()) {
      const auto& in_data = *replacements.array();
      const auto in_bitmap = in_data.GetValues<uint8_t>(0, /*absolute_offset=*/0);
      arrow::internal::CopyBitmap(in_bitmap, in_data.offset + in_offset, length,
                                  out_bitmap, out_offset);
    } else {
      BitUtil::SetBitsTo(out_bitmap, out_offset, length, !replacements_bitmap);
    }
  };

  Functor::CopyData(*array.type, out_values, /*out_offset=*/0, array, /*in_offset=*/0,
                    array.length);
  arrow::internal::BitBlockCounter value_counter(mask_values, mask.offset, mask.length);
  arrow::internal::OptionalBitBlockCounter valid_counter(mask_bitmap, mask.offset,
                                                         mask.length);
  int64_t out_offset = 0;
  int64_t replacements_offset = 0;
  while (out_offset < array.length) {
    BitBlockCount value_block = value_counter.NextWord();
    BitBlockCount valid_block = valid_counter.NextWord();
    DCHECK_EQ(value_block.length, valid_block.length);
    if (value_block.AllSet() && valid_block.AllSet()) {
      // Copy from replacement array
      if (replacements_offset + valid_block.length > replacements_length) {
        return ReplacementArrayTooShort(replacements_offset + valid_block.length,
                                        replacements_length);
      }
      Functor::CopyData(*array.type, out_values, out_offset, replacements,
                        replacements_offset, valid_block.length);
      if (replacements_bitmap) {
        copy_bitmap(out_offset, replacements_offset, valid_block.length);
      } else if (!replacements_bitmap && out_bitmap) {
        BitUtil::SetBitsTo(out_bitmap, out_offset, valid_block.length, true);
      }
      replacements_offset += valid_block.length;
    } else if ((value_block.NoneSet() && valid_block.AllSet()) || valid_block.NoneSet()) {
      // Do nothing
    } else {
      for (int64_t i = 0; i < valid_block.length; ++i) {
        if (BitUtil::GetBit(mask_values, out_offset + mask.offset + i) &&
            (!mask_bitmap ||
             BitUtil::GetBit(mask_bitmap, out_offset + mask.offset + i))) {
          if (replacements_offset >= replacements_length) {
            return ReplacementArrayTooShort(replacements_offset + 1, replacements_length);
          }
          Functor::CopyData(*array.type, out_values, out_offset + i, replacements,
                            replacements_offset,
                            /*length=*/1);
          if (replacements_bitmap) {
            copy_bitmap(out_offset + i, replacements_offset, 1);
          }
          replacements_offset++;
        }
      }
    }
    out_offset += valid_block.length;
  }

  if (mask.MayHaveNulls()) {
    arrow::internal::BitmapAnd(out_bitmap, /*left_offset=*/0, mask.buffers[0]->data(),
                               mask.offset, array.length,
                               /*out_offset=*/0, out_bitmap);
  }
  return Status::OK();
}

template <typename Type, typename Enable = void>
struct ReplaceWithMask {};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_number<Type>> {
  using T = typename TypeTraits<Type>::CType;

  static Result<std::shared_ptr<Buffer>> AllocateData(KernelContext* ctx, const DataType&,
                                                      const int64_t length) {
    return ctx->Allocate(length * sizeof(T));
  }

  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const Datum& in, const int64_t in_offset, const int64_t length) {
    if (in.is_array()) {
      const auto& in_data = *in.array();
      const auto in_arr =
          in_data.GetValues<uint8_t>(1, (in_offset + in_data.offset) * sizeof(T));
      std::memcpy(out + (out_offset * sizeof(T)), in_arr, length * sizeof(T));
    } else {
      T* begin = reinterpret_cast<T*>(out + (out_offset * sizeof(T)));
      T* end = begin + length;
      std::fill(begin, end, UnboxScalar<Type>::Unbox(*in.scalar()));
    }
  }

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask(ctx, array, mask, replacements, output);
  }

  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                       output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_boolean<Type>> {
  static Result<std::shared_ptr<Buffer>> AllocateData(KernelContext* ctx, const DataType&,
                                                      const int64_t length) {
    return ctx->AllocateBitmap(length);
  }

  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const Datum& in, const int64_t in_offset, const int64_t length) {
    if (in.is_array()) {
      const auto& in_data = *in.array();
      const auto in_arr = in_data.GetValues<uint8_t>(1, /*absolute_offset=*/0);
      arrow::internal::CopyBitmap(in_arr, in_offset + in_data.offset, length, out,
                                  out_offset);
    } else {
      BitUtil::SetBitsTo(out, out_offset, length, in.scalar()->is_valid);
    }
  }

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask(ctx, array, mask, replacements, output);
  }
  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                       output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_same<Type, FixedSizeBinaryType>> {
  static Result<std::shared_ptr<Buffer>> AllocateData(KernelContext* ctx,
                                                      const DataType& ty,
                                                      const int64_t length) {
    return ctx->Allocate(length *
                         checked_cast<const FixedSizeBinaryType&>(ty).byte_width());
  }

  static void CopyData(const DataType& ty, uint8_t* out, const int64_t out_offset,
                       const Datum& in, const int64_t in_offset, const int64_t length) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(ty).byte_width();
    uint8_t* begin = out + (out_offset * width);
    if (in.is_array()) {
      const auto& in_data = *in.array();
      const auto in_arr =
          in_data.GetValues<uint8_t>(1, (in_offset + in_data.offset) * width);
      std::memcpy(begin, in_arr, length * width);
    } else {
      const FixedSizeBinaryScalar& scalar =
          checked_cast<const FixedSizeBinaryScalar&>(*in.scalar());
      // Null scalar may have null value buffer
      if (!scalar.value) return;
      const Buffer& buffer = *scalar.value;
      const uint8_t* value = buffer.data();
      DCHECK_GE(buffer.size(), width);
      for (int i = 0; i < length; i++) {
        std::memcpy(begin, value, width);
        begin += width;
      }
    }
  }

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask(ctx, array, mask, replacements, output);
  }

  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                       output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_decimal<Type>> {
  using ScalarType = typename TypeTraits<Type>::ScalarType;

  static Result<std::shared_ptr<Buffer>> AllocateData(KernelContext* ctx,
                                                      const DataType& ty,
                                                      const int64_t length) {
    return ctx->Allocate(length *
                         checked_cast<const FixedSizeBinaryType&>(ty).byte_width());
  }

  static void CopyData(const DataType& ty, uint8_t* out, const int64_t out_offset,
                       const Datum& in, const int64_t in_offset, const int64_t length) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(ty).byte_width();
    uint8_t* begin = out + (out_offset * width);
    if (in.is_array()) {
      const auto& in_data = *in.array();
      const auto in_arr =
          in_data.GetValues<uint8_t>(1, (in_offset + in_data.offset) * width);
      std::memcpy(begin, in_arr, length * width);
    } else {
      const ScalarType& scalar = checked_cast<const ScalarType&>(*in.scalar());
      const auto value = scalar.value.ToBytes();
      for (int i = 0; i < length; i++) {
        std::memcpy(begin, value.data(), width);
        begin += width;
      }
    }
  }

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask(ctx, array, mask, replacements, output);
  }

  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                       output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_null<Type>> {
  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    *output = array;
    return Status::OK();
  }
  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    *output = array;
    return Status::OK();
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask(ctx, array, mask, replacements, output);
  }
  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    BuilderType builder(array.type, ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(array.length));
    RETURN_NOT_OK(builder.ReserveData(array.buffers[2]->size()));
    int64_t source_offset = 0;
    int64_t replacements_offset = 0;
    RETURN_NOT_OK(VisitArrayDataInline<BooleanType>(
        mask,
        [&](bool replace) {
          if (replace && replacements.is_scalar()) {
            const Scalar& scalar = *replacements.scalar();
            if (scalar.is_valid) {
              RETURN_NOT_OK(builder.Append(UnboxScalar<Type>::Unbox(scalar)));
            } else {
              RETURN_NOT_OK(builder.AppendNull());
            }
          } else {
            const ArrayData& source = replace ? *replacements.array() : array;
            const int64_t offset = replace ? replacements_offset++ : source_offset;
            if (!source.MayHaveNulls() ||
                BitUtil::GetBit(source.buffers[0]->data(), source.offset + offset)) {
              const uint8_t* data = source.buffers[2]->data();
              const offset_type* offsets = source.GetValues<offset_type>(1);
              const offset_type offset0 = offsets[offset];
              const offset_type offset1 = offsets[offset + 1];
              RETURN_NOT_OK(builder.Append(data + offset0, offset1 - offset0));
            } else {
              RETURN_NOT_OK(builder.AppendNull());
            }
          }
          source_offset++;
          return Status::OK();
        },
        [&]() {
          RETURN_NOT_OK(builder.AppendNull());
          source_offset++;
          return Status::OK();
        }));
    std::shared_ptr<Array> temp_output;
    RETURN_NOT_OK(builder.Finish(&temp_output));
    *output = *temp_output->data();
    // Builder type != logical type due to GenerateTypeAgnosticVarBinaryBase
    output->type = array.type;
    return Status::OK();
  }
};

template <typename Type>
struct ReplaceWithMaskFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ArrayData& array = *batch[0].array();
    const Datum& replacements = batch[2];
    ArrayData* output = out->array().get();
    output->length = array.length;

    // Needed for FixedSizeBinary/parameterized types
    if (!array.type->Equals(*replacements.type(), /*check_metadata=*/false)) {
      return Status::Invalid("Replacements must be of same type (expected ",
                             array.type->ToString(), " but got ",
                             replacements.type()->ToString(), ")");
    }

    if (!replacements.is_array() && !replacements.is_scalar()) {
      return Status::Invalid("Replacements must be array or scalar");
    }

    if (batch[1].is_scalar()) {
      return ReplaceWithMask<Type>::ExecScalarMask(
          ctx, array, batch[1].scalar_as<BooleanScalar>(), replacements, output);
    }
    const ArrayData& mask = *batch[1].array();
    if (array.length != mask.length) {
      return Status::Invalid("Mask must be of same length as array (expected ",
                             array.length, " items but got ", mask.length, " items)");
    }
    return ReplaceWithMask<Type>::ExecArrayMask(ctx, array, mask, replacements, output);
  }
};

}  // namespace

const FunctionDoc replace_with_mask_doc(
    "Replace items using a mask and replacement values",
    ("Given an array and a Boolean mask (either scalar or of equal length), "
     "along with replacement values (either scalar or array), "
     "each element of the array for which the corresponding mask element is "
     "true will be replaced by the next value from the replacements, "
     "or with null if the mask is null. "
     "Hence, for replacement arrays, len(replacements) == sum(mask == true)."),
    {"values", "mask", "replacements"});

void RegisterVectorReplace(FunctionRegistry* registry) {
  auto func = std::make_shared<VectorFunction>("replace_with_mask", Arity::Ternary(),
                                               &replace_with_mask_doc);
  auto add_kernel = [&](detail::GetTypeId get_id, ArrayKernelExec exec) {
    VectorKernel kernel;
    kernel.can_execute_chunkwise = false;
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::NO_PREALLOCATE;
    kernel.signature = KernelSignature::Make(
        {InputType::Array(get_id.id), InputType(boolean()), InputType(get_id.id)},
        OutputType(FirstType));
    kernel.exec = std::move(exec);
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  };
  auto add_primitive_kernel = [&](detail::GetTypeId get_id) {
    add_kernel(get_id, GenerateTypeAgnosticPrimitive<ReplaceWithMaskFunctor>(get_id));
  };
  for (const auto& ty : NumericTypes()) {
    add_primitive_kernel(ty);
  }
  for (const auto& ty : TemporalTypes()) {
    add_primitive_kernel(ty);
  }
  add_primitive_kernel(null());
  add_primitive_kernel(boolean());
  add_primitive_kernel(day_time_interval());
  add_primitive_kernel(month_interval());
  add_kernel(Type::FIXED_SIZE_BINARY, ReplaceWithMaskFunctor<FixedSizeBinaryType>::Exec);
  add_kernel(Type::DECIMAL128, ReplaceWithMaskFunctor<Decimal128Type>::Exec);
  add_kernel(Type::DECIMAL256, ReplaceWithMaskFunctor<Decimal256Type>::Exec);
  for (const auto& ty : BaseBinaryTypes()) {
    add_kernel(ty->id(), GenerateTypeAgnosticVarBinaryBase<ReplaceWithMaskFunctor>(*ty));
  }
  // TODO: list types
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // TODO(ARROW-9431): "replace_with_indices"
}
}  // namespace internal
}  // namespace compute
}  // namespace arrow
