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
template <typename Functor>
Status ReplaceWithScalarMask(KernelContext* ctx, const ArrayData& array,
                             const BooleanScalar& mask, const Datum& replacements,
                             ArrayData* output) {
  Datum source = array;
  if (!mask.is_valid) {
    // Output = null
    source = MakeNullScalar(output->type);
  } else if (mask.value) {
    // Output = replacement
    source = replacements;
  }
  uint8_t* out_bitmap = output->buffers[0]->mutable_data();
  uint8_t* out_values = output->buffers[1]->mutable_data();
  const int64_t out_offset = output->offset;
  if (source.is_array()) {
    const ArrayData& in_data = *source.array();
    if (in_data.length < array.length) {
      return ReplacementArrayTooShort(array.length, in_data.length);
    }
    Functor::CopyData(*array.type, out_values, out_offset, in_data, /*in_offset=*/0,
                      array.length);
    if (in_data.MayHaveNulls()) {
      arrow::internal::CopyBitmap(in_data.buffers[0]->data(), in_data.offset,
                                  array.length, out_bitmap, out_offset);
    } else {
      BitUtil::SetBitsTo(out_bitmap, out_offset, array.length, true);
    }
  } else {
    const Scalar& in_data = *source.scalar();
    Functor::CopyData(*array.type, out_values, out_offset, in_data, /*in_offset=*/0,
                      array.length);
    BitUtil::SetBitsTo(out_bitmap, out_offset, array.length, in_data.is_valid);
  }
  return Status::OK();
}

struct CopyArrayBitmap {
  const uint8_t* in_bitmap;
  int64_t in_offset;

  void CopyBitmap(uint8_t* out_bitmap, int64_t out_offset, int64_t offset,
                  int64_t length) const {
    arrow::internal::CopyBitmap(in_bitmap, in_offset + offset, length, out_bitmap,
                                out_offset);
  }

  void SetBit(uint8_t* out_bitmap, int64_t out_offset, int64_t offset) const {
    BitUtil::SetBitTo(out_bitmap, out_offset,
                      BitUtil::GetBit(in_bitmap, in_offset + offset));
  }
};

struct CopyScalarBitmap {
  const bool is_valid;

  void CopyBitmap(uint8_t* out_bitmap, int64_t out_offset, int64_t offset,
                  int64_t length) const {
    BitUtil::SetBitsTo(out_bitmap, out_offset, length, is_valid);
  }

  void SetBit(uint8_t* out_bitmap, int64_t out_offset, int64_t offset) const {
    BitUtil::SetBitTo(out_bitmap, out_offset, is_valid);
  }
};

// Helper to implement replace_with kernel with array mask for fixed-width types,
// using callbacks to handle both bool and byte-sized types and to handle
// scalar and array replacements
template <typename Functor, typename Data, typename CopyBitmap>
void ReplaceWithArrayMaskImpl(const ArrayData& array, const ArrayData& mask,
                              const Data& replacements, bool replacements_bitmap,
                              const CopyBitmap& copy_bitmap, const uint8_t* mask_bitmap,
                              const uint8_t* mask_values, uint8_t* out_bitmap,
                              uint8_t* out_values, const int64_t out_offset) {
  Functor::CopyData(*array.type, out_values, /*out_offset=*/0, array, /*in_offset=*/0,
                    array.length);
  arrow::internal::OptionalBinaryBitBlockCounter counter(
      mask_values, mask.offset, mask_bitmap, mask.offset, mask.length);
  int64_t write_offset = 0;
  int64_t replacements_offset = 0;
  while (write_offset < array.length) {
    BitBlockCount block = counter.NextAndBlock();
    if (block.AllSet()) {
      // Copy from replacement array
      Functor::CopyData(*array.type, out_values, out_offset + write_offset, replacements,
                        replacements_offset, block.length);
      if (replacements_bitmap) {
        copy_bitmap.CopyBitmap(out_bitmap, out_offset + write_offset, replacements_offset,
                               block.length);
      } else if (!replacements_bitmap && out_bitmap) {
        BitUtil::SetBitsTo(out_bitmap, out_offset + write_offset, block.length, true);
      }
      replacements_offset += block.length;
    } else if (block.popcount) {
      for (int64_t i = 0; i < block.length; ++i) {
        if (BitUtil::GetBit(mask_values, write_offset + mask.offset + i) &&
            (!mask_bitmap ||
             BitUtil::GetBit(mask_bitmap, write_offset + mask.offset + i))) {
          Functor::CopyData(*array.type, out_values, out_offset + write_offset + i,
                            replacements, replacements_offset, /*length=*/1);
          if (replacements_bitmap) {
            copy_bitmap.SetBit(out_bitmap, out_offset + write_offset + i,
                               replacements_offset);
          }
          replacements_offset++;
        }
      }
    }
    write_offset += block.length;
  }
}

template <typename Functor>
Status ReplaceWithArrayMask(KernelContext* ctx, const ArrayData& array,
                            const ArrayData& mask, const Datum& replacements,
                            ArrayData* output) {
  const int64_t out_offset = output->offset;
  uint8_t* out_bitmap = nullptr;
  uint8_t* out_values = output->buffers[1]->mutable_data();
  const uint8_t* mask_bitmap = mask.MayHaveNulls() ? mask.buffers[0]->data() : nullptr;
  const uint8_t* mask_values = mask.buffers[1]->data();
  const bool replacements_bitmap = replacements.is_array()
                                       ? replacements.array()->MayHaveNulls()
                                       : !replacements.scalar()->is_valid;
  if (replacements.is_array()) {
    // Check that we have enough replacement values
    const int64_t replacements_length = replacements.array()->length;

    BooleanArray mask_arr(mask.length, mask.buffers[1], mask.buffers[0], mask.null_count,
                          mask.offset);
    const int64_t count = mask_arr.true_count();
    if (count > replacements_length) {
      return ReplacementArrayTooShort(count, replacements_length);
    }
  }
  if (array.MayHaveNulls() || mask.MayHaveNulls() || replacements_bitmap) {
    out_bitmap = output->buffers[0]->mutable_data();
    output->null_count = -1;
    if (array.MayHaveNulls()) {
      // Copy array's bitmap
      arrow::internal::CopyBitmap(array.buffers[0]->data(), array.offset, array.length,
                                  out_bitmap, out_offset);
    } else {
      // Array has no bitmap but mask/replacements do, generate an all-valid bitmap
      BitUtil::SetBitsTo(out_bitmap, out_offset, array.length, true);
    }
  } else {
    BitUtil::SetBitsTo(output->buffers[0]->mutable_data(), out_offset, array.length,
                       true);
    output->null_count = 0;
  }

  if (replacements.is_array()) {
    const ArrayData& array_repl = *replacements.array();
    ReplaceWithArrayMaskImpl<Functor>(
        array, mask, array_repl, replacements_bitmap,
        CopyArrayBitmap{replacements_bitmap ? array_repl.buffers[0]->data() : nullptr,
                        array_repl.offset},
        mask_bitmap, mask_values, out_bitmap, out_values, out_offset);
  } else {
    const Scalar& scalar_repl = *replacements.scalar();
    ReplaceWithArrayMaskImpl<Functor>(array, mask, scalar_repl, replacements_bitmap,
                                      CopyScalarBitmap{scalar_repl.is_valid}, mask_bitmap,
                                      mask_values, out_bitmap, out_values, out_offset);
  }

  if (mask.MayHaveNulls()) {
    arrow::internal::BitmapAnd(out_bitmap, out_offset, mask.buffers[0]->data(),
                               mask.offset, array.length, out_offset, out_bitmap);
  }
  return Status::OK();
}

template <typename Type, typename Enable = void>
struct ReplaceWithMask {};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_number<Type>> {
  using T = typename TypeTraits<Type>::CType;

  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const ArrayData& in, const int64_t in_offset,
                       const int64_t length) {
    const auto in_arr = in.GetValues<uint8_t>(1, (in_offset + in.offset) * sizeof(T));
    std::memcpy(out + (out_offset * sizeof(T)), in_arr, length * sizeof(T));
  }

  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const Scalar& in, const int64_t in_offset, const int64_t length) {
    T* begin = reinterpret_cast<T*>(out + (out_offset * sizeof(T)));
    T* end = begin + length;
    std::fill(begin, end, UnboxScalar<Type>::Unbox(in));
  }

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                        output);
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
  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const ArrayData& in, const int64_t in_offset,
                       const int64_t length) {
    const auto in_arr = in.GetValues<uint8_t>(1, /*absolute_offset=*/0);
    arrow::internal::CopyBitmap(in_arr, in_offset + in.offset, length, out, out_offset);
  }
  static void CopyData(const DataType&, uint8_t* out, const int64_t out_offset,
                       const Scalar& in, const int64_t in_offset, const int64_t length) {
    BitUtil::SetBitsTo(out, out_offset, length, in.is_valid);
  }

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                        output);
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
  static void CopyData(const DataType& ty, uint8_t* out, const int64_t out_offset,
                       const ArrayData& in, const int64_t in_offset,
                       const int64_t length) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(ty).byte_width();
    uint8_t* begin = out + (out_offset * width);
    const auto in_arr = in.GetValues<uint8_t>(1, (in_offset + in.offset) * width);
    std::memcpy(begin, in_arr, length * width);
  }
  static void CopyData(const DataType& ty, uint8_t* out, const int64_t out_offset,
                       const Scalar& in, const int64_t in_offset, const int64_t length) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(ty).byte_width();
    uint8_t* begin = out + (out_offset * width);
    const auto& scalar = checked_cast<const FixedSizeBinaryScalar&>(in);
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

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                        output);
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
  static void CopyData(const DataType& ty, uint8_t* out, const int64_t out_offset,
                       const ArrayData& in, const int64_t in_offset,
                       const int64_t length) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(ty).byte_width();
    uint8_t* begin = out + (out_offset * width);
    const auto in_arr = in.GetValues<uint8_t>(1, (in_offset + in.offset) * width);
    std::memcpy(begin, in_arr, length * width);
  }
  static void CopyData(const DataType& ty, uint8_t* out, const int64_t out_offset,
                       const Scalar& in, const int64_t in_offset, const int64_t length) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(ty).byte_width();
    uint8_t* begin = out + (out_offset * width);
    const auto& scalar = checked_cast<const ScalarType&>(in);
    const auto value = scalar.value.ToBytes();
    for (int i = 0; i < length; i++) {
      std::memcpy(begin, value.data(), width);
      begin += width;
    }
  }

  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<ReplaceWithMask<Type>>(ctx, array, mask, replacements,
                                                        output);
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
    if (!mask.is_valid) {
      // Output = null
      ARROW_ASSIGN_OR_RAISE(
          auto replacement_array,
          MakeArrayOfNull(array.type, array.length, ctx->memory_pool()));
      *output = *replacement_array->data();
    } else if (mask.value) {
      // Output = replacement
      if (replacements.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(auto replacement_array,
                              MakeArrayFromScalar(*replacements.scalar(), array.length,
                                                  ctx->memory_pool()));
        *output = *replacement_array->data();
      } else {
        const ArrayData& replacement_array = *replacements.array();
        if (replacement_array.length < array.length) {
          return ReplacementArrayTooShort(array.length, replacement_array.length);
        }
        *output = replacement_array;
        output->length = array.length;
      }
    } else {
      // Output = input
      *output = array;
    }
    return Status::OK();
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
    if (is_fixed_width(get_id.id)) {
      kernel.null_handling = NullHandling::type::COMPUTED_PREALLOCATE;
    } else {
      kernel.can_write_into_slices = false;
      kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    }
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
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
