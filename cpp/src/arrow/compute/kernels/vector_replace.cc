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
#include "arrow/compute/kernels/copy_data_internal.h"
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
template <typename Type>
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
    CopyDataUtils<Type>::CopyData(*array.type, in_data, /*in_offset=*/0, out_values,
                                  out_offset, array.length);
    if (in_data.MayHaveNulls()) {
      arrow::internal::CopyBitmap(in_data.buffers[0]->data(), in_data.offset,
                                  array.length, out_bitmap, out_offset);
    } else {
      bit_util::SetBitsTo(out_bitmap, out_offset, array.length, true);
    }
  } else {
    const Scalar& in_data = *source.scalar();
    CopyDataUtils<Type>::CopyData(*array.type, in_data, /*in_offset=*/0, out_values,
                                  out_offset, array.length);
    bit_util::SetBitsTo(out_bitmap, out_offset, array.length, in_data.is_valid);
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
    bit_util::SetBitTo(
        out_bitmap, out_offset,
        in_bitmap ? bit_util::GetBit(in_bitmap, in_offset + offset) : true);
  }
};

struct CopyScalarBitmap {
  const bool is_valid;

  void CopyBitmap(uint8_t* out_bitmap, int64_t out_offset, int64_t offset,
                  int64_t length) const {
    bit_util::SetBitsTo(out_bitmap, out_offset, length, is_valid);
  }

  void SetBit(uint8_t* out_bitmap, int64_t out_offset, int64_t offset) const {
    bit_util::SetBitTo(out_bitmap, out_offset, is_valid);
  }
};

// Helper to implement replace_with kernel with array mask for fixed-width types,
// using callbacks to handle both bool and byte-sized types and to handle
// scalar and array replacements
template <typename Type, typename Data, typename CopyBitmap>
void ReplaceWithArrayMaskImpl(const ArrayData& array, const ArrayData& mask,
                              const Data& replacements, bool replacements_bitmap,
                              const CopyBitmap& copy_bitmap, const uint8_t* mask_bitmap,
                              const uint8_t* mask_values, uint8_t* out_bitmap,
                              uint8_t* out_values, const int64_t out_offset) {
  CopyDataUtils<Type>::CopyData(*array.type, array, /*in_offset=*/0, out_values,
                                /*out_offset=*/0, array.length);
  arrow::internal::OptionalBinaryBitBlockCounter counter(
      mask_values, mask.offset, mask_bitmap, mask.offset, mask.length);
  int64_t write_offset = 0;
  int64_t replacements_offset = 0;
  while (write_offset < array.length) {
    BitBlockCount block = counter.NextAndBlock();
    if (block.AllSet()) {
      // Copy from replacement array
      CopyDataUtils<Type>::CopyData(*array.type, replacements, replacements_offset,
                                    out_values, out_offset + write_offset, block.length);

      if (replacements_bitmap) {
        copy_bitmap.CopyBitmap(out_bitmap, out_offset + write_offset, replacements_offset,
                               block.length);
      } else if (out_bitmap) {
        bit_util::SetBitsTo(out_bitmap, out_offset + write_offset, block.length, true);
      }
      replacements_offset += block.length;
    } else if (block.popcount) {
      for (int64_t i = 0; i < block.length; ++i) {
        if (bit_util::GetBit(mask_values, write_offset + mask.offset + i) &&
            (!mask_bitmap ||
             bit_util::GetBit(mask_bitmap, write_offset + mask.offset + i))) {
          CopyDataUtils<Type>::CopyData(*array.type, replacements, replacements_offset,
                                        out_values, out_offset + write_offset + i,
                                        /*length=*/1);
          copy_bitmap.SetBit(out_bitmap, out_offset + write_offset + i,

                             replacements_offset);
          replacements_offset++;
        }
      }
    }
    write_offset += block.length;
  }
}

template <typename Type>
Status ReplaceWithArrayMask(KernelContext* ctx, const ArrayData& array,
                            const ArrayData& mask, const Datum& replacements,
                            ArrayData* output) {
  const int64_t out_offset = output->offset;
  uint8_t* out_bitmap = nullptr;
  uint8_t* out_values = output->buffers[1]->mutable_data();
  const uint8_t* mask_bitmap = mask.MayHaveNulls() ? mask.buffers[0]->data() : nullptr;
  const uint8_t* mask_values = mask.buffers[1]->data();
  const bool replacements_bitmap =
      replacements.is_array() ? replacements.array()->MayHaveNulls() : true;
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
      bit_util::SetBitsTo(out_bitmap, out_offset, array.length, true);
    }
  } else {
    bit_util::SetBitsTo(output->buffers[0]->mutable_data(), out_offset, array.length,
                        true);
    output->null_count = 0;
  }

  if (replacements.is_array()) {
    const ArrayData& array_repl = *replacements.array();
    ReplaceWithArrayMaskImpl<Type>(
        array, mask, array_repl, replacements_bitmap,
        CopyArrayBitmap{(replacements_bitmap) ? array_repl.buffers[0]->data() : nullptr,
                        array_repl.offset},
        mask_bitmap, mask_values, out_bitmap, out_values, out_offset);
  } else {
    const Scalar& scalar_repl = *replacements.scalar();
    ReplaceWithArrayMaskImpl<Type>(array, mask, scalar_repl, replacements_bitmap,
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
struct ReplaceWithMask<Type,
                       enable_if_t<is_number_type<Type>::value ||
                                   std::is_same<Type, MonthDayNanoIntervalType>::value>> {
  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<Type>(ctx, array, mask, replacements, output);
  }

  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<Type>(ctx, array, mask, replacements, output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_boolean<Type>> {
  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<Type>(ctx, array, mask, replacements, output);
  }
  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<Type>(ctx, array, mask, replacements, output);
  }
};

template <typename Type>
struct ReplaceWithMask<Type, enable_if_fixed_size_binary<Type>> {
  static Status ExecScalarMask(KernelContext* ctx, const ArrayData& array,
                               const BooleanScalar& mask, const Datum& replacements,
                               ArrayData* output) {
    return ReplaceWithScalarMask<Type>(ctx, array, mask, replacements, output);
  }

  static Status ExecArrayMask(KernelContext* ctx, const ArrayData& array,
                              const ArrayData& mask, const Datum& replacements,
                              ArrayData* output) {
    return ReplaceWithArrayMask<Type>(ctx, array, mask, replacements, output);
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
                bit_util::GetBit(source.buffers[0]->data(), source.offset + offset)) {
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

  static std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
    return KernelSignature::Make(
        {InputType::Array(get_id.id), InputType(boolean()), InputType(get_id.id)},
        OutputType(FirstType));
  }
};

// This is for fixed-size types only
template <typename Type>
void FillNullInDirectionImpl(const ArrayData& current_chunk, const uint8_t* null_bitmap,
                             ArrayData* output, int8_t direction,
                             const ArrayData& last_valid_value_chunk,
                             int64_t* last_valid_value_offset) {
  uint8_t* out_bitmap = output->buffers[0]->mutable_data();
  uint8_t* out_values = output->buffers[1]->mutable_data();
  arrow::internal::CopyBitmap(current_chunk.buffers[0]->data(), current_chunk.offset,
                              current_chunk.length, out_bitmap, output->offset);
  CopyDataUtils<Type>::CopyData(*current_chunk.type, current_chunk, /*in_offset=*/0,
                                out_values, /*out_offset=*/output->offset,
                                current_chunk.length);

  bool has_fill_value = *last_valid_value_offset != -1;
  int64_t write_offset = direction == 1 ? 0 : current_chunk.length - 1;
  int64_t bitmap_offset = 0;

  arrow::internal::OptionalBitBlockCounter counter(null_bitmap, output->offset,
                                                   current_chunk.length);
  bool use_current_chunk = false;
  while (bitmap_offset < current_chunk.length) {
    BitBlockCount block = counter.NextBlock();
    if (block.AllSet()) {
      *last_valid_value_offset =
          write_offset + direction * (block.length - 1 + bitmap_offset);
      has_fill_value = true;
      use_current_chunk = true;
    } else {
      uint64_t block_start_offset = write_offset + direction * bitmap_offset;
      uint64_t write_value_offset = block_start_offset;
      if (block.popcount) {
        for (int64_t i = 0; i < block.length; i++, write_value_offset += direction) {
          auto current_bit = bit_util::GetBit(null_bitmap, bitmap_offset + i);
          if (!current_bit) {
            if (has_fill_value) {
              CopyDataUtils<Type>::CopyData(
                  *current_chunk.type,
                  use_current_chunk ? current_chunk : last_valid_value_chunk,
                  *last_valid_value_offset, out_values, write_value_offset,
                  /*length=*/1);
              bit_util::SetBitTo(out_bitmap, write_value_offset, true);
            }
          } else {
            has_fill_value = true;
            use_current_chunk = true;
            *last_valid_value_offset = write_value_offset;
          }
        }
      } else {
        for (int64_t i = 0; i < block.length; i++, write_value_offset += direction) {
          if (has_fill_value) {
            CopyDataUtils<Type>::CopyData(
                *current_chunk.type,
                use_current_chunk ? current_chunk : last_valid_value_chunk,
                *last_valid_value_offset, out_values, write_value_offset,
                /*length=*/1);
            bit_util::SetBitTo(out_bitmap, write_value_offset, true);
          }
        }
      }
    }
    bitmap_offset += block.length;
  }
  output->null_count = -1;
  output->GetNullCount();
}

template <typename Type, typename Enable = void>
struct FillNullExecutor {};

template <typename Type>
struct FillNullExecutor<Type, enable_if_boolean<Type>> {
  static Status ExecFillNull(KernelContext* ctx, const ArrayData& array,
                             const uint8_t* reversed_bitmap, ArrayData* output,
                             int8_t direction, const ArrayData& last_valid_value_chunk,
                             int64_t* last_valid_value_offset) {
    FillNullInDirectionImpl<Type>(array, reversed_bitmap, output, direction,
                                  last_valid_value_chunk, last_valid_value_offset);
    return Status::OK();
  }
};

template <typename Type>
struct FillNullExecutor<
    Type, enable_if_t<is_number_type<Type>::value ||
                      std::is_same<Type, MonthDayNanoIntervalType>::value>> {
  static Status ExecFillNull(KernelContext* ctx, const ArrayData& array,
                             const uint8_t* reversed_bitmap, ArrayData* output,
                             int8_t direction, const ArrayData& last_valid_value_chunk,
                             int64_t* last_valid_value_offset) {
    FillNullInDirectionImpl<Type>(array, reversed_bitmap, output, direction,
                                  last_valid_value_chunk, last_valid_value_offset);
    return Status::OK();
  }
};

template <typename Type>
struct FillNullExecutor<Type, enable_if_fixed_size_binary<Type>> {
  static Status ExecFillNull(KernelContext* ctx, const ArrayData& array,
                             const uint8_t* reversed_bitmap, ArrayData* output,
                             int8_t direction, const ArrayData& last_valid_value_chunk,
                             int64_t* last_valid_value_offset) {
    FillNullInDirectionImpl<Type>(array, reversed_bitmap, output, direction,
                                  last_valid_value_chunk, last_valid_value_offset);
    return Status::OK();
  }
};

template <typename Type>
struct FillNullExecutor<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  static Status ExecFillNull(KernelContext* ctx, const ArrayData& current_chunk,
                             const uint8_t* reversed_bitmap, ArrayData* output,
                             int8_t direction, const ArrayData& last_valid_value_chunk,
                             int64_t* last_valid_value_offset) {
    BuilderType builder(current_chunk.type, ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(current_chunk.length));
    RETURN_NOT_OK(builder.ReserveData(current_chunk.buffers[2]->size()));
    int64_t array_value_index = direction == 1 ? 0 : current_chunk.length - 1;
    const uint8_t* data = current_chunk.buffers[2]->data();
    const uint8_t* data_prev = last_valid_value_chunk.buffers[2]->data();
    const offset_type* offsets = current_chunk.GetValues<offset_type>(1);
    const offset_type* offsets_prev = last_valid_value_chunk.GetValues<offset_type>(1);

    bool has_fill_value_last_chunk = *last_valid_value_offset != -1;
    bool has_fill_value_current_chunk = false;
    /*tuple for store: <use current_chunk(true) or last_valid_chunk(false),
     * start offset of the current value, end offset for the current value>*/
    std::vector<std::tuple<bool, offset_type, offset_type>> offsets_reversed;
    RETURN_NOT_OK(VisitNullBitmapInline<>(
        reversed_bitmap, output->offset, current_chunk.length,
        current_chunk.GetNullCount(),
        [&]() {
          const offset_type offset0 = offsets[array_value_index];
          const offset_type offset1 = offsets[array_value_index + 1];
          offsets_reversed.push_back(
              std::make_tuple(/*current_chunk=*/true, offset0, offset1 - offset0));
          *last_valid_value_offset = array_value_index;
          has_fill_value_current_chunk = true;
          has_fill_value_last_chunk = false;
          array_value_index += direction;
          return Status::OK();
        },
        [&]() {
          if (has_fill_value_current_chunk) {
            const offset_type offset0 = offsets[*last_valid_value_offset];
            const offset_type offset1 = offsets[*last_valid_value_offset + 1];
            offsets_reversed.push_back(
                std::make_tuple(/*current_chunk=*/true, offset0, offset1 - offset0));
          } else if (has_fill_value_last_chunk) {
            const offset_type offset0 = offsets_prev[*last_valid_value_offset];
            const offset_type offset1 = offsets_prev[*last_valid_value_offset + 1];
            offsets_reversed.push_back(
                std::make_tuple(/*current_chunk=*/false, offset0, offset1 - offset0));
          } else {
            offsets_reversed.push_back(std::make_tuple(/*current_chunk=*/false, -1, -1));
          }
          array_value_index += direction;
          return Status::OK();
        }));

    if (direction == 1) {
      for (auto it = offsets_reversed.begin(); it != offsets_reversed.end(); ++it) {
        if (std::get<1>(*it) == -1 && std::get<2>(*it) == -1) {
          RETURN_NOT_OK(builder.AppendNull());
        } else if (std::get<0>(*it)) {
          RETURN_NOT_OK(builder.Append(data + std::get<1>(*it), std::get<2>(*it)));
        } else {
          RETURN_NOT_OK(builder.Append(data_prev + std::get<1>(*it), std::get<2>(*it)));
        }
      }
    } else {
      for (auto it = offsets_reversed.rbegin(); it != offsets_reversed.rend(); ++it) {
        if (std::get<1>(*it) == -1 && std::get<2>(*it) == -1) {
          RETURN_NOT_OK(builder.AppendNull());
        } else if (std::get<0>(*it)) {
          RETURN_NOT_OK(builder.Append(data + std::get<1>(*it), std::get<2>(*it)));
        } else {
          RETURN_NOT_OK(builder.Append(data_prev + std::get<1>(*it), std::get<2>(*it)));
        }
      }
    }

    std::shared_ptr<Array> temp_output;
    RETURN_NOT_OK(builder.Finish(&temp_output));
    *output = *temp_output->data();
    // Builder type != logical type due to GenerateTypeAgnosticVarBinaryBase
    output->type = current_chunk.type;
    return Status::OK();
  }
};

template <typename Type>
struct FillNullExecutor<Type, enable_if_null<Type>> {
  static Status ExecFillNull(KernelContext* ctx, const ArrayData& array,
                             const uint8_t* reversed_bitmap, ArrayData* output,
                             int8_t direction, const ArrayData& last_valid_value_chunk,
                             int64_t* last_valid_value_offset) {
    *output = array;
    return Status::OK();
  }
};

template <typename Type>
struct FillNullForwardFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    switch (batch[0].kind()) {
      case Datum::ARRAY: {
        auto array_input = *batch[0].array();
        int64_t last_valid_value_offset = -1;
        return FillNullForwardArray(ctx, array_input, out, array_input,
                                    &last_valid_value_offset);
      }
      case Datum::CHUNKED_ARRAY: {
        return FillNullForwardChunkedArray(ctx, batch[0].chunked_array(), out);
      }
      default:
        break;
    }
    return Status::NotImplemented("Unsupported type for fill_null_forward: ",
                                  batch[0].ToString());
  }

  static Status FillNullForwardArray(KernelContext* ctx, const ArrayData& array,
                                     Datum* out, const ArrayData& last_valid_value_chunk,
                                     int64_t* last_valid_value_offset) {
    ArrayData* output = out->array().get();
    output->length = array.length;
    int8_t direction = 1;

    if (array.MayHaveNulls()) {
      ARROW_ASSIGN_OR_RAISE(
          auto null_bitmap,
          arrow::internal::CopyBitmap(ctx->memory_pool(), array.buffers[0]->data(),
                                      array.offset, array.length));
      return FillNullExecutor<Type>::ExecFillNull(ctx, array, null_bitmap->data(), output,
                                                  direction, last_valid_value_chunk,
                                                  last_valid_value_offset);
    } else {
      if (array.length > 0) {
        *last_valid_value_offset = array.length - 1;
      }
      *output = array;
    }
    return Status::OK();
  }

  static Status FillNullForwardChunkedArray(KernelContext* ctx,
                                            const std::shared_ptr<ChunkedArray>& values,
                                            Datum* out) {
    if (values->null_count() == 0) {
      *out = Datum(values);
      return Status::OK();
    }
    if (values->null_count() == values->length()) {
      *out = Datum(values);
      return Status::OK();
    }

    ArrayVector new_chunks;
    if (values->length() > 0) {
      ArrayData* array_with_current = values->chunk(/*first_chunk=*/0)->data().get();
      int64_t last_valid_value_offset = -1;

      for (const auto& chunk : values->chunks()) {
        if (is_fixed_width(out->type()->id())) {
          auto* output = out->mutable_array();
          auto bit_width = checked_cast<const FixedWidthType&>(*output->type).bit_width();
          auto data_bytes = bit_util::BytesForBits(bit_width * chunk->length());
          ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(chunk->length()));
          ARROW_ASSIGN_OR_RAISE(output->buffers[1], ctx->Allocate(data_bytes));
        }
        RETURN_NOT_OK(FillNullForwardArray(ctx, *chunk->data(), out, *array_with_current,
                                           &last_valid_value_offset));
        if (chunk->null_count() != chunk->length()) {
          array_with_current = &*chunk->data();
        }
        new_chunks.push_back(MakeArray(out->make_array()->data()->Copy()));
      }
    }

    auto output = std::make_shared<ChunkedArray>(std::move(new_chunks), values->type());
    *out = Datum(output);
    return Status::OK();
  }

  static std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
    return KernelSignature::Make({InputType::Array(get_id.id)}, OutputType(FirstType));
  }
};

template <typename Type>
struct FillNullBackwardFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    switch (batch[0].kind()) {
      case Datum::ARRAY: {
        auto array_input = *batch[0].array();
        int64_t last_valid_value_offset = -1;
        return FillNullBackwardArray(ctx, array_input, out, array_input,
                                     &last_valid_value_offset);
      }
      case Datum::CHUNKED_ARRAY: {
        return FillNullBackwardChunkedArray(ctx, batch[0].chunked_array(), out);
      }
      default:
        break;
    }
    return Status::NotImplemented("Unsupported type for fill_null_backward operation: ",
                                  batch[0].ToString());
  }

  static Status FillNullBackwardArray(KernelContext* ctx, const ArrayData& array,
                                      Datum* out, const ArrayData& last_valid_value_chunk,
                                      int64_t* last_valid_value_offset) {
    ArrayData* output = out->array().get();
    output->length = array.length;
    int8_t direction = -1;

    if (array.MayHaveNulls()) {
      ARROW_ASSIGN_OR_RAISE(
          auto reversed_bitmap,
          arrow::internal::ReverseBitmap(ctx->memory_pool(), array.buffers[0]->data(),
                                         array.offset, array.length));
      return FillNullExecutor<Type>::ExecFillNull(
          ctx, array, reversed_bitmap->data(), output, direction, last_valid_value_chunk,
          last_valid_value_offset);
    } else {
      if (array.length > 0) {
        *last_valid_value_offset = 0;
      }
      *output = array;
    }
    return Status::OK();
  }

  static Status FillNullBackwardChunkedArray(KernelContext* ctx,
                                             const std::shared_ptr<ChunkedArray>& values,
                                             Datum* out) {
    if (values->null_count() == 0) {
      *out = Datum(values);
      return Status::OK();
    }
    if (values->null_count() == values->length()) {
      *out = Datum(values);
      return Status::OK();
    }
    std::vector<std::shared_ptr<Array>> new_chunks;

    if (values->length() > 0) {
      auto chunks_length = static_cast<int>(values->chunks().size());
      ArrayData* array_with_current =
          values->chunk(/*first_chunk=*/chunks_length - 1)->data().get();
      int64_t last_valid_value_offset = -1;
      auto chunks = values->chunks();
      for (int i = chunks_length - 1; i >= 0; --i) {
        const auto& chunk = chunks[i];
        if (is_fixed_width(out->type()->id())) {
          auto* output = out->mutable_array();
          auto bit_width = checked_cast<const FixedWidthType&>(*output->type).bit_width();
          auto data_bytes = bit_util::BytesForBits(bit_width * chunk->length());
          ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(chunk->length()));
          ARROW_ASSIGN_OR_RAISE(output->buffers[1], ctx->Allocate(data_bytes));
        }
        RETURN_NOT_OK(FillNullBackwardArray(ctx, *chunk->data(), out, *array_with_current,
                                            &last_valid_value_offset));
        if (chunk->null_count() != chunk->length()) {
          array_with_current = &*chunk->data();
        }
        new_chunks.push_back(MakeArray(out->make_array()->data()->Copy()));
      }
    }

    std::reverse(new_chunks.begin(), new_chunks.end());
    auto output = std::make_shared<ChunkedArray>(std::move(new_chunks), values->type());
    *out = Datum(output);
    return Status::OK();
  }

  static std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
    return KernelSignature::Make({InputType::Array(get_id.id)}, OutputType(FirstType));
  }
};
}  // namespace

template <template <class> class Functor>
void RegisterVectorFunction(FunctionRegistry* registry,
                            std::shared_ptr<VectorFunction> func) {
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
    kernel.signature = Functor<FixedSizeBinaryType>::GetSignature(get_id.id);
    kernel.exec = std::move(exec);
    kernel.can_execute_chunkwise = false;
    kernel.output_chunked = false;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  };
  auto add_primitive_kernel = [&](detail::GetTypeId get_id) {
    add_kernel(get_id, GenerateTypeAgnosticPrimitive<Functor>(get_id));
  };
  for (const auto& ty : NumericTypes()) {
    add_primitive_kernel(ty);
  }
  for (const auto& ty : TemporalTypes()) {
    add_primitive_kernel(ty);
  }
  for (const auto& ty : IntervalTypes()) {
    add_primitive_kernel(ty);
  }
  add_primitive_kernel(null());
  add_primitive_kernel(boolean());
  add_kernel(Type::FIXED_SIZE_BINARY, Functor<FixedSizeBinaryType>::Exec);
  add_kernel(Type::DECIMAL128, Functor<FixedSizeBinaryType>::Exec);
  add_kernel(Type::DECIMAL256, Functor<FixedSizeBinaryType>::Exec);
  for (const auto& ty : BaseBinaryTypes()) {
    add_kernel(ty->id(), GenerateTypeAgnosticVarBinaryBase<Functor>(*ty));
  }
  // TODO: list types
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // TODO(ARROW-9431): "replace_with_indices"
}

const FunctionDoc replace_with_mask_doc(
    "Replace items selected with a mask",
    ("Given an array and a boolean mask (either scalar or of equal length),\n"
     "along with replacement values (either scalar or array),\n"
     "each element of the array for which the corresponding mask element is\n"
     "true will be replaced by the next value from the replacements,\n"
     "or with null if the mask is null.\n"
     "Hence, for replacement arrays, len(replacements) == sum(mask == true)."),
    {"values", "mask", "replacements"});

const FunctionDoc fill_null_forward_doc(
    "Carry non-null values forward to fill null slots",
    ("Given an array, propagate last valid observation forward to next valid\n"
     "or nothing if all previous values are null."),
    {"values"});

const FunctionDoc fill_null_backward_doc(
    "Carry non-null values backward to fill null slots",
    ("Given an array, propagate next valid observation backward to previous valid\n"
     "or nothing if all next values are null."),
    {"values"});

void RegisterVectorReplace(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<VectorFunction>("replace_with_mask", Arity::Ternary(),
                                                 &replace_with_mask_doc);
    RegisterVectorFunction<ReplaceWithMaskFunctor>(registry, func);
  }
  {
    auto func = std::make_shared<VectorFunction>("fill_null_forward", Arity::Unary(),
                                                 &fill_null_forward_doc);
    RegisterVectorFunction<FillNullForwardFunctor>(registry, func);
  }
  {
    auto func = std::make_shared<VectorFunction>("fill_null_backward", Arity::Unary(),
                                                 &fill_null_backward_doc);
    RegisterVectorFunction<FillNullBackwardFunctor>(registry, func);
  }
}
}  // namespace internal
}  // namespace compute
}  // namespace arrow
