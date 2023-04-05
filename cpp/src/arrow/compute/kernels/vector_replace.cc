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
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/copy_data_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

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

// Implement replace_with kernel with array mask for fixed-width types, using
// callbacks to handle both bool and byte-sized types and to handle scalar and
// array replacements
template <typename Type, typename Data, typename CopyBitmap>
int64_t ReplaceMaskArrayImpl(const ArraySpan& array, const ArraySpan& mask,
                             int64_t mask_offset, const Data& replacements,
                             bool replacements_bitmap, int64_t replacements_offset,
                             const CopyBitmap& copy_bitmap, uint8_t* out_bitmap,
                             uint8_t* out_values, const int64_t out_offset) {
  const uint8_t* mask_bitmap = mask.buffers[0].data;
  const uint8_t* mask_values = mask.buffers[1].data;
  CopyDataUtils<Type>::CopyData(*array.type, array, /*in_offset=*/0, out_values,
                                /*out_offset=*/0, array.length);
  arrow::internal::OptionalBinaryBitBlockCounter counter(
      mask_values, mask.offset + mask_offset, mask_bitmap, mask.offset + mask_offset,
      std::min(mask.length, array.length));
  int64_t write_offset = 0;
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
        if (bit_util::GetBit(mask_values, write_offset + mask.offset + mask_offset + i) &&
            (!mask_bitmap || bit_util::GetBit(mask_bitmap, write_offset + mask.offset +
                                                               mask_offset + i))) {
          CopyDataUtils<Type>::CopyData(*array.type, replacements, replacements_offset,
                                        out_values, out_offset + write_offset + i,
                                        /*length=*/1);
          if (replacements_bitmap) {
            copy_bitmap.SetBit(out_bitmap, out_offset + write_offset + i,
                               replacements_offset);
          } else if (out_bitmap) {
            bit_util::SetBitTo(out_bitmap, out_offset + write_offset + i, true);
          }
          replacements_offset++;
        }
      }
    }
    write_offset += block.length;
  }
  return replacements_offset;
}

template <typename Type, typename Enable = void>
struct ReplaceMaskImpl {};

template <typename Type>
struct ReplaceMaskImpl<
    Type, enable_if_t<!(is_base_binary_type<Type>::value || is_null_type<Type>::value)>> {
  static Result<int64_t> ExecScalarMask(KernelContext* ctx, const ArraySpan& array,
                                        const BooleanScalar& mask, ExecValue replacements,
                                        int64_t replacements_offset, ExecResult* out) {
    // Implement replace_with kernel with scalar mask for fixed-width types
    ExecValue source = array;
    int64_t source_offset = 0;
    std::shared_ptr<Scalar> null_scalar;
    if (!mask.is_valid) {
      // Output = null
      null_scalar = MakeNullScalar(out->type()->GetSharedPtr());
      source.SetScalar(null_scalar.get());
    } else if (mask.value) {
      // Output = replacement
      source = replacements;
      source_offset = replacements_offset;
    }
    ArrayData* out_arr = out->array_data().get();
    uint8_t* out_bitmap = out_arr->buffers[0]->mutable_data();
    uint8_t* out_values = out_arr->buffers[1]->mutable_data();
    const int64_t out_offset = out_arr->offset;
    if (source.is_array()) {
      const ArraySpan& in_data = source.array;
      CopyDataUtils<Type>::CopyData(*array.type, in_data, source_offset, out_values,
                                    out_offset, array.length);
      if (in_data.MayHaveNulls()) {
        arrow::internal::CopyBitmap(in_data.buffers[0].data,
                                    in_data.offset + source_offset, array.length,
                                    out_bitmap, out_offset);
      } else {
        bit_util::SetBitsTo(out_bitmap, out_offset, array.length, true);
      }
    } else {
      const Scalar& in_data = *source.scalar;
      CopyDataUtils<Type>::CopyData(*array.type, in_data, source_offset, out_values,
                                    out_offset, array.length);
      bit_util::SetBitsTo(out_bitmap, out_offset, array.length, in_data.is_valid);
    }
    return replacements_offset + array.length;
  }

  static Result<int64_t> ExecArrayMask(KernelContext* ctx, const ArraySpan& array,
                                       const ArraySpan& mask, int64_t mask_offset,
                                       ExecValue replacements,
                                       int64_t replacements_offset, ExecResult* out) {
    ArrayData* out_arr = out->array_data().get();
    out_arr->length = array.length;
    const int64_t out_offset = out_arr->offset;
    uint8_t* out_bitmap = nullptr;
    uint8_t* out_values = out_arr->buffers[1]->mutable_data();
    const bool replacements_bitmap =
        replacements.is_array() ? replacements.array.MayHaveNulls() : true;
    if (array.MayHaveNulls() || mask.MayHaveNulls() || replacements_bitmap) {
      out_bitmap = out_arr->buffers[0]->mutable_data();
      out_arr->null_count = kUnknownNullCount;
      if (array.MayHaveNulls()) {
        // Copy array's bitmap
        arrow::internal::CopyBitmap(array.buffers[0].data, array.offset, array.length,
                                    out_bitmap, out_offset);
      } else {
        // Array has no bitmap but mask/replacements do, generate an all-valid bitmap
        bit_util::SetBitsTo(out_bitmap, out_offset, array.length, true);
      }
    } else {
      bit_util::SetBitsTo(out_arr->buffers[0]->mutable_data(), out_offset, array.length,
                          true);
      out_arr->null_count = 0;
    }

    int64_t new_replacements_offset = replacements_offset;
    if (replacements.is_array()) {
      const ArraySpan& source = replacements.array;
      new_replacements_offset = ReplaceMaskArrayImpl<Type>(
          array, mask, mask_offset, source, replacements_bitmap, replacements_offset,
          CopyArrayBitmap{replacements_bitmap ? source.buffers[0].data : nullptr,
                          source.offset},
          out_bitmap, out_values, out_offset);
    } else {
      const Scalar& source = *replacements.scalar;
      new_replacements_offset = ReplaceMaskArrayImpl<Type>(
          array, mask, mask_offset, source, replacements_bitmap, replacements_offset,
          CopyScalarBitmap{source.is_valid}, out_bitmap, out_values, out_offset);
    }

    if (mask.MayHaveNulls()) {
      arrow::internal::BitmapAnd(out_bitmap, out_offset, mask.buffers[0].data,
                                 mask.offset + mask_offset, array.length, out_offset,
                                 out_bitmap);
    }
    return new_replacements_offset;
  }
};

template <typename Type>
struct ReplaceMaskImpl<Type, enable_if_null<Type>> {
  static Result<int64_t> ExecScalarMask(KernelContext* ctx, const ArraySpan& array,
                                        const BooleanScalar& mask, ExecValue replacements,
                                        int64_t replacements_offset, ExecResult* out) {
    out->value = array;
    return Status::OK();
  }
  static Result<int64_t> ExecArrayMask(KernelContext* ctx, const ArraySpan& array,
                                       const ArraySpan& mask, int64_t mask_offset,
                                       ExecValue replacements,
                                       int64_t replacements_offset, ExecResult* out) {
    out->value = array;
    return Status::OK();
  }
};

template <typename Type>
struct ReplaceMaskImpl<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  static Result<int64_t> ExecScalarMask(KernelContext* ctx, const ArraySpan& array,
                                        const BooleanScalar& mask, ExecValue replacements,
                                        int64_t replacements_offset, ExecResult* out) {
    if (!mask.is_valid) {
      // Output = null
      ARROW_ASSIGN_OR_RAISE(
          auto replacement_array,
          MakeArrayOfNull(array.type->GetSharedPtr(), array.length, ctx->memory_pool()));
      out->value = std::move(replacement_array->data());
      return replacements_offset;
    } else if (mask.value) {
      // Output = replacement
      if (replacements.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            auto replacement_array,
            MakeArrayFromScalar(*replacements.scalar, array.length, ctx->memory_pool()));
        out->value = std::move(replacement_array->data());
      } else {
        // Set to be a slice of replacements
        std::shared_ptr<ArrayData> result = replacements.array.ToArrayData();
        result->offset += replacements_offset;
        result->length = array.length;

        // TODO(wesm): why is the replacements null count not sufficient?
        result->null_count = kUnknownNullCount;
        out->value = result;
      }
      return replacements_offset + array.length;
    } else {
      // Output = input
      out->value = array.ToArrayData();
      return replacements_offset;
    }
  }
  static Result<int64_t> ExecArrayMask(KernelContext* ctx, const ArraySpan& array,
                                       const ArraySpan& mask, int64_t mask_offset,
                                       ExecValue replacements,
                                       int64_t replacements_offset, ExecResult* out) {
    BuilderType builder(array.type->GetSharedPtr(), ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(array.length));
    RETURN_NOT_OK(builder.ReserveData(array.buffers[2].size));
    int64_t source_offset = 0;

    ArraySpan adjusted_mask = mask;
    adjusted_mask.offset += mask_offset;
    adjusted_mask.length = std::min(adjusted_mask.length - mask_offset, array.length);
    RETURN_NOT_OK(VisitArraySpanInline<BooleanType>(
        adjusted_mask,
        [&](bool replace) {
          if (replace && replacements.is_scalar()) {
            const Scalar& scalar = *replacements.scalar;
            if (scalar.is_valid) {
              RETURN_NOT_OK(builder.Append(UnboxScalar<Type>::Unbox(scalar)));
            } else {
              RETURN_NOT_OK(builder.AppendNull());
            }
          } else {
            const ArraySpan& source = replace ? replacements.array : array;
            const int64_t offset = replace ? replacements_offset++ : source_offset;
            if (!source.MayHaveNulls() ||
                bit_util::GetBit(source.buffers[0].data, source.offset + offset)) {
              const uint8_t* data = source.buffers[2].data;
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
    std::shared_ptr<ArrayData> temp_output;
    RETURN_NOT_OK(builder.FinishInternal(&temp_output));
    // Builder type != logical type due to GenerateTypeAgnosticVarBinaryBase
    temp_output->type = array.type->GetSharedPtr();
    out->value = std::move(temp_output);
    return replacements_offset;
  }
};

Status CheckReplaceMaskInputs(const DataType& value_type, int64_t arr_length,
                              const ExecValue& mask_box,
                              const DataType& replacements_type,
                              int64_t replacements_length, bool replacements_is_array) {
  // Needed for FixedSizeBinary/parameterized types
  if (!value_type.Equals(replacements_type, /*check_metadata=*/false)) {
    return Status::Invalid("Replacements must be of same type (expected ",
                           value_type.ToString(), " but got ",
                           replacements_type.ToString(), ")");
  }

  int64_t mask_count = 0;
  if (mask_box.is_scalar()) {
    const auto& mask = mask_box.scalar_as<BooleanScalar>();
    mask_count = (mask.is_valid && mask.value) ? arr_length : 0;
  } else {
    const ArraySpan& mask = mask_box.array;
    mask_count = GetTrueCount(mask);
    if (mask.length != arr_length) {
      return Status::Invalid("Mask must be of same length as array (expected ",
                             arr_length, " items but got ", mask.length, " items)");
    }
  }
  if (replacements_is_array) {
    if (replacements_length < mask_count) {
      return Status::Invalid("Replacement array must be of appropriate length (expected ",
                             mask_count, " items but got ", replacements_length,
                             " items)");
    }
  }
  return Status::OK();
}

template <typename Type>
struct ReplaceMask {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& arr = batch[0].array;
    const ExecValue& mask = batch[1];
    const ExecValue& replacements = batch[2];
    RETURN_NOT_OK(CheckReplaceMaskInputs(*arr.type, arr.length, mask,
                                         *replacements.type(), replacements.length(),
                                         replacements.is_array()));
    if (mask.is_scalar()) {
      return ReplaceMaskImpl<Type>::ExecScalarMask(
                 ctx, arr, mask.scalar_as<BooleanScalar>(), replacements,
                 /*replacements_offset=*/0, out)
          .status();
    } else {
      // The extra mask offset is for dealing with chunked inputs, so zero when
      // there is only a single chunk to process
      return ReplaceMaskImpl<Type>::ExecArrayMask(ctx, arr, mask.array,
                                                  /*mask_offset=*/0, replacements,
                                                  /*replacements_offset=*/0, out)
          .status();
    }
  }

  static std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
    return KernelSignature::Make({InputType(get_id.id), boolean(), InputType(get_id.id)},
                                 FirstType);
  }
};

template <typename Type>
struct ReplaceMaskChunked {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const Datum& mask = batch[1];
    const Datum& replacements = batch[2];

    // TODO(wesm): these assertions that the arguments cannot be ChunkedArray
    // should happen someplace more generic, not here
    if (!mask.is_array() && !mask.is_scalar()) {
      return Status::Invalid("Mask must be array or scalar, not ", batch[1].ToString());
    }

    if (!replacements.is_array() && !replacements.is_scalar()) {
      return Status::Invalid("Replacements must be array or scalar, not ",
                             replacements.ToString());
    }

    const ChunkedArray& arr = *batch[0].chunked_array();

    RETURN_NOT_OK(CheckReplaceMaskInputs(*arr.type(), arr.length(), GetExecValue(mask),
                                         *replacements.type(), replacements.length(),
                                         replacements.is_arraylike()));

    ExecValue replacements_val = GetExecValue(replacements);

    // Chunked array
    ArrayVector output_chunks;
    output_chunks.reserve(arr.num_chunks());

    int64_t mask_offset = 0;
    int64_t replacements_offset = 0;
    for (const std::shared_ptr<Array>& chunk : arr.chunks()) {
      if (chunk->length() == 0) continue;
      // Allocate a new array
      ExecResult chunk_result;
      if (is_fixed_width(out->type()->id())) {
        auto chunk_out = std::make_shared<ArrayData>(chunk->type(), chunk->length());
        chunk_out->buffers.resize(2);
        ARROW_ASSIGN_OR_RAISE(chunk_out->buffers[0],
                              ctx->AllocateBitmap(chunk->length()));
        const int64_t slot_width = out->type()->byte_width();
        ARROW_ASSIGN_OR_RAISE(chunk_out->buffers[1],
                              ctx->Allocate(slot_width * chunk->length()));
        chunk_result.value = chunk_out;
      }
      if (batch[1].is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            replacements_offset,
            ReplaceMaskImpl<Type>::ExecScalarMask(
                ctx, *chunk->data(), batch[1].scalar_as<BooleanScalar>(),
                replacements_val, replacements_offset, &chunk_result));
      } else {
        ARROW_ASSIGN_OR_RAISE(replacements_offset,
                              ReplaceMaskImpl<Type>::ExecArrayMask(
                                  ctx, *chunk->data(), *batch[1].array(), mask_offset,
                                  replacements_val, replacements_offset, &chunk_result));
      }
      output_chunks.push_back(MakeArray(chunk_result.array_data()));
      mask_offset += chunk->length();
    }

    return ChunkedArray::Make(std::move(output_chunks), out->type()).Value(out);
  }
};

// This is for fixed-size types only
template <typename Type>
void FillNullInDirectionImpl(const ArraySpan& current_chunk, const uint8_t* null_bitmap,
                             ExecResult* out, int8_t direction,
                             const ArraySpan& last_valid_value_chunk,
                             int64_t* last_valid_value_offset) {
  ArrayData* out_arr = out->array_data().get();
  uint8_t* out_bitmap = out_arr->buffers[0]->mutable_data();
  uint8_t* out_values = out_arr->buffers[1]->mutable_data();
  arrow::internal::CopyBitmap(current_chunk.buffers[0].data, current_chunk.offset,
                              current_chunk.length, out_bitmap, out_arr->offset);
  CopyDataUtils<Type>::CopyData(*current_chunk.type, current_chunk, /*in_offset=*/0,
                                out_values, /*out_offset=*/out_arr->offset,
                                current_chunk.length);

  bool has_fill_value = *last_valid_value_offset != -1;
  int64_t write_offset = direction == 1 ? 0 : current_chunk.length - 1;
  int64_t bitmap_offset = 0;

  arrow::internal::OptionalBitBlockCounter counter(null_bitmap, out_arr->offset,
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
  out_arr->null_count = kUnknownNullCount;
}

template <typename Type, typename Enable = void>
struct FillNullImpl {};

template <typename Type>
struct FillNullImpl<
    Type,
    enable_if_t<is_number_type<Type>::value || is_boolean_type<Type>::value ||
                is_boolean_type<Type>::value || is_fixed_size_binary_type<Type>::value ||
                std::is_same<Type, MonthDayNanoIntervalType>::value>> {
  static Status Exec(KernelContext* ctx, const ArraySpan& array,
                     const uint8_t* reversed_bitmap, ExecResult* out, int8_t direction,
                     const ArraySpan& last_valid_value_chunk,
                     int64_t* last_valid_value_offset) {
    FillNullInDirectionImpl<Type>(array, reversed_bitmap, out, direction,
                                  last_valid_value_chunk, last_valid_value_offset);
    return Status::OK();
  }
};

template <typename Type>
struct FillNullImpl<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  static Status Exec(KernelContext* ctx, const ArraySpan& current_chunk,
                     const uint8_t* reversed_bitmap, ExecResult* out, int8_t direction,
                     const ArraySpan& last_valid_value_chunk,
                     int64_t* last_valid_value_offset) {
    ArrayData* out_arr = out->array_data().get();

    BuilderType builder(current_chunk.type->GetSharedPtr(), ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(current_chunk.length));
    RETURN_NOT_OK(builder.ReserveData(current_chunk.buffers[2].size));
    int64_t array_value_index = direction == 1 ? 0 : current_chunk.length - 1;
    const uint8_t* data = current_chunk.buffers[2].data;
    const uint8_t* data_prev = last_valid_value_chunk.buffers[2].data;
    const offset_type* offsets = current_chunk.GetValues<offset_type>(1);
    const offset_type* offsets_prev = last_valid_value_chunk.GetValues<offset_type>(1);

    bool has_fill_value_last_chunk = *last_valid_value_offset != -1;
    bool has_fill_value_current_chunk = false;
    /*tuple for store: <use current_chunk(true) or last_valid_chunk(false),
     * start offset of the current value, end offset for the current value>*/

    // TODO(wesm): using out_arr->offset here is a bit ugly because we
    // discard it later in the function
    std::vector<std::tuple<bool, offset_type, offset_type>> offsets_reversed;
    RETURN_NOT_OK(VisitNullBitmapInline<>(
        reversed_bitmap, out_arr->offset, current_chunk.length,
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
    out->value = std::move(temp_output->data());
    // Builder type != logical type due to GenerateTypeAgnosticVarBinaryBase
    out->array_data()->type = current_chunk.type->GetSharedPtr();
    return Status::OK();
  }
};

template <typename Type>
struct FillNullImpl<Type, enable_if_null<Type>> {
  static Status Exec(KernelContext* ctx, const ArraySpan& array,
                     const uint8_t* reversed_bitmap, ExecResult* out, int8_t direction,
                     const ArraySpan& last_valid_value_chunk,
                     int64_t* last_valid_value_offset) {
    out->value = array.ToArrayData();
    return Status::OK();
  }
};

template <typename Type>
struct FillNullForward {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& array_input = batch[0].array;
    int64_t last_valid_offset = -1;  // unused
    return ExecChunk(ctx, array_input, out, array_input, &last_valid_offset);
  }

  static Status ExecChunk(KernelContext* ctx, const ArraySpan& array, ExecResult* out,
                          const ArraySpan& last_valid_value_chunk,
                          int64_t* last_valid_value_offset) {
    ArrayData* output = out->array_data().get();
    output->length = array.length;
    int8_t direction = 1;
    if (array.MayHaveNulls()) {
      ARROW_ASSIGN_OR_RAISE(
          auto null_bitmap,
          arrow::internal::CopyBitmap(ctx->memory_pool(), array.buffers[0].data,
                                      array.offset, array.length));
      return FillNullImpl<Type>::Exec(ctx, array, null_bitmap->data(), out, direction,
                                      last_valid_value_chunk, last_valid_value_offset);
    } else {
      // TODO(wesm): zero copy optimization is a bit ugly...
      if (array.length > 0) {
        *last_valid_value_offset = array.length - 1;
      }
      out->value = array.ToArrayData();
    }
    return Status::OK();
  }

  static std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
    return KernelSignature::Make({InputType(get_id.id)}, FirstType);
  }
};

template <typename Type>
struct FillNullForwardChunked {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ChunkedArray& values = *batch[0].chunked_array();

    if (values.null_count() == 0) {
      *out = batch[0];
      return Status::OK();
    }
    if (values.null_count() == values.length()) {
      *out = batch[0];
      return Status::OK();
    }

    ArrayVector new_chunks;
    if (values.length() > 0) {
      ArrayData* array_with_current = values.chunk(/*first_chunk=*/0)->data().get();
      int64_t last_valid_value_offset = -1;
      for (const std::shared_ptr<Array>& chunk : values.chunks()) {
        if (is_fixed_width(out->type()->id())) {
          ArrayData* output = out->mutable_array();
          ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(chunk->length()));
          ARROW_ASSIGN_OR_RAISE(
              output->buffers[1],
              ctx->Allocate(out->type()->byte_width() * chunk->length()));
        }
        ExecResult chunk_result;
        chunk_result.value = out->array();
        RETURN_NOT_OK(FillNullForward<Type>::ExecChunk(ctx, *chunk->data(), &chunk_result,
                                                       *array_with_current,
                                                       &last_valid_value_offset));
        if (chunk->null_count() != chunk->length()) {
          array_with_current = chunk->data().get();
        }
        new_chunks.push_back(MakeArray(chunk_result.array_data()->Copy()));
      }
    }

    auto output = std::make_shared<ChunkedArray>(std::move(new_chunks), values.type());
    *out = Datum(output);
    return Status::OK();
  }
};

template <typename Type>
struct FillNullBackward {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    int64_t last_offset = -1;  // unused
    return ExecChunk(ctx, batch[0].array, out, batch[0].array, &last_offset);
  }

  static Status ExecChunk(KernelContext* ctx, const ArraySpan& array, ExecResult* out,
                          const ArraySpan& last_valid_value_chunk,
                          int64_t* last_valid_value_offset) {
    ArrayData* out_arr = out->array_data().get();
    out_arr->length = array.length;
    int8_t direction = -1;

    if (array.MayHaveNulls()) {
      ARROW_ASSIGN_OR_RAISE(
          auto reversed_bitmap,
          arrow::internal::ReverseBitmap(ctx->memory_pool(), array.buffers[0].data,
                                         array.offset, array.length));
      return FillNullImpl<Type>::Exec(ctx, array, reversed_bitmap->data(), out, direction,
                                      last_valid_value_chunk, last_valid_value_offset);
    } else {
      // Zero copy optimization
      if (array.length > 0) {
        *last_valid_value_offset = 0;
      }
      out->value = array.ToArrayData();
    }
    return Status::OK();
  }

  static std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
    return KernelSignature::Make({InputType(get_id.id)}, FirstType);
  }
};

template <typename Type>
struct FillNullBackwardChunked {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    DCHECK_EQ(Datum::CHUNKED_ARRAY, batch[0].kind());
    const ChunkedArray& values = *batch[0].chunked_array();
    if (values.null_count() == 0) {
      *out = Datum(values);
      return Status::OK();
    }
    if (values.null_count() == values.length()) {
      *out = Datum(values);
      return Status::OK();
    }
    std::vector<std::shared_ptr<Array>> new_chunks;

    if (values.length() > 0) {
      auto chunks_length = static_cast<int>(values.chunks().size());
      ArrayData* array_with_current =
          values.chunk(/*first_chunk=*/chunks_length - 1)->data().get();
      int64_t last_valid_value_offset = -1;
      auto chunks = values.chunks();
      for (int i = chunks_length - 1; i >= 0; --i) {
        const auto& chunk = chunks[i];
        if (is_fixed_width(out->type()->id())) {
          ArrayData* output = out->mutable_array();
          auto data_bytes = output->type->byte_width() * chunk->length();
          ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(chunk->length()));
          ARROW_ASSIGN_OR_RAISE(output->buffers[1], ctx->Allocate(data_bytes));
        }
        ExecResult chunk_result;
        chunk_result.value = out->array();
        RETURN_NOT_OK(FillNullBackward<Type>::ExecChunk(
            ctx, *chunk->data(), &chunk_result, *array_with_current,
            &last_valid_value_offset));
        if (chunk->null_count() != chunk->length()) {
          array_with_current = chunk->data().get();
        }
        new_chunks.push_back(MakeArray(chunk_result.array_data()->Copy()));
      }
    }

    std::reverse(new_chunks.begin(), new_chunks.end());
    *out = std::make_shared<ChunkedArray>(std::move(new_chunks), values.type());
    return Status::OK();
  }
};

}  // namespace

void AddKernel(Type::type type_id, std::shared_ptr<KernelSignature> signature,
               ArrayKernelExec exec, VectorKernel::ChunkedExec exec_chunked,
               FunctionRegistry* registry, VectorFunction* func) {
  VectorKernel kernel;
  kernel.can_execute_chunkwise = false;
  if (is_fixed_width(type_id)) {
    kernel.null_handling = NullHandling::type::COMPUTED_PREALLOCATE;
  } else {
    kernel.can_write_into_slices = false;
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
  }
  kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
  kernel.signature = std::move(signature);
  kernel.exec = std::move(exec);
  kernel.exec_chunked = exec_chunked;
  kernel.can_execute_chunkwise = false;
  kernel.output_chunked = false;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

template <template <class> class Functor, template <class> class ChunkedFunctor>
void RegisterVectorFunction(FunctionRegistry* registry,
                            std::shared_ptr<VectorFunction> func) {
  auto add_primitive_kernel = [&](detail::GetTypeId get_id) {
    AddKernel(
        get_id.id, Functor<FixedSizeBinaryType>::GetSignature(get_id),
        GenerateTypeAgnosticPrimitive<Functor, ArrayKernelExec>(get_id),
        GenerateTypeAgnosticPrimitive<ChunkedFunctor, VectorKernel::ChunkedExec>(get_id),
        registry, func.get());
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
  AddKernel(Type::FIXED_SIZE_BINARY,
            Functor<FixedSizeBinaryType>::GetSignature(Type::FIXED_SIZE_BINARY),
            Functor<FixedSizeBinaryType>::Exec, ChunkedFunctor<FixedSizeBinaryType>::Exec,
            registry, func.get());
  AddKernel(Type::DECIMAL128,
            Functor<FixedSizeBinaryType>::GetSignature(Type::DECIMAL128),
            Functor<FixedSizeBinaryType>::Exec, ChunkedFunctor<FixedSizeBinaryType>::Exec,
            registry, func.get());
  AddKernel(Type::DECIMAL256,
            Functor<FixedSizeBinaryType>::GetSignature(Type::DECIMAL256),
            Functor<FixedSizeBinaryType>::Exec, ChunkedFunctor<FixedSizeBinaryType>::Exec,
            registry, func.get());
  for (const auto& ty : BaseBinaryTypes()) {
    AddKernel(
        ty->id(), Functor<FixedSizeBinaryType>::GetSignature(ty->id()),
        GenerateTypeAgnosticVarBinaryBase<Functor, ArrayKernelExec>(*ty),
        GenerateTypeAgnosticVarBinaryBase<ChunkedFunctor, VectorKernel::ChunkedExec>(*ty),
        registry, func.get());
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
                                                 replace_with_mask_doc);
    RegisterVectorFunction<ReplaceMask, ReplaceMaskChunked>(registry, func);
  }
  {
    auto func = std::make_shared<VectorFunction>("fill_null_forward", Arity::Unary(),
                                                 fill_null_forward_doc);
    RegisterVectorFunction<FillNullForward, FillNullForwardChunked>(registry, func);
  }
  {
    auto func = std::make_shared<VectorFunction>("fill_null_backward", Arity::Unary(),
                                                 fill_null_backward_doc);
    RegisterVectorFunction<FillNullBackward, FillNullBackwardChunked>(registry, func);
  }
}
}  // namespace internal
}  // namespace compute
}  // namespace arrow
