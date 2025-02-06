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

// Implementation of casting to (or between) list types

#include <limits>
#include <set>
#include <utility>
#include <vector>

#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/int_util.h"

namespace arrow {

using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::CopyBitmap;

namespace compute::internal {

namespace {

Result<std::shared_ptr<Buffer>> GetNullBitmapBuffer(const ArraySpan& in_array,
                                                    MemoryPool* pool) {
  if (in_array.buffers[0].data == nullptr) {
    return nullptr;
  }

  if (in_array.offset == 0) {
    return in_array.GetBuffer(0);
  }

  // If a non-zero offset, we need to shift the bitmap
  return CopyBitmap(pool, in_array.buffers[0].data, in_array.offset, in_array.length);
}

// (Large)List<T> -> (Large)List<U>

// TODO(wesm): memory could be preallocated here and it would make
// things simpler
template <typename SrcType, typename DestType>
Status CastListOffsets(KernelContext* ctx, const ArraySpan& in_array,
                       ArrayData* out_array) {
  using src_offset_type = typename SrcType::offset_type;
  using dest_offset_type = typename DestType::offset_type;

  if constexpr (!std::is_same<src_offset_type, dest_offset_type>::value) {
    ARROW_ASSIGN_OR_RAISE(out_array->buffers[1], ctx->Allocate(sizeof(dest_offset_type) *
                                                               (in_array.length + 1)));
    ::arrow::internal::CastInts(in_array.GetValues<src_offset_type>(1),
                                out_array->GetMutableValues<dest_offset_type>(1),
                                in_array.length + 1);
  }

  return Status::OK();
}

template <typename SrcType, typename DestType>
struct CastList {
  using src_offset_type = typename SrcType::offset_type;
  using dest_offset_type = typename DestType::offset_type;

  static constexpr bool is_upcast = sizeof(src_offset_type) < sizeof(dest_offset_type);
  static constexpr bool is_downcast = sizeof(src_offset_type) > sizeof(dest_offset_type);

  static Status HandleOffsets(KernelContext* ctx, const ArraySpan& in_array,
                              ArrayData* out_array, std::shared_ptr<ArrayData>* values) {
    auto offsets = in_array.GetValues<src_offset_type>(1);

    // Handle list offsets
    // Several cases can arise:
    // - the source offset is non-zero, in which case we slice the underlying values
    //   and shift the list offsets (regardless of their respective types)
    // - the source offset is zero but source and destination types have
    //   different list offset types, in which case we cast the list offsets
    // - otherwise, we simply keep the original list offsets
    if (is_downcast) {
      if (offsets[in_array.length] > std::numeric_limits<dest_offset_type>::max()) {
        return Status::Invalid("Array of type ", in_array.type->ToString(),
                               " too large to convert to ", out_array->type->ToString());
      }
    }

    if (in_array.offset != 0) {
      ARROW_ASSIGN_OR_RAISE(
          out_array->buffers[1],
          ctx->Allocate(sizeof(dest_offset_type) * (in_array.length + 1)));

      auto shifted_offsets = out_array->GetMutableValues<dest_offset_type>(1);
      for (int64_t i = 0; i < in_array.length + 1; ++i) {
        shifted_offsets[i] = static_cast<dest_offset_type>(offsets[i] - offsets[0]);
      }

      *values = (*values)->Slice(offsets[0], offsets[in_array.length]);
    } else {
      RETURN_NOT_OK((CastListOffsets<SrcType, DestType>(ctx, in_array, out_array)));
    }

    return Status::OK();
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const CastOptions& options = CastState::Get(ctx);

    auto child_type = checked_cast<const DestType&>(*out->type()).value_type();

    const ArraySpan& in_array = batch[0].array;

    ArrayData* out_array = out->array_data().get();
    ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                          GetNullBitmapBuffer(in_array, ctx->memory_pool()));
    out_array->buffers[1] = in_array.GetBuffer(1);

    std::shared_ptr<ArrayData> values = in_array.child_data[0].ToArrayData();

    RETURN_NOT_OK(HandleOffsets(ctx, in_array, out_array, &values));

    // Handle values
    ARROW_ASSIGN_OR_RAISE(Datum cast_values,
                          Cast(values, child_type, options, ctx->exec_context()));

    DCHECK(cast_values.is_array());
    out_array->child_data.push_back(cast_values.array());
    return Status::OK();
  }
};

template <typename SrcType, typename DestType>
void AddListCast(CastFunction* func) {
  ScalarKernel kernel;
  kernel.exec = CastList<SrcType, DestType>::Exec;
  kernel.signature =
      KernelSignature::Make({InputType(SrcType::type_id)}, kOutputTargetType);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(SrcType::type_id, std::move(kernel)));
}

template <typename DestType>
struct CastFixedToVarList {
  using dest_offset_type = typename DestType::offset_type;

  /// \pre values->length == list_size * num_lists
  /// \pre values->offset >= 0
  ///
  /// \param num_lists The number of fixed-size lists in the input and lists in the
  ///                  output.
  /// \param list_validity The byte-aligned validity bitmap of the num_lists lists
  ///                      or NULLPTR.
  /// \param list_size The size of the fixed-size lists projected onto the
  ///                  values array.
  static Result<std::shared_ptr<ArrayData>> CastChildValues(
      KernelContext* ctx, int64_t num_lists, const uint8_t* list_validity,
      int32_t list_size, std::shared_ptr<ArrayData>&& values,
      const std::shared_ptr<DataType>& to_type) {
    DCHECK_EQ(values->length, list_size * num_lists);
    // XXX: is it OK to use options recursively without modifying it?
    const CastOptions& options = CastState::Get(ctx);
    Datum cast_values;
    if (list_validity && list_size > 0) {
      const int64_t full_values_length = values->offset + values->length;
      // If the fixed_size_list array has nulls, the child values aligned with
      // those null lists are unspecified (could be anything). We don't want the
      // casting of the child values array to fail because of values in these slots
      // (e.g. empty strings will fail a cast to numbers).
      auto original_values_validity = values->buffers[0];
      // So we need to create a new validity bitmap that is the same size as the
      // child values array, but with validity bits zeroed out for the null lists.
      std::shared_ptr<Buffer> combined_values_validity;
      if (original_values_validity) {
        ARROW_ASSIGN_OR_RAISE(
            combined_values_validity,
            original_values_validity->CopySlice(
                0, bit_util::BytesForBits(full_values_length), ctx->memory_pool()));
      } else {
        ARROW_ASSIGN_OR_RAISE(combined_values_validity,
                              AllocateBitmap(full_values_length, ctx->memory_pool()));
        auto* bitmap = combined_values_validity->mutable_data();
        bitmap[bit_util::BytesForBits(full_values_length) - 1] = 0;
        bit_util::SetBitsTo(bitmap, 0, values->offset, false);
        bit_util::SetBitsTo(bitmap, values->offset, values->length, true);
      }
      // Expand every 0 in list_validity into list_size 0s in combined_values_validity.
      auto* combined_values_validity_data = combined_values_validity->mutable_data();
      BitBlockCounter bit_counter{list_validity, 0, num_lists};
      for (int64_t list_idx = 0; list_idx < num_lists;) {
        BitBlockCount block = bit_counter.NextWord();
        if (block.NoneSet()) {
          bit_util::SetBitsTo(combined_values_validity_data,
                              values->offset + list_idx * list_size,
                              block.length * list_size, false);
        } else if (!block.AllSet()) {
          for (int64_t i = 0; i < block.length; i++) {
            if (!bit_util::GetBit(list_validity, list_idx + i)) {
              bit_util::SetBitsTo(combined_values_validity_data,
                                  values->offset + (list_idx + i) * list_size, list_size,
                                  false);
            }
          }
        }
        list_idx += block.length;
      }

      // Use the combined validity bitmap before casting the child values so
      // casting ignores the unspecified values in the null slots.
      values->null_count.store(kUnknownNullCount);
      values->buffers[0] = combined_values_validity;
      ARROW_ASSIGN_OR_RAISE(
          cast_values, Cast(std::move(values), to_type, options, ctx->exec_context()));
    } else {
      ARROW_ASSIGN_OR_RAISE(
          cast_values, Cast(std::move(values), to_type, options, ctx->exec_context()));
    }
    DCHECK(cast_values.is_array());
    return cast_values.array();
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // in_array: FixedSizeList<T> -> out_array: List<U>
    const ArraySpan& in_array = batch[0].array;
    const auto& in_type = checked_cast<const FixedSizeListType&>(*in_array.type);
    const int32_t list_size = in_type.list_size();
    ArrayData* out_array = out->array_data().get();
    DCHECK_EQ(in_array.length, out_array->length);
    auto out_child_type = checked_cast<const DestType&>(*out_array->type).value_type();

    // Share or copy the validity bitmap
    ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                          GetNullBitmapBuffer(in_array, ctx->memory_pool()));

    // Allocate a new offsets buffer
    ARROW_ASSIGN_OR_RAISE(out_array->buffers[1],
                          ctx->Allocate(sizeof(dest_offset_type) * (batch.length + 1)));
    auto* offsets = out_array->GetMutableValues<dest_offset_type>(1);
    dest_offset_type offset = 0;
    for (int64_t i = 0; i <= batch.length; ++i) {
      offsets[i] = offset;
      offset += list_size;
    }

    // Handle child values
    std::shared_ptr<ArrayData> child_values = in_array.child_data[0].ToArrayData();
    if (in_array.offset > 0 || child_values->length > in_array.length * list_size) {
      child_values =
          child_values->Slice(in_array.offset * list_size, in_array.length * list_size);
    }
    DCHECK_EQ(out_array->offset, 0);
    const uint8_t* list_validity =
        in_array.MayHaveNulls() ? out_array->GetValues<uint8_t>(0, 0) : nullptr;
    ARROW_ASSIGN_OR_RAISE(
        auto cast_values,
        CastChildValues(ctx, /*num_lists=*/in_array.length, list_validity, list_size,
                        std::move(child_values), out_child_type));
    out_array->child_data.push_back(std::move(cast_values));
    return Status::OK();
  }
};

template <typename SrcType>
struct CastVarToFixedList {
  using src_offset_type = typename SrcType::offset_type;

  // Validate the lengths of each list are equal to the list size.
  // Additionally, checks if the null slots are all the list size. Returns true
  // if so, which can be used to optimize the cast.
  static Result<bool> ValidateLengths(const ArraySpan& in_array, int32_t list_size) {
    const auto* offsets = in_array.GetValues<src_offset_type>(1);
    src_offset_type expected_offset = offsets[0] + list_size;
    // If there are nulls, it's possible that the null slots are the correct
    // size. That enables an optimization later in this function.
    bool nulls_have_correct_length = true;
    if (in_array.GetNullCount() > 0) {
      for (int64_t i = 1; i <= in_array.length; ++i) {
        if (in_array.IsNull(i - 1)) {
          if (offsets[i] != expected_offset) {
            nulls_have_correct_length = false;
          }
          // If element is null, it can be any size, so the next offset is valid.
          expected_offset = offsets[i] + list_size;
        } else {
          if (offsets[i] != expected_offset) {
            return Status::Invalid("ListType can only be casted to FixedSizeListType ",
                                   "if the lists are all the expected size.");
          }
          expected_offset += list_size;
        }
      }
    } else {
      // Don't need to check null slots if there are no nulls
      for (int64_t i = 1; i <= in_array.length; ++i) {
        if (offsets[i] != expected_offset) {
          return Status::Invalid("ListType can only be casted to FixedSizeListType ",
                                 "if the lists are all the expected size.");
        }
        expected_offset += list_size;
      }
    }
    return nulls_have_correct_length;
  }

  // Build take indices for the values. This is used to fill in the null slots
  // if they aren't already the correct size.
  static Result<std::shared_ptr<Array>> BuildTakeIndices(const ArraySpan& in_array,
                                                         int32_t list_size,
                                                         KernelContext* ctx) {
    const auto* offsets = in_array.GetValues<src_offset_type>(1);
    // We need to fill in the null slots, so we'll use Take on the values.
    auto builder = Int64Builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(in_array.length * list_size));
    for (int64_t offset_i = 0; offset_i < in_array.length; ++offset_i) {
      if (in_array.IsNull(offset_i)) {
        // If element is null, just fill in the null slots with first value.
        for (int64_t j = 0; j < list_size; ++j) {
          builder.UnsafeAppend(0);
        }
      } else {
        int64_t value_i = offsets[offset_i];
        for (int64_t j = 0; j < list_size; ++j) {
          builder.UnsafeAppend(value_i++);
        }
      }
    }
    return builder.Finish();
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const CastOptions& options = CastState::Get(ctx);

    auto child_type = checked_cast<const FixedSizeListType&>(*out->type()).value_type();

    const ArraySpan& in_array = batch[0].array;

    const auto& out_type = checked_cast<const FixedSizeListType&>(*out->type());
    const int32_t list_size = out_type.list_size();

    ARROW_ASSIGN_OR_RAISE(bool nulls_have_correct_length,
                          ValidateLengths(in_array, list_size));

    ArrayData* out_array = out->array_data().get();
    ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                          GetNullBitmapBuffer(in_array, ctx->memory_pool()));

    // Handle values
    std::shared_ptr<ArrayData> values = in_array.child_data[0].ToArrayData();
    ARROW_ASSIGN_OR_RAISE(Datum cast_values_datum,
                          Cast(values, child_type, options, ctx->exec_context()));

    DCHECK(cast_values_datum.is_array());
    std::shared_ptr<ArrayData> cast_values = cast_values_datum.array();

    const auto* offsets = in_array.GetValues<src_offset_type>(1);
    if (in_array.GetNullCount() > 0 && !nulls_have_correct_length) {
      // We need to fill in the null slots, so we'll use Take on the values.
      ARROW_ASSIGN_OR_RAISE(auto indices, BuildTakeIndices(in_array, list_size, ctx));
      ARROW_ASSIGN_OR_RAISE(auto take_result,
                            Take(cast_values, Datum(indices),
                                 TakeOptions::NoBoundsCheck(), ctx->exec_context()));
      DCHECK(take_result.is_array());
      cast_values = take_result.array();
    } else if (offsets[0] != 0) {
      // No nulls, but we need to slice the values
      cast_values = cast_values->Slice(offsets[0], in_array.length * list_size);
    }

    out_array->child_data.emplace_back(std::move(cast_values));

    return Status::OK();
  }
};

struct CastFixedList {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const CastOptions& options = CastState::Get(ctx);
    const auto& in_type = checked_cast<const FixedSizeListType&>(*batch[0].type());
    const auto& out_type = checked_cast<const FixedSizeListType&>(*out->type());
    auto in_size = in_type.list_size();
    auto out_size = out_type.list_size();

    if (in_size != out_size) {
      return Status::TypeError("Size of FixedSizeList is not the same.",
                               " input list: ", in_type.ToString(),
                               " output list: ", out_type.ToString());
    }

    const ArraySpan& in_array = batch[0].array;
    std::shared_ptr<ArrayData> values = in_array.child_data[0].ToArrayData();
    ArrayData* out_array = out->array_data().get();
    out_array->buffers[0] = in_array.GetBuffer(0);

    // Take care of data if input is a view.
    out_array->offset = in_array.offset;

    auto child_type = checked_cast<const FixedSizeListType&>(*out->type()).value_type();
    ARROW_ASSIGN_OR_RAISE(Datum cast_values,
                          Cast(values, child_type, options, ctx->exec_context()));
    DCHECK(cast_values.is_array());
    out_array->child_data.push_back(cast_values.array());
    return Status::OK();
  }
};

struct CastStruct {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    static constexpr int kFillNullSentinel = -2;

    const CastOptions& options = CastState::Get(ctx);
    const auto& in_type = checked_cast<const StructType&>(*batch[0].type());
    const auto& out_type = checked_cast<const StructType&>(*out->type());
    const int in_field_count = in_type.num_fields();
    const int out_field_count = out_type.num_fields();

    std::vector<int> fields_to_select(out_field_count, -1);

    std::set<std::string> all_in_field_names;
    for (int in_field_index = 0; in_field_index < in_field_count; ++in_field_index) {
      all_in_field_names.insert(in_type.field(in_field_index)->name());
    }

    for (int in_field_index = 0, out_field_index = 0;
         out_field_index < out_field_count;) {
      const auto& out_field = out_type.field(out_field_index);
      if (in_field_index < in_field_count) {
        const auto& in_field = in_type.field(in_field_index);
        // If there are more in_fields check if they match the out_field.
        if (in_field->name() == out_field->name()) {
          if (in_field->nullable() && !out_field->nullable()) {
            return Status::TypeError("cannot cast nullable field to non-nullable field: ",
                                     in_type.ToString(), " ", out_type.ToString());
          }
          // Found matching in_field and out_field.
          fields_to_select[out_field_index++] = in_field_index;
          // Using the same in_field for multiple out_fields is not allowed.
          in_field_index++;
          continue;
        }
      }
      if (all_in_field_names.count(out_field->name()) == 0 && out_field->nullable()) {
        // Didn't match current in_field, but we can fill with null.
        // Filling with null is only acceptable on nullable fields when there
        // is definitely no in_field with matching name.

        fields_to_select[out_field_index++] = kFillNullSentinel;
      } else if (in_field_index < in_field_count) {
        // Didn't match current in_field, and the we cannot fill with null, so
        // try next in_field.
        in_field_index++;
      } else {
        // Didn't match current in_field, we cannot fill with null, and there
        // are no more in_fields to try, so fail.
        return Status::TypeError(
            "struct fields don't match or are in the wrong order: Input fields: ",
            in_type.ToString(), " output fields: ", out_type.ToString());
      }
    }

    const ArraySpan& in_array = batch[0].array;
    ArrayData* out_array = out->array_data().get();

    if (in_array.buffers[0].data != nullptr) {
      ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                            CopyBitmap(ctx->memory_pool(), in_array.buffers[0].data,
                                       in_array.offset, in_array.length));
    }

    int out_field_index = 0;
    for (int field_index : fields_to_select) {
      const auto& target_type = out->type()->field(out_field_index++)->type();
      if (field_index == kFillNullSentinel) {
        ARROW_ASSIGN_OR_RAISE(auto nulls,
                              MakeArrayOfNull(target_type->GetSharedPtr(), batch.length));
        out_array->child_data.push_back(nulls->data());
      } else {
        const auto& values = (in_array.child_data[field_index].ToArrayData()->Slice(
            in_array.offset, in_array.length));
        ARROW_ASSIGN_OR_RAISE(Datum cast_values,
                              Cast(values, target_type, options, ctx->exec_context()));
        DCHECK(cast_values.is_array());
        out_array->child_data.push_back(cast_values.array());
      }
    }

    return Status::OK();
  }
};

template <typename CastFunctor, typename SrcT>
void AddTypeToTypeCast(CastFunction* func) {
  ScalarKernel kernel;
  kernel.exec = CastFunctor::Exec;
  kernel.signature = KernelSignature::Make({InputType(SrcT::type_id)}, kOutputTargetType);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(SrcT::type_id, std::move(kernel)));
}

template <typename DestType>
struct CastMap {
  using CastListImpl = CastList<MapType, DestType>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const CastOptions& options = CastState::Get(ctx);

    std::shared_ptr<DataType> entry_type =
        checked_cast<const DestType&>(*out->type()).value_type();
    // Assert is struct with two fields
    if (!(entry_type->id() == Type::STRUCT && entry_type->num_fields() == 2)) {
      return Status::TypeError(
          "Map type must be cast to a list<struct> with exactly two fields.");
    }
    std::shared_ptr<DataType> key_type = entry_type->field(0)->type();
    std::shared_ptr<DataType> value_type = entry_type->field(1)->type();

    const ArraySpan& in_array = batch[0].array;

    ArrayData* out_array = out->array_data().get();
    out_array->buffers[0] = in_array.GetBuffer(0);
    out_array->buffers[1] = in_array.GetBuffer(1);

    std::shared_ptr<ArrayData> entries = in_array.child_data[0].ToArrayData();

    // Shift bitmap in case the source offset is non-zero
    if (in_array.offset != 0 && in_array.buffers[0].data != nullptr) {
      ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                            CopyBitmap(ctx->memory_pool(), in_array.buffers[0].data,
                                       in_array.offset, in_array.length));
    }

    RETURN_NOT_OK(CastListImpl::HandleOffsets(ctx, in_array, out_array, &entries));

    // Handle keys
    const std::shared_ptr<ArrayData>& keys =
        entries->child_data[0]->Slice(entries->offset, entries->length);
    ARROW_ASSIGN_OR_RAISE(Datum cast_keys,
                          Cast(keys, key_type, options, ctx->exec_context()));
    DCHECK(cast_keys.is_array());

    // Handle values
    const std::shared_ptr<ArrayData>& values =
        entries->child_data[1]->Slice(entries->offset, entries->length);
    ARROW_ASSIGN_OR_RAISE(Datum cast_values,
                          Cast(values, value_type, options, ctx->exec_context()));
    DCHECK(cast_values.is_array());

    // Create struct array
    std::shared_ptr<ArrayData> struct_array =
        ArrayData::Make(entry_type, /*length=*/entries->length, {nullptr},
                        {cast_keys.array(), cast_values.array()}, /*null_count=*/0);
    out_array->child_data.push_back(struct_array);

    return Status::OK();
  }
};

template <typename DestType>
void AddMapCast(CastFunction* func) {
  ScalarKernel kernel;
  kernel.exec = CastMap<DestType>::Exec;
  kernel.signature =
      KernelSignature::Make({InputType(MapType::type_id)}, kOutputTargetType);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(MapType::type_id, std::move(kernel)));
}

}  // namespace

std::vector<std::shared_ptr<CastFunction>> GetNestedCasts() {
  // We use the list<T> from the CastOptions when resolving the output type

  auto cast_list = std::make_shared<CastFunction>("cast_list", Type::LIST);
  AddCommonCasts(Type::LIST, kOutputTargetType, cast_list.get());
  AddListCast<ListType, ListType>(cast_list.get());
  AddListCast<ListViewType, ListType>(cast_list.get());
  AddListCast<LargeListType, ListType>(cast_list.get());
  AddListCast<LargeListViewType, ListType>(cast_list.get());
  AddTypeToTypeCast<CastFixedToVarList<ListType>, FixedSizeListType>(cast_list.get());

  auto cast_large_list =
      std::make_shared<CastFunction>("cast_large_list", Type::LARGE_LIST);
  AddCommonCasts(Type::LARGE_LIST, kOutputTargetType, cast_large_list.get());
  AddListCast<ListType, LargeListType>(cast_large_list.get());
  AddListCast<ListViewType, LargeListType>(cast_large_list.get());
  AddListCast<LargeListType, LargeListType>(cast_large_list.get());
  AddListCast<LargeListViewType, LargeListType>(cast_large_list.get());
  AddTypeToTypeCast<CastFixedToVarList<LargeListType>, FixedSizeListType>(
      cast_large_list.get());

  auto cast_map = std::make_shared<CastFunction>("cast_map", Type::MAP);
  AddCommonCasts(Type::MAP, kOutputTargetType, cast_map.get());
  AddMapCast<MapType>(cast_map.get());
  AddMapCast<ListType>(cast_list.get());
  AddMapCast<LargeListType>(cast_large_list.get());

  // FSL is a bit incomplete at the moment
  auto cast_fsl =
      std::make_shared<CastFunction>("cast_fixed_size_list", Type::FIXED_SIZE_LIST);
  AddCommonCasts(Type::FIXED_SIZE_LIST, kOutputTargetType, cast_fsl.get());
  AddTypeToTypeCast<CastFixedList, FixedSizeListType>(cast_fsl.get());
  AddTypeToTypeCast<CastVarToFixedList<ListType>, ListType>(cast_fsl.get());
  AddTypeToTypeCast<CastVarToFixedList<ListViewType>, ListViewType>(cast_fsl.get());
  AddTypeToTypeCast<CastVarToFixedList<LargeListType>, LargeListType>(cast_fsl.get());
  AddTypeToTypeCast<CastVarToFixedList<LargeListViewType>, LargeListViewType>(
      cast_fsl.get());
  AddTypeToTypeCast<CastVarToFixedList<ListType>, MapType>(cast_fsl.get());

  // So is struct
  auto cast_struct = std::make_shared<CastFunction>("cast_struct", Type::STRUCT);
  AddCommonCasts(Type::STRUCT, kOutputTargetType, cast_struct.get());
  AddTypeToTypeCast<CastStruct, StructType>(cast_struct.get());

  // So is dictionary
  auto cast_dictionary =
      std::make_shared<CastFunction>("cast_dictionary", Type::DICTIONARY);
  AddCommonCasts(Type::DICTIONARY, kOutputTargetType, cast_dictionary.get());

  return {cast_list, cast_large_list, cast_map, cast_fsl, cast_struct, cast_dictionary};
}

}  // namespace compute::internal
}  // namespace arrow
