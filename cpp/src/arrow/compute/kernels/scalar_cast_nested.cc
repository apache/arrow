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
#include <utility>
#include <vector>

#include "arrow/array/builder_nested.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/int_util.h"

namespace arrow {

using internal::CopyBitmap;

namespace compute {
namespace internal {

namespace {

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
    out_array->buffers[0] = in_array.GetBuffer(0);
    out_array->buffers[1] = in_array.GetBuffer(1);

    std::shared_ptr<ArrayData> values = in_array.child_data[0].ToArrayData();

    // Shift bitmap in case the source offset is non-zero
    if (in_array.offset != 0 && in_array.buffers[0].data != nullptr) {
      ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                            CopyBitmap(ctx->memory_pool(), in_array.buffers[0].data,
                                       in_array.offset, in_array.length));
    }

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
    const CastOptions& options = CastState::Get(ctx);
    const auto& in_type = checked_cast<const StructType&>(*batch[0].type());
    const auto& out_type = checked_cast<const StructType&>(*out->type());
    const int in_field_count = in_type.num_fields();
    const int out_field_count = out_type.num_fields();

    std::vector<int> fields_to_select(out_field_count, -1);

    int out_field_index = 0;
    for (int in_field_index = 0;
         in_field_index < in_field_count && out_field_index < out_field_count;
         ++in_field_index) {
      const auto& in_field = in_type.field(in_field_index);
      const auto& out_field = out_type.field(out_field_index);
      if (in_field->name() == out_field->name()) {
        if (in_field->nullable() && !out_field->nullable()) {
          return Status::TypeError("cannot cast nullable field to non-nullable field: ",
                                   in_type.ToString(), " ", out_type.ToString());
        }
        fields_to_select[out_field_index++] = in_field_index;
      }
    }

    if (out_field_index < out_field_count) {
      return Status::TypeError(
          "struct fields don't match or are in the wrong order: Input fields: ",
          in_type.ToString(), " output fields: ", out_type.ToString());
    }

    const ArraySpan& in_array = batch[0].array;
    ArrayData* out_array = out->array_data().get();

    if (in_array.buffers[0].data != nullptr) {
      ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                            CopyBitmap(ctx->memory_pool(), in_array.buffers[0].data,
                                       in_array.offset, in_array.length));
    }

    out_field_index = 0;
    for (int field_index : fields_to_select) {
      const auto& values = (in_array.child_data[field_index].ToArrayData()->Slice(
          in_array.offset, in_array.length));
      const auto& target_type = out->type()->field(out_field_index++)->type();

      ARROW_ASSIGN_OR_RAISE(Datum cast_values,
                            Cast(values, target_type, options, ctx->exec_context()));

      DCHECK(cast_values.is_array());
      out_array->child_data.push_back(cast_values.array());
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
  DCHECK_OK(func->AddKernel(StructType::type_id, std::move(kernel)));
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
  AddListCast<LargeListType, ListType>(cast_list.get());

  auto cast_large_list =
      std::make_shared<CastFunction>("cast_large_list", Type::LARGE_LIST);
  AddCommonCasts(Type::LARGE_LIST, kOutputTargetType, cast_large_list.get());
  AddListCast<ListType, LargeListType>(cast_large_list.get());
  AddListCast<LargeListType, LargeListType>(cast_large_list.get());

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

}  // namespace internal
}  // namespace compute
}  // namespace arrow
