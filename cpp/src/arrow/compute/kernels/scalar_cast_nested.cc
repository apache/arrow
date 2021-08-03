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
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/int_util.h"

namespace arrow {

using internal::CopyBitmap;

namespace compute {
namespace internal {

namespace {

// (Large)List<T> -> (Large)List<U>

template <typename SrcType, typename DestType>
typename std::enable_if<SrcType::type_id == DestType::type_id, Status>::type
CastListOffsets(KernelContext* ctx, const ArrayData& in_array, ArrayData* out_array) {
  return Status::OK();
}

template <typename SrcType, typename DestType>
typename std::enable_if<SrcType::type_id != DestType::type_id, Status>::type
CastListOffsets(KernelContext* ctx, const ArrayData& in_array, ArrayData* out_array) {
  using src_offset_type = typename SrcType::offset_type;
  using dest_offset_type = typename DestType::offset_type;

  ARROW_ASSIGN_OR_RAISE(out_array->buffers[1],
                        ctx->Allocate(sizeof(dest_offset_type) * (in_array.length + 1)));
  ::arrow::internal::CastInts(in_array.GetValues<src_offset_type>(1),
                              out_array->GetMutableValues<dest_offset_type>(1),
                              in_array.length + 1);
  return Status::OK();
}

template <typename SrcType, typename DestType>
struct CastList {
  using src_offset_type = typename SrcType::offset_type;
  using dest_offset_type = typename DestType::offset_type;

  static constexpr bool is_upcast = sizeof(src_offset_type) < sizeof(dest_offset_type);
  static constexpr bool is_downcast = sizeof(src_offset_type) > sizeof(dest_offset_type);

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const CastOptions& options = CastState::Get(ctx);

    auto child_type = checked_cast<const DestType&>(*out->type()).value_type();

    if (out->kind() == Datum::SCALAR) {
      // The scalar case is simple, as only the underlying values must be cast
      const auto& in_scalar = checked_cast<const BaseListScalar&>(*batch[0].scalar());
      auto out_scalar = checked_cast<BaseListScalar*>(out->scalar().get());

      DCHECK(!out_scalar->is_valid);
      if (in_scalar.is_valid) {
        ARROW_ASSIGN_OR_RAISE(out_scalar->value, Cast(*in_scalar.value, child_type,
                                                      options, ctx->exec_context()));

        out_scalar->is_valid = true;
      }
      return Status::OK();
    }

    const ArrayData& in_array = *batch[0].array();
    auto offsets = in_array.GetValues<src_offset_type>(1);
    Datum values = in_array.child_data[0];

    ArrayData* out_array = out->mutable_array();
    out_array->buffers = in_array.buffers;

    // Shift bitmap in case the source offset is non-zero
    if (in_array.offset != 0 && in_array.buffers[0]) {
      ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                            CopyBitmap(ctx->memory_pool(), in_array.buffers[0]->data(),
                                       in_array.offset, in_array.length));
    }

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
      values = in_array.child_data[0]->Slice(offsets[0], offsets[in_array.length]);
    } else {
      RETURN_NOT_OK((CastListOffsets<SrcType, DestType>(ctx, in_array, out_array)));
    }

    // Handle values
    ARROW_ASSIGN_OR_RAISE(Datum cast_values,
                          Cast(values, child_type, options, ctx->exec_context()));

    DCHECK_EQ(Datum::ARRAY, cast_values.kind());
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

  // FSL is a bit incomplete at the moment
  auto cast_fsl =
      std::make_shared<CastFunction>("cast_fixed_size_list", Type::FIXED_SIZE_LIST);
  AddCommonCasts(Type::FIXED_SIZE_LIST, kOutputTargetType, cast_fsl.get());

  // So is struct
  auto cast_struct = std::make_shared<CastFunction>("cast_struct", Type::STRUCT);
  AddCommonCasts(Type::STRUCT, kOutputTargetType, cast_struct.get());

  // So is dictionary
  auto cast_dictionary =
      std::make_shared<CastFunction>("cast_dictionary", Type::DICTIONARY);
  AddCommonCasts(Type::DICTIONARY, kOutputTargetType, cast_dictionary.get());

  return {cast_list, cast_large_list, cast_fsl, cast_struct, cast_dictionary};
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
