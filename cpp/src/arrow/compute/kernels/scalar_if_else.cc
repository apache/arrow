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

#include <arrow/compute/api.h>
#include <arrow/util/bit_block_counter.h>
#include <arrow/util/bitmap_ops.h>

#include "codegen_internal.h"

namespace arrow {
using internal::BitBlockCount;
using internal::BitBlockCounter;

namespace compute {

namespace {

// nulls will be promoted as follows
// cond.val && (cond.data && left.val || ~cond.data && right.val)
Status promote_nulls(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* output) {
  if (!cond.MayHaveNulls() && !left.MayHaveNulls() && !right.MayHaveNulls()) {
    return Status::OK();  // no nulls to handle
  }
  const int64_t len = cond.length;

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_validity, ctx->AllocateBitmap(len));
  arrow::internal::InvertBitmap(out_validity->data(), 0, len,
                                out_validity->mutable_data(), 0);
  if (right.MayHaveNulls()) {
    // out_validity = right.val && ~cond.data
    arrow::internal::BitmapAndNot(right.buffers[0]->data(), right.offset,
                                  cond.buffers[1]->data(), cond.offset, len, 0,
                                  out_validity->mutable_data());
  }

  if (left.MayHaveNulls()) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> temp_buf, ctx->AllocateBitmap(len));
    // tmp_buf = left.val && cond.data
    arrow::internal::BitmapAnd(left.buffers[0]->data(), left.offset,
                               cond.buffers[1]->data(), cond.offset, len, 0,
                               temp_buf->mutable_data());
    // out_validity = cond.data && left.val || ~cond.data && right.val
    arrow::internal::BitmapOr(out_validity->data(), 0, temp_buf->data(), 0, len, 0,
                              out_validity->mutable_data());
  }

  if (cond.MayHaveNulls()) {
    // out_validity &= cond.val
    ::arrow::internal::BitmapAnd(out_validity->data(), 0, cond.buffers[0]->data(),
                                 cond.offset, len, 0, out_validity->mutable_data());
  }

  output->buffers[0] = std::move(out_validity);
  output->GetNullCount();  // update null count
  return Status::OK();
}

template <typename Type, bool swap = false, typename Enable = void>
struct IfElseFunctor {};

template <typename Type, bool swap>
struct IfElseFunctor<Type, swap, enable_if_t<is_number_type<Type>::value>> {
  using T = typename TypeTraits<Type>::CType;

  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(promote_nulls(ctx, cond, left, right, out));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                          arrow::internal::CopyBitmap(ctx->memory_pool(), )

                              ctx->Allocate(cond.length * sizeof(T)));
    T* out_values = reinterpret_cast<T*>(out_buf->mutable_data());

    // copy right data to out_buff
    const T* right_data = right.GetValues<T>(1);
    std::memcpy(out_values, right_data, right.length * sizeof(T));

    const auto* cond_data = cond.buffers[1]->data();  // this is a BoolArray
    BitBlockCounter bit_counter(cond_data, cond.offset, cond.length);

    // selectively copy values from left data
    const T* left_data = left.GetValues<T>(1);
    int64_t offset = cond.offset;

    // todo this can be improved by intrinsics. ex: _mm*_mask_store_e* (vmovdqa*)
    while (offset < cond.offset + cond.length) {
      const BitBlockCount& block = bit_counter.NextWord();
      if (block.AllSet()) {  // all from left
        std::memcpy(out_values, left_data, block.length * sizeof(T));
      } else if (block.popcount) {  // selectively copy from left
        for (int64_t i = 0; i < block.length; ++i) {
          if (BitUtil::GetBit(cond_data, offset + i)) {
            out_values[i] = left_data[i];
          }
        }
      }

      offset += block.length;
      out_values += block.length;
      left_data += block.length;
    }

    out->buffers[1] = std::move(out_buf);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    // todo impl
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const Scalar& cond, const Scalar& left,
                     const Scalar& right, Scalar* out) {
    // todo impl
    return Status::OK();
  }
};

template <typename Type, bool swap>
struct IfElseFunctor<Type, swap, enable_if_t<is_boolean_type<Type>::value>> {
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(promote_nulls(ctx, cond, left, right, out));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                          ctx->AllocateBitmap(cond.length));
    uint8_t* out_values = out_buf->mutable_data();

    // copy right data to out_buff
    const T* right_data = right.GetValues<T>(1);
    std::memcpy(out_values, right_data, right.length * sizeof(T));

    const auto* cond_data = cond.buffers[1]->data();  // this is a BoolArray
    BitBlockCounter bit_counter(cond_data, cond.offset, cond.length);

    // selectively copy values from left data
    const T* left_data = left.GetValues<T>(1);
    int64_t offset = cond.offset;

    // todo this can be improved by intrinsics. ex: _mm*_mask_store_e* (vmovdqa*)
    while (offset < cond.offset + cond.length) {
      const BitBlockCount& block = bit_counter.NextWord();
      if (block.AllSet()) {  // all from left
        std::memcpy(out_values, left_data, block.length * sizeof(T));
      } else if (block.popcount) {  // selectively copy from left
        for (int64_t i = 0; i < block.length; ++i) {
          if (BitUtil::GetBit(cond_data, offset + i)) {
            out_values[i] = left_data[i];
          }
        }
      }

      offset += block.length;
      out_values += block.length;
      left_data += block.length;
    }

    out->buffers[1] = std::move(out_buf);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    // todo impl
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const Scalar& cond, const Scalar& left,
                     const Scalar& right, Scalar* out) {
    // todo impl
    return Status::OK();
  }
};

template <typename Type, bool swap>
struct IfElseFunctor<Type, swap, enable_if_t<is_boolean_type<Type>::value>> {
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const Scalar& cond, const Scalar& left,
                     const Scalar& right, Scalar* out) {
    return Status::OK();
  }
};

template <typename Type, bool swap>
struct IfElseFunctor<Type, swap, enable_if_t<is_null_type<Type>::value>> {
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const Scalar& cond, const Scalar& left,
                     const Scalar& right, Scalar* out) {
    return Status::OK();
  }
};

template <typename Type>
struct ResolveExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch.length == 0) return Status::OK();

    if (batch[0].kind() == Datum::ARRAY) {
      if (batch[1].kind() == Datum::ARRAY) {
        if (batch[2].kind() == Datum::ARRAY) {  // AAA
          return IfElseFunctor<Type>::Call(ctx, *batch[0].array(), *batch[1].array(),
                                           *batch[2].array(), out->mutable_array());
        } else {  // AAS
          return IfElseFunctor<Type>::Call(ctx, *batch[0].array(), *batch[1].array(),
                                           *batch[2].scalar(), out->mutable_array());
        }
      } else {
        return Status::Invalid("");
        //        if (batch[2].kind() == Datum::ARRAY) {  // ASA
        //          return IfElseFunctor<Type, true>::Call(ctx, *batch[0].array(),
        //                                                 *batch[2].array(),
        //                                                 *batch[1].scalar(),
        //                                                 out->mutable_array());
        //        } else {  // ASS
        //          return IfElseFunctor<Type>::Call(ctx, *batch[0].array(),
        //          *batch[1].scalar(),
        //                                           *batch[2].scalar(),
        //                                           out->mutable_array());
        //        }
      }
    } else {
      if (batch[1].kind() == Datum::ARRAY) {
        return Status::Invalid("");
        //        if (batch[2].kind() == Datum::ARRAY) {  // SAA
        //          return IfElseFunctor<Type>::Call(ctx, *batch[0].scalar(),
        //          *batch[1].array(),
        //                                           *batch[2].array(),
        //                                           out->mutable_array());
        //        } else {  // SAS
        //          return IfElseFunctor<Type>::Call(ctx, *batch[0].scalar(),
        //          *batch[1].array(),
        //                                           *batch[2].scalar(),
        //                                           out->mutable_array());
        //        }
      } else {
        if (batch[2].kind() == Datum::ARRAY) {  // SSA
          return Status::Invalid("");
          //          return IfElseFunctor<Type>::Call(ctx, *batch[0].scalar(),
          //          *batch[1].scalar(),
          //                                           *batch[2].array(),
          //                                           out->mutable_array());
        } else {  // SSS
          return IfElseFunctor<Type>::Call(ctx, *batch[0].scalar(), *batch[1].scalar(),
                                           *batch[2].scalar(), out->scalar().get());
        }
      }
    }
  }
};

void AddPrimitiveKernels(const std::shared_ptr<ScalarFunction>& scalar_function,
                         const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = internal::GenerateTypeAgnosticPrimitive<ResolveExec>(*type);
    ScalarKernel kernel({boolean(), type, type}, type, exec);
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;

    DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
  }
}

}  // namespace

const FunctionDoc if_else_doc{"<fill this>", ("`<fill this>"), {"cond", "left", "right"}};

namespace internal {

void RegisterScalarIfElse(FunctionRegistry* registry) {
  ScalarKernel scalar_kernel;
  scalar_kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  scalar_kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;

  auto func = std::make_shared<ScalarFunction>("if_else", Arity::Ternary(), &if_else_doc);

  AddPrimitiveKernels(func, NumericTypes());
  AddPrimitiveKernels(func, TemporalTypes());
  // todo add temporal, boolean, null and binary kernels

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow