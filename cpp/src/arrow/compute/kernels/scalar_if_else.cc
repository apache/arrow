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

#include "arrow/util/bitmap.h"
#include "codegen_internal.h"

namespace arrow {
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::Bitmap;

namespace compute {

namespace {

// cond.valid && (cond.data && left.valid || ~cond.data && right.valid)
enum IEBitmapIndex { C_VALID, C_DATA, L_VALID, R_VALID };

Status PromoteNullsNew1(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                        const ArrayData& right, ArrayData* output) {
  uint8_t flag =
      !right.MayHaveNulls() * 4 + !left.MayHaveNulls() * 2 + !cond.MayHaveNulls();

  Bitmap bitmaps[4];
  bitmaps[C_VALID] = {cond.buffers[0], cond.offset, cond.length};
  bitmaps[C_DATA] = {cond.buffers[1], cond.offset, cond.length};
  bitmaps[L_VALID] = {left.buffers[0], left.offset, left.length};
  bitmaps[R_VALID] = {right.buffers[0], right.offset, right.length};

  uint64_t* out_validity = nullptr;
  if (flag < 6) {
    // there will be a validity buffer in the output
    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(cond.length));
    out_validity = output->GetMutableValues<uint64_t>(0);
  }

  // cond.valid && (cond.data && left.valid || ~cond.data && right.valid)
  int64_t i = 0;
  switch (flag) {
    case 7:  // RLC = 111
      break;
    case 6:  // RLC = 110
      output->buffers[0] = cond.buffers[0];
      break;
    case 5:  // RLC = 101
      Bitmap::VisitWords({bitmaps[C_DATA], bitmaps[L_VALID]},
                         [&](std::array<uint64_t, 2> words) {
                           out_validity[i] = (words[0] & words[1]) | ~words[0];
                           i++;
                         });
      break;
    case 4:  // RLC = 100
      Bitmap::VisitWords({bitmaps[C_VALID], bitmaps[C_DATA], bitmaps[L_VALID]},
                         [&](std::array<uint64_t, 3> words) {
                           out_validity[i] =
                               words[0] & ((words[1] & words[2]) | ~words[1]);
                           i++;
                         });
      break;
    case 3:  // RLC = 011
      Bitmap::VisitWords({bitmaps[C_DATA], bitmaps[R_VALID]},
                         [&](std::array<uint64_t, 2> words) {
                           out_validity[i] = words[0] | (~words[0] & words[1]);
                           i++;
                         });
      break;
    case 2:  // RLC = 010
      Bitmap::VisitWords({bitmaps[C_VALID], bitmaps[C_DATA], bitmaps[R_VALID]},
                         [&](std::array<uint64_t, 3> words) {
                           out_validity[i] =
                               words[0] & (words[1] | (~words[1] & words[2]));
                           i++;
                         });
      break;
    case 1:  // RLC = 001
      Bitmap::VisitWords({bitmaps[C_DATA], bitmaps[L_VALID], bitmaps[R_VALID]},
                         [&](std::array<uint64_t, 3> words) {
                           out_validity[i] =
                               (words[0] & words[1]) | (~words[0] & words[2]);
                           i++;
                         });
      break;
    case 0:  // RLC = 000
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        out_validity[i] = words[0] & ((words[1] & words[2]) | (~words[1] & words[3]));
        i++;
      });
      break;
  }
  return Status::OK();
}

/*Status PromoteNullsNew(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                       const ArrayData& right, ArrayData* output) {
  if (!cond.MayHaveNulls() && !left.MayHaveNulls() && !right.MayHaveNulls()) {
    return Status::OK();  // no nulls to handle
  }

  // there will be a validity buffer in the output
  ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(cond.length));
  auto out_validity = output->GetMutableValues<uint64_t>(0);

  Bitmap bitmaps[4];
  bitmaps[C_VALID] = {cond.buffers[0], cond.offset, cond.length};
  bitmaps[C_DATA] = {cond.buffers[1], cond.offset, cond.length};
  bitmaps[L_VALID] = {left.buffers[0], left.offset, left.length};
  bitmaps[R_VALID] = {right.buffers[0], right.offset, right.length};

  uint8_t flag =
      (cond.null_count == 0) * 4 + (left.null_count == 0) * 2 + (right.null_count == 0);

  int64_t i = 0;
  switch (flag) {
    case 0:  // all have nulls
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        out_validity[i] = words[0] & ((words[1] & words[2]) | (~words[1] & words[3]));
        i++;
      });
      break;
    case 1:  // right all valid
      Bitmap::VisitWords({bitmaps[C_VALID], bitmaps[C_DATA], bitmaps[L_VALID]},
                         [&](std::array<uint64_t, 3> words) {
                           out_validity[i] =
                               words[0] & ((words[1] & words[2]) | ~words[1]);
                           i++;
                         });
      break;
    case 2:  // left all valid
      Bitmap::VisitWords({bitmaps[C_VALID], bitmaps[C_DATA], bitmaps[R_VALID]},
                         [&](std::array<uint64_t, 3> words) {
                           out_validity[i] =
                               words[0] & (words[1] | (~words[1] & words[2]));
                           i++;
                         });
      break;
    case 3:  // left, right all valid
      *ou break;

    case 7:  // all valid. nothing to do
      return Status::OK();
  }

  if (cond.null_count == 0) {
  }

  if (right.null_count == 0) {
    Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
      apply(words[C_VALID], words[C_DATA], words[L_VALID], ~uint64_t(0));
    });
    return Status::OK();
  }

  DCHECK(left.null_count != 0 && right.null_count != 0);
  Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
    apply(words[C_VALID], words[C_DATA], words[L_VALID], words[R_VALID]);
  });

  return Status::OK();
}*/

// nulls will be promoted as follows:
// cond.valid && (cond.data && left.valid || ~cond.data && right.valid)
// Note: we have to work on ArrayData. Otherwise we won't be able to handle array
// offsets AAA
/*Status PromoteNulls(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                    const ArrayData& right, ArrayData* output) {
  if (!cond.MayHaveNulls() && !left.MayHaveNulls() && !right.MayHaveNulls()) {
    return Status::OK();  // no nulls to handle
  }
  const int64_t len = cond.length;

  // out_validity = ~cond.data --> mask right values
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<Buffer> out_validity,
      arrow::internal::InvertBitmap(ctx->memory_pool(), cond.buffers[1]->data(),
                                    cond.offset, len));

  if (right.MayHaveNulls()) {  // out_validity = right.valid && ~cond.data
    arrow::internal::BitmapAnd(right.buffers[0]->data(), right.offset,
                               out_validity->data(), 0, len, 0,
                               out_validity->mutable_data());
  }

  std::shared_ptr<Buffer> tmp_buf;
  if (left.MayHaveNulls()) {
    // tmp_buf = left.valid && cond.data
    ARROW_ASSIGN_OR_RAISE(
        tmp_buf, arrow::internal::BitmapAnd(ctx->memory_pool(), left.buffers[0]->data(),
                                            left.offset, cond.buffers[1]->data(),
                                            cond.offset, len, 0));
  } else {  // if left all valid --> tmp_buf = cond.data (zero copy slice)
    tmp_buf = SliceBuffer(cond.buffers[1], cond.offset, cond.length);
  }

  // out_validity = cond.data && left.valid || ~cond.data && right.valid
  arrow::internal::BitmapOr(out_validity->data(), 0, tmp_buf->data(), 0, len, 0,
                            out_validity->mutable_data());

  if (cond.MayHaveNulls()) {
    // out_validity = cond.valid && (cond.data && left.valid || ~cond.data && right.valid)
    ::arrow::internal::BitmapAnd(out_validity->data(), 0, cond.buffers[0]->data(),
                                 cond.offset, len, 0, out_validity->mutable_data());
  }

  output->buffers[0] = std::move(out_validity);
  output->GetNullCount();  // update null count
  return Status::OK();
}*/

// cond.valid && (cond.data && left.valid || ~cond.data && right.valid)
// ASA and AAS
Status PromoteNulls(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                    const ArrayData& right, ArrayData* output) {
  if (!cond.MayHaveNulls() && left.is_valid && !right.MayHaveNulls()) {
    return Status::OK();  // no nulls to handle
  }
  const int64_t len = cond.length;

  // out_validity = ~cond.data
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<Buffer> out_validity,
      arrow::internal::InvertBitmap(ctx->memory_pool(), cond.buffers[1]->data(),
                                    cond.offset, len));
  // out_validity = ~cond.data && right.valid
  if (right.MayHaveNulls()) {  // out_validity = right.valid && ~cond.data
    arrow::internal::BitmapAnd(right.buffers[0]->data(), right.offset,
                               out_validity->data(), 0, len, 0,
                               out_validity->mutable_data());
  }

  // out_validity = cond.data && left.valid || ~cond.data && right.valid
  if (left.is_valid) {
    arrow::internal::BitmapOr(out_validity->data(), 0, cond.buffers[1]->data(),
                              cond.offset, len, 0, out_validity->mutable_data());
  }

  // out_validity = cond.valid && (cond.data && left.valid || ~cond.data && right.valid)
  if (cond.MayHaveNulls()) {
    ::arrow::internal::BitmapAnd(out_validity->data(), 0, cond.buffers[0]->data(),
                                 cond.offset, len, 0, out_validity->mutable_data());
  }

  output->buffers[0] = std::move(out_validity);
  output->GetNullCount();  // update null count
  return Status::OK();
}
/*
// cond.valid && (cond.data && left.valid || ~cond.data && right.valid)
// ASS
Status PromoteNulls(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                    const Scalar& right, ArrayData* output) {
  if (!cond.MayHaveNulls() && left.is_valid && right.is_valid) {
    return Status::OK();  // no nulls to handle
  }
  const int64_t len = cond.length;

  std::shared_ptr<Buffer> out_validity;
  if (right.is_valid) {
    // out_validity = ~cond.data
    ARROW_ASSIGN_OR_RAISE(
        out_validity, arrow::internal::InvertBitmap(
                          ctx->memory_pool(), cond.buffers[1]->data(), cond.offset, len));
  } else {
    // out_validity = [0...]
    ARROW_ASSIGN_OR_RAISE(out_validity, ctx->AllocateBitmap(len));
  }

  // out_validity = cond.data && left.valid || ~cond.data && right.valid
  if (left.is_valid) {
    arrow::internal::BitmapOr(out_validity->data(), 0, cond.buffers[1]->data(),
                              cond.offset, len, 0, out_validity->mutable_data());
  }

  // out_validity = cond.valid && (cond.data && left.valid || ~cond.data && right.valid)
  if (cond.MayHaveNulls()) {
    ::arrow::internal::BitmapAnd(out_validity->data(), 0, cond.buffers[0]->data(),
                                 cond.offset, len, 0, out_validity->mutable_data());
  }

  output->buffers[0] = std::move(out_validity);
  output->GetNullCount();  // update null count
  return Status::OK();
}*/

// todo: this could be dangerous because the inverted arraydata buffer[1] may not be
//  available outside Exec's scope
Status InvertBoolArrayData(KernelContext* ctx, const ArrayData& input,
                           ArrayData* output) {
  // null buffer
  if (input.MayHaveNulls()) {
    output->buffers.emplace_back(
        SliceBuffer(input.buffers[0], input.offset, input.length));
  } else {
    output->buffers.push_back(NULLPTR);
  }

  // data buffer
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<Buffer> inv_data,
      arrow::internal::InvertBitmap(ctx->memory_pool(), input.buffers[1]->data(),
                                    input.offset, input.length));
  output->buffers.emplace_back(std::move(inv_data));
  return Status::OK();
}

template <typename Type, typename Enable = void>
struct IfElseFunctor {};

template <typename Type>
struct IfElseFunctor<
    Type, enable_if_t<is_number_type<Type>::value | is_temporal_type<Type>::value>> {
  using T = typename TypeTraits<Type>::CType;

  //  AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsNew1(ctx, cond, left, right, out));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
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

  // ASA and AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNulls(ctx, cond, left, right, out));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                          ctx->Allocate(cond.length * sizeof(T)));
    T* out_values = reinterpret_cast<T*>(out_buf->mutable_data());

    // copy right data to out_buff
    const T* right_data = right.GetValues<T>(1);
    std::memcpy(out_values, right_data, right.length * sizeof(T));

    const auto* cond_data = cond.buffers[1]->data();  // this is a BoolArray
    BitBlockCounter bit_counter(cond_data, cond.offset, cond.length);

    // selectively copy values from left data
    T left_data = internal::UnboxScalar<Type>::Unbox(left);
    int64_t offset = cond.offset;

    // todo this can be improved by intrinsics. ex: _mm*_mask_store_e* (vmovdqa*)
    while (offset < cond.offset + cond.length) {
      const BitBlockCount& block = bit_counter.NextWord();
      if (block.AllSet()) {  // all from left
        std::fill(out_values, out_values + block.length, left_data);
      } else if (block.popcount) {  // selectively copy from left
        for (int64_t i = 0; i < block.length; ++i) {
          if (BitUtil::GetBit(cond_data, offset + i)) {
            out_values[i] = left_data;
          }
        }
      }

      offset += block.length;
      out_values += block.length;
    }

    out->buffers[1] = std::move(out_buf);
    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    /*    ARROW_RETURN_NOT_OK(PromoteNulls(ctx, cond, left, right, out));

        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                              ctx->Allocate(cond.length * sizeof(T)));
        T* out_values = reinterpret_cast<T*>(out_buf->mutable_data());

        // copy right data to out_buff
        const T* right_data = right.GetValues<T>(1);
        std::memcpy(out_values, right_data, right.length * sizeof(T));

        const auto* cond_data = cond.buffers[1]->data();  // this is a BoolArray
        BitBlockCounter bit_counter(cond_data, cond.offset, cond.length);

        // selectively copy values from left data
        T left_data = internal::UnboxScalar<Type>::Unbox(left);
        int64_t offset = cond.offset;

        // todo this can be improved by intrinsics. ex: _mm*_mask_store_e* (vmovdqa*)
        while (offset < cond.offset + cond.length) {
          const BitBlockCount& block = bit_counter.NextWord();
          if (block.AllSet()) {  // all from left
            std::fill(out_values, out_values + block.length, left_data);
          } else if (block.popcount) {  // selectively copy from left
            for (int64_t i = 0; i < block.length; ++i) {
              if (BitUtil::GetBit(cond_data, offset + i)) {
                out_values[i] = left_data;
              }
            }
          }

          offset += block.length;
          out_values += block.length;
        }

        out->buffers[1] = std::move(out_buf);*/
    return Status::OK();
  }
};

template <typename Type>
struct IfElseFunctor<Type, enable_if_boolean<Type>> {
  // AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsNew1(ctx, cond, left, right, out));

    // out_buff = right & ~cond
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                          arrow::internal::BitmapAndNot(
                              ctx->memory_pool(), right.buffers[1]->data(), right.offset,
                              cond.buffers[1]->data(), cond.offset, cond.length, 0));

    // out_buff = left & cond
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> temp_buf,
                          arrow::internal::BitmapAnd(
                              ctx->memory_pool(), left.buffers[1]->data(), left.offset,
                              cond.buffers[1]->data(), cond.offset, cond.length, 0));

    arrow::internal::BitmapOr(out_buf->data(), 0, temp_buf->data(), 0, cond.length, 0,
                              out_buf->mutable_data());
    out->buffers[1] = std::move(out_buf);
    return Status::OK();
  }

  // ASA and AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNulls(ctx, cond, left, right, out));

    // out_buff = right & ~cond
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                          arrow::internal::BitmapAndNot(
                              ctx->memory_pool(), right.buffers[1]->data(), right.offset,
                              cond.buffers[1]->data(), cond.offset, cond.length, 0));

    // out_buff = left & cond
    bool left_data = internal::UnboxScalar<BooleanType>::Unbox(left);
    if (left_data) {
      arrow::internal::BitmapOr(out_buf->data(), 0, cond.buffers[1]->data(), cond.offset,
                                cond.length, 0, out_buf->mutable_data());
    }

    out->buffers[1] = std::move(out_buf);
    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    // todo impl
    return Status::OK();
  }
};

template <typename Type>
struct IfElseFunctor<Type, enable_if_null<Type>> {
  template <typename T>
  static inline Status ReturnCopy(const T& in, T* out) {
    // Nothing preallocated, so we assign in into the output
    *out = in;
    return Status::OK();
  }

  // AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    return ReturnCopy(left, out);
  }

  // ASA and AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    return ReturnCopy(right, out);
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    return ReturnCopy(cond, out);
  }
};

template <typename Type>
struct ResolveIfElseExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // cond is scalar
    if (batch[0].is_scalar()) {
      const auto& cond = batch[0].scalar_as<BooleanScalar>();

      if (batch[1].is_scalar() && batch[2].is_scalar()) {
        if (cond.is_valid) {
          *out = cond.value ? batch[1].scalar() : batch[2].scalar();
        } else {
          *out = MakeNullScalar(batch[1].type());
        }
      } else {  // either left or right is an array. output is always an array
        int64_t bcast_size = std::max(batch[1].length(), batch[2].length());
        if (cond.is_valid) {
          const auto& valid_data = cond.value ? batch[1] : batch[2];
          if (valid_data.is_array()) {
            *out = valid_data;
          } else {  // valid data is a scalar that needs to be broadcasted
            ARROW_ASSIGN_OR_RAISE(*out,
                                  MakeArrayFromScalar(*valid_data.scalar(), bcast_size,
                                                      ctx->memory_pool()));
          }
        } else {  // cond is null. create null array
          ARROW_ASSIGN_OR_RAISE(
              *out, MakeArrayOfNull(batch[1].type(), bcast_size, ctx->memory_pool()))
        }
      }
      return Status::OK();
    }

    // cond is array. Use functors to sort things out
    if (batch[1].kind() == Datum::ARRAY) {
      if (batch[2].kind() == Datum::ARRAY) {  // AAA
        return IfElseFunctor<Type>::Call(ctx, *batch[0].array(), *batch[1].array(),
                                         *batch[2].array(), out->mutable_array());
      } else {  // AAS
        ArrayData inv_cond;
        RETURN_NOT_OK(InvertBoolArrayData(ctx, *batch[0].array(), &inv_cond));
        return IfElseFunctor<Type>::Call(ctx, inv_cond, *batch[2].scalar(),
                                         *batch[1].array(), out->mutable_array());
      }
    } else {
      if (batch[2].kind() == Datum::ARRAY) {  // ASA
        return IfElseFunctor<Type>::Call(ctx, *batch[0].array(), *batch[1].scalar(),
                                         *batch[2].array(), out->mutable_array());
      } else {  // ASS
        return IfElseFunctor<Type>::Call(ctx, *batch[0].array(), *batch[1].scalar(),
                                         *batch[2].scalar(), out->mutable_array());
      }
    }
  }
};

void AddPrimitiveIfElseKernels(const std::shared_ptr<ScalarFunction>& scalar_function,
                               const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = internal::GenerateTypeAgnosticPrimitive<ResolveIfElseExec>(*type);
    // cond array needs to be boolean always
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

  AddPrimitiveIfElseKernels(func, NumericTypes());
  AddPrimitiveIfElseKernels(func, TemporalTypes());
  AddPrimitiveIfElseKernels(func, {boolean(), null()});
  // todo add binary kernels

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow