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
#include <arrow/compute/kernels/codegen_internal.h>
#include <arrow/compute/util_internal.h>
#include <arrow/util/bit_block_counter.h>
#include <arrow/util/bitmap.h>
#include <arrow/util/bitmap_ops.h>

namespace arrow {
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::Bitmap;

namespace compute {

namespace {

enum { COND_ALL_VALID = 1, LEFT_ALL_VALID = 2, RIGHT_ALL_VALID = 4 };

// if the condition is null then output is null otherwise we take validity from the
// selected argument
// ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
Status PromoteNullsVisitor(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                           const Scalar& right, ArrayData* output) {
  uint8_t flag = right.is_valid * 4 + left.is_valid * 2 + !cond.MayHaveNulls();

  if (flag < 6 && flag != 3) {
    // there will be a validity buffer in the output
    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(cond.length));
  }

  // if the condition is null then output is null otherwise we take validity from the
  // selected argument
  // ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
  switch (flag) {
    case COND_ALL_VALID | LEFT_ALL_VALID | RIGHT_ALL_VALID:  // = 7
      break;
    case LEFT_ALL_VALID | RIGHT_ALL_VALID:  // = 6
      // out_valid = c_valid
      output->buffers[0] = SliceBuffer(cond.buffers[0], cond.offset, cond.length);
      break;
    case COND_ALL_VALID | RIGHT_ALL_VALID:  // = 5
      // out_valid = ~cond.data
      arrow::internal::InvertBitmap(cond.buffers[1]->data(), cond.offset, cond.length,
                                    output->buffers[0]->mutable_data(), 0);
      break;
    case RIGHT_ALL_VALID:  // = 4
      // out_valid = c_valid & ~cond.data
      arrow::internal::BitmapAndNot(cond.buffers[0]->data(), cond.offset,
                                    cond.buffers[1]->data(), cond.offset, cond.length, 0,
                                    output->buffers[0]->mutable_data());
      break;
    case COND_ALL_VALID | LEFT_ALL_VALID:  // = 3
      // out_valid = cond.data
      output->buffers[0] = SliceBuffer(cond.buffers[1], cond.offset, cond.length);
      break;
    case LEFT_ALL_VALID:  // = 2
      // out_valid = cond.valid & cond.data
      arrow::internal::BitmapAnd(cond.buffers[0]->data(), cond.offset,
                                 cond.buffers[1]->data(), cond.offset, cond.length, 0,
                                 output->buffers[0]->mutable_data());
      break;
    case COND_ALL_VALID:  // = 1
      // out_valid = 0 --> nothing to do; but requires out_valid to be a all-zero buffer
      break;
    case 0:
      // out_valid = 0 --> nothing to do; but requires out_valid to be a all-zero buffer
      break;
  }
  return Status::OK();
}

Status PromoteNullsVisitor(KernelContext* ctx, const ArrayData& cond,
                           const ArrayData& left, const Scalar& right,
                           ArrayData* output) {
  uint8_t flag = right.is_valid * 4 + !left.MayHaveNulls() * 2 + !cond.MayHaveNulls();

  enum { C_VALID, C_DATA, L_VALID };

  Bitmap bitmaps[3];
  bitmaps[C_VALID] = {cond.buffers[0], cond.offset, cond.length};
  bitmaps[C_DATA] = {cond.buffers[1], cond.offset, cond.length};
  bitmaps[L_VALID] = {left.buffers[0], left.offset, left.length};

  uint64_t* out_validity = nullptr;
  if (flag < 6 && flag != 3) {
    // there will be a validity buffer in the output
    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(cond.length));
    out_validity = output->GetMutableValues<uint64_t>(0);
  }

  // lambda function that will be used inside the visitor
  int64_t i = 0;
  auto apply = [&](uint64_t c_valid, uint64_t c_data, uint64_t l_valid,
                   uint64_t r_valid) {
    out_validity[i] = c_valid & ((c_data & l_valid) | (~c_data & r_valid));
    i++;
  };

  // if the condition is null then output is null otherwise we take validity from the
  // selected argument
  // ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
  switch (flag) {
    case COND_ALL_VALID | LEFT_ALL_VALID | RIGHT_ALL_VALID:
      break;
    case LEFT_ALL_VALID | RIGHT_ALL_VALID:
      output->buffers[0] = SliceBuffer(cond.buffers[0], cond.offset, cond.length);
      break;
    case COND_ALL_VALID | RIGHT_ALL_VALID:
      // bitmaps[C_VALID] might be null; override to make it safe for Visit()
      bitmaps[C_VALID] = bitmaps[C_DATA];
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
        apply(UINT64_MAX, words[C_DATA], words[L_VALID], UINT64_MAX);
      });
      break;
    case RIGHT_ALL_VALID:
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
        apply(words[C_VALID], words[C_DATA], words[L_VALID], UINT64_MAX);
      });
      break;
    case COND_ALL_VALID | LEFT_ALL_VALID:
      // only cond.data is passed
      output->buffers[0] = SliceBuffer(cond.buffers[1], cond.offset, cond.length);
      break;
    case LEFT_ALL_VALID:
      // out_valid = cond.valid & cond.data
      arrow::internal::BitmapAnd(cond.buffers[0]->data(), cond.offset,
                                 cond.buffers[1]->data(), cond.offset, cond.length, 0,
                                 output->buffers[0]->mutable_data());
      break;
    case COND_ALL_VALID:
      // out_valid = cond.data & left.valid
      arrow::internal::BitmapAnd(cond.buffers[1]->data(), cond.offset,
                                 left.buffers[0]->data(), left.offset, cond.length, 0,
                                 output->buffers[0]->mutable_data());
      break;
    case 0:
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
        apply(words[C_VALID], words[C_DATA], words[L_VALID], 0);
      });
      break;
  }
  return Status::OK();
}

// if the condition is null then output is null otherwise we take validity from the
// selected argument
// ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
Status PromoteNullsVisitor(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                           const ArrayData& right, ArrayData* output) {
  uint8_t flag = !right.MayHaveNulls() * 4 + left.is_valid * 2 + !cond.MayHaveNulls();

  enum { C_VALID, C_DATA, R_VALID };

  Bitmap bitmaps[3];
  bitmaps[C_VALID] = {cond.buffers[0], cond.offset, cond.length};
  bitmaps[C_DATA] = {cond.buffers[1], cond.offset, cond.length};
  bitmaps[R_VALID] = {right.buffers[0], right.offset, right.length};

  uint64_t* out_validity = nullptr;
  if (flag < 6) {
    // there will be a validity buffer in the output
    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(cond.length));
    out_validity = output->GetMutableValues<uint64_t>(0);
  }

  // lambda function that will be used inside the visitor
  int64_t i = 0;
  auto apply = [&](uint64_t c_valid, uint64_t c_data, uint64_t l_valid,
                   uint64_t r_valid) {
    out_validity[i] = c_valid & ((c_data & l_valid) | (~c_data & r_valid));
    i++;
  };

  // if the condition is null then output is null otherwise we take validity from the
  // selected argument
  // ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
  switch (flag) {
    case COND_ALL_VALID | LEFT_ALL_VALID | RIGHT_ALL_VALID:
      break;
    case LEFT_ALL_VALID | RIGHT_ALL_VALID:
      output->buffers[0] = SliceBuffer(cond.buffers[0], cond.offset, cond.length);
      break;
    case COND_ALL_VALID | RIGHT_ALL_VALID:
      // out_valid = ~cond.data
      arrow::internal::InvertBitmap(cond.buffers[1]->data(), cond.offset, cond.length,
                                    output->buffers[0]->mutable_data(), 0);
      break;
    case RIGHT_ALL_VALID:
      // out_valid = c_valid & ~cond.data
      arrow::internal::BitmapAndNot(cond.buffers[0]->data(), cond.offset,
                                    cond.buffers[1]->data(), cond.offset, cond.length, 0,
                                    output->buffers[0]->mutable_data());
      break;
    case COND_ALL_VALID | LEFT_ALL_VALID:
      // bitmaps[C_VALID] might be null; override to make it safe for Visit()
      bitmaps[C_VALID] = bitmaps[C_DATA];
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
        apply(UINT64_MAX, words[C_DATA], UINT64_MAX, words[R_VALID]);
      });
      break;
    case LEFT_ALL_VALID:
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
        apply(words[C_VALID], words[C_DATA], UINT64_MAX, words[R_VALID]);
      });
      break;
    case COND_ALL_VALID:
      // out_valid =  ~cond.data & right.valid
      arrow::internal::BitmapAndNot(right.buffers[0]->data(), right.offset,
                                    cond.buffers[1]->data(), cond.offset, cond.length, 0,
                                    output->buffers[0]->mutable_data());
      break;
    case 0:
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
        apply(words[C_VALID], words[C_DATA], 0, words[R_VALID]);
      });
      break;
  }
  return Status::OK();
}

// if the condition is null then output is null otherwise we take validity from the
// selected argument
// ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
Status PromoteNullsVisitor(KernelContext* ctx, const ArrayData& cond,
                           const ArrayData& left, const ArrayData& right,
                           ArrayData* output) {
  uint8_t flag =
      !right.MayHaveNulls() * 4 + !left.MayHaveNulls() * 2 + !cond.MayHaveNulls();

  enum { C_VALID, C_DATA, L_VALID, R_VALID };

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

  // lambda function that will be used inside the visitor
  int64_t i = 0;
  auto apply = [&](uint64_t c_valid, uint64_t c_data, uint64_t l_valid,
                   uint64_t r_valid) {
    out_validity[i] = c_valid & ((c_data & l_valid) | (~c_data & r_valid));
    i++;
  };

  // if the condition is null then output is null otherwise we take validity from the
  // selected argument
  // ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
  switch (flag) {
    case COND_ALL_VALID | LEFT_ALL_VALID | RIGHT_ALL_VALID:
      break;
    case LEFT_ALL_VALID | RIGHT_ALL_VALID:
      output->buffers[0] = SliceBuffer(cond.buffers[0], cond.offset, cond.length);
      break;
    case COND_ALL_VALID | RIGHT_ALL_VALID:
      // bitmaps[C_VALID], bitmaps[R_VALID] might be null; override to make it safe for
      // Visit()
      bitmaps[C_VALID] = bitmaps[C_DATA];
      bitmaps[R_VALID] = bitmaps[C_DATA];
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(UINT64_MAX, words[C_DATA], words[L_VALID], UINT64_MAX);
      });
      break;
    case RIGHT_ALL_VALID:
      // bitmaps[R_VALID] might be null; override to make it safe for Visit()
      bitmaps[R_VALID] = bitmaps[C_DATA];
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[C_VALID], words[C_DATA], words[L_VALID], UINT64_MAX);
      });
      break;
    case COND_ALL_VALID | LEFT_ALL_VALID:
      // bitmaps[C_VALID], bitmaps[L_VALID] might be null; override to make it safe for
      // Visit()
      bitmaps[C_VALID] = bitmaps[C_DATA];
      bitmaps[L_VALID] = bitmaps[C_DATA];
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(UINT64_MAX, words[C_DATA], UINT64_MAX, words[R_VALID]);
      });
      break;
    case LEFT_ALL_VALID:
      // bitmaps[L_VALID] might be null; override to make it safe for Visit()
      bitmaps[L_VALID] = bitmaps[C_DATA];
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[C_VALID], words[C_DATA], UINT64_MAX, words[R_VALID]);
      });
      break;
    case COND_ALL_VALID:
      // bitmaps[C_VALID] might be null; override to make it safe for Visit()
      bitmaps[C_VALID] = bitmaps[C_DATA];
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(UINT64_MAX, words[C_DATA], words[L_VALID], words[R_VALID]);
      });
      break;
    case 0:
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[C_VALID], words[C_DATA], words[L_VALID], words[R_VALID]);
      });
      break;
  }
  return Status::OK();
}

template <typename Type, typename Enable = void>
struct IfElseFunctor {};

// only number types needs to be handled for Fixed sized primitive data types because,
// internal::GenerateTypeAgnosticPrimitive forwards types to the corresponding unsigned
// int type
template <typename Type>
struct IfElseFunctor<Type, enable_if_number<Type>> {
  using T = typename TypeTraits<Type>::CType;
  // A - Array
  // S - Scalar

  //  AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor(ctx, cond, left, right, out));

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

  // ASA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor(ctx, cond, left, right, out));

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

  // AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor(ctx, cond, left, right, out));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                          ctx->Allocate(cond.length * sizeof(T)));
    T* out_values = reinterpret_cast<T*>(out_buf->mutable_data());

    // copy left data to out_buff
    const T* left_data = left.GetValues<T>(1);
    std::memcpy(out_values, left_data, left.length * sizeof(T));

    const auto* cond_data = cond.buffers[1]->data();  // this is a BoolArray
    BitBlockCounter bit_counter(cond_data, cond.offset, cond.length);

    // selectively copy values from left data
    T right_data = internal::UnboxScalar<Type>::Unbox(right);
    int64_t offset = cond.offset;

    // todo this can be improved by intrinsics. ex: _mm*_mask_store_e* (vmovdqa*)
    // left data is already in the output buffer. Therefore, mask needs to be inverted
    while (offset < cond.offset + cond.length) {
      const BitBlockCount& block = bit_counter.NextWord();
      if (block.NoneSet()) {  // all from right
        std::fill(out_values, out_values + block.length, right_data);
      } else if (block.popcount) {  // selectively copy from right
        for (int64_t i = 0; i < block.length; ++i) {
          if (!BitUtil::GetBit(cond_data, offset + i)) {
            out_values[i] = right_data;
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
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor(ctx, cond, left, right, out));

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                          ctx->Allocate(cond.length * sizeof(T)));
    T* out_values = reinterpret_cast<T*>(out_buf->mutable_data());

    // copy right data to out_buff
    T right_data = internal::UnboxScalar<Type>::Unbox(right);
    std::fill(out_values, out_values + cond.length, right_data);

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
};

template <typename Type>
struct IfElseFunctor<Type, enable_if_boolean<Type>> {
  // AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor(ctx, cond, left, right, out));

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

  // ASA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor(ctx, cond, left, right, out));

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

  // AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor(ctx, cond, left, right, out));

    // out_buff = left & cond
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> out_buf,
                          arrow::internal::BitmapAnd(
                              ctx->memory_pool(), left.buffers[1]->data(), left.offset,
                              cond.buffers[1]->data(), cond.offset, cond.length, 0));

    bool right_data = internal::UnboxScalar<BooleanType>::Unbox(right);

    // out_buff = left & cond | right & ~cond
    if (right_data) {
      arrow::internal::BitmapOrNot(out_buf->data(), 0, cond.buffers[1]->data(),
                                   cond.offset, cond.length, 0, out_buf->mutable_data());
    }

    out->buffers[1] = std::move(out_buf);
    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor(ctx, cond, left, right, out));

    bool left_data = internal::UnboxScalar<BooleanType>::Unbox(left);
    bool right_data = internal::UnboxScalar<BooleanType>::Unbox(right);

    // out_buf = left & cond | right & ~cond
    std::shared_ptr<Buffer> out_buf = nullptr;
    if (left_data) {
      if (right_data) {
        // out_buf = ones
        ARROW_ASSIGN_OR_RAISE(out_buf, ctx->AllocateBitmap(cond.length));
        // filling with UINT8_MAX upto the buffer's size (in bytes)
        arrow::compute::internal::SetMemory<UINT8_MAX>(out_buf.get());
      } else {
        // out_buf = cond
        out_buf = SliceBuffer(cond.buffers[1], cond.offset, cond.length);
      }
    } else {
      if (right_data) {
        // out_buf = ~cond
        ARROW_ASSIGN_OR_RAISE(out_buf, arrow::internal::InvertBitmap(
                                           ctx->memory_pool(), cond.buffers[1]->data(),
                                           cond.offset, cond.length))
      } else {
        // out_buf = zeros
        ARROW_ASSIGN_OR_RAISE(out_buf, ctx->AllocateBitmap(cond.length));
      }
    }
    out->buffers[1] = std::move(out_buf);
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

  // ASA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    return ReturnCopy(right, out);
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    return ReturnCopy(left, out);
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
        // output size is the size of the array arg
        int64_t bcast_size = batch[1].is_array() ? batch[1].length() : batch[2].length();
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
        return IfElseFunctor<Type>::Call(ctx, *batch[0].array(), *batch[1].array(),
                                         *batch[2].scalar(), out->mutable_array());
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

const FunctionDoc if_else_doc{"Choose values based on a condition",
                              ("`cond` must be a Boolean scalar/ array. \n`left` or "
                               "`right` must be of the same type scalar/ array.\n"
                               "`null` values in `cond` will be promoted to the"
                               " output."),
                              {"cond", "left", "right"}};

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
