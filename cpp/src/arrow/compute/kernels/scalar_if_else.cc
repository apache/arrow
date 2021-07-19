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
#include <arrow/util/bitmap_reader.h>

namespace arrow {
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::Bitmap;
using internal::BitmapWordReader;

namespace compute {
namespace internal {

namespace {

constexpr uint64_t kAllNull = 0;
constexpr uint64_t kAllValid = ~kAllNull;

util::optional<uint64_t> GetConstantValidityWord(const Datum& data) {
  if (data.is_scalar()) {
    return data.scalar()->is_valid ? kAllValid : kAllNull;
  }

  if (data.array()->null_count == data.array()->length) return kAllNull;

  if (!data.array()->MayHaveNulls()) return kAllValid;

  // no constant validity word available
  return {};
}

inline Bitmap GetBitmap(const Datum& datum, int i) {
  if (datum.is_scalar()) return {};
  const ArrayData& a = *datum.array();
  return Bitmap{a.buffers[i], a.offset, a.length};
}

// if the condition is null then output is null otherwise we take validity from the
// selected argument
// ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
template <typename AllocateNullBitmap>
Status PromoteNullsVisitor(KernelContext* ctx, const Datum& cond_d, const Datum& left_d,
                           const Datum& right_d, ArrayData* output) {
  auto cond_const = GetConstantValidityWord(cond_d);
  auto left_const = GetConstantValidityWord(left_d);
  auto right_const = GetConstantValidityWord(right_d);

  enum { COND_CONST = 1, LEFT_CONST = 2, RIGHT_CONST = 4 };
  auto flag = COND_CONST * cond_const.has_value() | LEFT_CONST * left_const.has_value() |
              RIGHT_CONST * right_const.has_value();

  const ArrayData& cond = *cond_d.array();
  // cond.data will always be available
  Bitmap cond_data{cond.buffers[1], cond.offset, cond.length};
  Bitmap cond_valid{cond.buffers[0], cond.offset, cond.length};
  Bitmap left_valid = GetBitmap(left_d, 0);
  Bitmap right_valid = GetBitmap(right_d, 0);

  // cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
  // In the following cases, we dont need to allocate out_valid bitmap

  // if cond & left & right all ones, then output is all valid.
  // if output validity buffer is already allocated (NullHandling::
  // COMPUTED_PREALLOCATE) -> set all bits
  // else, return nullptr
  if (cond_const == kAllValid && left_const == kAllValid && right_const == kAllValid) {
    if (AllocateNullBitmap::value) {  // NullHandling::COMPUTED_NO_PREALLOCATE
      output->buffers[0] = nullptr;
    } else {  // NullHandling::COMPUTED_PREALLOCATE
      BitUtil::SetBitmap(output->buffers[0]->mutable_data(), output->offset,
                         output->length);
    }
    return Status::OK();
  }

  if (left_const == kAllValid && right_const == kAllValid) {
    // if both left and right are valid, no need to calculate out_valid bitmap. Copy
    // cond validity buffer
    if (AllocateNullBitmap::value) {  // NullHandling::COMPUTED_NO_PREALLOCATE
      // if there's an offset, copy bitmap (cannot slice a bitmap)
      if (cond.offset) {
        ARROW_ASSIGN_OR_RAISE(
            output->buffers[0],
            arrow::internal::CopyBitmap(ctx->memory_pool(), cond.buffers[0]->data(),
                                        cond.offset, cond.length));
      } else {  // just copy assign cond validity buffer
        output->buffers[0] = cond.buffers[0];
      }
    } else {  // NullHandling::COMPUTED_PREALLOCATE
      arrow::internal::CopyBitmap(cond.buffers[0]->data(), cond.offset, cond.length,
                                  output->buffers[0]->mutable_data(), output->offset);
    }
    return Status::OK();
  }

  // lambda function that will be used inside the visitor
  auto apply = [&](uint64_t c_valid, uint64_t c_data, uint64_t l_valid,
                   uint64_t r_valid) {
    return c_valid & ((c_data & l_valid) | (~c_data & r_valid));
  };

  if (AllocateNullBitmap::value) {
    // following cases requires a separate out_valid buffer. COMPUTED_NO_PREALLOCATE
    // would not have allocated buffers for it.
    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(cond.length));
  }

  std::array<Bitmap, 1> out_bitmaps{
      Bitmap{output->buffers[0], output->offset, output->length}};

  switch (flag) {
    case COND_CONST | LEFT_CONST | RIGHT_CONST: {
      std::array<Bitmap, 1> bitmaps{cond_data};
      Bitmap::VisitWordsAndWrite(bitmaps, &out_bitmaps,
                                 [&](const std::array<uint64_t, 1>& words_in,
                                     std::array<uint64_t, 1>* word_out) {
                                   word_out->at(0) = apply(*cond_const, words_in[0],
                                                           *left_const, *right_const);
                                 });
      break;
    }
    case LEFT_CONST | RIGHT_CONST: {
      std::array<Bitmap, 2> bitmaps{cond_valid, cond_data};
      Bitmap::VisitWordsAndWrite(bitmaps, &out_bitmaps,
                                 [&](const std::array<uint64_t, 2>& words_in,
                                     std::array<uint64_t, 1>* word_out) {
                                   word_out->at(0) = apply(words_in[0], words_in[1],
                                                           *left_const, *right_const);
                                 });
      break;
    }
    case COND_CONST | RIGHT_CONST: {
      // bitmaps[C_VALID], bitmaps[R_VALID] might be null; override to make it safe for
      // Visit()
      std::array<Bitmap, 2> bitmaps{cond_data, left_valid};
      Bitmap::VisitWordsAndWrite(bitmaps, &out_bitmaps,
                                 [&](const std::array<uint64_t, 2>& words_in,
                                     std::array<uint64_t, 1>* word_out) {
                                   word_out->at(0) = apply(*cond_const, words_in[0],
                                                           words_in[1], *right_const);
                                 });
      break;
    }
    case RIGHT_CONST: {
      // bitmaps[R_VALID] might be null; override to make it safe for Visit()
      std::array<Bitmap, 3> bitmaps{cond_valid, cond_data, left_valid};
      Bitmap::VisitWordsAndWrite(bitmaps, &out_bitmaps,
                                 [&](const std::array<uint64_t, 3>& words_in,
                                     std::array<uint64_t, 1>* word_out) {
                                   word_out->at(0) = apply(words_in[0], words_in[1],
                                                           words_in[2], *right_const);
                                 });
      break;
    }
    case COND_CONST | LEFT_CONST: {
      // bitmaps[C_VALID], bitmaps[L_VALID] might be null; override to make it safe for
      // Visit()
      std::array<Bitmap, 2> bitmaps{cond_data, right_valid};
      Bitmap::VisitWordsAndWrite(bitmaps, &out_bitmaps,
                                 [&](const std::array<uint64_t, 2>& words_in,
                                     std::array<uint64_t, 1>* word_out) {
                                   word_out->at(0) = apply(*cond_const, words_in[0],
                                                           *left_const, words_in[1]);
                                 });
      break;
    }
    case LEFT_CONST: {
      // bitmaps[L_VALID] might be null; override to make it safe for Visit()
      std::array<Bitmap, 3> bitmaps{cond_valid, cond_data, right_valid};
      Bitmap::VisitWordsAndWrite(bitmaps, &out_bitmaps,
                                 [&](const std::array<uint64_t, 3>& words_in,
                                     std::array<uint64_t, 1>* word_out) {
                                   word_out->at(0) = apply(words_in[0], words_in[1],
                                                           *left_const, words_in[2]);
                                 });
      break;
    }
    case COND_CONST: {
      // bitmaps[C_VALID] might be null; override to make it safe for Visit()
      std::array<Bitmap, 3> bitmaps{cond_data, left_valid, right_valid};
      Bitmap::VisitWordsAndWrite(bitmaps, &out_bitmaps,
                                 [&](const std::array<uint64_t, 3>& words_in,
                                     std::array<uint64_t, 1>* word_out) {
                                   word_out->at(0) = apply(*cond_const, words_in[0],
                                                           words_in[1], words_in[2]);
                                 });
      break;
    }
    case 0: {
      std::array<Bitmap, 4> bitmaps{cond_valid, cond_data, left_valid, right_valid};
      Bitmap::VisitWordsAndWrite(bitmaps, &out_bitmaps,
                                 [&](const std::array<uint64_t, 4>& words_in,
                                     std::array<uint64_t, 1>* word_out) {
                                   word_out->at(0) = apply(words_in[0], words_in[1],
                                                           words_in[2], words_in[3]);
                                 });
      break;
    }
  }
  return Status::OK();
}

using Word = uint64_t;
static constexpr int64_t word_len = sizeof(Word) * 8;

/// Runs the main if_else loop. Here, it is expected that the right data has already
/// been copied to the output.
/// If `invert` is meant to invert the cond.data. If is set to `true`, then the
/// buffer will be inverted before calling the handle_block or handle_each functions.
/// This is useful, when left is an array and right is scalar. Then rather than
/// copying data from the right to output, we can copy left data to the output and
/// invert the cond data to fill right values. Filling out with a scalar is presumed to
/// be more efficient than filling with an array
///
/// `HandleBlock` has the signature:
///     [](int64_t offset, int64_t length){...}
/// It should copy `length` number of elements from source array to output array with
/// `offset` offset in both arrays
template <typename HandleBlock, bool invert = false>
void RunIfElseLoop(const ArrayData& cond, const HandleBlock& handle_block) {
  int64_t data_offset = 0;
  int64_t bit_offset = cond.offset;
  const auto* cond_data = cond.buffers[1]->data();  // this is a BoolArray

  BitmapWordReader<Word> cond_reader(cond_data, cond.offset, cond.length);

  constexpr Word pickAll = invert ? 0 : UINT64_MAX;
  constexpr Word pickNone = ~pickAll;

  int64_t cnt = cond_reader.words();
  while (cnt--) {
    Word word = cond_reader.NextWord();

    if (word == pickAll) {
      handle_block(data_offset, word_len);
    } else if (word != pickNone) {
      for (int64_t i = 0; i < word_len; ++i) {
        if (BitUtil::GetBit(cond_data, bit_offset + i) != invert) {
          handle_block(data_offset + i, 1);
        }
      }
    }
    data_offset += word_len;
    bit_offset += word_len;
  }

  constexpr uint8_t pickAllByte = invert ? 0 : UINT8_MAX;
  // byte bit-wise inversion is int-wide. Hence XOR with 0xff
  constexpr uint8_t pickNoneByte = pickAllByte ^ 0xff;

  cnt = cond_reader.trailing_bytes();
  while (cnt--) {
    int valid_bits;
    uint8_t byte = cond_reader.NextTrailingByte(valid_bits);

    if (byte == pickAllByte && valid_bits == 8) {
      handle_block(data_offset, 8);
    } else if (byte != pickNoneByte) {
      for (int i = 0; i < valid_bits; ++i) {
        if (BitUtil::GetBit(cond_data, bit_offset + i) != invert) {
          handle_block(data_offset + i, 1);
        }
      }
    }
    data_offset += 8;
    bit_offset += 8;
  }
}

template <typename HandleBlock>
void RunIfElseLoopInverted(const ArrayData& cond, const HandleBlock& handle_block) {
  RunIfElseLoop<HandleBlock, true>(cond, handle_block);
}

/// Runs if-else when cond is a scalar. Two special functions are required,
/// 1.CopyArrayData, 2. BroadcastScalar
template <typename CopyArrayData, typename BroadcastScalar>
Status RunIfElseScalar(const BooleanScalar& cond, const Datum& left, const Datum& right,
                       Datum* out, const CopyArrayData& copy_array_data,
                       const BroadcastScalar& broadcast_scalar) {
  if (left.is_scalar() && right.is_scalar()) {  // output will be a scalar
    if (cond.is_valid) {
      *out = cond.value ? left.scalar() : right.scalar();
    } else {
      *out = MakeNullScalar(left.type());
    }
    return Status::OK();
  }

  // either left or right is an array. Output is always an array`
  const std::shared_ptr<ArrayData>& out_array = out->array();
  if (!cond.is_valid) {
    // cond is null; output is all null --> clear validity buffer
    BitUtil::ClearBitmap(out_array->buffers[0]->mutable_data(), out_array->offset,
                         out_array->length);
    return Status::OK();
  }

  // cond is a non-null scalar
  const auto& valid_data = cond.value ? left : right;
  if (valid_data.is_array()) {
    // valid_data is an array. Hence copy data to the output buffers
    const auto& valid_array = valid_data.array();
    if (valid_array->MayHaveNulls()) {
      arrow::internal::CopyBitmap(
          valid_array->buffers[0]->data(), valid_array->offset, valid_array->length,
          out_array->buffers[0]->mutable_data(), out_array->offset);
    } else {  // validity buffer is nullptr --> set all bits
      BitUtil::SetBitmap(out_array->buffers[0]->mutable_data(), out_array->offset,
                         out_array->length);
    }
    copy_array_data(*valid_array, out_array.get());
    return Status::OK();

  } else {  // valid data is scalar
    // valid data is a scalar that needs to be broadcasted
    const auto& valid_scalar = *valid_data.scalar();
    if (valid_scalar.is_valid) {  // if the scalar is non-null, broadcast
      BitUtil::SetBitmap(out_array->buffers[0]->mutable_data(), out_array->offset,
                         out_array->length);
      broadcast_scalar(*valid_data.scalar(), out_array.get());
    } else {  // scalar is null, clear the output validity buffer
      BitUtil::ClearBitmap(out_array->buffers[0]->mutable_data(), out_array->offset,
                           out_array->length);
    }
    return Status::OK();
  }
}

template <typename Type, typename Enable = void>
struct IfElseFunctor {};

// only number types needs to be handled for Fixed sized primitive data types because,
// internal::GenerateTypeAgnosticPrimitive forwards types to the corresponding unsigned
// int type
template <typename Type>
struct IfElseFunctor<Type, enable_if_number<Type>> {
  using T = typename TypeTraits<Type>::CType;
  // A - Array, S - Scalar, X = Array/Scalar

  // SXX
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const Datum& left,
                     const Datum& right, Datum* out) {
    return RunIfElseScalar(
        cond, left, right, out,
        /*CopyArrayData*/
        [&](const ArrayData& valid_array, ArrayData* out_array) {
          std::memcpy(out_array->GetMutableValues<T>(1), valid_array.GetValues<T>(1),
                      valid_array.length * sizeof(T));
        },
        /*BroadcastScalar*/
        [&](const Scalar& scalar, ArrayData* out_array) {
          T scalar_data = internal::UnboxScalar<Type>::Unbox(scalar);
          std::fill(out_array->GetMutableValues<T>(1),
                    out_array->GetMutableValues<T>(1) + out_array->length, scalar_data);
        });
  }

  //  AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    T* out_values = out->template GetMutableValues<T>(1);

    // copy right data to out_buff
    const T* right_data = right.GetValues<T>(1);
    std::memcpy(out_values, right_data, right.length * sizeof(T));

    // selectively copy values from left data
    const T* left_data = left.GetValues<T>(1);

    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::memcpy(out_values + data_offset, left_data + data_offset,
                  num_elems * sizeof(T));
    });

    return Status::OK();
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    T* out_values = out->template GetMutableValues<T>(1);

    // copy right data to out_buff
    const T* right_data = right.GetValues<T>(1);
    std::memcpy(out_values, right_data, right.length * sizeof(T));

    // selectively copy values from left data
    T left_data = internal::UnboxScalar<Type>::Unbox(left);

    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::fill(out_values + data_offset, out_values + data_offset + num_elems,
                left_data);
    });

    return Status::OK();
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    T* out_values = out->template GetMutableValues<T>(1);

    // copy left data to out_buff
    const T* left_data = left.GetValues<T>(1);
    std::memcpy(out_values, left_data, left.length * sizeof(T));

    T right_data = internal::UnboxScalar<Type>::Unbox(right);

    RunIfElseLoopInverted(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::fill(out_values + data_offset, out_values + data_offset + num_elems,
                right_data);
    });

    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    T* out_values = out->template GetMutableValues<T>(1);

    // copy right data to out_buff
    T right_data = internal::UnboxScalar<Type>::Unbox(right);
    std::fill(out_values, out_values + cond.length, right_data);

    // selectively copy values from left data
    T left_data = internal::UnboxScalar<Type>::Unbox(left);
    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::fill(out_values + data_offset, out_values + data_offset + num_elems,
                left_data);
    });

    return Status::OK();
  }
};

template <typename Type>
struct IfElseFunctor<Type, enable_if_boolean<Type>> {
  // A - Array, S - Scalar, X = Array/Scalar

  // SXX
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const Datum& left,
                     const Datum& right, Datum* out) {
    return RunIfElseScalar(
        cond, left, right, out,
        /*CopyArrayData*/
        [&](const ArrayData& valid_array, ArrayData* out_array) {
          arrow::internal::CopyBitmap(
              valid_array.buffers[1]->data(), valid_array.offset, valid_array.length,
              out_array->buffers[1]->mutable_data(), out_array->offset);
        },
        /*BroadcastScalar*/
        [&](const Scalar& scalar, ArrayData* out_array) {
          bool scalar_data = internal::UnboxScalar<Type>::Unbox(scalar);
          BitUtil::SetBitsTo(out_array->buffers[1]->mutable_data(), out_array->offset,
                             out_array->length, scalar_data);
        });
  }

  // AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    // out_buff = right & ~cond
    const auto& out_buf = out->buffers[1];
    arrow::internal::BitmapAndNot(right.buffers[1]->data(), right.offset,
                                  cond.buffers[1]->data(), cond.offset, cond.length,
                                  out->offset, out_buf->mutable_data());

    // out_buff = left & cond
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> temp_buf,
                          arrow::internal::BitmapAnd(
                              ctx->memory_pool(), left.buffers[1]->data(), left.offset,
                              cond.buffers[1]->data(), cond.offset, cond.length, 0));

    arrow::internal::BitmapOr(out_buf->data(), out->offset, temp_buf->data(), 0,
                              cond.length, out->offset, out_buf->mutable_data());

    return Status::OK();
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    // out_buff = right & ~cond
    const auto& out_buf = out->buffers[1];
    arrow::internal::BitmapAndNot(right.buffers[1]->data(), right.offset,
                                  cond.buffers[1]->data(), cond.offset, cond.length,
                                  out->offset, out_buf->mutable_data());

    // out_buff = left & cond
    bool left_data = internal::UnboxScalar<BooleanType>::Unbox(left);
    if (left_data) {
      arrow::internal::BitmapOr(out_buf->data(), out->offset, cond.buffers[1]->data(),
                                cond.offset, cond.length, out->offset,
                                out_buf->mutable_data());
    }

    return Status::OK();
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    // out_buff = left & cond
    const auto& out_buf = out->buffers[1];
    arrow::internal::BitmapAnd(left.buffers[1]->data(), left.offset,
                               cond.buffers[1]->data(), cond.offset, cond.length,
                               out->offset, out_buf->mutable_data());

    bool right_data = internal::UnboxScalar<BooleanType>::Unbox(right);

    // out_buff = left & cond | right & ~cond
    if (right_data) {
      arrow::internal::BitmapOrNot(out_buf->data(), out->offset, cond.buffers[1]->data(),
                                   cond.offset, cond.length, out->offset,
                                   out_buf->mutable_data());
    }

    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    bool left_data = internal::UnboxScalar<BooleanType>::Unbox(left);
    bool right_data = internal::UnboxScalar<BooleanType>::Unbox(right);

    const auto& out_buf = out->buffers[1];

    // out_buf = left & cond | right & ~cond
    //    std::shared_ptr<Buffer> out_buf = nullptr;
    if (left_data) {
      if (right_data) {
        // out_buf = ones
        BitUtil::SetBitmap(out_buf->mutable_data(), out->offset, cond.length);
      } else {
        // out_buf = cond
        arrow::internal::CopyBitmap(cond.buffers[1]->data(), cond.offset, cond.length,
                                    out_buf->mutable_data(), out->offset);
      }
    } else {
      if (right_data) {
        // out_buf = ~cond
        arrow::internal::InvertBitmap(cond.buffers[1]->data(), cond.offset, cond.length,
                                      out_buf->mutable_data(), out->offset);
      } else {
        // out_buf = zeros
        BitUtil::ClearBitmap(out_buf->mutable_data(), out->offset, cond.length);
      }
    }

    return Status::OK();
  }
};

template <typename Type>
struct IfElseFunctor<Type, enable_if_base_binary<Type>> {
  using OffsetType = typename TypeTraits<Type>::OffsetType::c_type;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  // A - Array, S - Scalar, X = Array/Scalar

  // SXX
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const Datum& left,
                     const Datum& right, Datum* out) {
    if (left.is_scalar() && right.is_scalar()) {
      if (cond.is_valid) {
        *out = cond.value ? left.scalar() : right.scalar();
      } else {
        *out = MakeNullScalar(left.type());
      }
      return Status::OK();
    }
    // either left or right is an array. Output is always an array
    int64_t out_arr_len = std::max(left.length(), right.length());
    if (!cond.is_valid) {
      // cond is null; just create a null array
      ARROW_ASSIGN_OR_RAISE(*out,
                            MakeArrayOfNull(left.type(), out_arr_len, ctx->memory_pool()))
      return Status::OK();
    }

    const auto& valid_data = cond.value ? left : right;
    if (valid_data.is_array()) {
      *out = valid_data;
    } else {
      // valid data is a scalar that needs to be broadcasted
      ARROW_ASSIGN_OR_RAISE(*out, MakeArrayFromScalar(*valid_data.scalar(), out_arr_len,
                                                      ctx->memory_pool()));
    }
    return Status::OK();
  }

  //  AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    const auto* left_offsets = left.GetValues<OffsetType>(1);
    const uint8_t* left_data = left.buffers[2]->data();
    const auto* right_offsets = right.GetValues<OffsetType>(1);
    const uint8_t* right_data = right.buffers[2]->data();

    // allocate data buffer conservatively
    int64_t data_buff_alloc = left_offsets[left.length] - left_offsets[0] +
                              right_offsets[right.length] - right_offsets[0];

    BuilderType builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(cond.length + 1));
    ARROW_RETURN_NOT_OK(builder.ReserveData(data_buff_alloc));

    RunLoop(
        cond, *out,
        [&](int64_t i) {
          builder.UnsafeAppend(left_data + left_offsets[i],
                               left_offsets[i + 1] - left_offsets[i]);
        },
        [&](int64_t i) {
          builder.UnsafeAppend(right_data + right_offsets[i],
                               right_offsets[i + 1] - right_offsets[i]);
        },
        [&]() { builder.UnsafeAppendNull(); });
    ARROW_ASSIGN_OR_RAISE(auto out_arr, builder.Finish());

    out->SetNullCount(out_arr->data()->null_count);
    out->buffers[0] = std::move(out_arr->data()->buffers[0]);
    out->buffers[1] = std::move(out_arr->data()->buffers[1]);
    out->buffers[2] = std::move(out_arr->data()->buffers[2]);
    return Status::OK();
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    util::string_view left_data = internal::UnboxScalar<Type>::Unbox(left);
    auto left_size = static_cast<OffsetType>(left_data.size());

    const auto* right_offsets = right.GetValues<OffsetType>(1);
    const uint8_t* right_data = right.buffers[2]->data();

    // allocate data buffer conservatively
    int64_t data_buff_alloc =
        left_size * cond.length + right_offsets[right.length] - right_offsets[0];

    BuilderType builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(cond.length + 1));
    ARROW_RETURN_NOT_OK(builder.ReserveData(data_buff_alloc));

    RunLoop(
        cond, *out, [&](int64_t i) { builder.UnsafeAppend(left_data.data(), left_size); },
        [&](int64_t i) {
          builder.UnsafeAppend(right_data + right_offsets[i],
                               right_offsets[i + 1] - right_offsets[i]);
        },
        [&]() { builder.UnsafeAppendNull(); });
    ARROW_ASSIGN_OR_RAISE(auto out_arr, builder.Finish());

    out->SetNullCount(out_arr->data()->null_count);
    out->buffers[0] = std::move(out_arr->data()->buffers[0]);
    out->buffers[1] = std::move(out_arr->data()->buffers[1]);
    out->buffers[2] = std::move(out_arr->data()->buffers[2]);
    return Status::OK();
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    const auto* left_offsets = left.GetValues<OffsetType>(1);
    const uint8_t* left_data = left.buffers[2]->data();

    util::string_view right_data = internal::UnboxScalar<Type>::Unbox(right);
    auto right_size = static_cast<OffsetType>(right_data.size());

    // allocate data buffer conservatively
    int64_t data_buff_alloc =
        right_size * cond.length + left_offsets[left.length] - left_offsets[0];

    BuilderType builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(cond.length + 1));
    ARROW_RETURN_NOT_OK(builder.ReserveData(data_buff_alloc));

    RunLoop(
        cond, *out,
        [&](int64_t i) {
          builder.UnsafeAppend(left_data + left_offsets[i],
                               left_offsets[i + 1] - left_offsets[i]);
        },
        [&](int64_t i) { builder.UnsafeAppend(right_data.data(), right_size); },
        [&]() { builder.UnsafeAppendNull(); });
    ARROW_ASSIGN_OR_RAISE(auto out_arr, builder.Finish());

    out->SetNullCount(out_arr->data()->null_count);
    out->buffers[0] = std::move(out_arr->data()->buffers[0]);
    out->buffers[1] = std::move(out_arr->data()->buffers[1]);
    out->buffers[2] = std::move(out_arr->data()->buffers[2]);
    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    util::string_view left_data = internal::UnboxScalar<Type>::Unbox(left);
    auto left_size = static_cast<OffsetType>(left_data.size());

    util::string_view right_data = internal::UnboxScalar<Type>::Unbox(right);
    auto right_size = static_cast<OffsetType>(right_data.size());

    // allocate data buffer conservatively
    int64_t data_buff_alloc = std::max(right_size, left_size) * cond.length;
    BuilderType builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(cond.length + 1));
    ARROW_RETURN_NOT_OK(builder.ReserveData(data_buff_alloc));

    RunLoop(
        cond, *out, [&](int64_t i) { builder.UnsafeAppend(left_data.data(), left_size); },
        [&](int64_t i) { builder.UnsafeAppend(right_data.data(), right_size); },
        [&]() { builder.UnsafeAppendNull(); });
    ARROW_ASSIGN_OR_RAISE(auto out_arr, builder.Finish());

    out->SetNullCount(out_arr->data()->null_count);
    out->buffers[0] = std::move(out_arr->data()->buffers[0]);
    out->buffers[1] = std::move(out_arr->data()->buffers[1]);
    out->buffers[2] = std::move(out_arr->data()->buffers[2]);
    return Status::OK();
  }

  template <typename HandleLeft, typename HandleRight, typename HandleNull>
  static void RunLoop(const ArrayData& cond, const ArrayData& output,
                      HandleLeft&& handle_left, HandleRight&& handle_right,
                      HandleNull&& handle_null) {
    const auto* cond_data = cond.buffers[1]->data();

    if (output.buffers[0]) {  // output may have nulls
      // output validity buffer is allocated internally from the IfElseFunctor. Therefore
      // it is cond.length'd with 0 offset.
      const auto* out_valid = output.buffers[0]->data();

      for (int64_t i = 0; i < cond.length; i++) {
        if (BitUtil::GetBit(out_valid, i)) {
          BitUtil::GetBit(cond_data, cond.offset + i) ? handle_left(i) : handle_right(i);
        } else {
          handle_null();
        }
      }
    } else {  // output is all valid (no nulls)
      for (int64_t i = 0; i < cond.length; i++) {
        BitUtil::GetBit(cond_data, cond.offset + i) ? handle_left(i) : handle_right(i);
      }
    }
  }
};

template <typename Type>
struct IfElseFunctor<Type, enable_if_fixed_size_binary<Type>> {
  // A - Array, S - Scalar, X = Array/Scalar

  // SXX
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const Datum& left,
                     const Datum& right, Datum* out) {
    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type(), *right.type()));
    return RunIfElseScalar(
        cond, left, right, out,
        /*CopyArrayData*/
        [&](const ArrayData& valid_array, ArrayData* out_array) {
          std::memcpy(
              out_array->buffers[1]->mutable_data() + out_array->offset * byte_width,
              valid_array.buffers[1]->data() + valid_array.offset * byte_width,
              valid_array.length * byte_width);
        },
        /*BroadcastScalar*/
        [&](const Scalar& scalar, ArrayData* out_array) {
          const util::string_view& scalar_data =
              internal::UnboxScalar<FixedSizeBinaryType>::Unbox(scalar);
          uint8_t* start =
              out_array->buffers[1]->mutable_data() + out_array->offset * byte_width;
          for (int64_t i = 0; i < out_array->length; i++) {
            std::memcpy(start + i * byte_width, scalar_data.data(), scalar_data.size());
          }
        });
  }

  //  AAA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type, *right.type));
    auto* out_values = out->buffers[1]->mutable_data() + out->offset * byte_width;

    // copy right data to out_buff
    const uint8_t* right_data = right.buffers[1]->data() + right.offset * byte_width;
    std::memcpy(out_values, right_data, right.length * byte_width);

    // selectively copy values from left data
    const uint8_t* left_data = left.buffers[1]->data() + left.offset * byte_width;

    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::memcpy(out_values + data_offset * byte_width,
                  left_data + data_offset * byte_width, num_elems * byte_width);
    });

    return Status::OK();
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type, *right.type));
    auto* out_values = out->buffers[1]->mutable_data() + out->offset * byte_width;

    // copy right data to out_buff
    const uint8_t* right_data = right.buffers[1]->data() + right.offset * byte_width;
    std::memcpy(out_values, right_data, right.length * byte_width);

    // selectively copy values from left data
    const util::string_view& left_data =
        internal::UnboxScalar<FixedSizeBinaryType>::Unbox(left);

    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      if (left_data.data()) {
        for (int64_t i = 0; i < num_elems; i++) {
          std::memcpy(out_values + (data_offset + i) * byte_width, left_data.data(),
                      left_data.size());
        }
      }
    });

    return Status::OK();
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type, *right.type));
    auto* out_values = out->buffers[1]->mutable_data() + out->offset * byte_width;

    // copy left data to out_buff
    const uint8_t* left_data = left.buffers[1]->data() + left.offset * byte_width;
    std::memcpy(out_values, left_data, left.length * byte_width);

    const util::string_view& right_data =
        internal::UnboxScalar<FixedSizeBinaryType>::Unbox(right);

    RunIfElseLoopInverted(cond, [&](int64_t data_offset, int64_t num_elems) {
      if (right_data.data()) {
        for (int64_t i = 0; i < num_elems; i++) {
          std::memcpy(out_values + (data_offset + i) * byte_width, right_data.data(),
                      right_data.size());
        }
      }
    });

    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type, *right.type));
    auto* out_values = out->buffers[1]->mutable_data() + out->offset * byte_width;

    // copy right data to out_buff
    const util::string_view& right_data =
        internal::UnboxScalar<FixedSizeBinaryType>::Unbox(right);
    if (right_data.data()) {
      for (int64_t i = 0; i < cond.length; i++) {
        std::memcpy(out_values + i * byte_width, right_data.data(), right_data.size());
      }
    }

    // selectively copy values from left data
    const util::string_view& left_data =
        internal::UnboxScalar<FixedSizeBinaryType>::Unbox(left);

    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      if (left_data.data()) {
        for (int64_t i = 0; i < num_elems; i++) {
          std::memcpy(out_values + (data_offset + i) * byte_width, left_data.data(),
                      left_data.size());
        }
      }
    });

    return Status::OK();
  }

  static Result<int32_t> GetByteWidth(const DataType& left_type,
                                      const DataType& right_type) {
    int width = checked_cast<const FixedSizeBinaryType&>(left_type).byte_width();
    if (width == checked_cast<const FixedSizeBinaryType&>(right_type).byte_width()) {
      return width;
    } else {
      return Status::Invalid("FixedSizeBinaryType byte_widths should be equal");
    }
  }
};

template <typename Type, typename AllocateMem>
struct ResolveIfElseExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // cond is scalar
    if (batch[0].is_scalar()) {
      const auto& cond = batch[0].scalar_as<BooleanScalar>();
      return IfElseFunctor<Type>::Call(ctx, cond, batch[1], batch[2], out);
    }

    // cond is array. Use functors to sort things out
    ARROW_RETURN_NOT_OK(PromoteNullsVisitor<AllocateMem>(ctx, batch[0], batch[1],
                                                         batch[2], out->mutable_array()));

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

template <typename AllocateMem>
struct ResolveIfElseExec<NullType, AllocateMem> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // if all are scalars, return a null scalar
    if (batch[0].is_scalar() && batch[1].is_scalar() && batch[2].is_scalar()) {
      *out = MakeNullScalar(null());
    } else {
      ARROW_ASSIGN_OR_RAISE(*out,
                            MakeArrayOfNull(null(), batch.length, ctx->memory_pool()));
    }
    return Status::OK();
  }
};

struct IfElseFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    // if 0th descriptor is null, replace with bool
    if (values->at(0).type->id() == Type::NA) {
      values->at(0).type = boolean();
    }

    // if-else 0'th descriptor is bool, so skip it
    std::vector<ValueDescr> values_copy(values->begin() + 1, values->end());
    internal::EnsureDictionaryDecoded(&values_copy);
    internal::ReplaceNullWithOtherType(&values_copy);

    if (auto type = internal::CommonNumeric(values_copy)) {
      internal::ReplaceTypes(type, &values_copy);
    }

    std::move(values_copy.begin(), values_copy.end(), values->begin() + 1);

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

void AddNullIfElseKernel(const std::shared_ptr<IfElseFunction>& scalar_function) {
  ScalarKernel kernel({boolean(), null(), null()}, null(),
                      ResolveIfElseExec<NullType,
                                        /*AllocateMem=*/std::true_type>::Exec);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  kernel.can_write_into_slices = false;

  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

void AddPrimitiveIfElseKernels(const std::shared_ptr<ScalarFunction>& scalar_function,
                               const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec =
        internal::GenerateTypeAgnosticPrimitive<ResolveIfElseExec,
                                                /*AllocateMem=*/std::false_type>(*type);
    // cond array needs to be boolean always
    ScalarKernel kernel({boolean(), type, type}, type, exec);
    kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::PREALLOCATE;
    kernel.can_write_into_slices = true;

    DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
  }
}

void AddBinaryIfElseKernels(const std::shared_ptr<IfElseFunction>& scalar_function,
                            const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec =
        internal::GenerateTypeAgnosticVarBinaryBase<ResolveIfElseExec,
                                                    /*AllocateMem=*/std::true_type>(
            *type);
    // cond array needs to be boolean always
    ScalarKernel kernel({boolean(), type, type}, type, exec);
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    kernel.can_write_into_slices = false;

    DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
  }
}

void AddFSBinaryIfElseKernel(const std::shared_ptr<IfElseFunction>& scalar_function) {
  // cond array needs to be boolean always
  ScalarKernel kernel(
      {boolean(), InputType(Type::FIXED_SIZE_BINARY), InputType(Type::FIXED_SIZE_BINARY)},
      OutputType([](KernelContext*, const std::vector<ValueDescr>& descrs) {
        return ValueDescr(descrs[1].type, ValueDescr::ANY);
      }),
      ResolveIfElseExec<FixedSizeBinaryType, /*AllocateMem=*/std::false_type>::Exec);
  kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::PREALLOCATE;
  kernel.can_write_into_slices = true;

  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

// Helper to copy or broadcast fixed-width values between buffers.
template <typename Type, typename Enable = void>
struct CopyFixedWidth {};
template <>
struct CopyFixedWidth<BooleanType> {
  static void CopyScalar(const Scalar& scalar, const int64_t length,
                         uint8_t* raw_out_values, const int64_t out_offset) {
    const bool value = UnboxScalar<BooleanType>::Unbox(scalar);
    BitUtil::SetBitsTo(raw_out_values, out_offset, length, value);
  }
  static void CopyArray(const DataType&, const uint8_t* in_values,
                        const int64_t in_offset, const int64_t length,
                        uint8_t* raw_out_values, const int64_t out_offset) {
    arrow::internal::CopyBitmap(in_values, in_offset, length, raw_out_values, out_offset);
  }
};
template <typename Type>
struct CopyFixedWidth<Type, enable_if_number<Type>> {
  using CType = typename TypeTraits<Type>::CType;
  static void CopyScalar(const Scalar& scalar, const int64_t length,
                         uint8_t* raw_out_values, const int64_t out_offset) {
    CType* out_values = reinterpret_cast<CType*>(raw_out_values);
    const CType value = UnboxScalar<Type>::Unbox(scalar);
    std::fill(out_values + out_offset, out_values + out_offset + length, value);
  }
  static void CopyArray(const DataType&, const uint8_t* in_values,
                        const int64_t in_offset, const int64_t length,
                        uint8_t* raw_out_values, const int64_t out_offset) {
    std::memcpy(raw_out_values + out_offset * sizeof(CType),
                in_values + in_offset * sizeof(CType), length * sizeof(CType));
  }
};
template <typename Type>
struct CopyFixedWidth<Type, enable_if_same<Type, FixedSizeBinaryType>> {
  static void CopyScalar(const Scalar& values, const int64_t length,
                         uint8_t* raw_out_values, const int64_t out_offset) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(*values.type).byte_width();
    uint8_t* next = raw_out_values + (width * out_offset);
    const auto& scalar = checked_cast<const FixedSizeBinaryScalar&>(values);
    // Scalar may have null value buffer
    if (!scalar.value) {
      std::memset(next, 0x00, width * length);
    } else {
      DCHECK_EQ(scalar.value->size(), width);
      for (int i = 0; i < length; i++) {
        std::memcpy(next, scalar.value->data(), width);
        next += width;
      }
    }
  }
  static void CopyArray(const DataType& type, const uint8_t* in_values,
                        const int64_t in_offset, const int64_t length,
                        uint8_t* raw_out_values, const int64_t out_offset) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(type).byte_width();
    uint8_t* next = raw_out_values + (width * out_offset);
    std::memcpy(next, in_values + in_offset * width, length * width);
  }
};
template <typename Type>
struct CopyFixedWidth<Type, enable_if_decimal<Type>> {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  static void CopyScalar(const Scalar& values, const int64_t length,
                         uint8_t* raw_out_values, const int64_t out_offset) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(*values.type).byte_width();
    uint8_t* next = raw_out_values + (width * out_offset);
    const auto& scalar = checked_cast<const ScalarType&>(values);
    const auto value = scalar.value.ToBytes();
    for (int i = 0; i < length; i++) {
      std::memcpy(next, value.data(), width);
      next += width;
    }
  }
  static void CopyArray(const DataType& type, const uint8_t* in_values,
                        const int64_t in_offset, const int64_t length,
                        uint8_t* raw_out_values, const int64_t out_offset) {
    const int32_t width = checked_cast<const FixedSizeBinaryType&>(type).byte_width();
    uint8_t* next = raw_out_values + (width * out_offset);
    std::memcpy(next, in_values + in_offset * width, length * width);
  }
};
// Copy fixed-width values from a scalar/array datum into an output values buffer
template <typename Type>
void CopyValues(const Datum& in_values, const int64_t in_offset, const int64_t length,
                uint8_t* out_valid, uint8_t* out_values, const int64_t out_offset) {
  if (in_values.is_scalar()) {
    const auto& scalar = *in_values.scalar();
    if (out_valid) {
      BitUtil::SetBitsTo(out_valid, out_offset, length, scalar.is_valid);
    }
    CopyFixedWidth<Type>::CopyScalar(scalar, length, out_values, out_offset);
  } else {
    const ArrayData& array = *in_values.array();
    if (out_valid) {
      if (array.MayHaveNulls()) {
        if (length == 1) {
          // CopyBitmap is slow for short runs
          BitUtil::SetBitTo(
              out_valid, out_offset,
              BitUtil::GetBit(array.buffers[0]->data(), array.offset + in_offset));
        } else {
          arrow::internal::CopyBitmap(array.buffers[0]->data(), array.offset + in_offset,
                                      length, out_valid, out_offset);
        }
      } else {
        BitUtil::SetBitsTo(out_valid, out_offset, length, true);
      }
    }
    CopyFixedWidth<Type>::CopyArray(*array.type, array.buffers[1]->data(),
                                    array.offset + in_offset, length, out_values,
                                    out_offset);
  }
}

// Specialized helper to copy a single value from a source array. Allows avoiding
// repeatedly calling MayHaveNulls and Buffer::data() which have internal checks that
// add up when called in a loop.
template <typename Type>
void CopyOneArrayValue(const DataType& type, const uint8_t* in_valid,
                       const uint8_t* in_values, const int64_t in_offset,
                       uint8_t* out_valid, uint8_t* out_values,
                       const int64_t out_offset) {
  if (out_valid) {
    BitUtil::SetBitTo(out_valid, out_offset,
                      !in_valid || BitUtil::GetBit(in_valid, in_offset));
  }
  CopyFixedWidth<Type>::CopyArray(type, in_values, in_offset, /*length=*/1, out_values,
                                  out_offset);
}

struct CaseWhenFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    // The first function is a struct of booleans, where the number of fields in the
    // struct is either equal to the number of other arguments or is one less.
    RETURN_NOT_OK(CheckArity(*values));
    EnsureDictionaryDecoded(values);
    auto first_type = (*values)[0].type;
    if (first_type->id() != Type::STRUCT) {
      return Status::TypeError("case_when: first argument must be STRUCT, not ",
                               *first_type);
    }
    auto num_fields = static_cast<size_t>(first_type->num_fields());
    if (num_fields < values->size() - 2 || num_fields >= values->size()) {
      return Status::Invalid(
          "case_when: number of struct fields must be equal to or one less than count of "
          "remaining arguments (",
          values->size() - 1, "), got: ", first_type->num_fields());
    }
    for (const auto& field : first_type->fields()) {
      if (field->type()->id() != Type::BOOL) {
        return Status::TypeError(
            "case_when: all fields of first argument must be BOOL, but ", field->name(),
            " was of type: ", *field->type());
      }
    }

    if (auto type = CommonNumeric(values->data() + 1, values->size() - 1)) {
      for (auto it = values->begin() + 1; it != values->end(); it++) {
        it->type = type;
      }
    }
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

// Implement a 'case when' (SQL)/'select' (NumPy) function for any scalar conditions
template <typename Type>
Status ExecScalarCaseWhen(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& conds = checked_cast<const StructScalar&>(*batch.values[0].scalar());
  if (!conds.is_valid) {
    return Status::Invalid("cond struct must not be null");
  }
  Datum result;
  for (size_t i = 0; i < batch.values.size() - 1; i++) {
    if (i < conds.value.size()) {
      const Scalar& cond = *conds.value[i];
      if (cond.is_valid && internal::UnboxScalar<BooleanType>::Unbox(cond)) {
        result = batch[i + 1];
        break;
      }
    } else {
      // ELSE clause
      result = batch[i + 1];
      break;
    }
  }
  if (out->is_scalar()) {
    *out = result.is_scalar() ? result.scalar() : MakeNullScalar(out->type());
    return Status::OK();
  }
  ArrayData* output = out->mutable_array();
  if (!result.is_value()) {
    // All conditions false, no 'else' argument
    result = MakeNullScalar(out->type());
  }
  CopyValues<Type>(result, /*in_offset=*/0, batch.length,
                   output->GetMutableValues<uint8_t>(0, 0),
                   output->GetMutableValues<uint8_t>(1, 0), output->offset);
  return Status::OK();
}

// Implement 'case when' for any mix of scalar/array arguments for any fixed-width type,
// given helper functions to copy data from a source array to a target array
template <typename Type>
Status ExecArrayCaseWhen(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& conds_array = *batch.values[0].array();
  if (conds_array.GetNullCount() > 0) {
    return Status::Invalid("cond struct must not have top-level nulls");
  }
  ArrayData* output = out->mutable_array();
  const int64_t out_offset = output->offset;
  const auto num_value_args = batch.values.size() - 1;
  const bool have_else_arg =
      static_cast<size_t>(conds_array.type->num_fields()) < num_value_args;
  uint8_t* out_valid = output->buffers[0]->mutable_data();
  uint8_t* out_values = output->buffers[1]->mutable_data();
  if (have_else_arg) {
    // Copy 'else' value into output
    CopyValues<Type>(batch.values.back(), /*in_offset=*/0, batch.length, out_valid,
                     out_values, out_offset);
  } else {
    // There's no 'else' argument, so we should have an all-null validity bitmap
    BitUtil::SetBitsTo(out_valid, out_offset, batch.length, false);
  }

  // Allocate a temporary bitmap to determine which elements still need setting.
  ARROW_ASSIGN_OR_RAISE(auto mask_buffer, ctx->AllocateBitmap(batch.length));
  uint8_t* mask = mask_buffer->mutable_data();
  std::memset(mask, 0xFF, mask_buffer->size());

  // Then iterate through each argument in turn and set elements.
  for (size_t i = 0; i < batch.values.size() - (have_else_arg ? 2 : 1); i++) {
    const ArrayData& cond_array = *conds_array.child_data[i];
    const int64_t cond_offset = conds_array.offset + cond_array.offset;
    const uint8_t* cond_values = cond_array.buffers[1]->data();
    const Datum& values_datum = batch[i + 1];
    int64_t offset = 0;

    if (cond_array.GetNullCount() == 0) {
      // If no valid buffer, visit mask & cond bitmap simultaneously
      BinaryBitBlockCounter counter(mask, /*start_offset=*/0, cond_values, cond_offset,
                                    batch.length);
      while (offset < batch.length) {
        const auto block = counter.NextAndWord();
        if (block.AllSet()) {
          CopyValues<Type>(values_datum, offset, block.length, out_valid, out_values,
                           out_offset + offset);
          BitUtil::SetBitsTo(mask, offset, block.length, false);
        } else if (block.popcount) {
          for (int64_t j = 0; j < block.length; ++j) {
            if (BitUtil::GetBit(mask, offset + j) &&
                BitUtil::GetBit(cond_values, cond_offset + offset + j)) {
              CopyValues<Type>(values_datum, offset + j, /*length=*/1, out_valid,
                               out_values, out_offset + offset + j);
              BitUtil::SetBitTo(mask, offset + j, false);
            }
          }
        }
        offset += block.length;
      }
    } else {
      // Visit mask & cond bitmap & cond validity
      const uint8_t* cond_valid = cond_array.buffers[0]->data();
      Bitmap bitmaps[3] = {{mask, /*offset=*/0, batch.length},
                           {cond_values, cond_offset, batch.length},
                           {cond_valid, cond_offset, batch.length}};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
        const uint64_t word = words[0] & words[1] & words[2];
        const int64_t block_length = std::min<int64_t>(64, batch.length - offset);
        if (word == std::numeric_limits<uint64_t>::max()) {
          CopyValues<Type>(values_datum, offset, block_length, out_valid, out_values,
                           out_offset + offset);
          BitUtil::SetBitsTo(mask, offset, block_length, false);
        } else if (word) {
          for (int64_t j = 0; j < block_length; ++j) {
            if (BitUtil::GetBit(mask, offset + j) &&
                BitUtil::GetBit(cond_valid, cond_offset + offset + j) &&
                BitUtil::GetBit(cond_values, cond_offset + offset + j)) {
              CopyValues<Type>(values_datum, offset + j, /*length=*/1, out_valid,
                               out_values, out_offset + offset + j);
              BitUtil::SetBitTo(mask, offset + j, false);
            }
          }
        }
      });
    }
  }
  if (!have_else_arg) {
    // Need to initialize any remaining null slots (uninitialized memory)
    BitBlockCounter counter(mask, /*offset=*/0, batch.length);
    int64_t offset = 0;
    auto bit_width = checked_cast<const FixedWidthType&>(*out->type()).bit_width();
    auto byte_width = BitUtil::BytesForBits(bit_width);
    while (offset < batch.length) {
      const auto block = counter.NextWord();
      if (block.AllSet()) {
        if (bit_width == 1) {
          BitUtil::SetBitsTo(out_values, out_offset + offset, block.length, false);
        } else {
          std::memset(out_values + (out_offset + offset) * byte_width, 0x00,
                      byte_width * block.length);
        }
      } else if (!block.NoneSet()) {
        for (int64_t j = 0; j < block.length; ++j) {
          if (BitUtil::GetBit(out_valid, out_offset + offset + j)) continue;
          if (bit_width == 1) {
            BitUtil::ClearBit(out_values, out_offset + offset + j);
          } else {
            std::memset(out_values + (out_offset + offset + j) * byte_width, 0x00,
                        byte_width);
          }
        }
      }
      offset += block.length;
    }
  }
  return Status::OK();
}

template <typename Type, typename Enable = void>
struct CaseWhenFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch.values[0].is_array()) {
      return ExecArrayCaseWhen<Type>(ctx, batch, out);
    }
    return ExecScalarCaseWhen<Type>(ctx, batch, out);
  }
};

template <>
struct CaseWhenFunctor<NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return Status::OK();
  }
};

struct CoalesceFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));
    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    EnsureDictionaryDecoded(values);
    if (auto type = CommonNumeric(*values)) {
      ReplaceTypes(type, values);
    }
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

// Implement a 'coalesce' (SQL) operator for any number of scalar inputs
Status ExecScalarCoalesce(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  for (const auto& datum : batch.values) {
    if (datum.scalar()->is_valid) {
      *out = datum;
      break;
    }
  }
  return Status::OK();
}

// Helper: copy from a source datum into all null slots of the output
template <typename Type>
void CopyValuesAllValid(Datum source, uint8_t* out_valid, uint8_t* out_values,
                        const int64_t out_offset, const int64_t length) {
  BitBlockCounter counter(out_valid, out_offset, length);
  int64_t offset = 0;
  while (offset < length) {
    const auto block = counter.NextWord();
    if (block.NoneSet()) {
      CopyValues<Type>(source, offset, block.length, out_valid, out_values,
                       out_offset + offset);
    } else if (!block.AllSet()) {
      for (int64_t j = 0; j < block.length; ++j) {
        if (!BitUtil::GetBit(out_valid, out_offset + offset + j)) {
          CopyValues<Type>(source, offset + j, 1, out_valid, out_values,
                           out_offset + offset + j);
        }
      }
    }
    offset += block.length;
  }
}

// Helper: zero the values buffer of the output wherever the slot is null
void InitializeNullSlots(const DataType& type, uint8_t* out_valid, uint8_t* out_values,
                         const int64_t out_offset, const int64_t length) {
  BitBlockCounter counter(out_valid, out_offset, length);
  int64_t offset = 0;
  auto bit_width = checked_cast<const FixedWidthType&>(type).bit_width();
  auto byte_width = BitUtil::BytesForBits(bit_width);
  while (offset < length) {
    const auto block = counter.NextWord();
    if (block.NoneSet()) {
      if (bit_width == 1) {
        BitUtil::SetBitsTo(out_values, out_offset + offset, block.length, false);
      } else {
        std::memset(out_values + (out_offset + offset) * byte_width, 0x00,
                    byte_width * block.length);
      }
    } else if (!block.AllSet()) {
      for (int64_t j = 0; j < block.length; ++j) {
        if (BitUtil::GetBit(out_valid, out_offset + offset + j)) continue;
        if (bit_width == 1) {
          BitUtil::ClearBit(out_values, out_offset + offset + j);
        } else {
          std::memset(out_values + (out_offset + offset + j) * byte_width, 0x00,
                      byte_width);
        }
      }
    }
    offset += block.length;
  }
}

// Implement 'coalesce' for any mix of scalar/array arguments for any fixed-width type
template <typename Type>
Status ExecArrayCoalesce(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ArrayData* output = out->mutable_array();
  const int64_t out_offset = output->offset;
  // Use output validity buffer as mask to decide what values to copy
  uint8_t* out_valid = output->buffers[0]->mutable_data();
  // Clear output buffer - no values are set initially
  BitUtil::SetBitsTo(out_valid, out_offset, batch.length, false);
  uint8_t* out_values = output->buffers[1]->mutable_data();

  for (const auto& datum : batch.values) {
    if ((datum.is_scalar() && datum.scalar()->is_valid) ||
        (datum.is_array() && !datum.array()->MayHaveNulls())) {
      // Valid scalar, or all-valid array
      CopyValuesAllValid<Type>(datum, out_valid, out_values, out_offset, batch.length);
      break;
    } else if (datum.is_array()) {
      // Array with nulls
      const ArrayData& arr = *datum.array();
      const DataType& type = *datum.type();
      const uint8_t* in_valid = arr.buffers[0]->data();
      const uint8_t* in_values = arr.buffers[1]->data();
      BinaryBitBlockCounter counter(in_valid, arr.offset, out_valid, out_offset,
                                    batch.length);
      int64_t offset = 0;
      while (offset < batch.length) {
        const auto block = counter.NextAndNotWord();
        if (block.AllSet()) {
          CopyValues<Type>(datum, offset, block.length, out_valid, out_values,
                           out_offset + offset);
        } else if (block.popcount) {
          for (int64_t j = 0; j < block.length; ++j) {
            if (!BitUtil::GetBit(out_valid, out_offset + offset + j) &&
                BitUtil::GetBit(in_valid, arr.offset + offset + j)) {
              // This version lets us avoid calling MayHaveNulls() on every iteration
              // (which does an atomic load and can add up)
              CopyOneArrayValue<Type>(type, in_valid, in_values, arr.offset + offset + j,
                                      out_valid, out_values, out_offset + offset + j);
            }
          }
        }
        offset += block.length;
      }
    }
  }

  // Initialize any remaining null slots (uninitialized memory)
  InitializeNullSlots(*out->type(), out_valid, out_values, out_offset, batch.length);
  return Status::OK();
}

template <typename Type, typename Enable = void>
struct CoalesceFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    for (const auto& datum : batch.values) {
      if (datum.is_array()) {
        return ExecArrayCoalesce<Type>(ctx, batch, out);
      }
    }
    return ExecScalarCoalesce(ctx, batch, out);
  }
};

template <>
struct CoalesceFunctor<NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return Status::OK();
  }
};

template <typename Type>
struct CoalesceFunctor<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    for (const auto& datum : batch.values) {
      if (datum.is_array()) {
        return ExecArray(ctx, batch, out);
      }
    }
    return ExecScalarCoalesce(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // Special case: grab any leading non-null scalar or array arguments
    for (const auto& datum : batch.values) {
      if (datum.is_scalar()) {
        if (!datum.scalar()->is_valid) continue;
        ARROW_ASSIGN_OR_RAISE(
            *out, MakeArrayFromScalar(*datum.scalar(), batch.length, ctx->memory_pool()));
        return Status::OK();
      } else if (datum.is_array() && !datum.array()->MayHaveNulls()) {
        *out = datum;
        return Status::OK();
      }
      break;
    }
    ArrayData* output = out->mutable_array();
    BuilderType builder(batch[0].type(), ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    for (int64_t i = 0; i < batch.length; i++) {
      bool set = false;
      for (const auto& datum : batch.values) {
        if (datum.is_scalar()) {
          if (datum.scalar()->is_valid) {
            RETURN_NOT_OK(builder.Append(UnboxScalar<Type>::Unbox(*datum.scalar())));
            set = true;
            break;
          }
        } else {
          const ArrayData& source = *datum.array();
          if (!source.MayHaveNulls() ||
              BitUtil::GetBit(source.buffers[0]->data(), source.offset + i)) {
            const uint8_t* data = source.buffers[2]->data();
            const offset_type* offsets = source.GetValues<offset_type>(1);
            const offset_type offset0 = offsets[i];
            const offset_type offset1 = offsets[i + 1];
            RETURN_NOT_OK(builder.Append(data + offset0, offset1 - offset0));
            set = true;
            break;
          }
        }
      }
      if (!set) RETURN_NOT_OK(builder.AppendNull());
    }
    ARROW_ASSIGN_OR_RAISE(auto temp_output, builder.Finish());
    *output = *temp_output->data();
    // Builder type != logical type due to GenerateTypeAgnosticVarBinaryBase
    output->type = batch[0].type();
    return Status::OK();
  }
};

Result<ValueDescr> LastType(KernelContext*, const std::vector<ValueDescr>& descrs) {
  ValueDescr result = descrs.back();
  result.shape = GetBroadcastShape(descrs);
  return result;
}

void AddCaseWhenKernel(const std::shared_ptr<CaseWhenFunction>& scalar_function,
                       detail::GetTypeId get_id, ArrayKernelExec exec) {
  ScalarKernel kernel(
      KernelSignature::Make({InputType(Type::STRUCT), InputType(get_id.id)},
                            OutputType(LastType),
                            /*is_varargs=*/true),
      exec);
  kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::PREALLOCATE;
  kernel.can_write_into_slices = is_fixed_width(get_id.id);
  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

void AddPrimitiveCaseWhenKernels(const std::shared_ptr<CaseWhenFunction>& scalar_function,
                                 const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = GenerateTypeAgnosticPrimitive<CaseWhenFunctor>(*type);
    AddCaseWhenKernel(scalar_function, type, std::move(exec));
  }
}

void AddCoalesceKernel(const std::shared_ptr<ScalarFunction>& scalar_function,
                       detail::GetTypeId get_id, ArrayKernelExec exec) {
  ScalarKernel kernel(KernelSignature::Make({InputType(get_id.id)}, OutputType(FirstType),
                                            /*is_varargs=*/true),
                      exec);
  kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::PREALLOCATE;
  kernel.can_write_into_slices = is_fixed_width(get_id.id);
  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

void AddPrimitiveCoalesceKernels(const std::shared_ptr<ScalarFunction>& scalar_function,
                                 const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = GenerateTypeAgnosticPrimitive<CoalesceFunctor>(*type);
    AddCoalesceKernel(scalar_function, type, std::move(exec));
  }
}

const FunctionDoc if_else_doc{"Choose values based on a condition",
                              ("`cond` must be a Boolean scalar/ array. \n`left` or "
                               "`right` must be of the same type scalar/ array.\n"
                               "`null` values in `cond` will be promoted to the"
                               " output."),
                              {"cond", "left", "right"}};

const FunctionDoc case_when_doc{
    "Choose values based on multiple conditions",
    ("`cond` must be a struct of Boolean values. `cases` can be a mix "
     "of scalar and array arguments (of any type, but all must be the "
     "same type or castable to a common type), with either exactly one "
     "datum per child of `cond`, or one more `cases` than children of "
     "`cond` (in which case we have an \"else\" value).\n"
     "Each row of the output will be the corresponding value of the "
     "first datum in `cases` for which the corresponding child of `cond` "
     "is true, or otherwise the \"else\" value (if given), or null. "
     "Essentially, this implements a switch-case or if-else, if-else... "
     "statement."),
    {"cond", "*cases"}};

const FunctionDoc coalesce_doc{
    "Select the first non-null value in each slot",
    ("Each row of the output will be the value from the first corresponding input "
     "for which the value is not null. If all inputs are null in a row, the output "
     "will be null."),
    {"*values"}};
}  // namespace

void RegisterScalarIfElse(FunctionRegistry* registry) {
  {
    auto func =
        std::make_shared<IfElseFunction>("if_else", Arity::Ternary(), &if_else_doc);

    AddPrimitiveIfElseKernels(func, NumericTypes());
    AddPrimitiveIfElseKernels(func, TemporalTypes());
    AddPrimitiveIfElseKernels(func, {boolean(), day_time_interval(), month_interval()});
    AddNullIfElseKernel(func);
    AddBinaryIfElseKernels(func, BaseBinaryTypes());
    AddFSBinaryIfElseKernel(func);
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<CaseWhenFunction>(
        "case_when", Arity::VarArgs(/*min_args=*/1), &case_when_doc);
    AddPrimitiveCaseWhenKernels(func, NumericTypes());
    AddPrimitiveCaseWhenKernels(func, TemporalTypes());
    AddPrimitiveCaseWhenKernels(
        func, {boolean(), null(), day_time_interval(), month_interval()});
    AddCaseWhenKernel(func, Type::FIXED_SIZE_BINARY,
                      CaseWhenFunctor<FixedSizeBinaryType>::Exec);
    AddCaseWhenKernel(func, Type::DECIMAL128, CaseWhenFunctor<Decimal128Type>::Exec);
    AddCaseWhenKernel(func, Type::DECIMAL256, CaseWhenFunctor<Decimal256Type>::Exec);
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<CoalesceFunction>(
        "coalesce", Arity::VarArgs(/*min_args=*/1), &coalesce_doc);
    AddPrimitiveCoalesceKernels(func, NumericTypes());
    AddPrimitiveCoalesceKernels(func, TemporalTypes());
    AddPrimitiveCoalesceKernels(
        func, {boolean(), null(), day_time_interval(), month_interval()});
    AddCoalesceKernel(func, Type::FIXED_SIZE_BINARY,
                      CoalesceFunctor<FixedSizeBinaryType>::Exec);
    AddCoalesceKernel(func, Type::DECIMAL128, CoalesceFunctor<Decimal128Type>::Exec);
    AddCoalesceKernel(func, Type::DECIMAL256, CoalesceFunctor<Decimal256Type>::Exec);
    for (const auto& ty : BaseBinaryTypes()) {
      AddCoalesceKernel(func, ty, GenerateTypeAgnosticVarBinaryBase<CoalesceFunctor>(ty));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
