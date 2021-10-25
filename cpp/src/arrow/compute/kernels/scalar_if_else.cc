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

#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/array/builder_union.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"

namespace arrow {

using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::Bitmap;
using internal::BitmapWordReader;
using internal::BitRunReader;

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

// Ensure parameterized types are identical.
Status CheckIdenticalTypes(const Datum* begin, size_t count) {
  const auto& ty = begin->type();
  const auto* end = begin + count;
  for (auto it = begin + 1; it != end; ++it) {
    const DataType& other_ty = *it->type();
    if (!ty->Equals(other_ty)) {
      return Status::TypeError("All types must be compatible, expected: ", *ty,
                               ", but got: ", other_ty);
    }
  }
  return Status::OK();
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
          const uint8_t* scalar_data = UnboxBinaryScalar(scalar);
          uint8_t* start =
              out_array->buffers[1]->mutable_data() + out_array->offset * byte_width;
          for (int64_t i = 0; i < out_array->length; i++) {
            std::memcpy(start + i * byte_width, scalar_data, byte_width);
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
    const uint8_t* left_data = UnboxBinaryScalar(left);

    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      if (left_data) {
        for (int64_t i = 0; i < num_elems; i++) {
          std::memcpy(out_values + (data_offset + i) * byte_width, left_data, byte_width);
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

    const uint8_t* right_data = UnboxBinaryScalar(right);

    RunIfElseLoopInverted(cond, [&](int64_t data_offset, int64_t num_elems) {
      if (right_data) {
        for (int64_t i = 0; i < num_elems; i++) {
          std::memcpy(out_values + (data_offset + i) * byte_width, right_data,
                      byte_width);
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
    const uint8_t* right_data = UnboxBinaryScalar(right);
    if (right_data) {
      for (int64_t i = 0; i < cond.length; i++) {
        std::memcpy(out_values + i * byte_width, right_data, byte_width);
      }
    }

    // selectively copy values from left data
    const uint8_t* left_data = UnboxBinaryScalar(left);
    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      if (left_data) {
        for (int64_t i = 0; i < num_elems; i++) {
          std::memcpy(out_values + (data_offset + i) * byte_width, left_data, byte_width);
        }
      }
    });

    return Status::OK();
  }

  static const uint8_t* UnboxBinaryScalar(const Scalar& scalar) {
    return reinterpret_cast<const uint8_t*>(
        checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).view().data());
  }

  template <typename T = Type>
  static enable_if_t<!is_decimal_type<T>::value, Result<int32_t>> GetByteWidth(
      const DataType& left_type, const DataType& right_type) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(left_type).byte_width();
    DCHECK_EQ(width, checked_cast<const FixedSizeBinaryType&>(right_type).byte_width());
    return width;
  }

  template <typename T = Type>
  static enable_if_decimal<T, Result<int32_t>> GetByteWidth(const DataType& left_type,
                                                            const DataType& right_type) {
    const auto& left = checked_cast<const T&>(left_type);
    const auto& right = checked_cast<const T&>(right_type);
    DCHECK_EQ(left.precision(), right.precision());
    DCHECK_EQ(left.scale(), right.scale());
    return left.byte_width();
  }
};

// Use builders for dictionaries - slower, but allows us to unify dictionaries
struct NestedIfElseExec {
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
    return RunLoop(
        ctx, cond, out,
        [&](ArrayBuilder* builder, int64_t i, int64_t length) {
          return builder->AppendArraySlice(left, i, length);
        },
        [&](ArrayBuilder* builder, int64_t i, int64_t length) {
          return builder->AppendArraySlice(right, i, length);
        });
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const ArrayData& right, ArrayData* out) {
    return RunLoop(
        ctx, cond, out,
        [&](ArrayBuilder* builder, int64_t i, int64_t length) {
          return builder->AppendScalar(left, length);
        },
        [&](ArrayBuilder* builder, int64_t i, int64_t length) {
          return builder->AppendArraySlice(right, i, length);
        });
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    return RunLoop(
        ctx, cond, out,
        [&](ArrayBuilder* builder, int64_t i, int64_t length) {
          return builder->AppendArraySlice(left, i, length);
        },
        [&](ArrayBuilder* builder, int64_t i, int64_t length) {
          return builder->AppendScalar(right, length);
        });
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArrayData& cond, const Scalar& left,
                     const Scalar& right, ArrayData* out) {
    return RunLoop(
        ctx, cond, out,
        [&](ArrayBuilder* builder, int64_t i, int64_t length) {
          return builder->AppendScalar(left, length);
        },
        [&](ArrayBuilder* builder, int64_t i, int64_t length) {
          return builder->AppendScalar(right, length);
        });
  }

  template <typename HandleLeft, typename HandleRight>
  static Status RunLoop(KernelContext* ctx, const ArrayData& cond, ArrayData* out,
                        HandleLeft&& handle_left, HandleRight&& handle_right) {
    std::unique_ptr<ArrayBuilder> raw_builder;
    RETURN_NOT_OK(MakeBuilderExactIndex(ctx->memory_pool(), out->type, &raw_builder));
    RETURN_NOT_OK(raw_builder->Reserve(out->length));

    const auto* cond_data = cond.buffers[1]->data();
    if (cond.buffers[0]) {
      BitRunReader reader(cond.buffers[0]->data(), cond.offset, cond.length);
      int64_t position = 0;
      while (true) {
        auto run = reader.NextRun();
        if (run.length == 0) break;
        if (run.set) {
          for (int j = 0; j < run.length; j++) {
            if (BitUtil::GetBit(cond_data, cond.offset + position + j)) {
              RETURN_NOT_OK(handle_left(raw_builder.get(), position + j, 1));
            } else {
              RETURN_NOT_OK(handle_right(raw_builder.get(), position + j, 1));
            }
          }
        } else {
          RETURN_NOT_OK(raw_builder->AppendNulls(run.length));
        }
        position += run.length;
      }
    } else {
      BitRunReader reader(cond_data, cond.offset, cond.length);
      int64_t position = 0;
      while (true) {
        auto run = reader.NextRun();
        if (run.length == 0) break;
        if (run.set) {
          RETURN_NOT_OK(handle_left(raw_builder.get(), position, run.length));
        } else {
          RETURN_NOT_OK(handle_right(raw_builder.get(), position, run.length));
        }
        position += run.length;
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto out_arr, raw_builder->Finish());
    *out = std::move(*out_arr->data());
    return Status::OK();
  }

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[1], /*count=*/2));
    if (batch[0].is_scalar()) {
      const auto& cond = batch[0].scalar_as<BooleanScalar>();
      return Call(ctx, cond, batch[1], batch[2], out);
    }
    if (batch[1].kind() == Datum::ARRAY) {
      if (batch[2].kind() == Datum::ARRAY) {  // AAA
        return Call(ctx, *batch[0].array(), *batch[1].array(), *batch[2].array(),
                    out->mutable_array());
      } else {  // AAS
        return Call(ctx, *batch[0].array(), *batch[1].array(), *batch[2].scalar(),
                    out->mutable_array());
      }
    } else {
      if (batch[2].kind() == Datum::ARRAY) {  // ASA
        return Call(ctx, *batch[0].array(), *batch[1].scalar(), *batch[2].array(),
                    out->mutable_array());
      } else {  // ASS
        return Call(ctx, *batch[0].array(), *batch[1].scalar(), *batch[2].scalar(),
                    out->mutable_array());
      }
    }
  }
};

template <typename Type, typename AllocateMem>
struct ResolveIfElseExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // Check is unconditional because parametric types like timestamp
    // are templated as integer
    RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[1], /*count=*/2));

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
    // Do not DispatchExact here because it'll let through something like (bool,
    // timestamp[s], timestamp[s, "UTC"])

    // if 0th descriptor is null, replace with bool
    if (values->at(0).type->id() == Type::NA) {
      values->at(0).type = boolean();
    }

    // if-else 0'th descriptor is bool, so skip it
    ValueDescr* left_arg = &(*values)[1];
    constexpr size_t num_args = 2;

    internal::ReplaceNullWithOtherType(left_arg, num_args);

    // If both are identical dictionary types, dispatch to the dictionary kernel
    // TODO(ARROW-14105): apply implicit casts to dictionary types too
    ValueDescr* right_arg = &(*values)[2];
    if (is_dictionary(left_arg->type->id()) && left_arg->type->Equals(right_arg->type)) {
      auto kernel = DispatchExactImpl(this, *values);
      DCHECK(kernel);
      return kernel;
    }

    internal::EnsureDictionaryDecoded(left_arg, num_args);

    if (auto type = internal::CommonNumeric(left_arg, num_args)) {
      internal::ReplaceTypes(type, left_arg, num_args);
    } else if (auto type = internal::CommonTemporal(left_arg, num_args)) {
      internal::ReplaceTypes(type, left_arg, num_args);
    } else if (auto type = internal::CommonBinary(left_arg, num_args)) {
      internal::ReplaceTypes(type, left_arg, num_args);
    } else if (HasDecimal(*values)) {
      RETURN_NOT_OK(CastDecimalArgs(left_arg, num_args));
    }

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
    std::shared_ptr<KernelSignature> sig;
    if (type->id() == Type::TIMESTAMP) {
      auto unit = checked_cast<const TimestampType&>(*type).unit();
      sig = KernelSignature::Make(
          {boolean(), match::TimestampTypeUnit(unit), match::TimestampTypeUnit(unit)},
          OutputType(LastType));
    } else {
      sig = KernelSignature::Make({boolean(), type, type}, type);
    }
    ScalarKernel kernel(std::move(sig), exec);
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

template <typename T>
void AddFixedWidthIfElseKernel(const std::shared_ptr<IfElseFunction>& scalar_function) {
  auto type_id = T::type_id;
  ScalarKernel kernel({boolean(), InputType(type_id), InputType(type_id)},
                      OutputType(LastType),
                      ResolveIfElseExec<T, /*AllocateMem=*/std::false_type>::Exec);
  kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::PREALLOCATE;
  kernel.can_write_into_slices = true;

  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

void AddNestedIfElseKernels(const std::shared_ptr<IfElseFunction>& scalar_function) {
  for (const auto type_id :
       {Type::LIST, Type::LARGE_LIST, Type::FIXED_SIZE_LIST, Type::STRUCT,
        Type::DENSE_UNION, Type::SPARSE_UNION, Type::DICTIONARY}) {
    ScalarKernel kernel({boolean(), InputType(type_id), InputType(type_id)},
                        OutputType(LastType), NestedIfElseExec::Exec);
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    kernel.can_write_into_slices = false;
    DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
  }
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
struct CopyFixedWidth<Type, enable_if_fixed_size_binary<Type>> {
  static void CopyScalar(const Scalar& values, const int64_t length,
                         uint8_t* raw_out_values, const int64_t out_offset) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(*values.type).byte_width();
    uint8_t* next = raw_out_values + (width * out_offset);
    const auto& scalar =
        checked_cast<const arrow::internal::PrimitiveScalarBase&>(values);
    // Scalar may have null value buffer
    if (!scalar.is_valid) {
      std::memset(next, 0x00, width * length);
    } else {
      util::string_view view = scalar.view();
      DCHECK_EQ(view.size(), static_cast<size_t>(width));
      for (int i = 0; i < length; i++) {
        std::memcpy(next, view.data(), width);
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

template <typename Type>
void CopyOneScalarValue(const Scalar& scalar, uint8_t* out_valid, uint8_t* out_values,
                        const int64_t out_offset) {
  if (out_valid) {
    BitUtil::SetBitTo(out_valid, out_offset, scalar.is_valid);
  }
  CopyFixedWidth<Type>::CopyScalar(scalar, /*length=*/1, out_values, out_offset);
}

template <typename Type>
void CopyOneValue(const Datum& in_values, const int64_t in_offset, uint8_t* out_valid,
                  uint8_t* out_values, const int64_t out_offset) {
  if (in_values.is_array()) {
    const ArrayData& array = *in_values.array();
    CopyOneArrayValue<Type>(*array.type, array.GetValues<uint8_t>(0, 0),
                            array.GetValues<uint8_t>(1, 0), array.offset + in_offset,
                            out_valid, out_values, out_offset);
  } else {
    CopyOneScalarValue<Type>(*in_values.scalar(), out_valid, out_values, out_offset);
  }
}

struct CaseWhenFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    // The first function is a struct of booleans, where the number of fields in the
    // struct is either equal to the number of other arguments or is one less.
    RETURN_NOT_OK(CheckArity(*values));
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

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);
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
  if (is_dictionary_type<Type>::value) {
    const Datum& dict_from = result.is_value() ? result : batch[1];
    if (dict_from.is_scalar()) {
      output->dictionary = checked_cast<const DictionaryScalar&>(*dict_from.scalar())
                               .value.dictionary->data();
    } else {
      output->dictionary = dict_from.array()->dictionary;
    }
  }
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

Status ExecVarWidthScalarCaseWhen(KernelContext* ctx, const ExecBatch& batch,
                                  Datum* out) {
  const auto& conds = checked_cast<const StructScalar&>(*batch.values[0].scalar());
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
    DCHECK(result.is_scalar() || result.kind() == Datum::NONE);
    *out = result.is_scalar() ? result.scalar() : MakeNullScalar(out->type());
    return Status::OK();
  }
  ArrayData* output = out->mutable_array();
  if (!result.is_value()) {
    // All conditions false, no 'else' argument
    ARROW_ASSIGN_OR_RAISE(
        auto array, MakeArrayOfNull(output->type, batch.length, ctx->memory_pool()));
    *output = *array->data();
  } else if (result.is_scalar()) {
    ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(*result.scalar(), batch.length,
                                                          ctx->memory_pool()));
    *output = *array->data();
  } else {
    *output = *result.array();
  }
  return Status::OK();
}

// Use std::function for reserve_data to avoid instantiating template so much
template <typename AppendScalar>
static Status ExecVarWidthArrayCaseWhenImpl(
    KernelContext* ctx, const ExecBatch& batch, Datum* out,
    std::function<Status(ArrayBuilder*)> reserve_data, AppendScalar append_scalar) {
  const auto& conds_array = *batch.values[0].array();
  ArrayData* output = out->mutable_array();
  const bool have_else_arg =
      static_cast<size_t>(conds_array.type->num_fields()) < (batch.values.size() - 1);
  std::unique_ptr<ArrayBuilder> raw_builder;
  RETURN_NOT_OK(MakeBuilderExactIndex(ctx->memory_pool(), out->type(), &raw_builder));
  RETURN_NOT_OK(raw_builder->Reserve(batch.length));
  RETURN_NOT_OK(reserve_data(raw_builder.get()));

  for (int64_t row = 0; row < batch.length; row++) {
    int64_t selected = have_else_arg ? static_cast<int64_t>(batch.values.size() - 1) : -1;
    for (int64_t arg = 0; static_cast<size_t>(arg) < conds_array.child_data.size();
         arg++) {
      const ArrayData& cond_array = *conds_array.child_data[arg];
      if ((!cond_array.buffers[0] ||
           BitUtil::GetBit(cond_array.buffers[0]->data(),
                           conds_array.offset + cond_array.offset + row)) &&
          BitUtil::GetBit(cond_array.buffers[1]->data(),
                          conds_array.offset + cond_array.offset + row)) {
        selected = arg + 1;
        break;
      }
    }
    if (selected < 0) {
      RETURN_NOT_OK(raw_builder->AppendNull());
      continue;
    }
    const Datum& source = batch.values[selected];
    if (source.is_scalar()) {
      const auto& scalar = *source.scalar();
      if (!scalar.is_valid) {
        RETURN_NOT_OK(raw_builder->AppendNull());
      } else {
        RETURN_NOT_OK(append_scalar(raw_builder.get(), scalar));
      }
    } else {
      const auto& array = source.array();
      if (!array->buffers[0] ||
          BitUtil::GetBit(array->buffers[0]->data(), array->offset + row)) {
        RETURN_NOT_OK(raw_builder->AppendArraySlice(*array, row, /*length=*/1));
      } else {
        RETURN_NOT_OK(raw_builder->AppendNull());
      }
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto temp_output, raw_builder->Finish());
  *output = *temp_output->data();
  return Status::OK();
}

// Single instantiation using ArrayBuilder::AppendScalar for append_scalar
static Status ExecVarWidthArrayCaseWhen(
    KernelContext* ctx, const ExecBatch& batch, Datum* out,
    std::function<Status(ArrayBuilder*)> reserve_data) {
  return ExecVarWidthArrayCaseWhenImpl(
      ctx, batch, out, std::move(reserve_data),
      [](ArrayBuilder* raw_builder, const Scalar& scalar) {
        return raw_builder->AppendScalar(scalar);
      });
}

template <typename Type>
struct CaseWhenFunctor<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return ExecVarWidthArrayCaseWhenImpl(
        ctx, batch, out,
        // ReserveData
        [&](ArrayBuilder* raw_builder) {
          int64_t reservation = 0;
          for (size_t arg = 1; arg < batch.values.size(); arg++) {
            auto source = batch.values[arg];
            if (source.is_scalar()) {
              const auto& scalar =
                  checked_cast<const BaseBinaryScalar&>(*source.scalar());
              if (!scalar.value) continue;
              reservation =
                  std::max<int64_t>(reservation, batch.length * scalar.value->size());
            } else {
              const auto& array = *source.array();
              const auto& offsets = array.GetValues<offset_type>(1);
              reservation =
                  std::max<int64_t>(reservation, offsets[array.length] - offsets[0]);
            }
          }
          // checked_cast works since (Large)StringBuilder <: (Large)BinaryBuilder
          return checked_cast<BuilderType*>(raw_builder)->ReserveData(reservation);
        },
        // AppendScalar
        [](ArrayBuilder* raw_builder, const Scalar& raw_scalar) {
          const auto& scalar = checked_cast<const BaseBinaryScalar&>(raw_scalar);
          return checked_cast<BuilderType*>(raw_builder)
              ->Append(scalar.value->data(),
                       static_cast<offset_type>(scalar.value->size()));
        });
  }
};

template <typename Type>
struct CaseWhenFunctor<Type, enable_if_var_size_list<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return ExecVarWidthArrayCaseWhen(
        ctx, batch, out,
        // ReserveData
        [&](ArrayBuilder* raw_builder) {
          auto builder = checked_cast<BuilderType*>(raw_builder);
          auto child_builder = builder->value_builder();

          int64_t reservation = 0;
          for (size_t arg = 1; arg < batch.values.size(); arg++) {
            auto source = batch.values[arg];
            if (!source.is_array()) {
              const auto& scalar = checked_cast<const BaseListScalar&>(*source.scalar());
              if (!scalar.value) continue;
              reservation =
                  std::max<int64_t>(reservation, batch.length * scalar.value->length());
            } else {
              const auto& array = *source.array();
              reservation = std::max<int64_t>(reservation, array.child_data[0]->length);
            }
          }
          return child_builder->Reserve(reservation);
        });
  }
};

// No-op reserve function, pulled out to avoid apparent miscompilation on MinGW
Status ReserveNoData(ArrayBuilder*) { return Status::OK(); }

template <>
struct CaseWhenFunctor<MapType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    std::function<Status(ArrayBuilder*)> reserve_data = ReserveNoData;
    return ExecVarWidthArrayCaseWhen(ctx, batch, out, std::move(reserve_data));
  }
};

template <>
struct CaseWhenFunctor<StructType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    std::function<Status(ArrayBuilder*)> reserve_data = ReserveNoData;
    return ExecVarWidthArrayCaseWhen(ctx, batch, out, std::move(reserve_data));
  }
};

template <>
struct CaseWhenFunctor<FixedSizeListType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& ty = checked_cast<const FixedSizeListType&>(*out->type());
    const int64_t width = ty.list_size();
    return ExecVarWidthArrayCaseWhen(
        ctx, batch, out,
        // ReserveData
        [&](ArrayBuilder* raw_builder) {
          int64_t children = batch.length * width;
          return checked_cast<FixedSizeListBuilder*>(raw_builder)
              ->value_builder()
              ->Reserve(children);
        });
  }
};

template <typename Type>
struct CaseWhenFunctor<Type, enable_if_union<Type>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    std::function<Status(ArrayBuilder*)> reserve_data = ReserveNoData;
    return ExecVarWidthArrayCaseWhen(ctx, batch, out, std::move(reserve_data));
  }
};

template <>
struct CaseWhenFunctor<DictionaryType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    std::function<Status(ArrayBuilder*)> reserve_data = ReserveNoData;
    return ExecVarWidthArrayCaseWhen(ctx, batch, out, std::move(reserve_data));
  }
};

struct CoalesceFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));
    using arrow::compute::detail::DispatchExactImpl;
    // Do not DispatchExact here since we want to rescale decimals if necessary
    EnsureDictionaryDecoded(values);
    if (auto type = CommonNumeric(*values)) {
      ReplaceTypes(type, values);
    }
    if (auto type = CommonBinary(values->data(), values->size())) {
      ReplaceTypes(type, values);
    }
    if (auto type = CommonTemporal(values->data(), values->size())) {
      ReplaceTypes(type, values);
    }
    if (HasDecimal(*values)) {
      RETURN_NOT_OK(CastDecimalArgs(values->data(), values->size()));
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
  BitRunReader bit_reader(out_valid, out_offset, length);
  int64_t offset = 0;
  while (true) {
    const auto run = bit_reader.NextRun();
    if (run.length == 0) {
      break;
    }
    if (!run.set) {
      CopyValues<Type>(source, offset, run.length, out_valid, out_values,
                       out_offset + offset);
    }
    offset += run.length;
  }
  DCHECK_EQ(offset, length);
}

// Helper: zero the values buffer of the output wherever the slot is null
void InitializeNullSlots(const DataType& type, uint8_t* out_valid, uint8_t* out_values,
                         const int64_t out_offset, const int64_t length) {
  BitRunReader bit_reader(out_valid, out_offset, length);
  int64_t offset = 0;
  const auto bit_width = checked_cast<const FixedWidthType&>(type).bit_width();
  const auto byte_width = BitUtil::BytesForBits(bit_width);
  while (true) {
    const auto run = bit_reader.NextRun();
    if (run.length == 0) {
      break;
    }
    if (!run.set) {
      if (bit_width == 1) {
        BitUtil::SetBitsTo(out_values, out_offset + offset, run.length, false);
      } else {
        std::memset(out_values + (out_offset + offset) * byte_width, 0,
                    byte_width * run.length);
      }
    }
    offset += run.length;
  }
  DCHECK_EQ(offset, length);
}

// Implement 'coalesce' for any mix of scalar/array arguments for any fixed-width type
template <typename Type>
Status ExecArrayCoalesce(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ArrayData* output = out->mutable_array();
  const int64_t out_offset = output->offset;
  // Use output validity buffer as mask to decide what values to copy
  uint8_t* out_valid = output->buffers[0]->mutable_data();

  // Clear output validity buffer - no values are set initially
  BitUtil::SetBitsTo(out_valid, out_offset, batch.length, false);
  uint8_t* out_values = output->buffers[1]->mutable_data();

  for (const auto& datum : batch.values) {
    if (datum.null_count() == 0) {
      // Valid scalar, or all-valid array
      CopyValuesAllValid<Type>(datum, out_valid, out_values, out_offset, batch.length);
      break;
    } else if (datum.is_array()) {
      // Array with nulls
      const ArrayData& arr = *datum.array();
      const int64_t in_offset = arr.offset;
      const int64_t in_null_count = arr.null_count;
      DCHECK_GT(in_null_count, 0);  // computed in datum.null_count()
      const DataType& type = *arr.type;
      const uint8_t* in_valid = arr.buffers[0]->data();
      const uint8_t* in_values = arr.buffers[1]->data();

      if (in_null_count < 0.8 * batch.length) {
        // The input is not mostly null, we deem it more efficient to
        // copy values even underlying null slots instead of the more
        // expensive bitmasking using BinaryBitBlockCounter.
        BitRunReader bit_reader(out_valid, out_offset, batch.length);
        int64_t offset = 0;
        while (true) {
          const auto run = bit_reader.NextRun();
          if (run.length == 0) {
            break;
          }
          if (!run.set) {
            // Copy from input
            CopyFixedWidth<Type>::CopyArray(type, in_values, in_offset + offset,
                                            run.length, out_values, out_offset + offset);
          }
          offset += run.length;
        }
        arrow::internal::BitmapOr(out_valid, out_offset, in_valid, in_offset,
                                  batch.length, out_offset, out_valid);
      } else {
        BinaryBitBlockCounter counter(in_valid, in_offset, out_valid, out_offset,
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
                  BitUtil::GetBit(in_valid, in_offset + offset + j)) {
                // This version lets us avoid calling MayHaveNulls() on every iteration
                // (which does an atomic load and can add up)
                CopyOneArrayValue<Type>(type, in_valid, in_values, in_offset + offset + j,
                                        out_valid, out_values, out_offset + offset + j);
              }
            }
          }
          offset += block.length;
        }
      }
    }
  }

  // Initialize any remaining null slots (uninitialized memory)
  InitializeNullSlots(*out->type(), out_valid, out_values, out_offset, batch.length);
  return Status::OK();
}

// Special case: implement 'coalesce' for an array and a scalar for any
// fixed-width type (a 'fill_null' operation)
template <typename Type>
Status ExecArrayScalarCoalesce(KernelContext* ctx, Datum left, Datum right,
                               int64_t length, Datum* out) {
  ArrayData* output = out->mutable_array();
  const int64_t out_offset = output->offset;
  uint8_t* out_valid = output->buffers[0]->mutable_data();
  uint8_t* out_values = output->buffers[1]->mutable_data();

  const ArrayData& left_arr = *left.array();
  const uint8_t* left_valid = left_arr.buffers[0]->data();
  const uint8_t* left_values = left_arr.buffers[1]->data();
  const Scalar& right_scalar = *right.scalar();

  if (left.null_count() < length * 0.2) {
    // There are less than 20% nulls in the left array, so first copy
    // the left values, then fill any nulls with the right value
    CopyFixedWidth<Type>::CopyArray(*left_arr.type, left_values, left_arr.offset, length,
                                    out_values, out_offset);

    BitRunReader reader(left_valid, left_arr.offset, left_arr.length);
    int64_t offset = 0;
    while (true) {
      const auto run = reader.NextRun();
      if (run.length == 0) break;
      if (!run.set) {
        // All from right
        CopyFixedWidth<Type>::CopyScalar(right_scalar, run.length, out_values,
                                         out_offset + offset);
      }
      offset += run.length;
    }
    DCHECK_EQ(offset, length);
  } else {
    BitRunReader reader(left_valid, left_arr.offset, left_arr.length);
    int64_t offset = 0;
    while (true) {
      const auto run = reader.NextRun();
      if (run.length == 0) break;
      if (run.set) {
        // All from left
        CopyFixedWidth<Type>::CopyArray(*left_arr.type, left_values,
                                        left_arr.offset + offset, run.length, out_values,
                                        out_offset + offset);
      } else {
        // All from right
        CopyFixedWidth<Type>::CopyScalar(right_scalar, run.length, out_values,
                                         out_offset + offset);
      }
      offset += run.length;
    }
    DCHECK_EQ(offset, length);
  }

  if (right_scalar.is_valid || !left_valid) {
    BitUtil::SetBitsTo(out_valid, out_offset, length, true);
  } else {
    arrow::internal::CopyBitmap(left_valid, left_arr.offset, length, out_valid,
                                out_offset);
  }
  return Status::OK();
}

// Special case: implement 'coalesce' for any 2 arguments for any fixed-width
// type (a 'fill_null' operation)
template <typename Type>
Status ExecBinaryCoalesce(KernelContext* ctx, Datum left, Datum right, int64_t length,
                          Datum* out) {
  if (left.is_scalar() && right.is_scalar()) {
    // Both scalar
    *out = left.scalar()->is_valid ? left : right;
    return Status::OK();
  }

  ArrayData* output = out->mutable_array();
  const int64_t out_offset = output->offset;
  uint8_t* out_valid = output->buffers[0]->mutable_data();
  uint8_t* out_values = output->buffers[1]->mutable_data();

  const int64_t left_null_count = left.null_count();
  const int64_t right_null_count = right.null_count();

  if (left.is_scalar()) {
    // (Scalar, Any)
    CopyValues<Type>(left.scalar()->is_valid ? left : right, /*in_offset=*/0, length,
                     out_valid, out_values, out_offset);
    return Status::OK();
  } else if (left_null_count == 0) {
    // LHS is array without nulls. Must copy (since we preallocate)
    CopyValues<Type>(left, /*in_offset=*/0, length, out_valid, out_values, out_offset);
    return Status::OK();
  } else if (right.is_scalar()) {
    // (Array, Scalar)
    return ExecArrayScalarCoalesce<Type>(ctx, left, right, length, out);
  }

  // (Array, Array)
  const ArrayData& left_arr = *left.array();
  const ArrayData& right_arr = *right.array();
  const uint8_t* left_valid = left_arr.buffers[0]->data();
  const uint8_t* left_values = left_arr.buffers[1]->data();
  const uint8_t* right_valid =
      right_null_count > 0 ? right_arr.buffers[0]->data() : nullptr;
  const uint8_t* right_values = right_arr.buffers[1]->data();

  BitRunReader bit_reader(left_valid, left_arr.offset, left_arr.length);
  int64_t offset = 0;
  while (true) {
    const auto run = bit_reader.NextRun();
    if (run.length == 0) {
      break;
    }
    if (run.set) {
      // All from left
      CopyFixedWidth<Type>::CopyArray(*left_arr.type, left_values,
                                      left_arr.offset + offset, run.length, out_values,
                                      out_offset + offset);
    } else {
      // All from right
      CopyFixedWidth<Type>::CopyArray(*right_arr.type, right_values,
                                      right_arr.offset + offset, run.length, out_values,
                                      out_offset + offset);
    }
    offset += run.length;
  }
  DCHECK_EQ(offset, length);

  if (right_null_count == 0) {
    BitUtil::SetBitsTo(out_valid, out_offset, length, true);
  } else {
    arrow::internal::BitmapOr(left_valid, left_arr.offset, right_valid, right_arr.offset,
                              length, out_offset, out_valid);
  }
  return Status::OK();
}

template <typename AppendScalar>
static Status ExecVarWidthCoalesceImpl(KernelContext* ctx, const ExecBatch& batch,
                                       Datum* out,
                                       std::function<Status(ArrayBuilder*)> reserve_data,
                                       AppendScalar append_scalar) {
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
  std::unique_ptr<ArrayBuilder> raw_builder;
  RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), out->type(), &raw_builder));
  RETURN_NOT_OK(raw_builder->Reserve(batch.length));
  RETURN_NOT_OK(reserve_data(raw_builder.get()));

  for (int64_t i = 0; i < batch.length; i++) {
    bool set = false;
    for (const auto& datum : batch.values) {
      if (datum.is_scalar()) {
        if (datum.scalar()->is_valid) {
          RETURN_NOT_OK(append_scalar(raw_builder.get(), *datum.scalar()));
          set = true;
          break;
        }
      } else {
        const ArrayData& source = *datum.array();
        if (!source.MayHaveNulls() ||
            BitUtil::GetBit(source.buffers[0]->data(), source.offset + i)) {
          RETURN_NOT_OK(raw_builder->AppendArraySlice(source, i, /*length=*/1));
          set = true;
          break;
        }
      }
    }
    if (!set) RETURN_NOT_OK(raw_builder->AppendNull());
  }
  ARROW_ASSIGN_OR_RAISE(auto temp_output, raw_builder->Finish());
  *output = *temp_output->data();
  output->type = batch[0].type();
  return Status::OK();
}

static Status ExecVarWidthCoalesce(KernelContext* ctx, const ExecBatch& batch, Datum* out,
                                   std::function<Status(ArrayBuilder*)> reserve_data) {
  return ExecVarWidthCoalesceImpl(ctx, batch, out, std::move(reserve_data),
                                  [](ArrayBuilder* builder, const Scalar& scalar) {
                                    return builder->AppendScalar(scalar);
                                  });
}

template <typename Type, typename Enable = void>
struct CoalesceFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (!TypeTraits<Type>::is_parameter_free) {
      RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[0], batch.values.size()));
    }
    // Special case for two arguments (since "fill_null" is a common operation)
    if (batch.num_values() == 2) {
      return ExecBinaryCoalesce<Type>(ctx, batch[0], batch[1], batch.length, out);
    }
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
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch.num_values() == 2 && batch.values[0].is_array() &&
        batch.values[1].is_scalar()) {
      // Specialized implementation for common case ('fill_null' operation)
      return ExecArrayScalar(ctx, *batch.values[0].array(), *batch.values[1].scalar(),
                             out);
    }
    for (const auto& datum : batch.values) {
      if (datum.is_array()) {
        return ExecArray(ctx, batch, out);
      }
    }
    return ExecScalarCoalesce(ctx, batch, out);
  }

  static Status ExecArrayScalar(KernelContext* ctx, const ArrayData& left,
                                const Scalar& right, Datum* out) {
    const int64_t null_count = left.GetNullCount();
    if (null_count == 0 || !right.is_valid) {
      *out = left;
      return Status::OK();
    }
    ArrayData* output = out->mutable_array();
    BuilderType builder(left.type, ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(left.length));
    const auto& scalar = checked_cast<const BaseBinaryScalar&>(right);
    const offset_type* offsets = left.GetValues<offset_type>(1);
    const int64_t data_reserve = static_cast<int64_t>(offsets[left.length] - offsets[0]) +
                                 null_count * scalar.value->size();
    if (data_reserve > std::numeric_limits<offset_type>::max()) {
      return Status::CapacityError(
          "Result will not fit in a 32-bit binary-like array, convert to large type");
    }
    RETURN_NOT_OK(builder.ReserveData(static_cast<offset_type>(data_reserve)));

    util::string_view fill_value(*scalar.value);
    VisitArrayDataInline<Type>(
        left, [&](util::string_view s) { builder.UnsafeAppend(s); },
        [&]() { builder.UnsafeAppend(fill_value); });

    ARROW_ASSIGN_OR_RAISE(auto temp_output, builder.Finish());
    *output = *temp_output->data();
    output->type = left.type;
    return Status::OK();
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return ExecVarWidthCoalesceImpl(
        ctx, batch, out,
        [&](ArrayBuilder* builder) {
          int64_t reservation = 0;
          for (const auto& datum : batch.values) {
            if (datum.is_array()) {
              const ArrayType array(datum.array());
              reservation = std::max<int64_t>(reservation, array.total_values_length());
            } else {
              const auto& scalar = *datum.scalar();
              if (scalar.is_valid) {
                const int64_t size = UnboxScalar<Type>::Unbox(scalar).size();
                reservation = std::max<int64_t>(reservation, batch.length * size);
              }
            }
          }
          return checked_cast<BuilderType*>(builder)->ReserveData(reservation);
        },
        [&](ArrayBuilder* builder, const Scalar& scalar) {
          return checked_cast<BuilderType*>(builder)->Append(
              UnboxScalar<Type>::Unbox(scalar));
        });
  }
};

template <typename Type>
struct CoalesceFunctor<
    Type, enable_if_t<is_nested_type<Type>::value && !is_union_type<Type>::value>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[0], batch.values.size()));
    for (const auto& datum : batch.values) {
      if (datum.is_array()) {
        return ExecArray(ctx, batch, out);
      }
    }
    return ExecScalarCoalesce(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    std::function<Status(ArrayBuilder*)> reserve_data = ReserveNoData;
    return ExecVarWidthCoalesce(ctx, batch, out, reserve_data);
  }
};

template <typename Type>
struct CoalesceFunctor<Type, enable_if_union<Type>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // Unions don't have top-level nulls, so a specialized implementation is needed
    RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[0], batch.values.size()));

    for (const auto& datum : batch.values) {
      if (datum.is_array()) {
        return ExecArray(ctx, batch, out);
      }
    }
    return ExecScalar(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayData* output = out->mutable_array();
    std::unique_ptr<ArrayBuilder> raw_builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), out->type(), &raw_builder));
    RETURN_NOT_OK(raw_builder->Reserve(batch.length));

    const UnionType& type = checked_cast<const UnionType&>(*out->type());
    for (int64_t i = 0; i < batch.length; i++) {
      bool set = false;
      for (const auto& datum : batch.values) {
        if (datum.is_scalar()) {
          const auto& scalar = checked_cast<const UnionScalar&>(*datum.scalar());
          if (scalar.is_valid && scalar.value->is_valid) {
            RETURN_NOT_OK(raw_builder->AppendScalar(scalar));
            set = true;
            break;
          }
        } else {
          const ArrayData& source = *datum.array();
          // Peek at the relevant child array's validity bitmap
          if (std::is_same<Type, SparseUnionType>::value) {
            const int8_t type_id = source.GetValues<int8_t>(1)[i];
            const int child_id = type.child_ids()[type_id];
            const ArrayData& child = *source.child_data[child_id];
            if (!child.MayHaveNulls() ||
                BitUtil::GetBit(child.buffers[0]->data(),
                                source.offset + child.offset + i)) {
              RETURN_NOT_OK(raw_builder->AppendArraySlice(source, i, /*length=*/1));
              set = true;
              break;
            }
          } else {
            const int8_t type_id = source.GetValues<int8_t>(1)[i];
            const int32_t offset = source.GetValues<int32_t>(2)[i];
            const int child_id = type.child_ids()[type_id];
            const ArrayData& child = *source.child_data[child_id];
            if (!child.MayHaveNulls() ||
                BitUtil::GetBit(child.buffers[0]->data(), child.offset + offset)) {
              RETURN_NOT_OK(raw_builder->AppendArraySlice(source, i, /*length=*/1));
              set = true;
              break;
            }
          }
        }
      }
      if (!set) RETURN_NOT_OK(raw_builder->AppendNull());
    }
    ARROW_ASSIGN_OR_RAISE(auto temp_output, raw_builder->Finish());
    *output = *temp_output->data();
    return Status::OK();
  }

  static Status ExecScalar(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    for (const auto& datum : batch.values) {
      const auto& scalar = checked_cast<const UnionScalar&>(*datum.scalar());
      // Union scalars can have top-level validity
      if (scalar.is_valid && scalar.value->is_valid) {
        *out = datum;
        break;
      }
    }
    return Status::OK();
  }
};

template <typename Type>
Status ExecScalarChoose(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& index_scalar = *batch[0].scalar();
  if (!index_scalar.is_valid) {
    if (out->is_array()) {
      auto source = MakeNullScalar(out->type());
      ArrayData* output = out->mutable_array();
      CopyValues<Type>(source, /*row=*/0, batch.length,
                       output->GetMutableValues<uint8_t>(0, /*absolute_offset=*/0),
                       output->GetMutableValues<uint8_t>(1, /*absolute_offset=*/0),
                       output->offset);
    }
    return Status::OK();
  }
  auto index = UnboxScalar<Int64Type>::Unbox(index_scalar);
  if (index < 0 || static_cast<size_t>(index + 1) >= batch.values.size()) {
    return Status::IndexError("choose: index ", index, " out of range");
  }
  auto source = batch.values[index + 1];
  if (out->is_scalar()) {
    *out = source;
  } else {
    ArrayData* output = out->mutable_array();
    CopyValues<Type>(source, /*row=*/0, batch.length,
                     output->GetMutableValues<uint8_t>(0, /*absolute_offset=*/0),
                     output->GetMutableValues<uint8_t>(1, /*absolute_offset=*/0),
                     output->offset);
  }
  return Status::OK();
}

template <typename Type>
Status ExecArrayChoose(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ArrayData* output = out->mutable_array();
  const int64_t out_offset = output->offset;
  // Need a null bitmap if any input has nulls
  uint8_t* out_valid = nullptr;
  if (std::any_of(batch.values.begin(), batch.values.end(),
                  [](const Datum& d) { return d.null_count() > 0; })) {
    out_valid = output->buffers[0]->mutable_data();
  } else {
    BitUtil::SetBitsTo(output->buffers[0]->mutable_data(), out_offset, batch.length,
                       true);
  }
  uint8_t* out_values = output->buffers[1]->mutable_data();
  int64_t row = 0;
  return VisitArrayValuesInline<Int64Type>(
      *batch[0].array(),
      [&](int64_t index) {
        if (index < 0 || static_cast<size_t>(index + 1) >= batch.values.size()) {
          return Status::IndexError("choose: index ", index, " out of range");
        }
        const auto& source = batch.values[index + 1];
        CopyOneValue<Type>(source, row, out_valid, out_values, out_offset + row);
        row++;
        return Status::OK();
      },
      [&]() {
        // Index is null, but we should still initialize the output with some value
        const auto& source = batch.values[1];
        CopyOneValue<Type>(source, row, out_valid, out_values, out_offset + row);
        BitUtil::ClearBit(out_valid, out_offset + row);
        row++;
        return Status::OK();
      });
}

template <typename Type, typename Enable = void>
struct ChooseFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch.values[0].is_scalar()) {
      return ExecScalarChoose<Type>(ctx, batch, out);
    }
    return ExecArrayChoose<Type>(ctx, batch, out);
  }
};

template <>
struct ChooseFunctor<NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return Status::OK();
  }
};

template <typename Type>
struct ChooseFunctor<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch.values[0].is_scalar()) {
      const auto& index_scalar = *batch[0].scalar();
      if (!index_scalar.is_valid) {
        if (out->is_array()) {
          ARROW_ASSIGN_OR_RAISE(
              auto temp_array,
              MakeArrayOfNull(out->type(), batch.length, ctx->memory_pool()));
          *out->mutable_array() = *temp_array->data();
        }
        return Status::OK();
      }
      auto index = UnboxScalar<Int64Type>::Unbox(index_scalar);
      if (index < 0 || static_cast<size_t>(index + 1) >= batch.values.size()) {
        return Status::IndexError("choose: index ", index, " out of range");
      }
      auto source = batch.values[index + 1];
      if (source.is_scalar() && out->is_array()) {
        ARROW_ASSIGN_OR_RAISE(
            auto temp_array,
            MakeArrayFromScalar(*source.scalar(), batch.length, ctx->memory_pool()));
        *out->mutable_array() = *temp_array->data();
      } else {
        *out = source;
      }
      return Status::OK();
    }

    // Row-wise implementation
    BuilderType builder(out->type(), ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    int64_t reserve_data = 0;
    for (const auto& value : batch.values) {
      if (value.is_scalar()) {
        if (!value.scalar()->is_valid) continue;
        const auto row_length =
            checked_cast<const BaseBinaryScalar&>(*value.scalar()).value->size();
        reserve_data = std::max<int64_t>(reserve_data, batch.length * row_length);
        continue;
      }
      const ArrayData& arr = *value.array();
      const offset_type* offsets = arr.GetValues<offset_type>(1);
      const offset_type values_length = offsets[arr.length] - offsets[0];
      reserve_data = std::max<int64_t>(reserve_data, values_length);
    }
    RETURN_NOT_OK(builder.ReserveData(reserve_data));
    int64_t row = 0;
    RETURN_NOT_OK(VisitArrayValuesInline<Int64Type>(
        *batch[0].array(),
        [&](int64_t index) {
          if (index < 0 || static_cast<size_t>(index + 1) >= batch.values.size()) {
            return Status::IndexError("choose: index ", index, " out of range");
          }
          const auto& source = batch.values[index + 1];
          return CopyValue(source, &builder, row++);
        },
        [&]() {
          row++;
          return builder.AppendNull();
        }));
    auto actual_type = out->type();
    std::shared_ptr<Array> temp_output;
    RETURN_NOT_OK(builder.Finish(&temp_output));
    ArrayData* output = out->mutable_array();
    *output = *temp_output->data();
    // Builder type != logical type due to GenerateTypeAgnosticVarBinaryBase
    output->type = std::move(actual_type);
    return Status::OK();
  }

  static Status CopyValue(const Datum& datum, BuilderType* builder, int64_t row) {
    if (datum.is_scalar()) {
      const auto& scalar = checked_cast<const BaseBinaryScalar&>(*datum.scalar());
      if (!scalar.value) return builder->AppendNull();
      return builder->Append(scalar.value->data(),
                             static_cast<offset_type>(scalar.value->size()));
    }
    const ArrayData& source = *datum.array();
    if (!source.MayHaveNulls() ||
        BitUtil::GetBit(source.buffers[0]->data(), source.offset + row)) {
      const uint8_t* data = source.buffers[2]->data();
      const offset_type* offsets = source.GetValues<offset_type>(1);
      const offset_type offset0 = offsets[row];
      const offset_type offset1 = offsets[row + 1];
      return builder->Append(data + offset0, offset1 - offset0);
    }
    return builder->AppendNull();
  }
};

struct ChooseFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    // The first argument is always int64 or promoted to it. The kernel is dispatched
    // based on the type of the rest of the arguments.
    RETURN_NOT_OK(CheckArity(*values));
    EnsureDictionaryDecoded(values);
    if (values->front().type->id() != Type::INT64) {
      values->front().type = int64();
    }
    if (auto type = CommonNumeric(values->data() + 1, values->size() - 1)) {
      for (auto it = values->begin() + 1; it != values->end(); it++) {
        it->type = type;
      }
    }
    if (auto kernel = DispatchExactImpl(this, {values->back()})) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

void AddCaseWhenKernel(const std::shared_ptr<CaseWhenFunction>& scalar_function,
                       detail::GetTypeId get_id, ArrayKernelExec exec) {
  ScalarKernel kernel(
      KernelSignature::Make({InputType(Type::STRUCT), InputType(get_id.id)},
                            OutputType(LastType),
                            /*is_varargs=*/true),
      exec);
  if (is_fixed_width(get_id.id)) {
    kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::PREALLOCATE;
    kernel.can_write_into_slices = true;
  } else {
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    kernel.can_write_into_slices = false;
  }
  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

void AddPrimitiveCaseWhenKernels(const std::shared_ptr<CaseWhenFunction>& scalar_function,
                                 const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = GenerateTypeAgnosticPrimitive<CaseWhenFunctor>(*type);
    AddCaseWhenKernel(scalar_function, type, std::move(exec));
  }
}

void AddBinaryCaseWhenKernels(const std::shared_ptr<CaseWhenFunction>& scalar_function,
                              const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = GenerateTypeAgnosticVarBinaryBase<CaseWhenFunctor>(*type);
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

void AddChooseKernel(const std::shared_ptr<ScalarFunction>& scalar_function,
                     detail::GetTypeId get_id, ArrayKernelExec exec) {
  ScalarKernel kernel(
      KernelSignature::Make({Type::INT64, InputType(get_id.id)}, OutputType(LastType),
                            /*is_varargs=*/true),
      exec);
  kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::PREALLOCATE;
  kernel.can_write_into_slices = is_fixed_width(get_id.id);
  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

void AddPrimitiveChooseKernels(const std::shared_ptr<ScalarFunction>& scalar_function,
                               const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = GenerateTypeAgnosticPrimitive<ChooseFunctor>(*type);
    AddChooseKernel(scalar_function, type, std::move(exec));
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

const FunctionDoc choose_doc{
    "Given indices and arrays, choose the value from the corresponding array for each "
    "index",
    ("For each row, the value of the first argument is used as a 0-based index into the "
     "rest of the arguments (i.e. index 0 selects the second argument). The output value "
     "is the corresponding value of the selected argument.\n"
     "If an index is null, the output will be null."),
    {"indices", "*values"}};
}  // namespace

void RegisterScalarIfElse(FunctionRegistry* registry) {
  {
    auto func =
        std::make_shared<IfElseFunction>("if_else", Arity::Ternary(), &if_else_doc);

    AddPrimitiveIfElseKernels(func, NumericTypes());
    AddPrimitiveIfElseKernels(func, TemporalTypes());
    AddPrimitiveIfElseKernels(func, IntervalTypes());
    AddPrimitiveIfElseKernels(func, {boolean()});
    AddNullIfElseKernel(func);
    AddBinaryIfElseKernels(func, BaseBinaryTypes());
    AddFixedWidthIfElseKernel<FixedSizeBinaryType>(func);
    AddFixedWidthIfElseKernel<Decimal128Type>(func);
    AddFixedWidthIfElseKernel<Decimal256Type>(func);
    AddNestedIfElseKernels(func);
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<CaseWhenFunction>(
        "case_when", Arity::VarArgs(/*min_args=*/2), &case_when_doc);
    AddPrimitiveCaseWhenKernels(func, NumericTypes());
    AddPrimitiveCaseWhenKernels(func, TemporalTypes());
    AddPrimitiveCaseWhenKernels(func, IntervalTypes());
    AddPrimitiveCaseWhenKernels(func, {boolean(), null()});
    AddCaseWhenKernel(func, Type::FIXED_SIZE_BINARY,
                      CaseWhenFunctor<FixedSizeBinaryType>::Exec);
    AddCaseWhenKernel(func, Type::DECIMAL128, CaseWhenFunctor<FixedSizeBinaryType>::Exec);
    AddCaseWhenKernel(func, Type::DECIMAL256, CaseWhenFunctor<FixedSizeBinaryType>::Exec);
    AddBinaryCaseWhenKernels(func, BaseBinaryTypes());
    AddCaseWhenKernel(func, Type::FIXED_SIZE_LIST,
                      CaseWhenFunctor<FixedSizeListType>::Exec);
    AddCaseWhenKernel(func, Type::LIST, CaseWhenFunctor<ListType>::Exec);
    AddCaseWhenKernel(func, Type::LARGE_LIST, CaseWhenFunctor<LargeListType>::Exec);
    AddCaseWhenKernel(func, Type::MAP, CaseWhenFunctor<MapType>::Exec);
    AddCaseWhenKernel(func, Type::STRUCT, CaseWhenFunctor<StructType>::Exec);
    AddCaseWhenKernel(func, Type::DENSE_UNION, CaseWhenFunctor<DenseUnionType>::Exec);
    AddCaseWhenKernel(func, Type::SPARSE_UNION, CaseWhenFunctor<SparseUnionType>::Exec);
    AddCaseWhenKernel(func, Type::DICTIONARY, CaseWhenFunctor<DictionaryType>::Exec);
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<CoalesceFunction>(
        "coalesce", Arity::VarArgs(/*min_args=*/1), &coalesce_doc);
    AddPrimitiveCoalesceKernels(func, NumericTypes());
    AddPrimitiveCoalesceKernels(func, TemporalTypes());
    AddPrimitiveCoalesceKernels(func, IntervalTypes());
    AddPrimitiveCoalesceKernels(func, {boolean(), null()});
    AddCoalesceKernel(func, Type::FIXED_SIZE_BINARY,
                      CoalesceFunctor<FixedSizeBinaryType>::Exec);
    AddCoalesceKernel(func, Type::DECIMAL128, CoalesceFunctor<FixedSizeBinaryType>::Exec);
    AddCoalesceKernel(func, Type::DECIMAL256, CoalesceFunctor<FixedSizeBinaryType>::Exec);
    for (const auto& ty : BaseBinaryTypes()) {
      AddCoalesceKernel(func, ty, GenerateTypeAgnosticVarBinaryBase<CoalesceFunctor>(ty));
    }
    AddCoalesceKernel(func, Type::FIXED_SIZE_LIST,
                      CoalesceFunctor<FixedSizeListType>::Exec);
    AddCoalesceKernel(func, Type::LIST, CoalesceFunctor<ListType>::Exec);
    AddCoalesceKernel(func, Type::LARGE_LIST, CoalesceFunctor<LargeListType>::Exec);
    AddCoalesceKernel(func, Type::MAP, CoalesceFunctor<MapType>::Exec);
    AddCoalesceKernel(func, Type::STRUCT, CoalesceFunctor<StructType>::Exec);
    AddCoalesceKernel(func, Type::DENSE_UNION, CoalesceFunctor<DenseUnionType>::Exec);
    AddCoalesceKernel(func, Type::SPARSE_UNION, CoalesceFunctor<SparseUnionType>::Exec);
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<ChooseFunction>("choose", Arity::VarArgs(/*min_args=*/2),
                                                 &choose_doc);
    AddPrimitiveChooseKernels(func, NumericTypes());
    AddPrimitiveChooseKernels(func, TemporalTypes());
    AddPrimitiveChooseKernels(func, IntervalTypes());
    AddPrimitiveChooseKernels(func, {boolean(), null()});
    AddChooseKernel(func, Type::FIXED_SIZE_BINARY,
                    ChooseFunctor<FixedSizeBinaryType>::Exec);
    AddChooseKernel(func, Type::DECIMAL128, ChooseFunctor<FixedSizeBinaryType>::Exec);
    AddChooseKernel(func, Type::DECIMAL256, ChooseFunctor<FixedSizeBinaryType>::Exec);
    for (const auto& ty : BaseBinaryTypes()) {
      AddChooseKernel(func, ty, GenerateTypeAgnosticVarBinaryBase<ChooseFunctor>(ty));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
