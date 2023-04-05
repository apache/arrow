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

#include <cstring>
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/array/builder_union.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/copy_data_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
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

inline Bitmap GetBitmap(const ExecValue& val, int i) {
  if (val.is_array()) {
    return Bitmap{val.array.buffers[i].data, val.array.offset, val.array.length};
  } else {
    // scalar
    return {};
  }
}

// Ensure parameterized types are identical.
Status CheckIdenticalTypes(const ExecValue* begin, int count) {
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

constexpr uint64_t kAllNull = 0;
constexpr uint64_t kAllValid = ~kAllNull;

std::optional<uint64_t> GetConstantValidityWord(const ExecValue& data) {
  if (data.is_scalar()) {
    return data.scalar->is_valid ? kAllValid : kAllNull;
  }

  if (data.array.null_count == data.array.length) return kAllNull;
  if (!data.array.MayHaveNulls()) return kAllValid;

  // no constant validity word available
  return {};
}

// if the condition is null then output is null otherwise we take validity from the
// selected argument
// ie. cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
struct IfElseNullPromoter {
  KernelContext* ctx;
  const ArraySpan& cond;
  const ExecValue& left_d;
  const ExecValue& right_d;
  ExecResult* output;

  enum { COND_CONST = 1, LEFT_CONST = 2, RIGHT_CONST = 4 };
  int64_t constant_validity_flag;
  std::optional<uint64_t> cond_const, left_const, right_const;
  Bitmap cond_data, cond_valid, left_valid, right_valid;

  IfElseNullPromoter(KernelContext* ctx, const ExecValue& cond_d, const ExecValue& left_d,
                     const ExecValue& right_d, ExecResult* output)
      : ctx(ctx), cond(cond_d.array), left_d(left_d), right_d(right_d), output(output) {
    cond_const = GetConstantValidityWord(cond_d);
    left_const = GetConstantValidityWord(left_d);
    right_const = GetConstantValidityWord(right_d);

    // Encodes whether each of the arguments is respectively all-null or
    // all-valid
    constant_validity_flag =
        (COND_CONST * cond_const.has_value() | LEFT_CONST * left_const.has_value() |
         RIGHT_CONST * right_const.has_value());

    // cond.data will always be available / is always an array
    cond_data = Bitmap{cond.buffers[1].data, cond.offset, cond.length};
    cond_valid = Bitmap{cond.buffers[0].data, cond.offset, cond.length};
    left_valid = GetBitmap(left_d, 0);
    right_valid = GetBitmap(right_d, 0);
  }

  Status ExecIntoArrayData(bool need_to_allocate) {
    ArrayData* out_arr = output->array_data().get();

    // cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
    // In the following cases, we dont need to allocate out_valid bitmap

    // if cond & left & right all ones, then output is all valid.
    // if output validity buffer is already allocated (NullHandling::
    // COMPUTED_PREALLOCATE) -> set all bits
    // else, return nullptr
    if (cond_const == kAllValid && left_const == kAllValid && right_const == kAllValid) {
      if (need_to_allocate) {  // NullHandling::COMPUTED_NO_PREALLOCATE
        out_arr->buffers[0] = nullptr;
      } else {  // NullHandling::COMPUTED_PREALLOCATE
        bit_util::SetBitmap(out_arr->buffers[0]->mutable_data(), out_arr->offset,
                            out_arr->length);
      }
    } else if (left_const == kAllValid && right_const == kAllValid) {
      // if both left and right are valid, no need to calculate out_valid bitmap. Copy
      // cond validity buffer
      if (need_to_allocate) {  // NullHandling::COMPUTED_NO_PREALLOCATE
        // if there's an offset, copy bitmap (cannot slice a bitmap)
        if (cond.offset) {
          ARROW_ASSIGN_OR_RAISE(
              out_arr->buffers[0],
              arrow::internal::CopyBitmap(ctx->memory_pool(), cond.buffers[0].data,
                                          cond.offset, cond.length));
        } else {
          // just copy assign cond validity buffer
          out_arr->buffers[0] = cond.GetBuffer(0);
        }
      } else {  // NullHandling::COMPUTED_PREALLOCATE
        arrow::internal::CopyBitmap(cond.buffers[0].data, cond.offset, cond.length,
                                    out_arr->buffers[0]->mutable_data(), out_arr->offset);
      }
    } else {
      if (need_to_allocate) {
        // following cases requires a separate out_valid buffer. COMPUTED_NO_PREALLOCATE
        // would not have allocated buffers for it.
        ARROW_ASSIGN_OR_RAISE(out_arr->buffers[0], ctx->AllocateBitmap(cond.length));
      }
      WriteOutput(
          Bitmap{out_arr->buffers[0]->mutable_data(), out_arr->offset, out_arr->length});
    }
    return Status::OK();
  }

  Status ExecIntoArraySpan() {
    ArraySpan* out_span = output->array_span_mutable();

    // cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
    // In the following cases, we dont need to allocate out_valid bitmap

    // if cond & left & right all ones, then output is all valid, so set all
    // bits
    if (cond_const == kAllValid && left_const == kAllValid && right_const == kAllValid) {
      bit_util::SetBitmap(out_span->buffers[0].data, out_span->offset, out_span->length);
    } else if (left_const == kAllValid && right_const == kAllValid) {
      // if both left and right are valid, no need to calculate out_valid
      // bitmap. Copy cond validity buffer
      arrow::internal::CopyBitmap(cond.buffers[0].data, cond.offset, cond.length,
                                  out_span->buffers[0].data, out_span->offset);
    } else {
      WriteOutput(Bitmap{out_span->buffers[0].data, out_span->offset, out_span->length});
    }
    return Status::OK();
  }

  Status Exec(bool need_to_allocate) {
    if (output->is_array_data()) {
      return ExecIntoArrayData(need_to_allocate);
    } else {
      DCHECK(!need_to_allocate) << "Conditional kernel must preallocate validity bitmap";
      return ExecIntoArraySpan();
    }
  }

  void WriteOutput(Bitmap out_bitmap) {
    // lambda function that will be used inside the visitor
    auto apply = [&](uint64_t c_valid, uint64_t c_data, uint64_t l_valid,
                     uint64_t r_valid) {
      return c_valid & ((c_data & l_valid) | (~c_data & r_valid));
    };

    std::array<Bitmap, 1> out_bitmaps{out_bitmap};

    switch (this->constant_validity_flag) {
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
  }
};

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
void RunIfElseLoop(const ArraySpan& cond, const HandleBlock& handle_block) {
  int64_t data_offset = 0;
  int64_t bit_offset = cond.offset;
  const auto* cond_data = cond.buffers[1].data;  // this is a BoolArray

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
        if (bit_util::GetBit(cond_data, bit_offset + i) != invert) {
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
        if (bit_util::GetBit(cond_data, bit_offset + i) != invert) {
          handle_block(data_offset + i, 1);
        }
      }
    }
    data_offset += 8;
    bit_offset += 8;
  }
}

template <typename HandleBlock>
void RunIfElseLoopInverted(const ArraySpan& cond, const HandleBlock& handle_block) {
  RunIfElseLoop<HandleBlock, true>(cond, handle_block);
}

/// Runs if-else when cond is a scalar. Two special functions are required,
/// 1.CopyArrayData, 2. BroadcastScalar
template <typename CopyArrayData, typename BroadcastScalar>
Status RunIfElseScalar(const BooleanScalar& cond, const ExecValue& left,
                       const ExecValue& right, ExecResult* out,
                       const CopyArrayData& copy_array_data,
                       const BroadcastScalar& broadcast_scalar) {
  // either left or right is an array. Output is always an array`
  ArraySpan* out_array = out->array_span_mutable();
  if (!cond.is_valid) {
    // cond is null; output is all null --> clear validity buffer
    bit_util::ClearBitmap(out_array->buffers[0].data, out_array->offset,
                          out_array->length);
    return Status::OK();
  }

  // One of left or right is an array

  // cond is a non-null scalar
  const auto& valid_data = cond.value ? left : right;
  if (valid_data.is_array()) {
    // valid_data is an array. Hence copy data to the output buffers
    const ArraySpan& valid_array = valid_data.array;
    if (valid_array.MayHaveNulls()) {
      arrow::internal::CopyBitmap(valid_array.buffers[0].data, valid_array.offset,
                                  valid_array.length, out_array->buffers[0].data,
                                  out_array->offset);
    } else {  // validity buffer is nullptr --> set all bits
      bit_util::SetBitmap(out_array->buffers[0].data, out_array->offset,
                          out_array->length);
    }
    copy_array_data(valid_array, out_array);
    return Status::OK();

  } else {  // valid data is scalar
    // valid data is a scalar that needs to be broadcasted
    const Scalar& valid_scalar = *valid_data.scalar;
    if (valid_scalar.is_valid) {  // if the scalar is non-null, broadcast
      bit_util::SetBitmap(out_array->buffers[0].data, out_array->offset,
                          out_array->length);
      broadcast_scalar(*valid_data.scalar, out_array);
    } else {  // scalar is null, clear the output validity buffer
      bit_util::ClearBitmap(out_array->buffers[0].data, out_array->offset,
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
struct IfElseFunctor<Type,
                     enable_if_t<is_number_type<Type>::value ||
                                 std::is_same<Type, MonthDayNanoIntervalType>::value>> {
  using T = typename TypeTraits<Type>::CType;
  // A - Array, S - Scalar, X = Array/Scalar

  // SXX
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const ExecValue& left,
                     const ExecValue& right, ExecResult* out) {
    return RunIfElseScalar(
        cond, left, right, out,
        /*CopyArrayData*/
        [&](const ArraySpan& valid_array, ArraySpan* out_array) {
          std::memcpy(out_array->GetValues<T>(1), valid_array.GetValues<T>(1),
                      valid_array.length * sizeof(T));
        },
        /*BroadcastScalar*/
        [&](const Scalar& scalar, ArraySpan* out_array) {
          T scalar_data = internal::UnboxScalar<Type>::Unbox(scalar);
          std::fill(out_array->GetValues<T>(1),
                    out_array->GetValues<T>(1) + out_array->length, scalar_data);
        });
  }

  //  AAA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const ArraySpan& right, ExecResult* out) {
    T* out_values = out->array_span_mutable()->GetValues<T>(1);

    // copy right data to out_buff
    std::memcpy(out_values, right.GetValues<T>(1), right.length * sizeof(T));

    // selectively copy values from left data
    const T* left_data = left.GetValues<T>(1);
    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::memcpy(out_values + data_offset, left_data + data_offset,
                  num_elems * sizeof(T));
    });

    return Status::OK();
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const ArraySpan& right, ExecResult* out) {
    T* out_values = out->array_span_mutable()->GetValues<T>(1);

    // copy right data to out_buff
    std::memcpy(out_values, right.GetValues<T>(1), right.length * sizeof(T));

    if (!left.is_valid) {  // left is null scalar, only need to copy right data to output
      return Status::OK();
    }

    // selectively copy values from left data
    T left_data = internal::UnboxScalar<Type>::Unbox(left);

    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::fill(out_values + data_offset, out_values + data_offset + num_elems,
                left_data);
    });

    return Status::OK();
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const Scalar& right, ExecResult* out) {
    T* out_values = out->array_span_mutable()->GetValues<T>(1);

    // copy left data to out_buff
    const T* left_data = left.GetValues<T>(1);
    std::memcpy(out_values, left_data, left.length * sizeof(T));

    if (!right.is_valid) {  // right is null scalar, only need to copy left data to output
      return Status::OK();
    }

    T right_data = internal::UnboxScalar<Type>::Unbox(right);

    RunIfElseLoopInverted(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::fill(out_values + data_offset, out_values + data_offset + num_elems,
                right_data);
    });

    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const Scalar& right, ExecResult* out) {
    T* out_values = out->array_span_mutable()->GetValues<T>(1);

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
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const ExecValue& left,
                     const ExecValue& right, ExecResult* out) {
    return RunIfElseScalar(
        cond, left, right, out,
        /*CopyArrayData*/
        [&](const ArraySpan& valid_array, ArraySpan* out_array) {
          arrow::internal::CopyBitmap(valid_array.buffers[1].data, valid_array.offset,
                                      valid_array.length, out_array->buffers[1].data,
                                      out_array->offset);
        },
        /*BroadcastScalar*/
        [&](const Scalar& scalar, ArraySpan* out_array) {
          bool scalar_data = internal::UnboxScalar<Type>::Unbox(scalar);
          bit_util::SetBitsTo(out_array->buffers[1].data, out_array->offset,
                              out_array->length, scalar_data);
        });
  }

  // AAA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const ArraySpan& right, ExecResult* out) {
    ArraySpan* out_arr = out->array_span_mutable();
    // out_buff = right & ~cond
    arrow::internal::BitmapAndNot(right.buffers[1].data, right.offset,
                                  cond.buffers[1].data, cond.offset, cond.length,
                                  out_arr->offset, out_arr->buffers[1].data);

    // out_buff = left & cond
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Buffer> temp_buf,
        arrow::internal::BitmapAnd(ctx->memory_pool(), left.buffers[1].data, left.offset,
                                   cond.buffers[1].data, cond.offset, cond.length, 0));

    arrow::internal::BitmapOr(out_arr->buffers[1].data, out_arr->offset, temp_buf->data(),
                              0, cond.length, out_arr->offset, out_arr->buffers[1].data);

    return Status::OK();
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const ArraySpan& right, ExecResult* out) {
    ArraySpan* out_arr = out->array_span_mutable();

    // out_buff = right & ~cond
    arrow::internal::BitmapAndNot(right.buffers[1].data, right.offset,
                                  cond.buffers[1].data, cond.offset, cond.length,
                                  out_arr->offset, out_arr->buffers[1].data);

    // out_buff = left & cond
    bool left_data = internal::UnboxScalar<BooleanType>::Unbox(left);
    if (left_data) {
      arrow::internal::BitmapOr(out_arr->buffers[1].data, out_arr->offset,
                                cond.buffers[1].data, cond.offset, cond.length,
                                out_arr->offset, out_arr->buffers[1].data);
    }

    return Status::OK();
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const Scalar& right, ExecResult* out) {
    ArraySpan* out_arr = out->array_span_mutable();

    // out_buff = left & cond
    arrow::internal::BitmapAnd(left.buffers[1].data, left.offset, cond.buffers[1].data,
                               cond.offset, cond.length, out_arr->offset,
                               out_arr->buffers[1].data);

    bool right_data = internal::UnboxScalar<BooleanType>::Unbox(right);

    // out_buff = left & cond | right & ~cond
    if (right_data) {
      arrow::internal::BitmapOrNot(out_arr->buffers[1].data, out_arr->offset,
                                   cond.buffers[1].data, cond.offset, cond.length,
                                   out_arr->offset, out_arr->buffers[1].data);
    }

    return Status::OK();
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const Scalar& right, ExecResult* out) {
    ArraySpan* out_arr = out->array_span_mutable();

    bool left_data = internal::UnboxScalar<BooleanType>::Unbox(left);
    bool right_data = internal::UnboxScalar<BooleanType>::Unbox(right);

    uint8_t* out_buf = out_arr->buffers[1].data;

    // out_buf = left & cond | right & ~cond
    if (left_data) {
      if (right_data) {
        // out_buf = ones
        bit_util::SetBitmap(out_buf, out_arr->offset, cond.length);
      } else {
        // out_buf = cond
        arrow::internal::CopyBitmap(cond.buffers[1].data, cond.offset, cond.length,
                                    out_buf, out_arr->offset);
      }
    } else {
      if (right_data) {
        // out_buf = ~cond
        arrow::internal::InvertBitmap(cond.buffers[1].data, cond.offset, cond.length,
                                      out_buf, out_arr->offset);
      } else {
        // out_buf = zeros
        bit_util::ClearBitmap(out_buf, out_arr->offset, cond.length);
      }
    }

    return Status::OK();
  }
};

static Status IfElseGenericSXXCall(KernelContext* ctx, const BooleanScalar& cond,
                                   const ExecValue& left, const ExecValue& right,
                                   ExecResult* out) {
  // Either left or right is an array
  int64_t out_arr_len = std::max(left.length(), right.length());
  if (!cond.is_valid) {
    // cond is null; just create a null array
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Array> result,
        MakeArrayOfNull(left.type()->GetSharedPtr(), out_arr_len, ctx->memory_pool()));
    out->value = std::move(result->data());
    return Status::OK();
  }

  const ExecValue& valid_data = cond.value ? left : right;
  if (valid_data.is_array()) {
    out->value = valid_data.array.ToArrayData();
  } else {
    // valid data is a scalar that needs to be broadcasted
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Array> result,
        MakeArrayFromScalar(*valid_data.scalar, out_arr_len, ctx->memory_pool()));
    out->value = std::move(result->data());
  }
  return Status::OK();
}

template <typename Type>
struct IfElseFunctor<Type, enable_if_base_binary<Type>> {
  using OffsetType = typename TypeTraits<Type>::OffsetType::c_type;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  // A - Array, S - Scalar, X = Array/Scalar

  // SXX
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const ExecValue& left,
                     const ExecValue& right, ExecResult* out) {
    return IfElseGenericSXXCall(ctx, cond, left, right, out);
  }

  static Status WrapResult(BuilderType* builder, ArrayData* out) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> out_arr, builder->Finish());
    out->SetNullCount(out_arr->data()->null_count);
    out->buffers = std::move(out_arr->data()->buffers);
    return Status::OK();
  }

  //  AAA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const ArraySpan& right, ExecResult* out) {
    const auto* left_offsets = left.GetValues<OffsetType>(1);
    const uint8_t* left_data = left.buffers[2].data;
    const auto* right_offsets = right.GetValues<OffsetType>(1);
    const uint8_t* right_data = right.buffers[2].data;

    // allocate data buffer conservatively
    int64_t data_buff_alloc = left_offsets[left.length] - left_offsets[0] +
                              right_offsets[right.length] - right_offsets[0];

    BuilderType builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(cond.length + 1));
    ARROW_RETURN_NOT_OK(builder.ReserveData(data_buff_alloc));

    RunLoop(
        cond, *out->array_data(),
        [&](int64_t i) {
          builder.UnsafeAppend(left_data + left_offsets[i],
                               left_offsets[i + 1] - left_offsets[i]);
        },
        [&](int64_t i) {
          builder.UnsafeAppend(right_data + right_offsets[i],
                               right_offsets[i + 1] - right_offsets[i]);
        },
        [&]() { builder.UnsafeAppendNull(); });
    return WrapResult(&builder, out->array_data().get());
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const ArraySpan& right, ExecResult* out) {
    const auto* right_offsets = right.GetValues<OffsetType>(1);
    const uint8_t* right_data = right.buffers[2].data;

    if (!left.is_valid) {  // left is null scalar, only need to copy right data to output
      auto* out_data = out->array_data().get();
      auto offset_length = (cond.length + 1) * sizeof(OffsetType);
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[1], ctx->Allocate(offset_length));
      std::memcpy(out_data->buffers[1]->mutable_data(), right_offsets, offset_length);

      auto right_data_length = right_offsets[right.length] - right_offsets[0];
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[2], ctx->Allocate(right_data_length));
      std::memcpy(out_data->buffers[2]->mutable_data(), right_data, right_data_length);
      return Status::OK();
    }

    std::string_view left_data = internal::UnboxScalar<Type>::Unbox(left);
    auto left_size = static_cast<OffsetType>(left_data.size());

    // allocate data buffer conservatively
    int64_t data_buff_alloc =
        left_size * cond.length + right_offsets[right.length] - right_offsets[0];

    BuilderType builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(cond.length + 1));
    ARROW_RETURN_NOT_OK(builder.ReserveData(data_buff_alloc));

    RunLoop(
        cond, *out->array_data(),
        [&](int64_t i) { builder.UnsafeAppend(left_data.data(), left_size); },
        [&](int64_t i) {
          builder.UnsafeAppend(right_data + right_offsets[i],
                               right_offsets[i + 1] - right_offsets[i]);
        },
        [&]() { builder.UnsafeAppendNull(); });
    return WrapResult(&builder, out->array_data().get());
  }

  // AAS
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const Scalar& right, ExecResult* out) {
    const auto* left_offsets = left.GetValues<OffsetType>(1);
    const uint8_t* left_data = left.buffers[2].data;

    if (!right.is_valid) {  // right is null scalar, only need to copy left data to output
      auto* out_data = out->array_data().get();
      auto offset_length = (cond.length + 1) * sizeof(OffsetType);
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[1], ctx->Allocate(offset_length));
      std::memcpy(out_data->buffers[1]->mutable_data(), left_offsets, offset_length);

      auto left_data_length = left_offsets[left.length] - left_offsets[0];
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[2], ctx->Allocate(left_data_length));
      std::memcpy(out_data->buffers[2]->mutable_data(), left_data, left_data_length);
      return Status::OK();
    }

    std::string_view right_data = internal::UnboxScalar<Type>::Unbox(right);
    auto right_size = static_cast<OffsetType>(right_data.size());

    // allocate data buffer conservatively
    int64_t data_buff_alloc =
        right_size * cond.length + left_offsets[left.length] - left_offsets[0];

    BuilderType builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(cond.length + 1));
    ARROW_RETURN_NOT_OK(builder.ReserveData(data_buff_alloc));

    RunLoop(
        cond, *out->array_data(),
        [&](int64_t i) {
          builder.UnsafeAppend(left_data + left_offsets[i],
                               left_offsets[i + 1] - left_offsets[i]);
        },
        [&](int64_t i) { builder.UnsafeAppend(right_data.data(), right_size); },
        [&]() { builder.UnsafeAppendNull(); });
    return WrapResult(&builder, out->array_data().get());
  }

  // ASS
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const Scalar& right, ExecResult* out) {
    std::string_view left_data = internal::UnboxScalar<Type>::Unbox(left);
    auto left_size = static_cast<OffsetType>(left_data.size());

    std::string_view right_data = internal::UnboxScalar<Type>::Unbox(right);
    auto right_size = static_cast<OffsetType>(right_data.size());

    // allocate data buffer conservatively
    int64_t data_buff_alloc = std::max(right_size, left_size) * cond.length;
    BuilderType builder(ctx->memory_pool());
    ARROW_RETURN_NOT_OK(builder.Reserve(cond.length + 1));
    ARROW_RETURN_NOT_OK(builder.ReserveData(data_buff_alloc));

    RunLoop(
        cond, *out->array_data(),
        [&](int64_t i) { builder.UnsafeAppend(left_data.data(), left_size); },
        [&](int64_t i) { builder.UnsafeAppend(right_data.data(), right_size); },
        [&]() { builder.UnsafeAppendNull(); });
    return WrapResult(&builder, out->array_data().get());
  }

  template <typename HandleLeft, typename HandleRight, typename HandleNull>
  static void RunLoop(const ArraySpan& cond, const ArrayData& output,
                      HandleLeft&& handle_left, HandleRight&& handle_right,
                      HandleNull&& handle_null) {
    const uint8_t* cond_data = cond.buffers[1].data;

    if (output.buffers[0]) {  // output may have nulls
      // output validity buffer is allocated internally from the IfElseFunctor. Therefore
      // it is cond.length'd with 0 offset.
      const auto* out_valid = output.buffers[0]->data();

      for (int64_t i = 0; i < cond.length; i++) {
        if (bit_util::GetBit(out_valid, i)) {
          bit_util::GetBit(cond_data, cond.offset + i) ? handle_left(i) : handle_right(i);
        } else {
          handle_null();
        }
      }
    } else {  // output is all valid (no nulls)
      for (int64_t i = 0; i < cond.length; i++) {
        bit_util::GetBit(cond_data, cond.offset + i) ? handle_left(i) : handle_right(i);
      }
    }
  }
};

template <typename Type>
struct IfElseFunctor<Type, enable_if_fixed_size_binary<Type>> {
  // A - Array, S - Scalar, X = Array/Scalar

  // SXX
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const ExecValue& left,
                     const ExecValue& right, ExecResult* out) {
    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type(), *right.type()));
    return RunIfElseScalar(
        cond, left, right, out,
        /*CopyArrayData*/
        [&](const ArraySpan& valid_array, ArraySpan* out_array) {
          std::memcpy(out_array->buffers[1].data + out_array->offset * byte_width,
                      valid_array.buffers[1].data + valid_array.offset * byte_width,
                      valid_array.length * byte_width);
        },
        /*BroadcastScalar*/
        [&](const Scalar& scalar, ArraySpan* out_array) {
          const uint8_t* scalar_data = UnboxBinaryScalar(scalar);
          uint8_t* start = out_array->buffers[1].data + out_array->offset * byte_width;
          for (int64_t i = 0; i < out_array->length; i++) {
            std::memcpy(start + i * byte_width, scalar_data, byte_width);
          }
        });
  }

  //  AAA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const ArraySpan& right, ExecResult* out) {
    ArraySpan* out_arr = out->array_span_mutable();

    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type, *right.type));
    auto* out_values = out_arr->buffers[1].data + out_arr->offset * byte_width;

    // copy right data to out_buff
    const uint8_t* right_data = right.buffers[1].data + right.offset * byte_width;
    std::memcpy(out_values, right_data, right.length * byte_width);

    // selectively copy values from left data
    const uint8_t* left_data = left.buffers[1].data + left.offset * byte_width;

    RunIfElseLoop(cond, [&](int64_t data_offset, int64_t num_elems) {
      std::memcpy(out_values + data_offset * byte_width,
                  left_data + data_offset * byte_width, num_elems * byte_width);
    });

    return Status::OK();
  }

  // ASA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const ArraySpan& right, ExecResult* out) {
    ArraySpan* out_arr = out->array_span_mutable();

    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type, *right.type));
    auto* out_values = out_arr->buffers[1].data + out_arr->offset * byte_width;

    // copy right data to out_buff
    const uint8_t* right_data = right.buffers[1].data + right.offset * byte_width;
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
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const Scalar& right, ExecResult* out) {
    ArraySpan* out_arr = out->array_span_mutable();

    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type, *right.type));
    auto* out_values = out_arr->buffers[1].data + out_arr->offset * byte_width;

    // copy left data to out_buff
    const uint8_t* left_data = left.buffers[1].data + left.offset * byte_width;
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
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const Scalar& right, ExecResult* out) {
    ArraySpan* out_arr = out->array_span_mutable();

    ARROW_ASSIGN_OR_RAISE(auto byte_width, GetByteWidth(*left.type, *right.type));
    auto* out_values = out_arr->buffers[1].data + out_arr->offset * byte_width;

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
  static Status Call(KernelContext* ctx, const BooleanScalar& cond, const ExecValue& left,
                     const ExecValue& right, ExecResult* out) {
    return IfElseGenericSXXCall(ctx, cond, left, right, out);
  }

  //  AAA
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const ArraySpan& right, ExecResult* out) {
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
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const ArraySpan& right, ExecResult* out) {
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
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const ArraySpan& left,
                     const Scalar& right, ExecResult* out) {
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
  static Status Call(KernelContext* ctx, const ArraySpan& cond, const Scalar& left,
                     const Scalar& right, ExecResult* out) {
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
  static Status RunLoop(KernelContext* ctx, const ArraySpan& cond, ExecResult* out,
                        HandleLeft&& handle_left, HandleRight&& handle_right) {
    std::unique_ptr<ArrayBuilder> raw_builder;
    RETURN_NOT_OK(MakeBuilderExactIndex(ctx->memory_pool(), out->type()->GetSharedPtr(),
                                        &raw_builder));
    RETURN_NOT_OK(raw_builder->Reserve(out->length()));

    const auto* cond_data = cond.buffers[1].data;
    if (cond.buffers[0].data != nullptr) {
      BitRunReader reader(cond.buffers[0].data, cond.offset, cond.length);
      int64_t position = 0;
      while (true) {
        auto run = reader.NextRun();
        if (run.length == 0) break;
        if (run.set) {
          for (int j = 0; j < run.length; j++) {
            if (bit_util::GetBit(cond_data, cond.offset + position + j)) {
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
    out->value = std::move(out_arr->data());
    return Status::OK();
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[1], /*count=*/2));
    if (batch[0].is_scalar()) {
      const auto& cond = batch[0].scalar_as<BooleanScalar>();
      return Call(ctx, cond, batch[1], batch[2], out);
    }
    if (batch[1].is_array()) {
      if (batch[2].is_array()) {  // AAA
        return Call(ctx, batch[0].array, batch[1].array, batch[2].array, out);
      } else {  // AAS
        return Call(ctx, batch[0].array, batch[1].array, *batch[2].scalar, out);
      }
    } else {
      if (batch[2].is_array()) {  // ASA
        return Call(ctx, batch[0].array, *batch[1].scalar, batch[2].array, out);
      } else {  // ASS
        return Call(ctx, batch[0].array, *batch[1].scalar, *batch[2].scalar, out);
      }
    }
  }
};

template <typename Type, typename AllocateMem>
struct ResolveIfElseExec {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // Check is unconditional because parametric types like timestamp
    // are templated as integer
    RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[1], /*count=*/2));

    // cond is scalar
    if (batch[0].is_scalar()) {
      const auto& cond = batch[0].scalar_as<BooleanScalar>();
      return IfElseFunctor<Type>::Call(ctx, cond, batch[1], batch[2], out);
    }

    // cond is array. Compute the output validity bitmap and then invoke the
    // correct functor
    IfElseNullPromoter null_promoter(ctx, batch[0], batch[1], batch[2], out);
    ARROW_RETURN_NOT_OK(null_promoter.Exec(AllocateMem::value));

    if (batch[1].is_array()) {
      if (batch[2].is_array()) {  // AAA
        return IfElseFunctor<Type>::Call(ctx, batch[0].array, batch[1].array,
                                         batch[2].array, out);
      } else {  // AAS
        return IfElseFunctor<Type>::Call(ctx, batch[0].array, batch[1].array,
                                         *batch[2].scalar, out);
      }
    } else {
      if (batch[2].is_array()) {  // ASA
        return IfElseFunctor<Type>::Call(ctx, batch[0].array, *batch[1].scalar,
                                         batch[2].array, out);
      } else {  // ASS
        return IfElseFunctor<Type>::Call(ctx, batch[0].array, *batch[1].scalar,
                                         *batch[2].scalar, out);
      }
    }
  }
};

template <typename AllocateMem>
struct ResolveIfElseExec<NullType, AllocateMem> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> result,
                          MakeArrayOfNull(null(), batch.length, ctx->memory_pool()));
    out->value = std::move(result->data());
    return Status::OK();
  }
};

struct IfElseFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));

    using arrow::compute::detail::DispatchExactImpl;
    // Do not DispatchExact here because it'll let through something like (bool,
    // timestamp[s], timestamp[s, "UTC"])

    // if 0th type is null, replace with bool
    if (types->at(0).id() == Type::NA) {
      (*types)[0] = boolean();
    }

    // if-else 0'th type is bool, so skip it
    TypeHolder* left_arg = &(*types)[1];
    constexpr size_t num_args = 2;

    internal::ReplaceNullWithOtherType(left_arg, num_args);

    // If both are identical dictionary types, dispatch to the dictionary kernel
    // TODO(ARROW-14105): apply implicit casts to dictionary types too
    TypeHolder* right_arg = &(*types)[2];
    if (is_dictionary(left_arg->id()) && left_arg->type->Equals(*right_arg->type)) {
      auto kernel = DispatchExactImpl(this, *types);
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
    } else if (HasDecimal(*types)) {
      RETURN_NOT_OK(CastDecimalArgs(left_arg, num_args));
    }

    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    return arrow::compute::detail::NoMatchingKernel(this, *types);
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
        internal::GenerateTypeAgnosticPrimitive<ResolveIfElseExec, ArrayKernelExec,
                                                /*AllocateMem=*/std::false_type>(*type);
    // cond array needs to be boolean always
    std::shared_ptr<KernelSignature> sig;
    if (type->id() == Type::TIMESTAMP) {
      auto unit = checked_cast<const TimestampType&>(*type).unit();
      sig = KernelSignature::Make(
          {boolean(), match::TimestampTypeUnit(unit), match::TimestampTypeUnit(unit)},
          LastType);
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
        internal::GenerateTypeAgnosticVarBinaryBase<ResolveIfElseExec, ArrayKernelExec,
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
  ScalarKernel kernel({boolean(), InputType(type_id), InputType(type_id)}, LastType,
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
    ScalarKernel kernel({boolean(), InputType(type_id), InputType(type_id)}, LastType,
                        NestedIfElseExec::Exec);
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    kernel.can_write_into_slices = false;
    DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
  }
}

// Copy fixed-width values from a scalar/array into an output values buffer
template <typename Type>
void CopyValues(const ExecValue& in_values, const int64_t in_offset, const int64_t length,
                uint8_t* out_valid, uint8_t* out_values, const int64_t out_offset) {
  if (in_values.is_scalar()) {
    const Scalar& scalar = *in_values.scalar;
    if (out_valid) {
      bit_util::SetBitsTo(out_valid, out_offset, length, scalar.is_valid);
    }
    CopyDataUtils<Type>::CopyData(*scalar.type, scalar, /*in_offset=*/0, out_values,
                                  out_offset, length);
  } else {
    const ArraySpan& array = in_values.array;
    if (out_valid) {
      if (array.MayHaveNulls()) {
        if (length == 1) {
          // CopyBitmap is slow for short runs
          bit_util::SetBitTo(
              out_valid, out_offset,
              bit_util::GetBit(array.buffers[0].data, array.offset + in_offset));
        } else {
          arrow::internal::CopyBitmap(array.buffers[0].data, array.offset + in_offset,
                                      length, out_valid, out_offset);
        }
      } else {
        bit_util::SetBitsTo(out_valid, out_offset, length, true);
      }
    }
    CopyDataUtils<Type>::CopyData(*array.type, array.buffers[1].data,
                                  array.offset + in_offset, out_values, out_offset,
                                  length);
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
    bit_util::SetBitTo(out_valid, out_offset,
                       !in_valid || bit_util::GetBit(in_valid, in_offset));
  }
  CopyDataUtils<Type>::CopyData(type, in_values, in_offset, out_values, out_offset,
                                /*length=*/1);
}

template <typename Type>
void CopyOneScalarValue(const Scalar& scalar, uint8_t* out_valid, uint8_t* out_values,
                        const int64_t out_offset) {
  if (out_valid) {
    bit_util::SetBitTo(out_valid, out_offset, scalar.is_valid);
  }
  CopyDataUtils<Type>::CopyData(*scalar.type, scalar, /*in_offset=*/0, out_values,
                                out_offset, /*length=*/1);
}

template <typename Type>
void CopyOneValue(const ExecValue& in_values, const int64_t in_offset, uint8_t* out_valid,
                  uint8_t* out_values, const int64_t out_offset) {
  if (in_values.is_array()) {
    const ArraySpan& array = in_values.array;
    CopyOneArrayValue<Type>(*array.type, array.GetValues<uint8_t>(0, 0),
                            array.GetValues<uint8_t>(1, 0), array.offset + in_offset,
                            out_valid, out_values, out_offset);
  } else {
    CopyOneScalarValue<Type>(*in_values.scalar, out_valid, out_values, out_offset);
  }
}

struct CaseWhenFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    // The first function is a struct of booleans, where the number of fields in the
    // struct is either equal to the number of other arguments or is one less.
    RETURN_NOT_OK(CheckArity(types->size()));
    auto first_type = (*types)[0].type;
    if (first_type->id() != Type::STRUCT) {
      return Status::TypeError("case_when: first argument must be STRUCT, not ",
                               *first_type);
    }
    auto num_fields = static_cast<size_t>(first_type->num_fields());
    if (num_fields < types->size() - 2 || num_fields >= types->size()) {
      return Status::Invalid(
          "case_when: number of struct fields must be equal to or one less than count of "
          "remaining arguments (",
          types->size() - 1, "), got: ", first_type->num_fields());
    }
    for (const auto& field : first_type->fields()) {
      if (field->type()->id() != Type::BOOL) {
        return Status::TypeError(
            "case_when: all fields of first argument must be BOOL, but ", field->name(),
            " was of type: ", *field->type());
      }
    }

    // TODO(ARROW-14105): also apply casts to dictionary indices/values
    if (is_dictionary((*types)[1].id()) &&
        std::all_of(types->begin() + 2, types->end(),
                    [&](const TypeHolder& type) { return type == (*types)[1]; })) {
      auto kernel = DispatchExactImpl(this, *types);
      DCHECK(kernel);
      return kernel;
    }

    EnsureDictionaryDecoded(types);
    TypeHolder* first_arg = &(*types)[1];
    const size_t num_args = types->size() - 1;
    if (auto type = CommonNumeric(first_arg, num_args)) {
      ReplaceTypes(type, first_arg, num_args);
    }
    if (auto type = CommonBinary(first_arg, num_args)) {
      ReplaceTypes(type, first_arg, num_args);
    }
    if (auto type = CommonTemporal(first_arg, num_args)) {
      ReplaceTypes(type, first_arg, num_args);
    }
    if (HasDecimal(*types)) {
      RETURN_NOT_OK(CastDecimalArgs(first_arg, num_args));
    }
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

// Implement a 'case when' (SQL)/'select' (NumPy) function for any scalar conditions
template <typename Type>
Status ExecScalarCaseWhen(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const auto& conds = checked_cast<const StructScalar&>(*batch[0].scalar);
  if (!conds.is_valid) {
    return Status::Invalid("cond struct must not be null");
  }
  ExecValue result;
  bool has_result = false;
  for (size_t i = 0; i < batch.values.size() - 1; i++) {
    if (i < conds.value.size()) {
      const Scalar& cond = *conds.value[i];
      if (cond.is_valid && internal::UnboxScalar<BooleanType>::Unbox(cond)) {
        result = batch[i + 1];
        has_result = true;
        break;
      }
    } else {
      // ELSE clause
      result = batch[i + 1];
      has_result = true;
      break;
    }
  }

  std::shared_ptr<Scalar> temp;
  if (!has_result) {
    // All conditions false, no 'else' argument
    temp = MakeNullScalar(out->type()->GetSharedPtr());
    result = temp.get();
  }

  // TODO(wesm): clean this up to have less duplication
  if (out->is_array_data()) {
    ArrayData* output = out->array_data().get();
    if (is_dictionary_type<Type>::value) {
      const ExecValue& dict_from = has_result ? result : batch[1];
      if (dict_from.is_scalar()) {
        output->dictionary = checked_cast<const DictionaryScalar&>(*dict_from.scalar)
                                 .value.dictionary->data();
      } else {
        output->dictionary = dict_from.array.ToArrayData()->dictionary;
      }
    }
    CopyValues<Type>(result, /*in_offset=*/0, batch.length,
                     output->GetMutableValues<uint8_t>(0, 0),
                     output->GetMutableValues<uint8_t>(1, 0), output->offset);
  } else {
    // ArraySpan
    ArraySpan* output = out->array_span_mutable();
    if (is_dictionary_type<Type>::value) {
      const ExecValue& dict_from = has_result ? result : batch[1];
      output->child_data.resize(1);
      if (dict_from.is_scalar()) {
        output->child_data[0].SetMembers(
            *checked_cast<const DictionaryScalar&>(*dict_from.scalar)
                 .value.dictionary->data());
      } else {
        output->child_data[0] = dict_from.array;
      }
    }
    CopyValues<Type>(result, /*in_offset=*/0, batch.length,
                     output->GetValues<uint8_t>(0, 0), output->GetValues<uint8_t>(1, 0),
                     output->offset);
  }
  return Status::OK();
}

// Implement 'case when' for any mix of scalar/array arguments for any fixed-width type,
// given helper functions to copy data from a source array to a target array
template <typename Type>
Status ExecArrayCaseWhen(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& conds_array = batch[0].array;
  if (conds_array.GetNullCount() > 0) {
    return Status::Invalid(
        "cond struct must not be a null scalar or "
        "have top-level nulls");
  }
  ArraySpan* output = out->array_span_mutable();
  const int64_t out_offset = output->offset;
  const auto num_value_args = batch.values.size() - 1;
  const bool have_else_arg =
      static_cast<size_t>(conds_array.type->num_fields()) < num_value_args;
  uint8_t* out_valid = output->buffers[0].data;
  uint8_t* out_values = output->buffers[1].data;

  if (have_else_arg) {
    // Copy 'else' value into output
    CopyValues<Type>(batch.values.back(), /*in_offset=*/0, batch.length, out_valid,
                     out_values, out_offset);
  } else {
    // There's no 'else' argument, so we should have an all-null validity bitmap
    bit_util::SetBitsTo(out_valid, out_offset, batch.length, false);
  }

  // Allocate a temporary bitmap to determine which elements still need setting.
  ARROW_ASSIGN_OR_RAISE(auto mask_buffer, ctx->AllocateBitmap(batch.length));
  uint8_t* mask = mask_buffer->mutable_data();
  std::memset(mask, 0xFF, mask_buffer->size());

  // Then iterate through each argument in turn and set elements.
  for (int i = 0; i < batch.num_values() - (have_else_arg ? 2 : 1); i++) {
    const ArraySpan& cond_array = conds_array.child_data[i];
    const int64_t cond_offset = conds_array.offset + cond_array.offset;
    const uint8_t* cond_values = cond_array.buffers[1].data;
    const ExecValue& value = batch[i + 1];
    int64_t offset = 0;

    if (cond_array.GetNullCount() == 0) {
      // If no valid buffer, visit mask & cond bitmap simultaneously
      BinaryBitBlockCounter counter(mask, /*start_offset=*/0, cond_values, cond_offset,
                                    batch.length);
      while (offset < batch.length) {
        const auto block = counter.NextAndWord();
        if (block.AllSet()) {
          CopyValues<Type>(value, offset, block.length, out_valid, out_values,
                           out_offset + offset);
          bit_util::SetBitsTo(mask, offset, block.length, false);
        } else if (block.popcount) {
          for (int64_t j = 0; j < block.length; ++j) {
            if (bit_util::GetBit(mask, offset + j) &&
                bit_util::GetBit(cond_values, cond_offset + offset + j)) {
              CopyValues<Type>(value, offset + j, /*length=*/1, out_valid, out_values,
                               out_offset + offset + j);
              bit_util::SetBitTo(mask, offset + j, false);
            }
          }
        }
        offset += block.length;
      }
    } else {
      // Visit mask & cond bitmap & cond validity
      const uint8_t* cond_valid = cond_array.buffers[0].data;
      Bitmap bitmaps[3] = {{mask, /*offset=*/0, batch.length},
                           {cond_values, cond_offset, batch.length},
                           {cond_valid, cond_offset, batch.length}};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
        const uint64_t word = words[0] & words[1] & words[2];
        const int64_t block_length = std::min<int64_t>(64, batch.length - offset);
        if (word == std::numeric_limits<uint64_t>::max()) {
          CopyValues<Type>(value, offset, block_length, out_valid, out_values,
                           out_offset + offset);
          bit_util::SetBitsTo(mask, offset, block_length, false);
        } else if (word) {
          for (int64_t j = 0; j < block_length; ++j) {
            if (bit_util::GetBit(mask, offset + j) &&
                bit_util::GetBit(cond_valid, cond_offset + offset + j) &&
                bit_util::GetBit(cond_values, cond_offset + offset + j)) {
              CopyValues<Type>(value, offset + j, /*length=*/1, out_valid, out_values,
                               out_offset + offset + j);
              bit_util::SetBitTo(mask, offset + j, false);
            }
          }
        }
        offset += block_length;
      });
    }
  }
  if (!have_else_arg) {
    // Need to initialize any remaining null slots (uninitialized memory)
    BitBlockCounter counter(mask, /*offset=*/0, batch.length);
    int64_t offset = 0;
    auto bit_width = checked_cast<const FixedWidthType&>(*out->type()).bit_width();
    auto byte_width = bit_util::BytesForBits(bit_width);
    while (offset < batch.length) {
      const auto block = counter.NextWord();
      if (block.AllSet()) {
        if (bit_width == 1) {
          bit_util::SetBitsTo(out_values, out_offset + offset, block.length, false);
        } else {
          std::memset(out_values + (out_offset + offset) * byte_width, 0x00,
                      byte_width * block.length);
        }
      } else if (!block.NoneSet()) {
        for (int64_t j = 0; j < block.length; ++j) {
          if (bit_util::GetBit(out_valid, out_offset + offset + j)) continue;
          if (bit_width == 1) {
            bit_util::ClearBit(out_values, out_offset + offset + j);
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
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    if (batch.values[0].is_array()) {
      return ExecArrayCaseWhen<Type>(ctx, batch, out);
    }
    return ExecScalarCaseWhen<Type>(ctx, batch, out);
  }
};

template <>
struct CaseWhenFunctor<NullType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return Status::OK();
  }
};

Status ExecVarWidthScalarCaseWhen(KernelContext* ctx, const ExecSpan& batch,
                                  ExecResult* out) {
  const auto& conds = checked_cast<const StructScalar&>(*batch[0].scalar);
  ExecValue result;
  bool has_result = false;
  for (int i = 0; i < batch.num_values() - 1; i++) {
    if (i < static_cast<int>(conds.value.size())) {
      const Scalar& cond = *conds.value[i];
      if (cond.is_valid && internal::UnboxScalar<BooleanType>::Unbox(cond)) {
        result = batch[i + 1];
        has_result = true;
        break;
      }
    } else {
      // ELSE clause
      result = batch[i + 1];
      has_result = true;
      break;
    }
  }
  if (!has_result) {
    // All conditions false, no 'else' argument
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<Array> array,
        MakeArrayOfNull(out->type()->GetSharedPtr(), batch.length, ctx->memory_pool()));
    out->value = std::move(array->data());
  } else if (result.is_scalar()) {
    ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(*result.scalar, batch.length,
                                                          ctx->memory_pool()));
    out->value = std::move(array->data());
  } else {
    out->value = result.array.ToArrayData();
  }
  return Status::OK();
}

// Use std::function for reserve_data to avoid instantiating as many templates
template <typename AppendScalar>
static Status ExecVarWidthArrayCaseWhenImpl(
    KernelContext* ctx, const ExecSpan& batch, ExecResult* out,
    std::function<Status(ArrayBuilder*)> reserve_data, AppendScalar append_scalar) {
  const ArraySpan& conds_array = batch[0].array;
  const bool have_else_arg = conds_array.type->num_fields() < (batch.num_values() - 1);
  std::unique_ptr<ArrayBuilder> raw_builder;
  RETURN_NOT_OK(MakeBuilderExactIndex(ctx->memory_pool(), out->type()->GetSharedPtr(),
                                      &raw_builder));
  RETURN_NOT_OK(raw_builder->Reserve(batch.length));
  RETURN_NOT_OK(reserve_data(raw_builder.get()));

  for (int64_t row = 0; row < batch.length; row++) {
    int64_t selected = have_else_arg ? (batch.num_values() - 1) : -1;
    for (int64_t arg = 0; static_cast<size_t>(arg) < conds_array.child_data.size();
         arg++) {
      const ArraySpan& cond_array = conds_array.child_data[arg];
      if ((cond_array.buffers[0].data == nullptr ||
           bit_util::GetBit(cond_array.buffers[0].data,
                            conds_array.offset + cond_array.offset + row)) &&
          bit_util::GetBit(cond_array.buffers[1].data,
                           conds_array.offset + cond_array.offset + row)) {
        selected = arg + 1;
        break;
      }
    }
    if (selected < 0) {
      RETURN_NOT_OK(raw_builder->AppendNull());
      continue;
    }
    const ExecValue& source = batch[selected];
    if (source.is_scalar()) {
      const Scalar& scalar = *source.scalar;
      if (!scalar.is_valid) {
        RETURN_NOT_OK(raw_builder->AppendNull());
      } else {
        RETURN_NOT_OK(append_scalar(raw_builder.get(), scalar));
      }
    } else {
      const ArraySpan& array = source.array;
      if (array.buffers[0].data == nullptr ||
          bit_util::GetBit(array.buffers[0].data, array.offset + row)) {
        RETURN_NOT_OK(raw_builder->AppendArraySlice(array, row, /*length=*/1));
      } else {
        RETURN_NOT_OK(raw_builder->AppendNull());
      }
    }
  }
  ARROW_ASSIGN_OR_RAISE(auto temp_output, raw_builder->Finish());
  out->value = std::move(temp_output->data());
  return Status::OK();
}

// Single instantiation using ArrayBuilder::AppendScalar for append_scalar
static Status ExecVarWidthArrayCaseWhen(
    KernelContext* ctx, const ExecSpan& batch, ExecResult* out,
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
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    /// TODO(wesm): should this be a DCHECK? Or checked elsewhere
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ExecVarWidthArrayCaseWhenImpl(
        ctx, batch, out,
        // ReserveData
        [&](ArrayBuilder* raw_builder) {
          int64_t reservation = 0;
          for (int arg = 1; arg < batch.num_values(); arg++) {
            const ExecValue& source = batch[arg];
            if (source.is_scalar()) {
              const auto& scalar = checked_cast<const BaseBinaryScalar&>(*source.scalar);
              if (!scalar.value) continue;
              reservation =
                  std::max<int64_t>(reservation, batch.length * scalar.value->size());
            } else {
              const ArraySpan& array = source.array;
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
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    /// TODO(wesm): should this be a DCHECK? Or checked elsewhere
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ExecVarWidthArrayCaseWhen(
        ctx, batch, out,
        // ReserveData
        [&](ArrayBuilder* raw_builder) {
          auto builder = checked_cast<BuilderType*>(raw_builder);
          auto child_builder = builder->value_builder();

          int64_t reservation = 0;
          for (int arg = 1; arg < batch.num_values(); arg++) {
            const ExecValue& source = batch[arg];
            if (!source.is_array()) {
              const auto& scalar = checked_cast<const BaseListScalar&>(*source.scalar);
              if (!scalar.value) continue;
              reservation =
                  std::max<int64_t>(reservation, batch.length * scalar.value->length());
            } else {
              const ArraySpan& array = source.array;
              reservation = std::max<int64_t>(reservation, array.child_data[0].length);
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
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    /// TODO(wesm): should this be a DCHECK? Or checked elsewhere
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ExecVarWidthArrayCaseWhen(ctx, batch, out, ReserveNoData);
  }
};

template <>
struct CaseWhenFunctor<StructType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    /// TODO(wesm): should this be a DCHECK? Or checked elsewhere
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ExecVarWidthArrayCaseWhen(ctx, batch, out, ReserveNoData);
  }
};

template <>
struct CaseWhenFunctor<FixedSizeListType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    /// TODO(wesm): should this be a DCHECK? Or checked elsewhere
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
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
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    /// TODO(wesm): should this be a DCHECK? Or checked elsewhere
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecVarWidthArrayCaseWhen(ctx, batch, out, ReserveNoData);
  }
};

template <>
struct CaseWhenFunctor<DictionaryType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    /// TODO(wesm): should this be a DCHECK? Or checked elsewhere
    if (batch[0].null_count() > 0) {
      return Status::Invalid("cond struct must not have outer nulls");
    }
    if (batch[0].is_scalar()) {
      return ExecVarWidthScalarCaseWhen(ctx, batch, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ExecVarWidthArrayCaseWhen(ctx, batch, out, ReserveNoData);
  }
};

struct CoalesceFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));
    using arrow::compute::detail::DispatchExactImpl;

    // TODO(ARROW-14105): also apply casts to dictionary indices/values
    if (is_dictionary((*types)[0].id()) &&
        std::all_of(types->begin() + 1, types->end(),
                    [&](const TypeHolder& type) { return type == (*types)[0]; })) {
      auto kernel = DispatchExactImpl(this, *types);
      DCHECK(kernel);
      return kernel;
    }

    // Do not DispatchExact here since we want to rescale decimals if necessary
    EnsureDictionaryDecoded(types);
    if (auto type = CommonNumeric(types->data(), types->size())) {
      ReplaceTypes(type, types);
    }
    if (auto type = CommonBinary(types->data(), types->size())) {
      ReplaceTypes(type, types);
    }
    if (auto type = CommonTemporal(types->data(), types->size())) {
      ReplaceTypes(type, types);
    }
    if (HasDecimal(*types)) {
      RETURN_NOT_OK(CastDecimalArgs(types->data(), types->size()));
    }
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

// Helper: copy from a source value into all null slots of the output
template <typename Type>
void CopyValuesAllValid(const ExecValue& source, uint8_t* out_valid, uint8_t* out_values,
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
  const auto byte_width = bit_util::BytesForBits(bit_width);
  while (true) {
    const auto run = bit_reader.NextRun();
    if (run.length == 0) {
      break;
    }
    if (!run.set) {
      if (bit_width == 1) {
        bit_util::SetBitsTo(out_values, out_offset + offset, run.length, false);
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
Status ExecArrayCoalesce(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ArraySpan* output = out->array_span_mutable();
  const int64_t out_offset = output->offset;
  // Use output validity buffer as mask to decide what values to copy
  uint8_t* out_valid = output->buffers[0].data;

  // Clear output validity buffer - no values are set initially
  bit_util::SetBitsTo(out_valid, out_offset, batch.length, false);
  uint8_t* out_values = output->buffers[1].data;

  for (const ExecValue& value : batch.values) {
    if (value.null_count() == 0) {
      // Valid scalar, or all-valid array
      CopyValuesAllValid<Type>(value, out_valid, out_values, out_offset, batch.length);
      break;
    } else if (value.is_array()) {
      // Array with nulls
      const ArraySpan& arr = value.array;
      const int64_t in_offset = arr.offset;
      const int64_t in_null_count = arr.GetNullCount();
      DCHECK_GT(in_null_count, 0);
      const DataType& type = *arr.type;
      const uint8_t* in_valid = arr.buffers[0].data;
      const uint8_t* in_values = arr.buffers[1].data;

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
            CopyDataUtils<Type>::CopyData(type, in_values, in_offset + offset, out_values,
                                          out_offset + offset, run.length);
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
            CopyValues<Type>(value, offset, block.length, out_valid, out_values,
                             out_offset + offset);
          } else if (block.popcount) {
            for (int64_t j = 0; j < block.length; ++j) {
              if (!bit_util::GetBit(out_valid, out_offset + offset + j) &&
                  bit_util::GetBit(in_valid, in_offset + offset + j)) {
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
Status ExecArrayScalarCoalesce(KernelContext* ctx, const ExecValue& left,
                               const ExecValue& right, int64_t length, ExecResult* out) {
  ArraySpan* output = out->array_span_mutable();
  const int64_t out_offset = output->offset;
  uint8_t* out_valid = output->buffers[0].data;
  uint8_t* out_values = output->buffers[1].data;

  const ArraySpan& left_arr = left.array;
  const uint8_t* left_valid = left_arr.buffers[0].data;
  const uint8_t* left_values = left_arr.buffers[1].data;
  const Scalar& right_scalar = *right.scalar;

  if (left.null_count() < length * 0.2) {
    // There are less than 20% nulls in the left array, so first copy
    // the left values, then fill any nulls with the right value
    CopyDataUtils<Type>::CopyData(*left_arr.type, left_values, left_arr.offset,
                                  out_values, out_offset, length);

    BitRunReader reader(left_valid, left_arr.offset, left_arr.length);
    int64_t offset = 0;
    while (true) {
      const auto run = reader.NextRun();
      if (run.length == 0) break;
      if (!run.set) {
        // All from right
        CopyDataUtils<Type>::CopyData(*right_scalar.type, right_scalar,
                                      /*in_offset=*/0, out_values, out_offset + offset,
                                      run.length);
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
        CopyDataUtils<Type>::CopyData(*left_arr.type, left_values,
                                      left_arr.offset + offset, out_values,
                                      out_offset + offset, run.length);
      } else {
        // All from right
        CopyDataUtils<Type>::CopyData(*right_scalar.type, right_scalar,
                                      /*in_offset=*/0, out_values, out_offset + offset,
                                      run.length);
      }
      offset += run.length;
    }
    DCHECK_EQ(offset, length);
  }

  if (right_scalar.is_valid || !left_valid) {
    bit_util::SetBitsTo(out_valid, out_offset, length, true);
  } else {
    arrow::internal::CopyBitmap(left_valid, left_arr.offset, length, out_valid,
                                out_offset);
  }
  return Status::OK();
}

// Special case: implement 'coalesce' for any 2 arguments for any fixed-width
// type (a 'fill_null' operation)
template <typename Type>
Status ExecBinaryCoalesce(KernelContext* ctx, const ExecValue& left,
                          const ExecValue& right, int64_t length, ExecResult* out) {
  ArraySpan* output = out->array_span_mutable();
  const int64_t out_offset = output->offset;
  uint8_t* out_valid = output->buffers[0].data;
  uint8_t* out_values = output->buffers[1].data;

  const int64_t left_null_count = left.null_count();
  const int64_t right_null_count = right.null_count();

  if (left.is_scalar()) {
    // (Scalar, Any)
    CopyValues<Type>(left.scalar->is_valid ? left : right, /*in_offset=*/0, length,
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
  const ArraySpan& left_arr = left.array;
  const ArraySpan& right_arr = right.array;
  const uint8_t* left_valid = left_arr.buffers[0].data;
  const uint8_t* left_values = left_arr.buffers[1].data;
  const uint8_t* right_valid = right_arr.buffers[0].data;
  const uint8_t* right_values = right_arr.buffers[1].data;

  BitRunReader bit_reader(left_valid, left_arr.offset, left_arr.length);
  int64_t offset = 0;
  while (true) {
    const auto run = bit_reader.NextRun();
    if (run.length == 0) {
      break;
    }
    if (run.set) {
      // All from left
      CopyDataUtils<Type>::CopyData(*left_arr.type, left_values, left_arr.offset + offset,
                                    out_values, out_offset + offset, run.length);
    } else {
      // All from right
      CopyDataUtils<Type>::CopyData(*right_arr.type, right_values,
                                    right_arr.offset + offset, out_values,
                                    out_offset + offset, run.length);
    }
    offset += run.length;
  }
  DCHECK_EQ(offset, length);

  if (right_null_count == 0) {
    bit_util::SetBitsTo(out_valid, out_offset, length, true);
  } else {
    arrow::internal::BitmapOr(left_valid, left_arr.offset, right_valid, right_arr.offset,
                              length, out_offset, out_valid);
  }
  return Status::OK();
}

template <typename AppendScalar>
static Status ExecVarWidthCoalesceImpl(KernelContext* ctx, const ExecSpan& batch,
                                       ExecResult* out,
                                       std::function<Status(ArrayBuilder*)> reserve_data,
                                       AppendScalar append_scalar) {
  // Special case: grab any leading non-null scalar or array arguments
  for (const ExecValue& value : batch.values) {
    if (value.is_scalar()) {
      if (!value.scalar->is_valid) continue;
      ARROW_ASSIGN_OR_RAISE(
          std::shared_ptr<Array> result,
          MakeArrayFromScalar(*value.scalar, batch.length, ctx->memory_pool()));
      out->value = std::move(result->data());
      return Status::OK();
    } else if (value.is_array() && !value.array.MayHaveNulls()) {
      out->value = value.array.ToArrayData();
      return Status::OK();
    }
    break;
  }
  std::unique_ptr<ArrayBuilder> raw_builder;
  RETURN_NOT_OK(MakeBuilderExactIndex(ctx->memory_pool(), out->type()->GetSharedPtr(),
                                      &raw_builder));
  RETURN_NOT_OK(raw_builder->Reserve(batch.length));
  RETURN_NOT_OK(reserve_data(raw_builder.get()));

  for (int64_t i = 0; i < batch.length; i++) {
    bool set = false;
    for (const auto& value : batch.values) {
      if (value.is_scalar()) {
        if (value.scalar->is_valid) {
          RETURN_NOT_OK(append_scalar(raw_builder.get(), *value.scalar));
          set = true;
          break;
        }
      } else {
        const ArraySpan& source = value.array;
        if (!source.MayHaveNulls() ||
            bit_util::GetBit(source.buffers[0].data, source.offset + i)) {
          RETURN_NOT_OK(raw_builder->AppendArraySlice(source, i, /*length=*/1));
          set = true;
          break;
        }
      }
    }
    if (!set) RETURN_NOT_OK(raw_builder->AppendNull());
  }
  ARROW_ASSIGN_OR_RAISE(auto temp_output, raw_builder->Finish());
  out->value = std::move(temp_output->data());
  out->array_data()->type = batch[0].type()->GetSharedPtr();
  return Status::OK();
}

static Status ExecVarWidthCoalesce(KernelContext* ctx, const ExecSpan& batch,
                                   ExecResult* out,
                                   std::function<Status(ArrayBuilder*)> reserve_data) {
  return ExecVarWidthCoalesceImpl(ctx, batch, out, std::move(reserve_data),
                                  [](ArrayBuilder* builder, const Scalar& scalar) {
                                    return builder->AppendScalar(scalar);
                                  });
}

template <typename Type, typename Enable = void>
struct CoalesceFunctor {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    if (!TypeTraits<Type>::is_parameter_free) {
      RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[0], batch.num_values()));
    }
    // Special case for two arguments (since "fill_null" is a common operation)
    if (batch.num_values() == 2) {
      return ExecBinaryCoalesce<Type>(ctx, batch[0], batch[1], batch.length, out);
    }
    return ExecArrayCoalesce<Type>(ctx, batch, out);
  }
};

template <>
struct CoalesceFunctor<NullType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return Status::OK();
  }
};

template <typename Type>
struct CoalesceFunctor<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    if (batch.num_values() == 2 && batch[0].is_array() && batch[1].is_scalar()) {
      // Specialized implementation for common case ('fill_null' operation)
      return ExecArrayScalar(ctx, batch[0].array, *batch[1].scalar, out);
    }
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArrayScalar(KernelContext* ctx, const ArraySpan& left,
                                const Scalar& right, ExecResult* out) {
    const int64_t null_count = left.GetNullCount();
    if (null_count == 0 || !right.is_valid) {
      // TODO(wesm): avoid ToArrayData()
      out->value = left.ToArrayData();
      return Status::OK();
    }
    BuilderType builder(left.type->GetSharedPtr(), ctx->memory_pool());
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

    std::string_view fill_value(*scalar.value);
    VisitArraySpanInline<Type>(
        left, [&](std::string_view s) { builder.UnsafeAppend(s); },
        [&]() { builder.UnsafeAppend(fill_value); });

    ARROW_ASSIGN_OR_RAISE(auto temp_output, builder.Finish());
    out->value = std::move(temp_output->data());
    out->array_data()->type = left.type->GetSharedPtr();
    return Status::OK();
  }

  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // TODO: do this without ToArrayData()?
    return ExecVarWidthCoalesceImpl(
        ctx, batch, out,
        [&](ArrayBuilder* builder) {
          int64_t reservation = 0;
          for (const auto& value : batch.values) {
            if (value.is_array()) {
              const ArrayType array(value.array.ToArrayData());
              reservation = std::max<int64_t>(reservation, array.total_values_length());
            } else {
              const Scalar& scalar = *value.scalar;
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
    Type, enable_if_t<(is_nested_type<Type>::value || is_dictionary_type<Type>::value) &&
                      !is_union_type<Type>::value>> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[0], batch.num_values()));
    return ExecArray(ctx, batch, out);
  }

  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ExecVarWidthCoalesce(ctx, batch, out, ReserveNoData);
  }
};

const Scalar& GetUnionScalar(const DenseUnionType&, const Scalar& value) {
  return *checked_cast<const DenseUnionScalar&>(value).value;
}

const Scalar& GetUnionScalar(const SparseUnionType&, const Scalar& value) {
  const auto& union_scalar = checked_cast<const SparseUnionScalar&>(value);
  return *union_scalar.value[union_scalar.child_id];
}

template <typename Type>
struct CoalesceFunctor<Type, enable_if_union<Type>> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // Unions don't have top-level nulls, so a specialized implementation is needed
    RETURN_NOT_OK(CheckIdenticalTypes(&batch.values[0], batch.num_values()));

    std::unique_ptr<ArrayBuilder> raw_builder;
    RETURN_NOT_OK(MakeBuilderExactIndex(ctx->memory_pool(), out->type()->GetSharedPtr(),
                                        &raw_builder));
    RETURN_NOT_OK(raw_builder->Reserve(batch.length));

    const auto& type = checked_cast<const Type&>(*out->type());
    for (int64_t i = 0; i < batch.length; i++) {
      bool set = false;
      for (const auto& value : batch.values) {
        if (value.is_scalar()) {
          const Scalar& scalar = *value.scalar;
          const auto& union_scalar = GetUnionScalar(type, scalar);
          if (scalar.is_valid && union_scalar.is_valid) {
            RETURN_NOT_OK(raw_builder->AppendScalar(scalar));
            set = true;
            break;
          }
        } else {
          const ArraySpan& source = value.array;
          // Peek at the relevant child array's validity bitmap
          if (std::is_same<Type, SparseUnionType>::value) {
            const int8_t type_id = source.GetValues<int8_t>(1)[i];
            const int child_id = type.child_ids()[type_id];
            const ArraySpan& child = source.child_data[child_id];
            if (!child.MayHaveNulls() ||
                bit_util::GetBit(child.buffers[0].data,
                                 source.offset + child.offset + i)) {
              RETURN_NOT_OK(raw_builder->AppendArraySlice(source, i, /*length=*/1));
              set = true;
              break;
            }
          } else {
            const int8_t type_id = source.GetValues<int8_t>(1)[i];
            const int32_t offset = source.GetValues<int32_t>(2)[i];
            const int child_id = type.child_ids()[type_id];
            const ArraySpan& child = source.child_data[child_id];
            if (!child.MayHaveNulls() ||
                bit_util::GetBit(child.buffers[0].data, child.offset + offset)) {
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
    out->value = std::move(temp_output->data());
    return Status::OK();
  }
};

template <typename Type>
Status ExecScalarChoose(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  DCHECK(!out->is_array_data());
  // For fixed-width types, this kernel has a full preallocation
  const auto& index_scalar = *batch[0].scalar;
  if (!index_scalar.is_valid) {
    if (out->is_array_span()) {
      // TODO(wesm): more graceful implementation than using
      // MakeNullScalar, which is a little bit lazy
      std::shared_ptr<Scalar> source = MakeNullScalar(out->type()->GetSharedPtr());
      ArraySpan* output = out->array_span_mutable();
      ExecValue copy_source;
      copy_source.SetScalar(source.get());
      CopyValues<Type>(copy_source, /*row=*/0, batch.length,
                       output->GetValues<uint8_t>(0, /*absolute_offset=*/0),
                       output->GetValues<uint8_t>(1, /*absolute_offset=*/0),
                       output->offset);
    }
    return Status::OK();
  }
  auto index = UnboxScalar<Int64Type>::Unbox(index_scalar);
  if (index < 0 || static_cast<size_t>(index + 1) >= batch.values.size()) {
    return Status::IndexError("choose: index ", index, " out of range");
  }
  auto source = batch[index + 1];
  ArraySpan* output = out->array_span_mutable();
  CopyValues<Type>(source, /*row=*/0, batch.length,
                   output->GetValues<uint8_t>(0, /*absolute_offset=*/0),
                   output->GetValues<uint8_t>(1, /*absolute_offset=*/0), output->offset);
  return Status::OK();
}

template <typename Type>
Status ExecArrayChoose(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ArraySpan* output = out->array_span_mutable();
  const int64_t out_offset = output->offset;
  // Need a null bitmap if any input has nulls
  uint8_t* out_valid = nullptr;
  if (std::any_of(batch.values.begin(), batch.values.end(),
                  [](const ExecValue& d) { return d.null_count() > 0; })) {
    out_valid = output->buffers[0].data;
  } else {
    bit_util::SetBitsTo(output->buffers[0].data, out_offset, batch.length, true);
  }
  uint8_t* out_values = output->buffers[1].data;
  int64_t row = 0;
  return VisitArrayValuesInline<Int64Type>(
      batch[0].array,
      [&](int64_t index) {
        if (index < 0 || (index + 1) >= batch.num_values()) {
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
        bit_util::ClearBit(out_valid, out_offset + row);
        row++;
        return Status::OK();
      });
}

template <typename Type, typename Enable = void>
struct ChooseFunctor {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    if (batch.values[0].is_scalar()) {
      return ExecScalarChoose<Type>(ctx, batch, out);
    }
    return ExecArrayChoose<Type>(ctx, batch, out);
  }
};

template <>
struct ChooseFunctor<NullType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return Status::OK();
  }
};

template <typename Type>
struct ChooseFunctor<Type, enable_if_base_binary<Type>> {
  using offset_type = typename Type::offset_type;
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    if (batch[0].is_scalar()) {
      const Scalar& index_scalar = *batch[0].scalar;
      if (!index_scalar.is_valid) {
        if (out->is_array_data()) {
          ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> temp_array,
                                MakeArrayOfNull(out->type()->GetSharedPtr(), batch.length,
                                                ctx->memory_pool()));
          out->value = std::move(temp_array->data());
        }
        return Status::OK();
      }
      auto index = UnboxScalar<Int64Type>::Unbox(index_scalar);
      if (index < 0 || (index + 1) >= batch.num_values()) {
        return Status::IndexError("choose: index ", index, " out of range");
      }
      const ExecValue& source = batch.values[index + 1];
      if (source.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            std::shared_ptr<Array> temp_array,
            MakeArrayFromScalar(*source.scalar, batch.length, ctx->memory_pool()));
        out->value = std::move(temp_array->data());
      } else {
        DCHECK(out->is_array_data());
        // source is an array
        // TODO(wesm): avoiding ToArrayData()
        out->value = source.array.ToArrayData();
      }
      return Status::OK();
    }

    // Row-wise implementation
    BuilderType builder(out->type()->GetSharedPtr(), ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    int64_t reserve_data = 0;

    // The first value in the batch is the index array which is int64
    for (int i = 1; i < batch.num_values(); ++i) {
      const ExecValue& value = batch[i];
      if (value.is_scalar()) {
        if (!value.scalar->is_valid) continue;
        const auto row_length =
            checked_cast<const BaseBinaryScalar&>(*value.scalar).value->size();
        reserve_data = std::max<int64_t>(reserve_data, batch.length * row_length);
        continue;
      }
      const ArraySpan& arr = value.array;
      const offset_type* offsets = arr.GetValues<offset_type>(1);
      const offset_type values_length = offsets[arr.length] - offsets[0];
      reserve_data = std::max<int64_t>(reserve_data, values_length);
    }
    RETURN_NOT_OK(builder.ReserveData(reserve_data));
    int64_t row = 0;
    RETURN_NOT_OK(VisitArrayValuesInline<Int64Type>(
        batch[0].array,
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
    std::shared_ptr<Array> temp_output;
    RETURN_NOT_OK(builder.Finish(&temp_output));
    std::shared_ptr<DataType> actual_result_type = out->type()->GetSharedPtr();
    out->value = std::move(temp_output->data());
    // Builder type != logical type due to GenerateTypeAgnosticVarBinaryBase
    out->array_data()->type = std::move(actual_result_type);
    return Status::OK();
  }

  static Status CopyValue(const ExecValue& value, BuilderType* builder, int64_t row) {
    if (value.is_scalar()) {
      const auto& scalar = checked_cast<const BaseBinaryScalar&>(*value.scalar);
      if (!scalar.value) return builder->AppendNull();
      return builder->Append(scalar.value->data(),
                             static_cast<offset_type>(scalar.value->size()));
    }
    const ArraySpan& source = value.array;
    if (!source.MayHaveNulls() ||
        bit_util::GetBit(source.buffers[0].data, source.offset + row)) {
      const uint8_t* data = source.buffers[2].data;
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

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    // The first argument is always int64 or promoted to it. The kernel is dispatched
    // based on the type of the rest of the arguments.
    RETURN_NOT_OK(CheckArity(types->size()));
    EnsureDictionaryDecoded(types);
    if (types->front().id() != Type::INT64) {
      (*types)[0] = int64();
    }
    if (auto type = CommonNumeric(types->data() + 1, types->size() - 1)) {
      for (auto it = types->begin() + 1; it != types->end(); it++) {
        *it = type;
      }
    }
    if (auto kernel = DispatchExactImpl(this, {types->front(), types->back()})) {
      return kernel;
    }
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

void AddCaseWhenKernel(const std::shared_ptr<CaseWhenFunction>& scalar_function,
                       detail::GetTypeId get_id, ArrayKernelExec exec) {
  ScalarKernel kernel(
      KernelSignature::Make({InputType(Type::STRUCT), InputType(get_id.id)}, LastType,
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
  ScalarKernel kernel(KernelSignature::Make({InputType(get_id.id)}, FirstType,
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
  ScalarKernel kernel(KernelSignature::Make({Type::INT64, InputType(get_id.id)}, LastType,
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
    ("`cond` must be a struct of Boolean values. `cases` can be a mix\n"
     "of scalar and array arguments (of any type, but all must be the\n"
     "same type or castable to a common type), with either exactly one\n"
     "datum per child of `cond`, or one more `cases` than children of\n"
     "`cond` (in which case we have an \"else\" value).\n"
     "\n"
     "Each row of the output will be the corresponding value of the\n"
     "first datum in `cases` for which the corresponding child of `cond`\n"
     "is true, or otherwise the \"else\" value (if given), or null.\n"
     "\n"
     "Essentially, this implements a switch-case or if-else, if-else... "
     "statement."),
    {"cond", "*cases"}};

const FunctionDoc coalesce_doc{
    "Select the first non-null value",
    ("Each row of the output will be the value from the first corresponding input\n"
     "for which the value is not null. If all inputs are null in a row, the output\n"
     "will be null."),
    {"*values"}};

const FunctionDoc choose_doc{
    "Choose values from several arrays",
    ("For each row, the value of the first argument is used as a 0-based index\n"
     "into the list of `values` arrays (i.e. index 0 selects the first of the\n"
     "`values` arrays). The output value is the corresponding value of the\n"
     "selected argument.\n"
     "\n"
     "If an index is null, the output will be null."),
    {"indices", "*values"}};
}  // namespace

void RegisterScalarIfElse(FunctionRegistry* registry) {
  {
    auto func =
        std::make_shared<IfElseFunction>("if_else", Arity::Ternary(), if_else_doc);

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
        "case_when", Arity::VarArgs(/*min_args=*/2), case_when_doc);
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
        "coalesce", Arity::VarArgs(/*min_args=*/1), coalesce_doc);
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
    AddCoalesceKernel(func, Type::DICTIONARY, CoalesceFunctor<DictionaryType>::Exec);
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<ChooseFunction>("choose", Arity::VarArgs(/*min_args=*/2),
                                                 choose_doc);
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
