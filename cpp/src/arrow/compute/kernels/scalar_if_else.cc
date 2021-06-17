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

  // if cond & left & right all ones, then output is all valid. output validity buffer
  // is already allocated, hence set all bits
  if (cond_const == kAllValid && left_const == kAllValid && right_const == kAllValid) {
    BitUtil::SetBitmap(output->buffers[0]->mutable_data(), output->offset,
                       output->length);
    return Status::OK();
  }

  if (left_const == kAllValid && right_const == kAllValid) {
    // if both left and right are valid, no need to calculate out_valid bitmap. Copy
    // cond validity buffer
    arrow::internal::CopyBitmap(cond.buffers[0]->data(), cond.offset, cond.length,
                                output->buffers[0]->mutable_data(), output->offset);
    return Status::OK();
  }

  // lambda function that will be used inside the visitor
  auto apply = [&](uint64_t c_valid, uint64_t c_data, uint64_t l_valid,
                   uint64_t r_valid) {
    return c_valid & ((c_data & l_valid) | (~c_data & r_valid));
  };

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
/// buffer will be inverted before calling the handle_bulk or handle_each functions.
/// This is useful, when left is an array and right is scalar. Then rather than
/// copying data from the right to output, we can copy left data to the output and
/// invert the cond data to fill right values. Filling out with a scalar is presumed to
/// be more efficient than filling with an array
template <typename HandleBulk, typename HandleEach, bool invert = false>
static void RunIfElseLoop(const ArrayData& cond, HandleBulk handle_bulk,
                          HandleEach handle_each) {
  int64_t data_offset = 0;
  int64_t bit_offset = cond.offset;
  const auto* cond_data = cond.buffers[1]->data();  // this is a BoolArray

  BitmapWordReader<Word> cond_reader(cond_data, cond.offset, cond.length);

  int64_t cnt = cond_reader.words();
  while (cnt--) {
    Word word = cond_reader.NextWord();
    if (invert) {
      if (word == 0) {
        handle_bulk(data_offset, word_len);
      } else if (word != UINT64_MAX) {
        for (int64_t i = 0; i < word_len; ++i) {
          if (!BitUtil::GetBit(cond_data, bit_offset + i)) {
            handle_each(data_offset + i);
          }
        }
      }
    } else {
      if (word == UINT64_MAX) {
        handle_bulk(data_offset, word_len);
      } else if (word) {
        for (int64_t i = 0; i < word_len; ++i) {
          if (BitUtil::GetBit(cond_data, bit_offset + i)) {
            handle_each(data_offset + i);
          }
        }
      }
    }
    data_offset += word_len;
    bit_offset += word_len;
  }

  cnt = cond_reader.trailing_bytes();
  while (cnt--) {
    int valid_bits;
    uint8_t byte = cond_reader.NextTrailingByte(valid_bits);
    if (invert) {
      if (byte == 0 && valid_bits == 8) {
        handle_bulk(data_offset, 8);
      } else if (byte != UINT8_MAX) {
        for (int i = 0; i < valid_bits; ++i) {
          if (!BitUtil::GetBit(cond_data, bit_offset + i)) {
            handle_each(data_offset + i);
          }
        }
      }
    } else {
      if (byte == UINT8_MAX && valid_bits == 8) {
        handle_bulk(data_offset, 8);
      } else if (byte) {
        for (int i = 0; i < valid_bits; ++i) {
          if (BitUtil::GetBit(cond_data, bit_offset + i)) {
            handle_each(data_offset + i);
          }
        }
      }
    }
    data_offset += 8;
    bit_offset += 8;
  }
}

template <typename HandleBulk, typename HandleEach>
static void RunIfElseLoopInverted(const ArrayData& cond, HandleBulk handle_bulk,
                                  HandleEach handle_each) {
  return RunIfElseLoop<HandleBulk, HandleEach, true>(cond, handle_bulk, handle_each);
}

/// Runs if-else when cond is a scalar. Two special functions are required,
/// 1.CopyArrayData, 2. BroadcastScalar
template <typename CopyArrayData, typename BroadcastScalar>
static Status RunIfElseScalar(const BooleanScalar& cond, const Datum& left,
                              const Datum& right, Datum* out,
                              CopyArrayData copy_array_data,
                              BroadcastScalar broadcast_scalar) {
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

    RunIfElseLoop(
        cond,
        [&](int64_t data_offset, int64_t num_elems) {
          std::memcpy(out_values + data_offset, left_data + data_offset,
                      num_elems * sizeof(T));
        },
        [&](int64_t data_offset) { out_values[data_offset] = left_data[data_offset]; });

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

    RunIfElseLoop(
        cond,
        [&](int64_t data_offset, int64_t num_elems) {
          std::fill(out_values + data_offset, out_values + data_offset + num_elems,
                    left_data);
        },
        [&](int64_t data_offset) { out_values[data_offset] = left_data; });

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

    RunIfElseLoopInverted(
        cond,
        [&](int64_t data_offset, int64_t num_elems) {
          std::fill(out_values + data_offset, out_values + data_offset + num_elems,
                    right_data);
        },
        [&](int64_t data_offset) { out_values[data_offset] = right_data; });

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
    RunIfElseLoop(
        cond,
        [&](int64_t data_offset, int64_t num_elems) {
          std::fill(out_values + data_offset, out_values + data_offset + num_elems,
                    left_data);
        },
        [&](int64_t data_offset) { out_values[data_offset] = left_data; });

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
struct ResolveIfElseExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // cond is scalar
    if (batch[0].is_scalar()) {
      const auto& cond = batch[0].scalar_as<BooleanScalar>();
      return IfElseFunctor<Type>::Call(ctx, cond, batch[1], batch[2], out);
    }

    // cond is array. Use functors to sort things out
    ARROW_RETURN_NOT_OK(
        PromoteNullsVisitor(ctx, batch[0], batch[1], batch[2], out->mutable_array()));

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

template <>
struct ResolveIfElseExec<NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].is_scalar()) {
      *out = MakeNullScalar(null());
    } else {
      const std::shared_ptr<ArrayData>& cond_array = batch[0].array();
      ARROW_ASSIGN_OR_RAISE(
          *out, MakeArrayOfNull(null(), cond_array->length, ctx->memory_pool()));
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
                      ResolveIfElseExec<NullType>::Exec);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  kernel.can_write_into_slices = false;

  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

void AddPrimitiveIfElseKernels(const std::shared_ptr<ScalarFunction>& scalar_function,
                               const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = internal::GenerateTypeAgnosticPrimitive<ResolveIfElseExec>(*type);
    // cond array needs to be boolean always
    ScalarKernel kernel({boolean(), type, type}, type, exec);
    kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::PREALLOCATE;
    kernel.can_write_into_slices = true;

    DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
  }
}

// Helper to copy or broadcast fixed-width values between buffers.
template <typename Type, typename Enable = void>
struct CopyFixedWidth {};
template <>
struct CopyFixedWidth<BooleanType> {
  static void CopyScalar(const Scalar& scalar, uint8_t* out_values, const int64_t offset,
                         const int64_t length) {
    const bool value = UnboxScalar<BooleanType>::Unbox(scalar);
    BitUtil::SetBitsTo(out_values, offset, length, value);
  }
  static void CopyArray(const ArrayData& array, uint8_t* out_values, const int64_t offset,
                        const int64_t length) {
    arrow::internal::CopyBitmap(array.buffers[1]->data(), array.offset + offset, length,
                                out_values, offset);
  }
};
template <typename Type>
struct CopyFixedWidth<Type, enable_if_number<Type>> {
  using CType = typename TypeTraits<Type>::CType;
  static void CopyScalar(const Scalar& values, uint8_t* raw_out_values,
                         const int64_t offset, const int64_t length) {
    CType* out_values = reinterpret_cast<CType*>(raw_out_values);
    const CType value = UnboxScalar<Type>::Unbox(values);
    std::fill(out_values + offset, out_values + offset + length, value);
  }
  static void CopyArray(const ArrayData& array, uint8_t* raw_out_values,
                        const int64_t offset, const int64_t length) {
    CType* out_values = reinterpret_cast<CType*>(raw_out_values);
    const CType* in_values = array.GetValues<CType>(1);
    std::copy(in_values + offset, in_values + offset + length, out_values + offset);
  }
};
template <typename Type>
struct CopyFixedWidth<Type, enable_if_same<Type, FixedSizeBinaryType>> {
  static void CopyScalar(const Scalar& values, uint8_t* out_values, const int64_t offset,
                         const int64_t length) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(*values.type).byte_width();
    uint8_t* next = out_values + (width * offset);
    const auto& scalar = checked_cast<const FixedSizeBinaryScalar&>(values);
    if (!scalar.is_valid) return;
    DCHECK_EQ(scalar.value->size(), width);
    for (int i = 0; i < length; i++) {
      std::memcpy(next, scalar.value->data(), width);
      next += width;
    }
  }
  static void CopyArray(const ArrayData& array, uint8_t* out_values, const int64_t offset,
                        const int64_t length) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(*array.type).byte_width();
    uint8_t* next = out_values + (width * offset);
    const auto* in_values = array.GetValues<uint8_t>(1, (offset + array.offset) * width);
    std::memcpy(next, in_values, length * width);
  }
};
template <typename Type>
struct CopyFixedWidth<Type, enable_if_decimal<Type>> {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  static void CopyScalar(const Scalar& values, uint8_t* out_values, const int64_t offset,
                         const int64_t length) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(*values.type).byte_width();
    uint8_t* next = out_values + (width * offset);
    const auto& scalar = checked_cast<const ScalarType&>(values);
    const auto value = scalar.value.ToBytes();
    for (int i = 0; i < length; i++) {
      std::memcpy(next, value.data(), width);
      next += width;
    }
  }
  static void CopyArray(const ArrayData& array, uint8_t* out_values, const int64_t offset,
                        const int64_t length) {
    const int32_t width =
        checked_cast<const FixedSizeBinaryType&>(*array.type).byte_width();
    uint8_t* next = out_values + (width * offset);
    const auto* in_values = array.GetValues<uint8_t>(1, (offset + array.offset) * width);
    std::memcpy(next, in_values, length * width);
  }
};
// Copy fixed-width values from a scalar/array datum into an output values buffer
template <typename Type>
void CopyValues(const Datum& values, uint8_t* out_valid, uint8_t* out_values,
                const int64_t offset, const int64_t length) {
  using Copier = CopyFixedWidth<Type>;
  if (values.is_scalar()) {
    const auto& scalar = *values.scalar();
    if (out_valid) {
      BitUtil::SetBitsTo(out_valid, offset, length, scalar.is_valid);
    }
    Copier::CopyScalar(scalar, out_values, offset, length);
  } else {
    const ArrayData& array = *values.array();
    if (out_valid) {
      if (array.MayHaveNulls()) {
        arrow::internal::CopyBitmap(array.buffers[0]->data(), array.offset + offset,
                                    length, out_valid, offset);
      } else {
        BitUtil::SetBitsTo(out_valid, offset, length, true);
      }
    }
    Copier::CopyArray(array, out_values, offset, length);
  }
}

struct CaseWhenFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));
    std::vector<ValueDescr> value_types;
    for (size_t i = 0; i < values->size() - 1; i += 2) {
      ValueDescr* cond = &(*values)[i];
      if (cond->type->id() == Type::NA) {
        cond->type = boolean();
      }
      if (cond->type->id() != Type::BOOL) {
        return Status::TypeError("Condition arguments must be boolean, but argument ", i,
                                 " was ", cond->type->ToString());
      }
      value_types.push_back((*values)[i + 1]);
    }
    if (values->size() % 2 != 0) {
      // Have an ELSE clause
      value_types.push_back(values->back());
    }
    EnsureDictionaryDecoded(&value_types);
    if (auto type = CommonNumeric(value_types)) {
      ReplaceTypes(type, &value_types);
    }

    const DataType& common_values_type = *value_types.front().type;
    auto next_type = value_types.cbegin();
    for (size_t i = 0; i < values->size(); i += 2) {
      if (!common_values_type.Equals(next_type->type)) {
        return Status::TypeError("Value arguments must be of same type, but argument ", i,
                                 " was ", next_type->type->ToString(), " (expected ",
                                 common_values_type.ToString(), ")");
      }
      if (i == values->size() - 1) {
        // ELSE
        (*values)[i] = *next_type++;
      } else {
        (*values)[i + 1] = *next_type++;
      }
    }

    // We register a unary kernel for each value type and dispatch to it after validation.
    if (auto kernel = DispatchExactImpl(this, {values->back()})) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

// Implement a 'case when' (SQL)/'select' (NumPy) function for any scalar arguments
Status ExecScalarCaseWhen(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  for (size_t i = 0; i < batch.values.size() - 1; i += 2) {
    const Scalar& cond = *batch[i].scalar();
    if (cond.is_valid && internal::UnboxScalar<BooleanType>::Unbox(cond)) {
      *out = batch[i + 1];
      return Status::OK();
    }
  }
  if (batch.values.size() % 2 == 0) {
    // No ELSE
    *out = MakeNullScalar(batch[1].type());
  } else {
    *out = batch.values.back();
  }
  return Status::OK();
}

// Implement 'case when' for any mix of scalar/array arguments for any fixed-width type,
// given helper functions to copy data from a source array to a target array and to
// allocate a values buffer
template <typename Type>
Status ExecArrayCaseWhen(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ArrayData* output = out->mutable_array();
  const bool have_else_arg = batch.values.size() % 2 != 0;
  // Check if we may need a validity bitmap
  uint8_t* out_valid = nullptr;

  bool need_valid_bitmap = false;
  if (!have_else_arg) {
    // If we don't have an else arg -> need a bitmap since we may emit nulls
    need_valid_bitmap = true;
  } else if (batch.values.back().null_count() > 0) {
    // If the 'else' array has a null count we need a validity bitmap
    need_valid_bitmap = true;
  } else {
    // Otherwise if any value array has a null count we need a validity bitmap
    for (size_t i = 1; i < batch.values.size(); i += 2) {
      if (batch[i].null_count() > 0) {
        need_valid_bitmap = true;
        break;
      }
    }
  }
  if (need_valid_bitmap) {
    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(batch.length));
    out_valid = output->buffers[0]->mutable_data();
  }

  // Initialize values buffer
  uint8_t* out_values = output->buffers[1]->mutable_data();
  if (have_else_arg) {
    // Copy 'else' value into output
    CopyValues<Type>(batch.values.back(), out_valid, out_values, /*offset=*/0,
                     batch.length);
  } else if (need_valid_bitmap) {
    // There's no 'else' argument, so we should have an all-null validity bitmap
    std::memset(out_valid, 0x00, output->buffers[0]->size());
  }

  // Allocate a temporary bitmap to determine which elements still need setting.
  ARROW_ASSIGN_OR_RAISE(auto mask_buffer, ctx->AllocateBitmap(batch.length));
  uint8_t* mask = mask_buffer->mutable_data();
  std::memset(mask, 0xFF, mask_buffer->size());
  // Then iterate through each argument in turn and set elements.
  for (size_t i = 0; i < batch.values.size() - 1; i += 2) {
    const Datum& cond_datum = batch[i];
    const Datum& values_datum = batch[i + 1];
    if (cond_datum.is_scalar()) {
      const Scalar& cond_scalar = *cond_datum.scalar();
      const bool cond =
          cond_scalar.is_valid && UnboxScalar<BooleanType>::Unbox(cond_scalar);
      if (!cond) continue;
      BitBlockCounter counter(mask, /*start_offset=*/0, batch.length);
      int64_t offset = 0;
      while (offset < batch.length) {
        const auto block = counter.NextWord();
        if (block.AllSet()) {
          CopyValues<Type>(values_datum, out_valid, out_values, offset, block.length);
        } else if (block.popcount) {
          for (int64_t j = 0; j < block.length; ++j) {
            if (BitUtil::GetBit(mask, offset + j)) {
              CopyValues<Type>(values_datum, out_valid, out_values, offset + j,
                               /*length=*/1);
            }
          }
        }
        offset += block.length;
      }
      break;
    }

    const ArrayData& cond_array = *cond_datum.array();
    const uint8_t* cond_values = cond_array.buffers[1]->data();
    int64_t offset = 0;
    // If no valid buffer, visit mask & value bitmap simultaneously
    if (cond_array.GetNullCount() == 0) {
      BinaryBitBlockCounter counter(mask, /*start_offset=*/0, cond_values,
                                    cond_array.offset, batch.length);
      while (offset < batch.length) {
        const auto block = counter.NextAndWord();
        if (block.AllSet()) {
          CopyValues<Type>(values_datum, out_valid, out_values, offset, block.length);
          BitUtil::SetBitsTo(mask, offset, block.length, false);
        } else if (block.popcount) {
          for (int64_t j = 0; j < block.length; ++j) {
            if (BitUtil::GetBit(mask, offset + j) &&
                BitUtil::GetBit(cond_values, cond_array.offset + offset + j)) {
              CopyValues<Type>(values_datum, out_valid, out_values, offset + j,
                               /*length=*/1);
              BitUtil::SetBitTo(mask, offset + j, false);
            }
          }
        }
        offset += block.length;
      }
      continue;
    }

    // Else visit all three bitmaps simultaneously
    const uint8_t* cond_valid = cond_array.buffers[0]->data();
    Bitmap bitmaps[3] = {{mask, /*offset=*/0, batch.length},
                         {cond_values, cond_array.offset, batch.length},
                         {cond_valid, cond_array.offset, batch.length}};
    Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 3> words) {
      const uint64_t word = words[0] & words[1] & words[2];
      const int64_t block_length = std::min<int64_t>(64, batch.length - offset);
      if (word == std::numeric_limits<uint64_t>::max()) {
        CopyValues<Type>(values_datum, out_valid, out_values, offset, block_length);
        BitUtil::SetBitsTo(mask, offset, block_length, false);
      } else if (word) {
        for (int64_t j = 0; j < block_length; ++j) {
          if (BitUtil::GetBit(mask, offset + j) &&
              BitUtil::GetBit(cond_valid, cond_array.offset + offset + j) &&
              BitUtil::GetBit(cond_values, cond_array.offset + offset + j)) {
            CopyValues<Type>(values_datum, out_valid, out_values, offset + j,
                             /*length=*/1);
            BitUtil::SetBitTo(mask, offset + j, false);
          }
        }
      }
      offset += block_length;
    });
  }
  return Status::OK();
}

template <typename Type, typename Enable = void>
struct CaseWhenFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    for (const auto& datum : batch.values) {
      if (datum.is_array()) {
        return ExecArrayCaseWhen<Type>(ctx, batch, out);
      }
    }
    return ExecScalarCaseWhen(ctx, batch, out);
  }
};

template <>
struct CaseWhenFunctor<NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
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
  ScalarKernel kernel(KernelSignature::Make({InputType(get_id.id)}, OutputType(LastType),
                                            /*is_varargs=*/true),
                      exec);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::PREALLOCATE;
  DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
}

void AddPrimitiveCaseWhenKernels(const std::shared_ptr<CaseWhenFunction>& scalar_function,
                                 const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = GenerateTypeAgnosticPrimitive<CaseWhenFunctor>(*type);
    AddCaseWhenKernel(scalar_function, type, std::move(exec));
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
    ("`cond` must be a sequence of alternating Boolean condition data "
     "and value data (of any type, but all must be the same type or "
     "castable to a common type), along with an optional datum of "
     "\"else\" values. At least one datum must be given.\n"
     "Each row of the output will be the corresponding value of the "
     "first value datum for which the corresponding condition datum "
     "is true, or otherwise the \"else\" value (if given), or null. "
     "Essentially, this implements a switch-case or if-else if-else "
     "statement."),
    {"*cond"}};
}  // namespace

void RegisterScalarIfElse(FunctionRegistry* registry) {
  {
    auto func =
        std::make_shared<IfElseFunction>("if_else", Arity::Ternary(), &if_else_doc);

    AddPrimitiveIfElseKernels(func, NumericTypes());
    AddPrimitiveIfElseKernels(func, TemporalTypes());
    AddPrimitiveIfElseKernels(func, {boolean(), day_time_interval(), month_interval()});
    AddNullIfElseKernel(func);
    // todo add binary kernels
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
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
