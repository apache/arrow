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

#include "arrow/compute/kernels/boolean.h"

#include <bitset>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::Bitmap;
using internal::BitmapAnd;
using internal::BitmapOr;
using internal::BitmapXor;
using internal::CountSetBits;
using internal::InvertBitmap;

namespace compute {

class BooleanUnaryKernel : public UnaryKernel {
 public:
  std::shared_ptr<DataType> out_type() const override { return boolean(); }
};

class InvertKernel : public BooleanUnaryKernel {
  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());
    constexpr int64_t kZeroDestOffset = 0;

    const ArrayData& in_data = *input.array();
    std::shared_ptr<ArrayData> result = out->array();
    result->type = boolean();

    // Handle output data buffer
    if (in_data.length > 0) {
      RETURN_NOT_OK(detail::PropagateNulls(ctx, in_data, result.get()));
      const Buffer& data_buffer = *in_data.buffers[1];
      DCHECK_LE(BitUtil::BytesForBits(in_data.length), data_buffer.size());
      InvertBitmap(data_buffer.data(), in_data.offset, in_data.length,
                   result->buffers[1]->mutable_data(), kZeroDestOffset);
    }
    return Status::OK();
  }
};

Status Invert(FunctionContext* ctx, const Datum& value, Datum* out) {
  InvertKernel invert;
  detail::PrimitiveAllocatingUnaryKernel kernel(&invert);

  std::vector<Datum> result;
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, &kernel, value, &result));

  *out = detail::WrapDatumsLike(value, invert.out_type(), result);
  return Status::OK();
}

enum class ResolveNull { KLEENE_LOGIC, PROPAGATE };

class BinaryBooleanKernel : public BinaryKernel {
 public:
  explicit BinaryBooleanKernel(ResolveNull resolve_null) : resolve_null_(resolve_null) {}

 protected:
  virtual Status Compute(FunctionContext* ctx, const ArrayData& left,
                         const ArrayData& right, ArrayData* out) = 0;

  Status Call(FunctionContext* ctx, const Datum& left, const Datum& right,
              Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, right.kind());
    DCHECK_EQ(Datum::ARRAY, left.kind());

    const ArrayData& left_data = *left.array();
    const ArrayData& right_data = *right.array();
    DCHECK_EQ(left_data.length, right_data.length);
    ArrayData* result;

    result = out->array().get();
    return Compute(ctx, left_data, right_data, result);
  }

  std::shared_ptr<DataType> out_type() const override { return boolean(); }

  enum BitmapIndex { LEFT_VALID, LEFT_DATA, RIGHT_VALID, RIGHT_DATA };

  template <typename ComputeWord>
  Status ComputeKleene(ComputeWord&& compute_word, FunctionContext* ctx,
                       const ArrayData& left, const ArrayData& right, ArrayData* out) {
    DCHECK(left.null_count != 0 || right.null_count != 0);

    Bitmap bitmaps[4];
    bitmaps[LEFT_VALID] = {left.buffers[0], left.offset, left.length};
    bitmaps[LEFT_DATA] = {left.buffers[1], left.offset, left.length};

    bitmaps[RIGHT_VALID] = {right.buffers[0], right.offset, right.length};
    bitmaps[RIGHT_DATA] = {right.buffers[1], right.offset, right.length};

    ARROW_ASSIGN_OR_RAISE(out->buffers[0],
                          AllocateEmptyBitmap(out->length, ctx->memory_pool()));

    auto out_validity = out->GetMutableValues<uint64_t>(0);
    auto out_data = out->GetMutableValues<uint64_t>(1);

    int64_t i = 0;
    auto apply = [&](uint64_t left_valid, uint64_t left_data, uint64_t right_valid,
                     uint64_t right_data) {
      auto left_true = left_valid & left_data;
      auto left_false = left_valid & ~left_data;

      auto right_true = right_valid & right_data;
      auto right_false = right_valid & ~right_data;

      compute_word(left_true, left_false, right_true, right_false, &out_validity[i],
                   &out_data[i]);
      ++i;
    };

    if (right.null_count == 0 || left.null_count == 0) {
      if (left.null_count == 0) {
        // ensure only bitmaps[RIGHT_VALID].buffer might be null
        std::swap(bitmaps[LEFT_VALID], bitmaps[RIGHT_VALID]);
        std::swap(bitmaps[LEFT_DATA], bitmaps[RIGHT_DATA]);
      }
      // override bitmaps[RIGHT_VALID] to make it safe for Visit()
      bitmaps[RIGHT_VALID] = bitmaps[RIGHT_DATA];

      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[LEFT_VALID], words[LEFT_DATA], ~uint64_t(0), words[RIGHT_DATA]);
      });
    } else {
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[LEFT_VALID], words[LEFT_DATA], words[RIGHT_VALID], words[RIGHT_DATA]);
      });
    }
    return Status::OK();
  }

  ResolveNull resolve_null_;
};

class AndKernel : public BinaryBooleanKernel {
 public:
  using BinaryBooleanKernel::BinaryBooleanKernel;

 private:
  Status Compute(FunctionContext* ctx, const ArrayData& left, const ArrayData& right,
                 ArrayData* out) override {
    if (resolve_null_ == ResolveNull::PROPAGATE ||
        (left.GetNullCount() == 0 && right.GetNullCount() == 0)) {
      RETURN_NOT_OK(detail::AssignNullIntersection(ctx, left, right, out));
      if (right.length > 0) {
        BitmapAnd(left.buffers[1]->data(), left.offset, right.buffers[1]->data(),
                  right.offset, right.length, 0, out->buffers[1]->mutable_data());
      }
      return Status::OK();
    }

    auto compute_word = [](uint64_t left_true, uint64_t left_false, uint64_t right_true,
                           uint64_t right_false, uint64_t* out_valid,
                           uint64_t* out_data) {
      *out_data = left_true & right_true;
      *out_valid = left_false | right_false | (left_true & right_true);
    };

    return ComputeKleene(compute_word, ctx, left, right, out);
  }
};

Status And(FunctionContext* ctx, const Datum& left, const Datum& right, Datum* out) {
  AndKernel and_kernel(ResolveNull::PROPAGATE);
  detail::PrimitiveAllocatingBinaryKernel kernel(&and_kernel);
  return detail::InvokeBinaryArrayKernel(ctx, &kernel, left, right, out);
}

Status KleeneAnd(FunctionContext* ctx, const Datum& left, const Datum& right,
                 Datum* out) {
  AndKernel and_kernel(ResolveNull::KLEENE_LOGIC);
  detail::PrimitiveAllocatingBinaryKernel kernel(&and_kernel);
  return detail::InvokeBinaryArrayKernel(ctx, &kernel, left, right, out);
}

class OrKernel : public BinaryBooleanKernel {
 public:
  using BinaryBooleanKernel::BinaryBooleanKernel;

 private:
  Status Compute(FunctionContext* ctx, const ArrayData& left, const ArrayData& right,
                 ArrayData* out) override {
    if (resolve_null_ == ResolveNull::PROPAGATE ||
        (left.GetNullCount() == 0 && right.GetNullCount() == 0)) {
      RETURN_NOT_OK(detail::AssignNullIntersection(ctx, left, right, out));
      if (right.length > 0) {
        BitmapOr(left.buffers[1]->data(), left.offset, right.buffers[1]->data(),
                 right.offset, right.length, 0, out->buffers[1]->mutable_data());
      }
      return Status::OK();
    }

    static auto compute_word = [](uint64_t left_true, uint64_t left_false,
                                  uint64_t right_true, uint64_t right_false,
                                  uint64_t* out_valid, uint64_t* out_data) {
      *out_data = left_true | right_true;
      *out_valid = left_true | right_true | (left_false & right_false);
    };

    return ComputeKleene(compute_word, ctx, left, right, out);
  }
};

Status Or(FunctionContext* ctx, const Datum& left, const Datum& right, Datum* out) {
  OrKernel or_kernel(ResolveNull::PROPAGATE);
  detail::PrimitiveAllocatingBinaryKernel kernel(&or_kernel);
  return detail::InvokeBinaryArrayKernel(ctx, &kernel, left, right, out);
}

Status KleeneOr(FunctionContext* ctx, const Datum& left, const Datum& right, Datum* out) {
  OrKernel or_kernel(ResolveNull::KLEENE_LOGIC);
  detail::PrimitiveAllocatingBinaryKernel kernel(&or_kernel);
  return detail::InvokeBinaryArrayKernel(ctx, &kernel, left, right, out);
}

class XorKernel : public BinaryBooleanKernel {
 public:
  XorKernel() : BinaryBooleanKernel(ResolveNull::PROPAGATE) {}

 private:
  Status Compute(FunctionContext* ctx, const ArrayData& left, const ArrayData& right,
                 ArrayData* out) override {
    RETURN_NOT_OK(detail::AssignNullIntersection(ctx, left, right, out));
    if (right.length > 0) {
      BitmapXor(left.buffers[1]->data(), left.offset, right.buffers[1]->data(),
                right.offset, right.length, 0, out->buffers[1]->mutable_data());
    }
    return Status::OK();
  }
};

Status Xor(FunctionContext* ctx, const Datum& left, const Datum& right, Datum* out) {
  XorKernel xor_kernel;
  detail::PrimitiveAllocatingBinaryKernel kernel(&xor_kernel);
  return detail::InvokeBinaryArrayKernel(ctx, &kernel, left, right, out);
}

}  // namespace compute
}  // namespace arrow
