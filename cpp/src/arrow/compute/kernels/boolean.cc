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

  *out = detail::WrapDatumsLike(value, result);
  return Status::OK();
}

class BinaryBooleanKernel : public BinaryKernel {
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
};

enum class ResolveNull { KLEENE_LOGIC, PROPAGATE };

class AndKernel : public BinaryBooleanKernel {
 public:
  explicit AndKernel(ResolveNull resolve_null) : resolve_null_(resolve_null) {}

 private:
  Status Compute(FunctionContext* ctx, const ArrayData& left, const ArrayData& right,
                 ArrayData* out) override {
    if (resolve_null_ == ResolveNull::PROPAGATE) {
      RETURN_NOT_OK(detail::AssignNullIntersection(ctx, left, right, out));
      if (right.length > 0) {
        BitmapAnd(left.buffers[1]->data(), left.offset, right.buffers[1]->data(),
                  right.offset, right.length, 0, out->buffers[1]->mutable_data());
      }
    } else {
      Bitmap bitmaps[4];
      for (int i = 0, buffer_index = 0; buffer_index != 2; ++buffer_index) {
        for (const ArrayData* operand : {&left, &right}) {
          bitmaps[i++] =
              Bitmap(operand->buffers[buffer_index], operand->offset, operand->length);
        }
      }
      Bitmap out_validity(out->buffers[0], out->offset, out->length);
      Bitmap out_values(out->buffers[1], out->offset, out->length);
      int64_t i = 0;
      Bitmap::Visit(bitmaps, [&](std::bitset<4> bits) {
        bool left_valid = bits[0], right_valid = bits[1];
        bool left_value = !left_valid || bits[2];
        bool right_value = !right_valid || bits[3];
        if (left_valid && right_valid) {
          out_validity.SetBitTo(i, true);
        } else if (!left_valid && !right_valid) {
          out_validity.SetBitTo(i, false);
        } else {
          // one is null, so out will be null or false
          out_validity.SetBitTo(i, !(left_value && right_value));
        }
        out_values.SetBitTo(i, left_value && right_value);
      });
    }
    return Status::OK();
  }

  ResolveNull resolve_null_;
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
  Status Compute(FunctionContext* ctx, const ArrayData& left, const ArrayData& right,
                 ArrayData* out) override {
    RETURN_NOT_OK(detail::AssignNullIntersection(ctx, left, right, out));
    if (right.length > 0) {
      BitmapOr(left.buffers[1]->data(), left.offset, right.buffers[1]->data(),
               right.offset, right.length, 0, out->buffers[1]->mutable_data());
    }
    return Status::OK();
  }
};

Status Or(FunctionContext* ctx, const Datum& left, const Datum& right, Datum* out) {
  OrKernel or_kernel;
  detail::PrimitiveAllocatingBinaryKernel kernel(&or_kernel);
  return detail::InvokeBinaryArrayKernel(ctx, &kernel, left, right, out);
}

class XorKernel : public BinaryBooleanKernel {
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
