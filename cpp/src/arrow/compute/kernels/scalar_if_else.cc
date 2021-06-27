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
  // sometimes Bitmaps will be ignored, in which case we replace access to them with
  // duplicated (probably elided) access to cond_data
  const Bitmap& _ = cond_data;

  // lambda function that will be used inside the visitor
  uint64_t* out_validity = nullptr;
  int64_t i = 0;
  auto apply = [&](uint64_t c_valid, uint64_t c_data, uint64_t l_valid,
                   uint64_t r_valid) {
    out_validity[i] = c_valid & ((c_data & l_valid) | (~c_data & r_valid));
    i++;
  };

  // cond.valid & (cond.data & left.valid | ~cond.data & right.valid)
  // In the following cases, we dont need to allocate out_valid bitmap

  // if cond & left & right all ones, then output is all valid --> out_valid = nullptr
  if (cond_const == kAllValid && left_const == kAllValid && right_const == kAllValid) {
    return Status::OK();
  }

  if (left_const == kAllValid && right_const == kAllValid) {
    // if both left and right are valid, no need to calculate out_valid bitmap. Pass
    // cond validity buffer
    // if there's an offset, copy bitmap (cannot slice a bitmap)
    if (cond.offset) {
      ARROW_ASSIGN_OR_RAISE(
          output->buffers[0],
          arrow::internal::CopyBitmap(ctx->memory_pool(), cond.buffers[0]->data(),
                                      cond.offset, cond.length));
    } else {  // just copy assign cond validity buffer
      output->buffers[0] = cond.buffers[0];
    }
    return Status::OK();
  }

  // following cases requires a separate out_valid buffer
  ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(cond.length));
  out_validity = output->GetMutableValues<uint64_t>(0);

  enum { C_VALID, C_DATA, L_VALID, R_VALID };

  switch (flag) {
    case COND_CONST | LEFT_CONST | RIGHT_CONST: {
      Bitmap bitmaps[] = {_, cond_data, _, _};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(*cond_const, words[C_DATA], *left_const, *right_const);
      });
      break;
    }
    case LEFT_CONST | RIGHT_CONST: {
      Bitmap bitmaps[] = {cond_valid, cond_data, _, _};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[C_VALID], words[C_DATA], *left_const, *right_const);
      });
      break;
    }
    case COND_CONST | RIGHT_CONST: {
      // bitmaps[C_VALID], bitmaps[R_VALID] might be null; override to make it safe for
      // Visit()
      Bitmap bitmaps[] = {_, cond_data, left_valid, _};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(*cond_const, words[C_DATA], words[L_VALID], *right_const);
      });
      break;
    }
    case RIGHT_CONST: {
      // bitmaps[R_VALID] might be null; override to make it safe for Visit()
      Bitmap bitmaps[] = {cond_valid, cond_data, left_valid, _};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[C_VALID], words[C_DATA], words[L_VALID], *right_const);
      });
      break;
    }
    case COND_CONST | LEFT_CONST: {
      // bitmaps[C_VALID], bitmaps[L_VALID] might be null; override to make it safe for
      // Visit()
      Bitmap bitmaps[] = {_, cond_data, _, right_valid};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(*cond_const, words[C_DATA], *left_const, words[R_VALID]);
      });
      break;
    }
    case LEFT_CONST: {
      // bitmaps[L_VALID] might be null; override to make it safe for Visit()
      Bitmap bitmaps[] = {cond_valid, cond_data, _, right_valid};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[C_VALID], words[C_DATA], *left_const, words[R_VALID]);
      });
      break;
    }
    case COND_CONST: {
      // bitmaps[C_VALID] might be null; override to make it safe for Visit()
      Bitmap bitmaps[] = {_, cond_data, left_valid, right_valid};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(*cond_const, words[C_DATA], words[L_VALID], words[R_VALID]);
      });
      break;
    }
    case 0: {
      Bitmap bitmaps[] = {cond_valid, cond_data, left_valid, right_valid};
      Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 4> words) {
        apply(words[C_VALID], words[C_DATA], words[L_VALID], words[R_VALID]);
      });
      break;
    }
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
    bool left_data = internal::UnboxScalar<BooleanType>::Unbox(left);
    bool right_data = internal::UnboxScalar<BooleanType>::Unbox(right);

    // out_buf = left & cond | right & ~cond
    std::shared_ptr<Buffer> out_buf = nullptr;
    if (left_data) {
      if (right_data) {
        // out_buf = ones
        ARROW_ASSIGN_OR_RAISE(out_buf, ctx->AllocateBitmap(cond.length));
        // filling with UINT8_MAX upto the buffer's size (in bytes)
        std::memset(out_buf->mutable_data(), UINT8_MAX, out_buf->size());
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
        return Status::OK();
      }
      // either left or right is an array. Output is always an array
      if (!cond.is_valid) {
        // cond is null; just create a null array
        ARROW_ASSIGN_OR_RAISE(
            *out, MakeArrayOfNull(batch[1].type(), batch.length, ctx->memory_pool()))
        return Status::OK();
      }

      const auto& valid_data = cond.value ? batch[1] : batch[2];
      if (valid_data.is_array()) {
        *out = valid_data;
      } else {
        // valid data is a scalar that needs to be broadcasted
        ARROW_ASSIGN_OR_RAISE(
            *out,
            MakeArrayFromScalar(*valid_data.scalar(), batch.length, ctx->memory_pool()));
      }
      return Status::OK();
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

void AddPrimitiveIfElseKernels(const std::shared_ptr<IfElseFunction>& scalar_function,
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

  auto func = std::make_shared<IfElseFunction>("if_else", Arity::Ternary(), &if_else_doc);

  AddPrimitiveIfElseKernels(func, NumericTypes());
  AddPrimitiveIfElseKernels(func, TemporalTypes());
  AddPrimitiveIfElseKernels(func, {boolean(), null()});
  // todo add binary kernels

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
