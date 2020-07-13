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

// Implementation of casting to integer or floating point types

#include "arrow/array/array_base.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/result.h"
#include "arrow/util/formatting.h"
#include "arrow/util/optional.h"
#include "arrow/util/utf8.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::StringFormatter;
using util::InitializeUTF8;
using util::ValidateUTF8;

namespace compute {
namespace internal {

// ----------------------------------------------------------------------
// Number / Boolean to String

template <typename I, typename O>
struct CastFunctor<O, I,
                   enable_if_t<is_string_like_type<O>::value &&
                               (is_number_type<I>::value || is_boolean_type<I>::value)>> {
  using value_type = typename TypeTraits<I>::CType;
  using BuilderType = typename TypeTraits<O>::BuilderType;
  using FormatterType = StringFormatter<I>;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const ArrayData& input = *batch[0].array();
    ArrayData* output = out->mutable_array();
    ctx->SetStatus(Convert(ctx, input, output));
  }

  static Status Convert(KernelContext* ctx, const ArrayData& input, ArrayData* output) {
    FormatterType formatter(input.type);
    BuilderType builder(input.type, ctx->memory_pool());
    RETURN_NOT_OK(VisitArrayDataInline<I>(
        input,
        [&](value_type v) {
          return formatter(v, [&](util::string_view v) { return builder.Append(v); });
        },
        [&]() { return builder.AppendNull(); }));

    std::shared_ptr<Array> output_array;
    RETURN_NOT_OK(builder.Finish(&output_array));
    *output = std::move(*output_array->data());
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Binary to String
//

#if defined(_MSC_VER)
// Silence warning: """'visitor': unreferenced local variable"""
#pragma warning(push)
#pragma warning(disable : 4101)
#endif

struct Utf8Validator {
  Status VisitNull() { return Status::OK(); }

  Status VisitValue(util::string_view str) {
    if (ARROW_PREDICT_FALSE(!ValidateUTF8(str))) {
      return Status::Invalid("Invalid UTF8 payload");
    }
    return Status::OK();
  }
};

template <typename I, typename O>
struct BinaryToStringSameWidthCastFunctor {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;
    if (!options.allow_invalid_utf8) {
      InitializeUTF8();
      const ArrayData& input = *batch[0].array();

      ArrayDataVisitor<I> visitor;
      Utf8Validator validator;
      Status st = visitor.Visit(input, &validator);
      if (!st.ok()) {
        ctx->SetStatus(st);
        return;
      }
    }
    // It's OK to call this because base binary types do not preallocate
    // anything
    ZeroCopyCastExec(ctx, batch, out);
  }
};

template <>
struct CastFunctor<StringType, BinaryType>
    : public BinaryToStringSameWidthCastFunctor<StringType, BinaryType> {};

template <>
struct CastFunctor<LargeStringType, LargeBinaryType>
    : public BinaryToStringSameWidthCastFunctor<LargeStringType, LargeBinaryType> {};

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

// String casts available
//
// * Numbers and boolean to String / LargeString
// * Binary / LargeBinary to String / LargeString with UTF8 validation

template <typename OutType>
void AddNumberToStringCasts(std::shared_ptr<DataType> out_ty, CastFunction* func) {
  DCHECK_OK(func->AddKernel(Type::BOOL, {boolean()}, out_ty,
                            CastFunctor<OutType, BooleanType>::Exec,
                            NullHandling::COMPUTED_NO_PREALLOCATE));

  for (const std::shared_ptr<DataType>& in_ty : NumericTypes()) {
    DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, out_ty,
                              GenerateNumeric<CastFunctor, OutType>(*in_ty),
                              NullHandling::COMPUTED_NO_PREALLOCATE));
  }
}

std::vector<std::shared_ptr<CastFunction>> GetBinaryLikeCasts() {
  auto cast_binary = std::make_shared<CastFunction>("cast_binary", Type::BINARY);
  AddCommonCasts(Type::BINARY, binary(), cast_binary.get());
  AddZeroCopyCast(Type::STRING, {utf8()}, binary(), cast_binary.get());

  auto cast_large_binary =
      std::make_shared<CastFunction>("cast_large_binary", Type::LARGE_BINARY);
  AddCommonCasts(Type::LARGE_BINARY, large_binary(), cast_large_binary.get());
  AddZeroCopyCast(Type::LARGE_STRING, {large_utf8()}, large_binary(),
                  cast_large_binary.get());

  auto cast_fsb =
      std::make_shared<CastFunction>("cast_fixed_size_binary", Type::FIXED_SIZE_BINARY);
  AddCommonCasts(Type::FIXED_SIZE_BINARY, OutputType(ResolveOutputFromOptions),
                 cast_fsb.get());

  auto cast_string = std::make_shared<CastFunction>("cast_string", Type::STRING);
  AddCommonCasts(Type::STRING, utf8(), cast_string.get());
  AddNumberToStringCasts<StringType>(utf8(), cast_string.get());
  DCHECK_OK(cast_string->AddKernel(Type::BINARY, {binary()}, utf8(),
                                   CastFunctor<StringType, BinaryType>::Exec,
                                   NullHandling::COMPUTED_NO_PREALLOCATE));

  auto cast_large_string =
      std::make_shared<CastFunction>("cast_large_string", Type::LARGE_STRING);
  AddCommonCasts(Type::LARGE_STRING, large_utf8(), cast_large_string.get());
  AddNumberToStringCasts<LargeStringType>(large_utf8(), cast_large_string.get());
  DCHECK_OK(
      cast_large_string->AddKernel(Type::LARGE_BINARY, {large_binary()}, large_utf8(),
                                   CastFunctor<LargeStringType, LargeBinaryType>::Exec,
                                   NullHandling::COMPUTED_NO_PREALLOCATE));

  return {cast_binary, cast_fsb, cast_large_binary, cast_string, cast_large_string};
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
