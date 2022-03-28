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

#include <limits>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/compute/kernels/temporal_internal.h"
#include "arrow/result.h"
#include "arrow/util/formatting.h"
#include "arrow/util/int_util.h"
#include "arrow/util/optional.h"
#include "arrow/util/utf8.h"
#include "arrow/visit_data_inline.h"

namespace arrow {

using internal::StringFormatter;
using util::InitializeUTF8;
using util::ValidateUTF8;

namespace compute {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// Number / Boolean to String

template <typename O, typename I>
struct NumericToStringCastFunctor {
  using value_type = typename TypeTraits<I>::CType;
  using BuilderType = typename TypeTraits<O>::BuilderType;
  using FormatterType = StringFormatter<I>;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    DCHECK(out->is_array());
    const ArrayData& input = *batch[0].array();
    ArrayData* output = out->mutable_array();
    return Convert(ctx, input, output);
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
// Temporal to String

template <typename O, typename I>
struct TemporalToStringCastFunctor {
  using value_type = typename TypeTraits<I>::CType;
  using BuilderType = typename TypeTraits<O>::BuilderType;
  using FormatterType = StringFormatter<I>;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    DCHECK(out->is_array());
    const ArrayData& input = *batch[0].array();
    ArrayData* output = out->mutable_array();
    return Convert(ctx, input, output);
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

template <typename O>
struct TemporalToStringCastFunctor<O, TimestampType> {
  using value_type = typename TypeTraits<TimestampType>::CType;
  using BuilderType = typename TypeTraits<O>::BuilderType;
  using FormatterType = StringFormatter<TimestampType>;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    DCHECK(out->is_array());
    const ArrayData& input = *batch[0].array();
    ArrayData* output = out->mutable_array();
    return Convert(ctx, input, output);
  }

  static Status Convert(KernelContext* ctx, const ArrayData& input, ArrayData* output) {
    const auto& timezone = GetInputTimezone(*input.type);
    const auto& ty = checked_cast<const TimestampType&>(*input.type);
    BuilderType builder(input.type, ctx->memory_pool());

    // Preallocate
    int64_t string_length = 19;  // YYYY-MM-DD HH:MM:SS
    if (ty.unit() == TimeUnit::MILLI) {
      string_length += 4;  // .SSS
    } else if (ty.unit() == TimeUnit::MICRO) {
      string_length += 7;  // .SSSSSS
    } else if (ty.unit() == TimeUnit::NANO) {
      string_length += 10;  // .SSSSSSSSS
    }
    if (!timezone.empty()) string_length += 5;  // +0000
    RETURN_NOT_OK(builder.Reserve(input.length));
    RETURN_NOT_OK(
        builder.ReserveData((input.length - input.GetNullCount()) * string_length));

    if (timezone.empty()) {
      FormatterType formatter(input.type);
      RETURN_NOT_OK(VisitArrayDataInline<TimestampType>(
          input,
          [&](value_type v) {
            return formatter(v, [&](util::string_view v) { return builder.Append(v); });
          },
          [&]() {
            builder.UnsafeAppendNull();
            return Status::OK();
          }));
    } else {
      switch (ty.unit()) {
        case TimeUnit::SECOND:
          RETURN_NOT_OK(ConvertZoned<std::chrono::seconds>(input, timezone, &builder));
          break;
        case TimeUnit::MILLI:
          RETURN_NOT_OK(
              ConvertZoned<std::chrono::milliseconds>(input, timezone, &builder));
          break;
        case TimeUnit::MICRO:
          RETURN_NOT_OK(
              ConvertZoned<std::chrono::microseconds>(input, timezone, &builder));
          break;
        case TimeUnit::NANO:
          RETURN_NOT_OK(
              ConvertZoned<std::chrono::nanoseconds>(input, timezone, &builder));
          break;
        default:
          DCHECK(false);
          return Status::NotImplemented("Unimplemented time unit");
      }
    }
    std::shared_ptr<Array> output_array;
    RETURN_NOT_OK(builder.Finish(&output_array));
    *output = std::move(*output_array->data());
    return Status::OK();
  }

  template <typename Duration>
  static Status ConvertZoned(const ArrayData& input, const std::string& timezone,
                             BuilderType* builder) {
    static const std::string kFormatString = "%Y-%m-%d %H:%M:%S%z";
    static const std::string kUtcFormatString = "%Y-%m-%d %H:%M:%SZ";
    DCHECK(!timezone.empty());
    ARROW_ASSIGN_OR_RAISE(const time_zone* tz, LocateZone(timezone));
    ARROW_ASSIGN_OR_RAISE(std::locale locale, GetLocale("C"));
    TimestampFormatter<Duration> formatter{
        timezone == "UTC" ? kUtcFormatString : kFormatString, tz, locale};
    return VisitArrayDataInline<TimestampType>(
        input,
        [&](value_type v) {
          ARROW_ASSIGN_OR_RAISE(auto formatted, formatter(v));
          return builder->Append(std::move(formatted));
        },
        [&]() {
          builder->UnsafeAppendNull();
          return Status::OK();
        });
  }
};

// ----------------------------------------------------------------------
// Binary-like to binary-like
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
Status CastBinaryToBinaryOffsets(KernelContext* ctx, const ArrayData& input,
                                 ArrayData* output) {
  static_assert(std::is_same<I, O>::value, "Cast same-width offsets (no-op)");
  return Status::OK();
}

// Upcast offsets
template <>
Status CastBinaryToBinaryOffsets<int32_t, int64_t>(KernelContext* ctx,
                                                   const ArrayData& input,
                                                   ArrayData* output) {
  using input_offset_type = int32_t;
  using output_offset_type = int64_t;
  ARROW_ASSIGN_OR_RAISE(
      output->buffers[1],
      ctx->Allocate((output->length + output->offset + 1) * sizeof(output_offset_type)));
  memset(output->buffers[1]->mutable_data(), 0,
         output->offset * sizeof(output_offset_type));
  ::arrow::internal::CastInts(input.GetValues<input_offset_type>(1),
                              output->GetMutableValues<output_offset_type>(1),
                              output->length + 1);
  return Status::OK();
}

// Downcast offsets
template <>
Status CastBinaryToBinaryOffsets<int64_t, int32_t>(KernelContext* ctx,
                                                   const ArrayData& input,
                                                   ArrayData* output) {
  using input_offset_type = int64_t;
  using output_offset_type = int32_t;

  constexpr input_offset_type kMaxOffset = std::numeric_limits<output_offset_type>::max();

  auto input_offsets = input.GetValues<input_offset_type>(1);

  // Binary offsets are ascending, so it's enough to check the last one for overflow.
  if (input_offsets[input.length] > kMaxOffset) {
    return Status::Invalid("Failed casting from ", input.type->ToString(), " to ",
                           output->type->ToString(), ": input array too large");
  } else {
    ARROW_ASSIGN_OR_RAISE(output->buffers[1],
                          ctx->Allocate((output->length + output->offset + 1) *
                                        sizeof(output_offset_type)));
    memset(output->buffers[1]->mutable_data(), 0,
           output->offset * sizeof(output_offset_type));
    ::arrow::internal::CastInts(input.GetValues<input_offset_type>(1),
                                output->GetMutableValues<output_offset_type>(1),
                                output->length + 1);
    return Status::OK();
  }
}

template <typename O, typename I>
enable_if_base_binary<I, Status> BinaryToBinaryCastExec(KernelContext* ctx,
                                                        const ExecBatch& batch,
                                                        Datum* out) {
  DCHECK(out->is_array());
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;
  const ArrayData& input = *batch[0].array();

  if (!I::is_utf8 && O::is_utf8 && !options.allow_invalid_utf8) {
    InitializeUTF8();

    ArrayDataVisitor<I> visitor;
    Utf8Validator validator;
    RETURN_NOT_OK(visitor.Visit(input, &validator));
  }

  // Start with a zero-copy cast, but change indices to expected size
  RETURN_NOT_OK(ZeroCopyCastExec(ctx, batch, out));
  return CastBinaryToBinaryOffsets<typename I::offset_type, typename O::offset_type>(
      ctx, input, out->mutable_array());
}

template <typename O, typename I>
enable_if_t<std::is_same<I, FixedSizeBinaryType>::value &&
                !std::is_same<O, FixedSizeBinaryType>::value,
            Status>
BinaryToBinaryCastExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  DCHECK(out->is_array());
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;
  const ArrayData& input = *batch[0].array();
  ArrayData* output = out->mutable_array();

  if (O::is_utf8 && !options.allow_invalid_utf8) {
    InitializeUTF8();

    ArrayDataVisitor<I> visitor;
    Utf8Validator validator;
    RETURN_NOT_OK(visitor.Visit(input, &validator));
  }

  // Check for overflow
  using output_offset_type = typename O::offset_type;
  constexpr output_offset_type kMaxOffset =
      std::numeric_limits<output_offset_type>::max();
  const int32_t width =
      checked_cast<const FixedSizeBinaryType&>(*input.type).byte_width();
  const int64_t max_offset = width * input.length;
  if (max_offset > kMaxOffset) {
    return Status::Invalid("Failed casting from ", input.type->ToString(), " to ",
                           output->type->ToString(), ": input array too large");
  }

  // Copy buffers over, then generate indices
  output->length = input.length;
  output->SetNullCount(input.null_count);
  if (input.offset == output->offset) {
    output->buffers[0] = input.buffers[0];
  } else {
    ARROW_ASSIGN_OR_RAISE(
        output->buffers[0],
        arrow::internal::CopyBitmap(ctx->memory_pool(), input.GetValues<uint8_t>(0, 0),
                                    input.offset, input.length));
  }
  output->buffers[2] = input.buffers[1];
  output_offset_type* offsets = output->GetMutableValues<output_offset_type>(1);

  offsets[0] = static_cast<output_offset_type>(input.offset * width);
  for (int64_t i = 0; i < input.length; i++) {
    offsets[i + 1] = offsets[i] + width;
  }
  return Status::OK();
}

template <typename O, typename I>
enable_if_t<std::is_same<I, FixedSizeBinaryType>::value &&
                std::is_same<O, FixedSizeBinaryType>::value,
            Status>
BinaryToBinaryCastExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  DCHECK(out->is_array());
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;
  const ArrayData& input = *batch[0].array();
  const int32_t in_width =
      checked_cast<const FixedSizeBinaryType&>(*input.type).byte_width();
  const int32_t out_width =
      checked_cast<const FixedSizeBinaryType&>(*options.to_type).byte_width();

  if (in_width != out_width) {
    return Status::Invalid("Failed casting from ", input.type->ToString(), " to ",
                           options.to_type->ToString(), ": widths must match");
  }

  return ZeroCopyCastExec(ctx, batch, out);
}

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

// ----------------------------------------------------------------------
// Cast functions registration

template <typename OutType>
void AddNumberToStringCasts(CastFunction* func) {
  auto out_ty = TypeTraits<OutType>::type_singleton();

  DCHECK_OK(func->AddKernel(Type::BOOL, {boolean()}, out_ty,
                            TrivialScalarUnaryAsArraysExec(
                                NumericToStringCastFunctor<OutType, BooleanType>::Exec),
                            NullHandling::COMPUTED_NO_PREALLOCATE));

  for (const std::shared_ptr<DataType>& in_ty : NumericTypes()) {
    DCHECK_OK(
        func->AddKernel(in_ty->id(), {in_ty}, out_ty,
                        TrivialScalarUnaryAsArraysExec(
                            GenerateNumeric<NumericToStringCastFunctor, OutType>(*in_ty)),
                        NullHandling::COMPUTED_NO_PREALLOCATE));
  }
}

template <typename OutType>
void AddTemporalToStringCasts(CastFunction* func) {
  auto out_ty = TypeTraits<OutType>::type_singleton();
  for (const std::shared_ptr<DataType>& in_ty : TemporalTypes()) {
    DCHECK_OK(func->AddKernel(
        in_ty->id(), {InputType(in_ty->id())}, out_ty,
        TrivialScalarUnaryAsArraysExec(
            GenerateTemporal<TemporalToStringCastFunctor, OutType>(*in_ty)),
        NullHandling::COMPUTED_NO_PREALLOCATE));
  }
}

template <typename OutType, typename InType>
void AddBinaryToBinaryCast(CastFunction* func) {
  auto out_ty = TypeTraits<OutType>::type_singleton();

  DCHECK_OK(func->AddKernel(
      InType::type_id, {InputType(InType::type_id)}, out_ty,
      TrivialScalarUnaryAsArraysExec(BinaryToBinaryCastExec<OutType, InType>),
      NullHandling::COMPUTED_NO_PREALLOCATE));
}

template <typename OutType>
void AddBinaryToBinaryCast(CastFunction* func) {
  AddBinaryToBinaryCast<OutType, StringType>(func);
  AddBinaryToBinaryCast<OutType, BinaryType>(func);
  AddBinaryToBinaryCast<OutType, LargeStringType>(func);
  AddBinaryToBinaryCast<OutType, LargeBinaryType>(func);
  AddBinaryToBinaryCast<OutType, FixedSizeBinaryType>(func);
}

}  // namespace

std::vector<std::shared_ptr<CastFunction>> GetBinaryLikeCasts() {
  auto cast_binary = std::make_shared<CastFunction>("cast_binary", Type::BINARY);
  AddCommonCasts(Type::BINARY, binary(), cast_binary.get());
  AddBinaryToBinaryCast<BinaryType>(cast_binary.get());

  auto cast_large_binary =
      std::make_shared<CastFunction>("cast_large_binary", Type::LARGE_BINARY);
  AddCommonCasts(Type::LARGE_BINARY, large_binary(), cast_large_binary.get());
  AddBinaryToBinaryCast<LargeBinaryType>(cast_large_binary.get());

  auto cast_string = std::make_shared<CastFunction>("cast_string", Type::STRING);
  AddCommonCasts(Type::STRING, utf8(), cast_string.get());
  AddNumberToStringCasts<StringType>(cast_string.get());
  AddTemporalToStringCasts<StringType>(cast_string.get());
  AddBinaryToBinaryCast<StringType>(cast_string.get());

  auto cast_large_string =
      std::make_shared<CastFunction>("cast_large_string", Type::LARGE_STRING);
  AddCommonCasts(Type::LARGE_STRING, large_utf8(), cast_large_string.get());
  AddNumberToStringCasts<LargeStringType>(cast_large_string.get());
  AddTemporalToStringCasts<LargeStringType>(cast_large_string.get());
  AddBinaryToBinaryCast<LargeStringType>(cast_large_string.get());

  auto cast_fsb =
      std::make_shared<CastFunction>("cast_fixed_size_binary", Type::FIXED_SIZE_BINARY);
  AddCommonCasts(Type::FIXED_SIZE_BINARY, OutputType(ResolveOutputFromOptions),
                 cast_fsb.get());
  DCHECK_OK(cast_fsb->AddKernel(
      Type::FIXED_SIZE_BINARY, {InputType(Type::FIXED_SIZE_BINARY)},
      OutputType(FirstType),
      TrivialScalarUnaryAsArraysExec(
          BinaryToBinaryCastExec<FixedSizeBinaryType, FixedSizeBinaryType>),
      NullHandling::COMPUTED_NO_PREALLOCATE));

  return {cast_binary, cast_large_binary, cast_string, cast_large_string, cast_fsb};
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
