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
#include <optional>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/compute/kernels/temporal_internal.h"
#include "arrow/result.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/formatting.h"
#include "arrow/util/int_util.h"
#include "arrow/util/span.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/utf8_internal.h"
#include "arrow/visit_data_inline.h"

namespace arrow {

using internal::StringFormatter;
using util::InitializeUTF8;
using util::ValidateUTF8Inline;

namespace compute::internal {

namespace {

// ----------------------------------------------------------------------
// Number / Boolean to String

template <typename O, typename I>
struct NumericToStringCastFunctor {
  using value_type = typename TypeTraits<I>::CType;
  using BuilderType = typename TypeTraits<O>::BuilderType;
  using FormatterType = StringFormatter<I>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& input = batch[0].array;
    FormatterType formatter(input.type);
    BuilderType builder(TypeTraits<O>::type_singleton(), ctx->memory_pool());
    RETURN_NOT_OK(VisitArraySpanInline<I>(
        input,
        [&](value_type v) {
          return formatter(v, [&](std::string_view v) { return builder.Append(v); });
        },
        [&]() { return builder.AppendNull(); }));

    std::shared_ptr<Array> output_array;
    RETURN_NOT_OK(builder.Finish(&output_array));
    out->value = std::move(output_array->data());
    return Status::OK();
  }
};

template <typename O, typename I>
struct DecimalToStringCastFunctor {
  using value_type = typename TypeTraits<I>::CType;
  using BuilderType = typename TypeTraits<O>::BuilderType;
  using FormatterType = StringFormatter<I>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& input = batch[0].array;
    FormatterType formatter(input.type);
    BuilderType builder(TypeTraits<O>::type_singleton(), ctx->memory_pool());
    RETURN_NOT_OK(VisitArraySpanInline<I>(
        input,
        [&](std::string_view bytes) {
          value_type value(reinterpret_cast<const uint8_t*>(bytes.data()));
          return formatter(value, [&](std::string_view v) { return builder.Append(v); });
        },
        [&]() { return builder.AppendNull(); }));

    std::shared_ptr<Array> output_array;
    RETURN_NOT_OK(builder.Finish(&output_array));
    out->value = std::move(output_array->data());
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

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& input = batch[0].array;
    FormatterType formatter(input.type);
    BuilderType builder(TypeTraits<O>::type_singleton(), ctx->memory_pool());
    RETURN_NOT_OK(VisitArraySpanInline<I>(
        input,
        [&](value_type v) {
          return formatter(v, [&](std::string_view v) { return builder.Append(v); });
        },
        [&]() { return builder.AppendNull(); }));

    std::shared_ptr<Array> output_array;
    RETURN_NOT_OK(builder.Finish(&output_array));
    out->value = std::move(output_array->data());
    return Status::OK();
  }
};

template <typename O>
struct TemporalToStringCastFunctor<O, TimestampType> {
  using value_type = typename TypeTraits<TimestampType>::CType;
  using BuilderType = typename TypeTraits<O>::BuilderType;
  using FormatterType = StringFormatter<TimestampType>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& input = batch[0].array;
    const auto& timezone = GetInputTimezone(*input.type);
    const auto& ty = checked_cast<const TimestampType&>(*input.type);
    BuilderType builder(TypeTraits<O>::type_singleton(), ctx->memory_pool());

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
      RETURN_NOT_OK(VisitArraySpanInline<TimestampType>(
          input,
          [&](value_type v) {
            return formatter(v, [&](std::string_view v) { return builder.Append(v); });
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
    out->value = std::move(output_array->data());
    return Status::OK();
  }

  template <typename Duration>
  static Status ConvertZoned(const ArraySpan& input, const std::string& timezone,
                             BuilderType* builder) {
    static const std::string kFormatString = "%Y-%m-%d %H:%M:%S%z";
    static const std::string kUtcFormatString = "%Y-%m-%d %H:%M:%SZ";
    DCHECK(!timezone.empty());
    ARROW_ASSIGN_OR_RAISE(const time_zone* tz, LocateZone(timezone));
    ARROW_ASSIGN_OR_RAISE(std::locale locale, GetLocale("C"));
    TimestampFormatter<Duration> formatter{
        timezone == "UTC" ? kUtcFormatString : kFormatString, tz, locale};
    return VisitArraySpanInline<TimestampType>(
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

  Status VisitValue(std::string_view str) {
    if (ARROW_PREDICT_FALSE(!ValidateUTF8Inline(str))) {
      return Status::Invalid("Invalid UTF8 payload");
    }
    return Status::OK();
  }
};

template <typename I, typename O>
Status CastBinaryToBinaryOffsets(KernelContext* ctx, const ArraySpan& input,
                                 ArrayData* output) {
  static_assert(std::is_same<I, O>::value, "Cast same-width offsets (no-op)");
  return Status::OK();
}

// Upcast offsets
template <>
Status CastBinaryToBinaryOffsets<int32_t, int64_t>(KernelContext* ctx,
                                                   const ArraySpan& input,
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
                                                   const ArraySpan& input,
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
    ::arrow::internal::CastInts(input_offsets,
                                output->GetMutableValues<output_offset_type>(1),
                                output->length + 1);
    return Status::OK();
  }
}

template <typename O, typename I>
Status BinaryToBinaryCastExec(KernelContext* ctx, const ExecSpan& batch,
                              ExecResult* out) {
  const ArraySpan& input = batch[0].array;

  // This presupposes that one was created in the invocation layer
  ArrayData* output = out->array_data().get();
  output->SetNullCount(input.null_count);

  const auto& options = CastState::Get(ctx);
  bool check_utf8 = !I::is_utf8 && O::is_utf8 && !options.allow_invalid_utf8;
  if (check_utf8) {
    InitializeUTF8();
  }

  [[maybe_unused]] auto SimpleUtf8Validation = [&] {
    if (check_utf8) {
      Utf8Validator validator;
      return ArraySpanVisitor<I>::Visit(input, &validator);
    }
    return Status::OK();
  };

  constexpr bool kInputOffsets =
      std::is_base_of_v<BinaryType, I> || std::is_base_of_v<LargeBinaryType, I>;

  constexpr bool kInputViews = std::is_base_of_v<BinaryViewType, I>;

  constexpr bool kInputFixed = std::is_same_v<FixedSizeBinaryType, I>;

  constexpr bool kOutputOffsets =
      std::is_base_of_v<BinaryType, O> || std::is_base_of_v<LargeBinaryType, O>;

  constexpr bool kOutputViews = std::is_base_of_v<BinaryViewType, O>;

  constexpr bool kOutputFixed = std::is_same_v<FixedSizeBinaryType, O>;

  if constexpr (kInputOffsets && kOutputOffsets) {
    // Start with a zero-copy cast, but change indices to expected size
    RETURN_NOT_OK(SimpleUtf8Validation());
    RETURN_NOT_OK(ZeroCopyCastExec(ctx, batch, out));
    // FIXME(bkietz) this discards preallocated storage. It seems preferable to me to
    // allocate a new null bitmap if necessary than to always allocate new offsets.
    return CastBinaryToBinaryOffsets<typename I::offset_type, typename O::offset_type>(
        ctx, input, out->array_data().get());
  }

  if constexpr (kInputViews && kOutputViews) {
    return SimpleUtf8Validation() & ZeroCopyCastExec(ctx, batch, out);
  }

  if constexpr (kInputViews && kOutputOffsets) {
    if (input.MayHaveNulls()) {
      ARROW_ASSIGN_OR_RAISE(
          output->buffers[0],
          arrow::internal::CopyBitmap(ctx->memory_pool(), input.buffers[0].data,
                                      input.offset, input.length));
    } else {
      output->buffers[0] = nullptr;
    }

    using offset_type = typename O::offset_type;

    auto* offset = output->buffers[1]->mutable_data_as<offset_type>();
    offset[0] = 0;

    util::span char_buffers{
        reinterpret_cast<const std::shared_ptr<Buffer>*>(input.buffers[2].data),
        static_cast<size_t>(input.buffers[2].size / sizeof(std::shared_ptr<Buffer>))};
    int64_t char_count = 0;
    for (const auto& buf : char_buffers) {
      char_count += buf->size();
    }
    BufferBuilder char_builder{ctx->memory_pool()};
    RETURN_NOT_OK(char_builder.Reserve(char_count));

    RETURN_NOT_OK(VisitArraySpanInline<StringViewType>(
        input,
        [&](std::string_view v) {
          if constexpr (std::is_same_v<offset_type, int32_t>) {
            if (ARROW_PREDICT_FALSE(
                    char_builder.length() + v.size() >
                    static_cast<size_t>(std::numeric_limits<int32_t>::max()))) {
              return Status::Invalid("Failed casting from ", input.type->ToString(),
                                     " to ", out->type()->ToString(),
                                     ": input array viewed too many characters");
            }
          }
          offset[1] = static_cast<offset_type>(offset[0] + v.size());
          ++offset;
          return char_builder.Append(v);
        },
        [&] {
          offset[1] = offset[0];
          ++offset;
          return Status::OK();
        }));

    RETURN_NOT_OK(SimpleUtf8Validation());
    return char_builder.Finish(&output->buffers[2]);
  }

  if constexpr ((kInputOffsets || kInputFixed) && kOutputViews) {
    // TODO(bkietz) when outputting views, we *could* output into slices,
    // provided we have a threadsafe place to stash accumulated buffers
    // of character data.
    if (input.MayHaveNulls()) {
      ARROW_ASSIGN_OR_RAISE(
          output->buffers[0],
          arrow::internal::CopyBitmap(ctx->memory_pool(), input.buffers[0].data,
                                      input.offset, input.length));
    } else {
      output->buffers[0] = nullptr;
    }

    // Borrow the input's character buffer
    output->buffers.resize(3);
    output->buffers[2] = input.GetBuffer(kInputFixed ? 1 : 2);
    auto* buffer_data = output->buffers[2]->data_as<char>();

    auto* headers = output->buffers[1]->mutable_data_as<StringHeader>();
    if (check_utf8) {
      Utf8Validator validator;
      return VisitArraySpanInline<I>(
          input,
          [&](std::string_view v) {
            new (headers++)
                StringHeader{v.data(), static_cast<uint32_t>(v.size()), 0, buffer_data};
            return validator.VisitValue(v);
          },
          [&] {
            *headers++ = StringHeader{};
            return Status::OK();
          });
    } else {
      VisitArraySpanInline<I>(
          input,
          [&](std::string_view v) {
            new (headers++)
                StringHeader{v.data(), static_cast<uint32_t>(v.size()), 0, buffer_data};
          },
          [&] { *headers++ = StringHeader{}; });
      return Status::OK();
    }
  }

  if constexpr (kInputFixed && kOutputOffsets) {
    RETURN_NOT_OK(SimpleUtf8Validation());

    using output_offset_type = typename O::offset_type;

    int32_t width = input.type->byte_width();

    if constexpr (std::is_same_v<output_offset_type, int32_t>) {
      // Check for overflow
      if (width * input.length > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("Failed casting from ", input.type->ToString(), " to ",
                               out->type()->ToString(), ": input array too large");
      }
    }

    // Copy buffers over, then generate indices
    output->length = input.length;
    output->SetNullCount(input.null_count);
    if (input.offset == output->offset) {
      output->buffers[0] = input.GetBuffer(0);
    } else {
      ARROW_ASSIGN_OR_RAISE(
          output->buffers[0],
          arrow::internal::CopyBitmap(ctx->memory_pool(), input.buffers[0].data,
                                      input.offset, input.length));
    }

    // This buffer is preallocated
    auto* offsets = output->buffers[1]->mutable_data_as<output_offset_type>();
    offsets[0] = static_cast<output_offset_type>(input.offset * width);
    for (int64_t i = 0; i < input.length; i++) {
      offsets[i + 1] = offsets[i] + width;
    }

    // Data buffer (index 1) for FWBinary becomes data buffer for VarBinary
    // (index 2). After ARROW-16757, we need to copy this memory instead of
    // zero-copy it because a Scalar value promoted to an ArraySpan may be
    // referencing a temporary buffer whose scope does not extend beyond the
    // kernel execution. In that scenario, the validity bitmap above can be
    // zero-copied because it points to static memory (either a byte with a 1 or
    // a 0 depending on whether the value is null or not).
    if (std::shared_ptr<Buffer> input_data = input.GetBuffer(1)) {
      ARROW_ASSIGN_OR_RAISE(
          output->buffers[2],
          input_data->CopySlice(0, input_data->size(), ctx->memory_pool()));
    } else {
      // TODO(wesm): it should already be nullptr, so we may be able to remove
      // this
      output->buffers[2] = nullptr;
    }

    return Status::OK();
  }

  if constexpr (kInputFixed && kOutputFixed) {
    if (input.type->byte_width() != output->type->byte_width()) {
      return Status::Invalid("Failed casting from ", input.type->ToString(), " to ",
                             output->type->ToString(), ": widths must match");
    }
    return ZeroCopyCastExec(ctx, batch, out);
  }

  Unreachable();
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
                            NumericToStringCastFunctor<OutType, BooleanType>::Exec,
                            NullHandling::COMPUTED_NO_PREALLOCATE));

  for (const std::shared_ptr<DataType>& in_ty : NumericTypes()) {
    DCHECK_OK(
        func->AddKernel(in_ty->id(), {in_ty}, out_ty,
                        GenerateNumeric<NumericToStringCastFunctor, OutType>(*in_ty),
                        NullHandling::COMPUTED_NO_PREALLOCATE));
  }
}

template <typename OutType>
void AddDecimalToStringCasts(CastFunction* func) {
  auto out_ty = TypeTraits<OutType>::type_singleton();
  for (const auto& in_tid : std::vector<Type::type>{Type::DECIMAL128, Type::DECIMAL256}) {
    DCHECK_OK(
        func->AddKernel(in_tid, {in_tid}, out_ty,
                        GenerateDecimal<DecimalToStringCastFunctor, OutType>(in_tid),
                        NullHandling::COMPUTED_NO_PREALLOCATE));
  }
}

template <typename OutType>
void AddTemporalToStringCasts(CastFunction* func) {
  auto out_ty = TypeTraits<OutType>::type_singleton();
  for (const auto& types : {TemporalTypes(), DurationTypes()}) {
    for (const std::shared_ptr<DataType>& in_ty : types) {
      DCHECK_OK(
          func->AddKernel(in_ty->id(), {InputType(in_ty->id())}, out_ty,
                          GenerateTemporal<TemporalToStringCastFunctor, OutType>(*in_ty),
                          NullHandling::COMPUTED_NO_PREALLOCATE));
    }
  }
}

template <typename OutType, typename InType>
void AddBinaryToBinaryCast(CastFunction* func) {
  auto out_ty = TypeTraits<OutType>::type_singleton();

  DCHECK_OK(func->AddKernel(InType::type_id, {InputType(InType::type_id)}, out_ty,
                            BinaryToBinaryCastExec<OutType, InType>,
                            NullHandling::COMPUTED_NO_PREALLOCATE));
}

template <typename OutType>
void AddBinaryToBinaryCast(CastFunction* func) {
  AddBinaryToBinaryCast<OutType, StringType>(func);
  AddBinaryToBinaryCast<OutType, BinaryType>(func);
  AddBinaryToBinaryCast<OutType, StringViewType>(func);
  AddBinaryToBinaryCast<OutType, BinaryViewType>(func);
  AddBinaryToBinaryCast<OutType, LargeStringType>(func);
  AddBinaryToBinaryCast<OutType, LargeBinaryType>(func);
  AddBinaryToBinaryCast<OutType, FixedSizeBinaryType>(func);
}

}  // namespace

std::vector<std::shared_ptr<CastFunction>> GetBinaryLikeCasts() {
  auto cast_binary = std::make_shared<CastFunction>("cast_binary", Type::BINARY);
  AddCommonCasts(Type::BINARY, binary(), cast_binary.get());
  AddBinaryToBinaryCast<BinaryType>(cast_binary.get());

  auto cast_binary_view =
      std::make_shared<CastFunction>("cast_binary_view", Type::BINARY_VIEW);
  AddCommonCasts(Type::BINARY_VIEW, binary_view(), cast_binary_view.get());
  AddBinaryToBinaryCast<BinaryViewType>(cast_binary_view.get());

  auto cast_large_binary =
      std::make_shared<CastFunction>("cast_large_binary", Type::LARGE_BINARY);
  AddCommonCasts(Type::LARGE_BINARY, large_binary(), cast_large_binary.get());
  AddBinaryToBinaryCast<LargeBinaryType>(cast_large_binary.get());

  auto cast_string = std::make_shared<CastFunction>("cast_string", Type::STRING);
  AddCommonCasts(Type::STRING, utf8(), cast_string.get());
  AddNumberToStringCasts<StringType>(cast_string.get());
  AddDecimalToStringCasts<StringType>(cast_string.get());
  AddTemporalToStringCasts<StringType>(cast_string.get());
  AddBinaryToBinaryCast<StringType>(cast_string.get());

  auto cast_string_view =
      std::make_shared<CastFunction>("cast_string_view", Type::STRING_VIEW);
  AddCommonCasts(Type::STRING_VIEW, utf8_view(), cast_string_view.get());
  AddNumberToStringCasts<StringViewType>(cast_string_view.get());
  AddDecimalToStringCasts<StringViewType>(cast_string_view.get());
  AddTemporalToStringCasts<StringViewType>(cast_string_view.get());
  AddBinaryToBinaryCast<StringViewType>(cast_string_view.get());

  auto cast_large_string =
      std::make_shared<CastFunction>("cast_large_string", Type::LARGE_STRING);
  AddCommonCasts(Type::LARGE_STRING, large_utf8(), cast_large_string.get());
  AddNumberToStringCasts<LargeStringType>(cast_large_string.get());
  AddDecimalToStringCasts<LargeStringType>(cast_large_string.get());
  AddTemporalToStringCasts<LargeStringType>(cast_large_string.get());
  AddBinaryToBinaryCast<LargeStringType>(cast_large_string.get());

  auto cast_fsb =
      std::make_shared<CastFunction>("cast_fixed_size_binary", Type::FIXED_SIZE_BINARY);
  AddCommonCasts(Type::FIXED_SIZE_BINARY, kOutputTargetType, cast_fsb.get());
  DCHECK_OK(cast_fsb->AddKernel(
      Type::FIXED_SIZE_BINARY, {InputType(Type::FIXED_SIZE_BINARY)}, kOutputTargetType,
      BinaryToBinaryCastExec<FixedSizeBinaryType, FixedSizeBinaryType>,
      NullHandling::COMPUTED_NO_PREALLOCATE));

  return {
      cast_binary,      cast_binary_view,  cast_large_binary, cast_string,
      cast_string_view, cast_large_string, cast_fsb,
  };
}

}  // namespace compute::internal
}  // namespace arrow
