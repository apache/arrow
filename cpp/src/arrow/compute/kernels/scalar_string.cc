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

#include <utf8proc.h>
#include <algorithm>
#include <cctype>
#include <string>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_string_internal.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

// Code units in the range [a-z] can only be an encoding of an ascii
// character/codepoint, not the 2nd, 3rd or 4th code unit (byte) of an different
// codepoint. This guaranteed by non-overlap design of the unicode standard. (see
// section 2.5 of Unicode Standard Core Specification v13.0)

uint8_t ascii_tolower(uint8_t utf8_code_unit) {
  return ((utf8_code_unit >= 'A') && (utf8_code_unit <= 'Z')) ? (utf8_code_unit + 32)
                                                              : utf8_code_unit;
}

uint8_t ascii_toupper(uint8_t utf8_code_unit) {
  return ((utf8_code_unit >= 'a') && (utf8_code_unit <= 'z')) ? (utf8_code_unit - 32)
                                                              : utf8_code_unit;
}

// TODO: optional ascii validation

struct AsciiLength {
  template <typename OUT, typename ARG0 = util::string_view>
  static OUT Call(KernelContext*, ARG0 val) {
    return static_cast<OUT>(val.size());
  }
};

template <typename Type, typename Base>
struct Utf8Transform {
  using offset_type = typename Type::offset_type;

  static offset_type Transform(const uint8_t* input, offset_type input_string_ncodeunits,
                               uint8_t* output) {
    offset_type input_string_leftover_ncodeunits = input_string_ncodeunits;
    offset_type encoded_nbytes_total = 0;
    // try ascii first (much faster)
    while (input_string_leftover_ncodeunits && (*input < 128)) {
      *output++ = Base::TransformAscii(*input++);
      input_string_leftover_ncodeunits--;
      encoded_nbytes_total++;
    }
    while (input_string_leftover_ncodeunits) {
      utf8proc_int32_t decoded_codepoint;
      utf8proc_ssize_t decoded_nbytes =
          utf8proc_iterate(input, input_string_leftover_ncodeunits, &decoded_codepoint);
      if (decoded_nbytes < 0) {
        // if we encounter invalid data, we trim the string
        return encoded_nbytes_total;
      }
      input_string_leftover_ncodeunits -= decoded_nbytes;
      input += decoded_nbytes;

      utf8proc_int32_t transformed_codepoint =
          Base::TransformCodepoint(decoded_codepoint);
      utf8proc_ssize_t encoded_nbytes =
          utf8proc_encode_char(transformed_codepoint, output);
      if (encoded_nbytes < 0) {
        // if we encounter invalid data, we trim the string
        return encoded_nbytes_total;
      }
      output += encoded_nbytes;
      encoded_nbytes_total += encoded_nbytes;
    }
    return encoded_nbytes_total;
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      const ArrayData& input = *batch[0].array();
      ArrayData* output = out->mutable_array();

      utf8proc_uint8_t const* input_str = input.buffers[2]->data();
      offset_type const* input_string_offsets = input.GetValues<offset_type>(1);
      offset_type input_ncodeunits = input.buffers[2]->size();
      offset_type input_nstrings = (input.buffers[1]->size() / sizeof(offset_type)) - 1;

      // Section 5.18 of the Unicode spec claim that the number of codepoints for case
      // mapping can grow by a factor of 3. This means grow by a factor of 3 in bytes
      // However, since we don't support all casings (SpecialCasing.txt) the growth
      // is actually only at max 3/2 (as covered by the unittest).
      // Note that rounding down the 3/2 is ok, since only codepoints encoded by
      // two code units (even) can grow to 3 code units.
      KERNEL_RETURN_IF_ERROR(
          ctx, ctx->Allocate(input_ncodeunits * 3 / 2).Value(&output->buffers[2]));
      // We could reuse the buffer if it is all ascii, benchmarking showed this not to
      // matter
      // output->buffers[1] = input.buffers[1];
      KERNEL_RETURN_IF_ERROR(
          ctx, ctx->Allocate(input.buffers[1]->size()).Value(&output->buffers[1]));
      utf8proc_uint8_t* output_str = output->buffers[2]->mutable_data();
      offset_type* output_string_offsets = output->GetMutableValues<offset_type>(1);
      offset_type output_ncodeunits = 0;

      offset_type output_string_offset = 0;
      *output_string_offsets = output_string_offset;
      for (int64_t i = 0; i < input_nstrings; i++) {
        offset_type input_string_offset = input_string_offsets[i];
        offset_type input_string_end = input_string_offsets[i + 1];
        offset_type input_string_ncodeunits = input_string_end - input_string_offset;
        offset_type encoded_nbytes =
            Base::Transform(input_str + input_string_offset, input_string_ncodeunits,
                            output_str + output_ncodeunits);
        output_ncodeunits += encoded_nbytes;
        output_string_offsets[i + 1] = output_ncodeunits;
      }

      // trim the codepoint buffer, since we allocated too much
      KERNEL_RETURN_IF_ERROR(
          ctx,
          output->buffers[2]->CopySlice(0, output_ncodeunits).Value(&output->buffers[2]));
    } else {
      const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
      auto result = checked_pointer_cast<BaseBinaryScalar>(MakeNullScalar(out->type()));
      if (input.is_valid) {
        result->is_valid = true;
        int64_t data_nbytes = input.value->size();
        // See note above in the Array version explaining the 3 / 2
        KERNEL_RETURN_IF_ERROR(ctx,
                               ctx->Allocate(data_nbytes * 3 / 2).Value(&result->value));
        offset_type encoded_nbytes = Base::Transform(input.value->data(), data_nbytes,
                                                     result->value->mutable_data());
        KERNEL_RETURN_IF_ERROR(
            ctx, result->value->CopySlice(0, encoded_nbytes).Value(&result->value));
      }
      out->value = result;
    }
  }
};

template <typename Type>
struct Utf8Upper : Utf8Transform<Type, Utf8Upper<Type>> {
  inline static utf8proc_int32_t TransformAscii(utf8proc_int8_t utf8_code_unit) {
    return ascii_toupper(utf8_code_unit);
  }
  inline static utf8proc_int32_t TransformCodepoint(utf8proc_int32_t codepoint) {
    return utf8proc_toupper(codepoint);
  }
};

template <typename Type>
struct Utf8Lower : Utf8Transform<Type, Utf8Lower<Type>> {
  inline static utf8proc_int32_t TransformAscii(utf8proc_int8_t utf8_code_unit) {
    return ascii_tolower(utf8_code_unit);
  }
  static utf8proc_int32_t TransformCodepoint(utf8proc_int32_t codepoint) {
    return utf8proc_tolower(codepoint);
  }
};

using TransformFunc = std::function<void(const uint8_t*, int64_t, uint8_t*)>;

// Transform a buffer of offsets to one which begins with 0 and has same
// value lengths.
template <typename T>
Status GetShiftedOffsets(KernelContext* ctx, const Buffer& input_buffer, int64_t offset,
                         int64_t length, std::shared_ptr<Buffer>* out) {
  ARROW_ASSIGN_OR_RAISE(*out, ctx->Allocate((length + 1) * sizeof(T)));
  const T* input_offsets = reinterpret_cast<const T*>(input_buffer.data()) + offset;
  T* out_offsets = reinterpret_cast<T*>((*out)->mutable_data());
  T first_offset = *input_offsets;
  for (int64_t i = 0; i < length; ++i) {
    *out_offsets++ = input_offsets[i] - first_offset;
  }
  *out_offsets = input_offsets[length] - first_offset;
  return Status::OK();
}

// Apply `transform` to input character data- this function cannot change the
// length
template <typename Type>
void StringDataTransform(KernelContext* ctx, const ExecBatch& batch,
                         TransformFunc transform, Datum* out) {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using offset_type = typename Type::offset_type;

  if (batch[0].kind() == Datum::ARRAY) {
    const ArrayData& input = *batch[0].array();
    ArrayType input_boxed(batch[0].array());

    ArrayData* out_arr = out->mutable_array();

    if (input.offset == 0) {
      // We can reuse offsets from input
      out_arr->buffers[1] = input.buffers[1];
    } else {
      DCHECK(input.buffers[1]);
      // We must allocate new space for the offsets and shift the existing offsets
      KERNEL_RETURN_IF_ERROR(
          ctx, GetShiftedOffsets<offset_type>(ctx, *input.buffers[1], input.offset,
                                              input.length, &out_arr->buffers[1]));
    }

    // Allocate space for output data
    int64_t data_nbytes = input_boxed.total_values_length();
    KERNEL_RETURN_IF_ERROR(ctx, ctx->Allocate(data_nbytes).Value(&out_arr->buffers[2]));
    if (input.length > 0) {
      transform(input.buffers[2]->data() + input_boxed.value_offset(0), data_nbytes,
                out_arr->buffers[2]->mutable_data());
    }
  } else {
    const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
    auto result = checked_pointer_cast<BaseBinaryScalar>(MakeNullScalar(out->type()));
    if (input.is_valid) {
      result->is_valid = true;
      int64_t data_nbytes = input.value->size();
      KERNEL_RETURN_IF_ERROR(ctx, ctx->Allocate(data_nbytes).Value(&result->value));
      transform(input.value->data(), data_nbytes, result->value->mutable_data());
    }
    out->value = result;
  }
}

void TransformAsciiUpper(const uint8_t* input, int64_t length, uint8_t* output) {
  std::transform(input, input + length, output, ascii_toupper);
}

template <typename Type>
struct AsciiUpper {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    StringDataTransform<Type>(ctx, batch, TransformAsciiUpper, out);
  }
};

void TransformAsciiLower(const uint8_t* input, int64_t length, uint8_t* output) {
  std::transform(input, input + length, output, ascii_tolower);
}

template <typename Type>
struct AsciiLower {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    StringDataTransform<Type>(ctx, batch, TransformAsciiLower, out);
  }
};

void AddAsciiLength(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("ascii_length", Arity::Unary());
  ArrayKernelExec exec_offset_32 =
      applicator::ScalarUnaryNotNull<Int32Type, StringType, AsciiLength>::Exec;
  ArrayKernelExec exec_offset_64 =
      applicator::ScalarUnaryNotNull<Int64Type, LargeStringType, AsciiLength>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, int32(), exec_offset_32));
  DCHECK_OK(func->AddKernel({large_utf8()}, int64(), exec_offset_64));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// strptime string parsing

using StrptimeState = OptionsWrapper<StrptimeOptions>;

struct ParseStrptime {
  explicit ParseStrptime(const StrptimeOptions& options)
      : parser(TimestampParser::MakeStrptime(options.format)), unit(options.unit) {}

  template <typename... Ignored>
  int64_t Call(KernelContext* ctx, util::string_view val) const {
    int64_t result = 0;
    if (!(*parser)(val.data(), val.size(), unit, &result)) {
      ctx->SetStatus(Status::Invalid("Failed to parse string ", val));
    }
    return result;
  }

  std::shared_ptr<TimestampParser> parser;
  TimeUnit::type unit;
};

template <typename InputType>
void StrptimeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  applicator::ScalarUnaryNotNullStateful<TimestampType, InputType, ParseStrptime> kernel{
      ParseStrptime(StrptimeState::Get(ctx))};
  return kernel.Exec(ctx, batch, out);
}

Result<ValueDescr> StrptimeResolve(KernelContext* ctx, const std::vector<ValueDescr>&) {
  if (ctx->state()) {
    return ::arrow::timestamp(StrptimeState::Get(ctx).unit);
  }

  return Status::Invalid("strptime does not provide default StrptimeOptions");
}

void AddStrptime(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("strptime", Arity::Unary());
  DCHECK_OK(func->AddKernel({utf8()}, OutputType(StrptimeResolve),
                            StrptimeExec<StringType>, StrptimeState::Init));
  DCHECK_OK(func->AddKernel({large_utf8()}, OutputType(StrptimeResolve),
                            StrptimeExec<LargeStringType>, StrptimeState::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <template <typename> class ExecFunctor>
void MakeUnaryStringBatchKernel(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary());
  auto exec_32 = ExecFunctor<StringType>::Exec;
  auto exec_64 = ExecFunctor<LargeStringType>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, utf8(), exec_32));
  DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(), exec_64));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <template <typename> typename Transformer>
void MakeUnaryStringUtf8TransformKernel(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary());
  ArrayKernelExec exec = Transformer<StringType>::Exec;
  ArrayKernelExec exec_large = Transformer<LargeStringType>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, utf8(), exec));
  DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(), exec_large));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace

void RegisterScalarStringAscii(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernel<AsciiUpper>("ascii_upper", registry);
  MakeUnaryStringBatchKernel<AsciiLower>("ascii_lower", registry);
  MakeUnaryStringUtf8TransformKernel<Utf8Upper>("utf8_upper", registry);
  MakeUnaryStringUtf8TransformKernel<Utf8Lower>("utf8_lower", registry);
  AddAsciiLength(registry);
  AddStrptime(registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
