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

#include <algorithm>
#include <cctype>
#include <string>

#ifdef ARROW_WITH_UTF8PROC
#include <utf8proc.h>
#endif

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_string_internal.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

// Code units in the range [a-z] can only be an encoding of an ascii
// character/codepoint, not the 2nd, 3rd or 4th code unit (byte) of an different
// codepoint. This guaranteed by non-overlap design of the unicode standard. (see
// section 2.5 of Unicode Standard Core Specification v13.0)

static inline uint8_t ascii_tolower(uint8_t utf8_code_unit) {
  return ((utf8_code_unit >= 'A') && (utf8_code_unit <= 'Z')) ? (utf8_code_unit + 32)
                                                              : utf8_code_unit;
}

static inline uint8_t ascii_toupper(uint8_t utf8_code_unit) {
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

#ifdef ARROW_WITH_UTF8PROC

// Direct lookup tables for unicode properties
constexpr uint32_t kMaxCodepointLookup =
    0xffff;  // up to this codepoint is in a lookup table
std::vector<uint32_t> lut_upper_codepoint;
std::vector<uint32_t> lut_lower_codepoint;
std::once_flag flag_case_luts;

void EnsureLookupTablesFilled() {
  std::call_once(flag_case_luts, []() {
    lut_upper_codepoint.reserve(kMaxCodepointLookup + 1);
    lut_lower_codepoint.reserve(kMaxCodepointLookup + 1);
    for (uint32_t i = 0; i <= kMaxCodepointLookup; i++) {
      lut_upper_codepoint.push_back(utf8proc_toupper(i));
      lut_lower_codepoint.push_back(utf8proc_tolower(i));
    }
  });
}

template <typename Type, typename Derived>
struct UTF8Transform {
  using offset_type = typename Type::offset_type;
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  static bool Transform(const uint8_t* input, offset_type input_string_ncodeunits,
                        uint8_t* output, offset_type* output_written) {
    uint8_t* output_start = output;
    if (ARROW_PREDICT_FALSE(
            !arrow::util::UTF8Transform(input, input + input_string_ncodeunits, &output,
                                        Derived::TransformCodepoint))) {
      return false;
    }
    *output_written = static_cast<offset_type>(output - output_start);
    return true;
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      EnsureLookupTablesFilled();
      const ArrayData& input = *batch[0].array();
      ArrayType input_boxed(batch[0].array());
      ArrayData* output = out->mutable_array();

      offset_type input_ncodeunits = input_boxed.total_values_length();
      offset_type input_nstrings = static_cast<offset_type>(input.length);

      // Section 5.18 of the Unicode spec claim that the number of codepoints for case
      // mapping can grow by a factor of 3. This means grow by a factor of 3 in bytes
      // However, since we don't support all casings (SpecialCasing.txt) the growth
      // is actually only at max 3/2 (as covered by the unittest).
      // Note that rounding down the 3/2 is ok, since only codepoints encoded by
      // two code units (even) can grow to 3 code units.

      int64_t output_ncodeunits_max = static_cast<int64_t>(input_ncodeunits) * 3 / 2;
      if (output_ncodeunits_max > std::numeric_limits<offset_type>::max()) {
        ctx->SetStatus(Status::CapacityError(
            "Result might not fit in a 32bit utf8 array, convert to large_utf8"));
        return;
      }

      KERNEL_ASSIGN_OR_RAISE(auto values_buffer, ctx,
                             ctx->Allocate(output_ncodeunits_max));
      output->buffers[2] = values_buffer;

      // We could reuse the indices if the data is all ascii, benchmarking showed this
      // not to matter.
      //   output->buffers[1] = input.buffers[1];
      KERNEL_ASSIGN_OR_RAISE(output->buffers[1], ctx,
                             ctx->Allocate((input_nstrings + 1) * sizeof(offset_type)));
      uint8_t* output_str = output->buffers[2]->mutable_data();
      offset_type* output_string_offsets = output->GetMutableValues<offset_type>(1);
      offset_type output_ncodeunits = 0;

      output_string_offsets[0] = 0;
      for (int64_t i = 0; i < input_nstrings; i++) {
        offset_type input_string_ncodeunits;
        const uint8_t* input_string = input_boxed.GetValue(i, &input_string_ncodeunits);
        offset_type encoded_nbytes = 0;
        if (ARROW_PREDICT_FALSE(!Derived::Transform(input_string, input_string_ncodeunits,
                                                    output_str + output_ncodeunits,
                                                    &encoded_nbytes))) {
          ctx->SetStatus(Status::Invalid("Invalid UTF8 sequence in input"));
          return;
        }
        output_ncodeunits += encoded_nbytes;
        output_string_offsets[i + 1] = output_ncodeunits;
      }

      // Trim the codepoint buffer, since we allocated too much
      KERNEL_RETURN_IF_ERROR(
          ctx, values_buffer->Resize(output_ncodeunits, /*shrink_to_fit=*/true));
    } else {
      const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
      auto result = checked_pointer_cast<BaseBinaryScalar>(MakeNullScalar(out->type()));
      if (input.is_valid) {
        result->is_valid = true;
        offset_type data_nbytes = static_cast<offset_type>(input.value->size());

        // See note above in the Array version explaining the 3 / 2
        int64_t output_ncodeunits_max = static_cast<int64_t>(data_nbytes) * 3 / 2;
        if (output_ncodeunits_max > std::numeric_limits<offset_type>::max()) {
          ctx->SetStatus(Status::CapacityError(
              "Result might not fit in a 32bit utf8 array, convert to large_utf8"));
          return;
        }
        KERNEL_ASSIGN_OR_RAISE(auto value_buffer, ctx,
                               ctx->Allocate(output_ncodeunits_max));
        result->value = value_buffer;
        offset_type encoded_nbytes = 0;
        if (ARROW_PREDICT_FALSE(!Derived::Transform(input.value->data(), data_nbytes,
                                                    value_buffer->mutable_data(),
                                                    &encoded_nbytes))) {
          ctx->SetStatus(Status::Invalid("Invalid UTF8 sequence in input"));
          return;
        }
        KERNEL_RETURN_IF_ERROR(
            ctx, value_buffer->Resize(encoded_nbytes, /*shrink_to_fit=*/true));
      }
      out->value = result;
    }
  }
};

template <typename Type>
struct UTF8Upper : UTF8Transform<Type, UTF8Upper<Type>> {
  inline static uint32_t TransformCodepoint(uint32_t codepoint) {
    return codepoint <= kMaxCodepointLookup ? lut_upper_codepoint[codepoint]
                                            : utf8proc_toupper(codepoint);
  }
};

template <typename Type>
struct UTF8Lower : UTF8Transform<Type, UTF8Lower<Type>> {
  static uint32_t TransformCodepoint(uint32_t codepoint) {
    return codepoint <= kMaxCodepointLookup ? lut_lower_codepoint[codepoint]
                                            : utf8proc_tolower(codepoint);
  }
};

#endif  // ARROW_WITH_UTF8PROC

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
// exact pattern detection

using StrToBoolTransformFunc =
    std::function<void(const void*, const uint8_t*, int64_t, int64_t, uint8_t*)>;

// Apply `transform` to input character data- this function cannot change the
// length
template <typename Type>
void StringBoolTransform(KernelContext* ctx, const ExecBatch& batch,
                         StrToBoolTransformFunc transform, Datum* out) {
  using offset_type = typename Type::offset_type;

  if (batch[0].kind() == Datum::ARRAY) {
    const ArrayData& input = *batch[0].array();
    ArrayData* out_arr = out->mutable_array();
    if (input.length > 0) {
      transform(
          reinterpret_cast<const offset_type*>(input.buffers[1]->data()) + input.offset,
          input.buffers[2]->data(), input.length, out_arr->offset,
          out_arr->buffers[1]->mutable_data());
    }
  } else {
    const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
    if (input.is_valid) {
      auto result = checked_pointer_cast<BooleanScalar>(MakeNullScalar(out->type()));
      uint8_t result_value = 0;
      result->is_valid = true;
      std::array<offset_type, 2> offsets{0,
                                         static_cast<offset_type>(input.value->size())};
      transform(offsets.data(), input.value->data(), 1, /*output_offset=*/0,
                &result_value);
      out->value = std::make_shared<BooleanScalar>(result_value > 0);
    }
  }
}

template <typename offset_type>
void TransformBinaryContainsExact(const uint8_t* pattern, int64_t pattern_length,
                                  const offset_type* offsets, const uint8_t* data,
                                  int64_t length, int64_t output_offset,
                                  uint8_t* output) {
  // This is an implementation of the Knuth-Morris-Pratt algorithm

  // Phase 1: Build the prefix table
  std::vector<offset_type> prefix_table(pattern_length + 1);
  offset_type prefix_length = -1;
  prefix_table[0] = -1;
  for (offset_type pos = 0; pos < pattern_length; ++pos) {
    // The prefix cannot be expanded, reset.
    if (prefix_length >= 0 && pattern[pos] != pattern[prefix_length]) {
      prefix_length = prefix_table[prefix_length];
    }
    prefix_length++;
    prefix_table[pos + 1] = prefix_length;
  }

  // Phase 2: Find the prefix in the data
  FirstTimeBitmapWriter bitmap_writer(output, output_offset, length);
  for (int64_t i = 0; i < length; ++i) {
    const uint8_t* current_data = data + offsets[i];
    int64_t current_length = offsets[i + 1] - offsets[i];

    int64_t pattern_pos = 0;
    for (int64_t k = 0; k < current_length; k++) {
      if (pattern[pattern_pos] == current_data[k]) {
        pattern_pos++;
        if (pattern_pos == pattern_length) {
          bitmap_writer.Set();
          break;
        }
      } else {
        pattern_pos = std::max<offset_type>(0, prefix_table[pattern_pos]);
      }
    }
    bitmap_writer.Next();
  }
  bitmap_writer.Finish();
}

using BinaryContainsExactState = OptionsWrapper<BinaryContainsExactOptions>;

template <typename Type>
struct BinaryContainsExact {
  using offset_type = typename Type::offset_type;
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    BinaryContainsExactOptions arg = BinaryContainsExactState::Get(ctx);
    const uint8_t* pat = reinterpret_cast<const uint8_t*>(arg.pattern.c_str());
    const int64_t pat_size = arg.pattern.length();
    StringBoolTransform<Type>(
        ctx, batch,
        [pat, pat_size](const void* offsets, const uint8_t* data, int64_t length,
                        int64_t output_offset, uint8_t* output) {
          TransformBinaryContainsExact<offset_type>(
              pat, pat_size, reinterpret_cast<const offset_type*>(offsets), data, length,
              output_offset, output);
        },
        out);
  }
};

void AddBinaryContainsExact(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("binary_contains_exact", Arity::Unary());
  auto exec_32 = BinaryContainsExact<StringType>::Exec;
  auto exec_64 = BinaryContainsExact<LargeStringType>::Exec;
  DCHECK_OK(
      func->AddKernel({utf8()}, boolean(), exec_32, BinaryContainsExactState::Init));
  DCHECK_OK(func->AddKernel({large_utf8()}, boolean(), exec_64,
                            BinaryContainsExactState::Init));
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

#ifdef ARROW_WITH_UTF8PROC

template <template <typename> class Transformer>
void MakeUnaryStringUTF8TransformKernel(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary());
  ArrayKernelExec exec_32 = Transformer<StringType>::Exec;
  ArrayKernelExec exec_64 = Transformer<LargeStringType>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, utf8(), exec_32));
  DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(), exec_64));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#endif

}  // namespace

void RegisterScalarStringAscii(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernel<AsciiUpper>("ascii_upper", registry);
  MakeUnaryStringBatchKernel<AsciiLower>("ascii_lower", registry);
#ifdef ARROW_WITH_UTF8PROC
  MakeUnaryStringUTF8TransformKernel<UTF8Upper>("utf8_upper", registry);
  MakeUnaryStringUTF8TransformKernel<UTF8Lower>("utf8_lower", registry);
#endif
  AddAsciiLength(registry);
  AddBinaryContainsExact(registry);
  AddStrptime(registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
