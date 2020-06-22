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

// lookup tables
std::vector<uint32_t> lut_upper_codepoint;
std::vector<uint32_t> lut_lower_codepoint;
std::once_flag flag_case_luts;

constexpr uint32_t REPLACEMENT_CHAR =
    '?';  // the proper replacement char would be the 0xFFFD codepoint, but that can
          // increase string length by a factor of 3
constexpr int MAX_CODEPOINT_LUT = 0xffff;  // up to this codepoint is in a lookup table

static inline void utf8_encode(uint8_t*& str, uint32_t codepoint) {
  if (codepoint < 0x80) {
    *str++ = codepoint;
  } else if (codepoint < 0x800) {
    *str++ = 0xC0 + (codepoint >> 6);
    *str++ = 0x80 + (codepoint & 0x3F);
  } else if (codepoint < 0x10000) {
    *str++ = 0xE0 + (codepoint >> 12);
    *str++ = 0x80 + ((codepoint >> 6) & 0x3F);
    *str++ = 0x80 + (codepoint & 0x3F);
  } else if (codepoint < 0x200000) {
    *str++ = 0xF0 + (codepoint >> 18);
    *str++ = 0x80 + ((codepoint >> 12) & 0x3F);
    *str++ = 0x80 + ((codepoint >> 6) & 0x3F);
    *str++ = 0x80 + (codepoint & 0x3F);
  } else {
    *str++ = codepoint;
  }
}

static inline bool utf8_is_continuation(const uint8_t codeunit) {
  return (codeunit & 0xC0) == 0x80;  // upper two bits should be 10
}

static inline uint32_t utf8_decode(const uint8_t*& str, int64_t& length) {
  if (*str < 0x80) {  //
    length -= 1;
    return *str++;
  } else if (*str < 0xC0) {  // invalid non-ascii char
    length -= 1;
    str++;
    return REPLACEMENT_CHAR;
  } else if (*str < 0xE0) {
    uint8_t code_unit_1 = (*str++) & 0x1F;  // take last 5 bits
    length -= 1;
    if (!utf8_is_continuation(*str)) {
      return REPLACEMENT_CHAR;
    }
    uint8_t code_unit_2 = (*str++) & 0x3F;  // take last 6 bits
    length -= 1;
    char32_t codepoint = (code_unit_1 << 6) + code_unit_2;
    return codepoint;
  } else if (*str < 0xF0) {
    uint8_t code_unit_1 = (*str++) & 0x0F;  // take last 4 bits
    length -= 1;
    if (!utf8_is_continuation(*str)) {
      return REPLACEMENT_CHAR;
    }
    uint8_t code_unit_2 = (*str++) & 0x3F;  // take last 6 bits
    length -= 1;
    if (!utf8_is_continuation(*str)) {
      return REPLACEMENT_CHAR;
    }
    uint8_t code_unit_3 = (*str++) & 0x3F;  // take last 6 bits
    length -= 1;
    char32_t codepoint = (code_unit_1 << 12) + (code_unit_2 << 6) + code_unit_3;
    return codepoint;
  } else if (*str < 0xF8) {
    uint8_t code_unit_1 = (*str++) & 0x07;  // take last 3 bits
    length -= 1;
    if (!utf8_is_continuation(*str)) {
      return REPLACEMENT_CHAR;
    }
    uint8_t code_unit_2 = (*str++) & 0x3F;  // take last 6 bits
    length -= 1;
    if (!utf8_is_continuation(*str)) {
      return REPLACEMENT_CHAR;
    }
    uint8_t code_unit_3 = (*str++) & 0x3F;  // take last 6 bits
    length -= 1;
    if (!utf8_is_continuation(*str)) {
      return REPLACEMENT_CHAR;
    }
    uint8_t code_unit_4 = (*str++) & 0x3F;  // take last 6 bits
    char32_t codepoint =
        (code_unit_1 << 18) + (code_unit_2 << 12) + (code_unit_3 << 6) + code_unit_4;
    return codepoint;
  } else {  // invalid non-ascii char
    length -= 1;
    str++;
    return REPLACEMENT_CHAR;
  }
}

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

template <class UnaryOperation>
static inline void utf8_transform(const uint8_t* first, const uint8_t* last,
                                  uint8_t*& destination, UnaryOperation unary_op) {
  int64_t length = last - first;
  while (length > 0) {
    utf8_encode(destination, unary_op(utf8_decode(first, length)));
  }
}

// TODO: optional ascii validation

struct AsciiLength {
  template <typename OUT, typename ARG0 = util::string_view>
  static OUT Call(KernelContext*, ARG0 val) {
    return static_cast<OUT>(val.size());
  }
};

template <typename Type, template <typename> class Derived>
struct Utf8Transform {
  using offset_type = typename Type::offset_type;
  using DerivedClass = Derived<Type>;
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  static offset_type Transform(const uint8_t* input, offset_type input_string_ncodeunits,
                               uint8_t* output) {
    uint8_t* dest = output;
    utf8_transform(input, input + input_string_ncodeunits, dest,
                   DerivedClass::TransformCodepoint);
    return (offset_type)(dest - output);
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      std::call_once(flag_case_luts, []() {
        lut_upper_codepoint.reserve(MAX_CODEPOINT_LUT + 1);
        lut_lower_codepoint.reserve(MAX_CODEPOINT_LUT + 1);
        for (int i = 0; i <= MAX_CODEPOINT_LUT; i++) {
          lut_upper_codepoint.push_back(utf8proc_toupper(i));
          lut_lower_codepoint.push_back(utf8proc_tolower(i));
        }
      });
      const ArrayData& input = *batch[0].array();
      ArrayType input_boxed(batch[0].array());
      ArrayData* output = out->mutable_array();

      offset_type const* input_string_offsets = input.GetValues<offset_type>(1);
      utf8proc_uint8_t const* input_str =
          input.buffers[2]->data() + input_boxed.value_offset(0);
      offset_type input_ncodeunits = input_boxed.total_values_length();
      offset_type input_nstrings = (offset_type)input.length;

      // Section 5.18 of the Unicode spec claim that the number of codepoints for case
      // mapping can grow by a factor of 3. This means grow by a factor of 3 in bytes
      // However, since we don't support all casings (SpecialCasing.txt) the growth
      // is actually only at max 3/2 (as covered by the unittest).
      // Note that rounding down the 3/2 is ok, since only codepoints encoded by
      // two code units (even) can grow to 3 code units.

      int64_t output_ncodeunits_max = ((int64_t)input_ncodeunits) * 3 / 2;
      if (output_ncodeunits_max > std::numeric_limits<offset_type>::max()) {
        ctx->SetStatus(Status::CapacityError(
            "Result might not fit in a 32bit utf8 array, convert to large_utf8"));
        return;
      }

      KERNEL_RETURN_IF_ERROR(
          ctx, ctx->Allocate(output_ncodeunits_max).Value(&output->buffers[2]));
      // We could reuse the buffer if it is all ascii, benchmarking showed this not to
      // matter
      // output->buffers[1] = input.buffers[1];
      KERNEL_RETURN_IF_ERROR(ctx,
                             ctx->Allocate((input_nstrings + 1) * sizeof(offset_type))
                                 .Value(&output->buffers[1]));
      utf8proc_uint8_t* output_str = output->buffers[2]->mutable_data();
      offset_type* output_string_offsets = output->GetMutableValues<offset_type>(1);
      offset_type output_ncodeunits = 0;

      offset_type output_string_offset = 0;
      *output_string_offsets = output_string_offset;
      offset_type input_string_first_offset = input_string_offsets[0];
      for (int64_t i = 0; i < input_nstrings; i++) {
        offset_type input_string_offset =
            input_string_offsets[i] - input_string_first_offset;
        offset_type input_string_end =
            input_string_offsets[i + 1] - input_string_first_offset;
        offset_type input_string_ncodeunits = input_string_end - input_string_offset;
        offset_type encoded_nbytes = DerivedClass::Transform(
            input_str + input_string_offset, input_string_ncodeunits,
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
        offset_type data_nbytes = (offset_type)input.value->size();
        // See note above in the Array version explaining the 3 / 2
        KERNEL_RETURN_IF_ERROR(ctx,
                               ctx->Allocate(data_nbytes * 3 / 2).Value(&result->value));
        offset_type encoded_nbytes = DerivedClass::Transform(
            input.value->data(), data_nbytes, result->value->mutable_data());
        KERNEL_RETURN_IF_ERROR(
            ctx, result->value->CopySlice(0, encoded_nbytes).Value(&result->value));
      }
      out->value = result;
    }
  }
};

template <typename Type>
struct Utf8Upper : Utf8Transform<Type, Utf8Upper> {
  inline static uint32_t TransformCodepoint(char32_t codepoint) {
    return codepoint <= MAX_CODEPOINT_LUT ? lut_upper_codepoint[codepoint]
                                          : utf8proc_toupper(codepoint);
  }
};

template <typename Type>
struct Utf8Lower : Utf8Transform<Type, Utf8Lower> {
  static uint32_t TransformCodepoint(char32_t codepoint) {
    return codepoint <= MAX_CODEPOINT_LUT ? lut_lower_codepoint[codepoint]
                                          : utf8proc_tolower(codepoint);
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

template <template <typename> class Transformer>
void MakeUnaryStringUtf8TransformKernel(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary());
  ArrayKernelExec exec_32 = Transformer<StringType>::Exec;
  ArrayKernelExec exec_64 = Transformer<LargeStringType>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, utf8(), exec_32));
  DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(), exec_64));
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
