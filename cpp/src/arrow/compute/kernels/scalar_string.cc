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

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
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

template <typename T>
static inline bool IsAsciiCharacter(T character) {
  return character < 128;
}

struct BinaryLength {
  template <typename OutValue, typename Arg0Value = util::string_view>
  static OutValue Call(KernelContext*, Arg0Value val) {
    return static_cast<OutValue>(val.size());
  }
};

#ifdef ARROW_WITH_UTF8PROC

// Direct lookup tables for unicode properties
constexpr uint32_t kMaxCodepointLookup =
    0xffff;  // up to this codepoint is in a lookup table
std::vector<uint32_t> lut_upper_codepoint;
std::vector<uint32_t> lut_lower_codepoint;
std::vector<utf8proc_category_t> lut_category;
std::once_flag flag_case_luts;

void EnsureLookupTablesFilled() {
  std::call_once(flag_case_luts, []() {
    lut_upper_codepoint.reserve(kMaxCodepointLookup + 1);
    lut_lower_codepoint.reserve(kMaxCodepointLookup + 1);
    for (uint32_t i = 0; i <= kMaxCodepointLookup; i++) {
      lut_upper_codepoint.push_back(utf8proc_toupper(i));
      lut_lower_codepoint.push_back(utf8proc_tolower(i));
      lut_category.push_back(utf8proc_category(i));
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
    EnsureLookupTablesFilled();
    if (batch[0].kind() == Datum::ARRAY) {
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

      // String offsets are preallocated
      offset_type* output_string_offsets = output->GetMutableValues<offset_type>(1);
      uint8_t* output_str = output->buffers[2]->mutable_data();
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
  inline static uint32_t TransformCodepoint(uint32_t codepoint) {
    return codepoint <= kMaxCodepointLookup ? lut_lower_codepoint[codepoint]
                                            : utf8proc_tolower(codepoint);
  }
};

#else

void EnsureLookupTablesFilled() {}

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
      uint8_t result_value = 0;
      std::array<offset_type, 2> offsets{0,
                                         static_cast<offset_type>(input.value->size())};
      transform(offsets.data(), input.value->data(), 1, /*output_offset=*/0,
                &result_value);
      out->value = std::make_shared<BooleanScalar>(result_value > 0);
    }
  }
}

template <typename offset_type>
void TransformMatchSubstring(const uint8_t* pattern, int64_t pattern_length,
                             const offset_type* offsets, const uint8_t* data,
                             int64_t length, int64_t output_offset, uint8_t* output) {
  // This is an implementation of the Knuth-Morris-Pratt algorithm

  // Phase 1: Build the prefix table
  std::vector<offset_type> prefix_table(pattern_length + 1);
  offset_type prefix_length = -1;
  prefix_table[0] = -1;
  for (offset_type pos = 0; pos < pattern_length; ++pos) {
    // The prefix cannot be expanded, reset.
    while (prefix_length >= 0 && pattern[pos] != pattern[prefix_length]) {
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
      while ((pattern_pos >= 0) && (pattern[pattern_pos] != current_data[k])) {
        pattern_pos = prefix_table[pattern_pos];
      }
      pattern_pos++;
      if (pattern_pos == pattern_length) {
        bitmap_writer.Set();
        break;
      }
    }
    bitmap_writer.Next();
  }
  bitmap_writer.Finish();
}

using MatchSubstringState = OptionsWrapper<MatchSubstringOptions>;

template <typename Type>
struct MatchSubstring {
  using offset_type = typename Type::offset_type;
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    MatchSubstringOptions arg = MatchSubstringState::Get(ctx);
    const uint8_t* pat = reinterpret_cast<const uint8_t*>(arg.pattern.c_str());
    const int64_t pat_size = arg.pattern.length();
    StringBoolTransform<Type>(
        ctx, batch,
        [pat, pat_size](const void* offsets, const uint8_t* data, int64_t length,
                        int64_t output_offset, uint8_t* output) {
          TransformMatchSubstring<offset_type>(
              pat, pat_size, reinterpret_cast<const offset_type*>(offsets), data, length,
              output_offset, output);
        },
        out);
  }
};

const FunctionDoc match_substring_doc(
    "Match strings against literal pattern",
    ("For each string in `strings`, emit true iff it contains a given pattern.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions");

void AddMatchSubstring(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("match_substring", Arity::Unary(),
                                               &match_substring_doc);
  auto exec_32 = MatchSubstring<StringType>::Exec;
  auto exec_64 = MatchSubstring<LargeStringType>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, boolean(), exec_32, MatchSubstringState::Init));
  DCHECK_OK(
      func->AddKernel({large_utf8()}, boolean(), exec_64, MatchSubstringState::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// IsAlpha/Digit etc

#ifdef ARROW_WITH_UTF8PROC

static inline bool HasAnyUnicodeGeneralCategory(uint32_t codepoint, uint32_t mask) {
  utf8proc_category_t general_category = codepoint <= kMaxCodepointLookup
                                             ? lut_category[codepoint]
                                             : utf8proc_category(codepoint);
  uint32_t general_category_bit = 1 << general_category;
  // for e.g. undefined (but valid) codepoints, general_category == 0 ==
  // UTF8PROC_CATEGORY_CN
  return (general_category != UTF8PROC_CATEGORY_CN) &&
         ((general_category_bit & mask) != 0);
}

template <typename... Categories>
static inline bool HasAnyUnicodeGeneralCategory(uint32_t codepoint, uint32_t mask,
                                                utf8proc_category_t category,
                                                Categories... categories) {
  return HasAnyUnicodeGeneralCategory(codepoint, mask | (1 << category), categories...);
}

template <typename... Categories>
static inline bool HasAnyUnicodeGeneralCategory(uint32_t codepoint,
                                                utf8proc_category_t category,
                                                Categories... categories) {
  return HasAnyUnicodeGeneralCategory(codepoint, static_cast<uint32_t>(1u << category),
                                      categories...);
}

static inline bool IsCasedCharacterUnicode(uint32_t codepoint) {
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LU,
                                      UTF8PROC_CATEGORY_LL, UTF8PROC_CATEGORY_LT) ||
         ((static_cast<uint32_t>(utf8proc_toupper(codepoint)) != codepoint) ||
          (static_cast<uint32_t>(utf8proc_tolower(codepoint)) != codepoint));
}

static inline bool IsLowerCaseCharacterUnicode(uint32_t codepoint) {
  // although this trick seems to work for upper case, this is not enough for lower case
  // testing, see https://github.com/JuliaStrings/utf8proc/issues/195 . But currently the
  // best we can do
  return (HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LL) ||
          ((static_cast<uint32_t>(utf8proc_toupper(codepoint)) != codepoint) &&
           (static_cast<uint32_t>(utf8proc_tolower(codepoint)) == codepoint))) &&
         !HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LT);
}

static inline bool IsUpperCaseCharacterUnicode(uint32_t codepoint) {
  // this seems to be a good workaround for utf8proc not having case information
  // https://github.com/JuliaStrings/utf8proc/issues/195
  return (HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LU) ||
          ((static_cast<uint32_t>(utf8proc_toupper(codepoint)) == codepoint) &&
           (static_cast<uint32_t>(utf8proc_tolower(codepoint)) != codepoint))) &&
         !HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LT);
}

static inline bool IsAlphaNumericCharacterUnicode(uint32_t codepoint) {
  return HasAnyUnicodeGeneralCategory(
      codepoint, UTF8PROC_CATEGORY_LU, UTF8PROC_CATEGORY_LL, UTF8PROC_CATEGORY_LT,
      UTF8PROC_CATEGORY_LM, UTF8PROC_CATEGORY_LO, UTF8PROC_CATEGORY_ND,
      UTF8PROC_CATEGORY_NL, UTF8PROC_CATEGORY_NO);
}

static inline bool IsAlphaCharacterUnicode(uint32_t codepoint) {
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_LU,
                                      UTF8PROC_CATEGORY_LL, UTF8PROC_CATEGORY_LT,
                                      UTF8PROC_CATEGORY_LM, UTF8PROC_CATEGORY_LO);
}

static inline bool IsDecimalCharacterUnicode(uint32_t codepoint) {
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_ND);
}

static inline bool IsDigitCharacterUnicode(uint32_t codepoint) {
  // Python defines this as Numeric_Type=Digit or Numeric_Type=Decimal.
  // utf8proc has no support for this, this is the best we can do:
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_ND);
}

static inline bool IsNumericCharacterUnicode(uint32_t codepoint) {
  // Formally this is not correct, but utf8proc does not allow us to query for Numerical
  // properties, e.g. Numeric_Value and Numeric_Type
  // Python defines Numeric as Numeric_Type=Digit, Numeric_Type=Decimal or
  // Numeric_Type=Numeric.
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_ND,
                                      UTF8PROC_CATEGORY_NL, UTF8PROC_CATEGORY_NO);
}

static inline bool IsSpaceCharacterUnicode(uint32_t codepoint) {
  auto property = utf8proc_get_property(codepoint);
  return HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_ZS) ||
         property->bidi_class == UTF8PROC_BIDI_CLASS_WS ||
         property->bidi_class == UTF8PROC_BIDI_CLASS_B ||
         property->bidi_class == UTF8PROC_BIDI_CLASS_S;
}

static inline bool IsPrintableCharacterUnicode(uint32_t codepoint) {
  uint32_t general_category = utf8proc_category(codepoint);
  return (general_category != UTF8PROC_CATEGORY_CN) &&
         !HasAnyUnicodeGeneralCategory(codepoint, UTF8PROC_CATEGORY_CC,
                                       UTF8PROC_CATEGORY_CF, UTF8PROC_CATEGORY_CS,
                                       UTF8PROC_CATEGORY_CO, UTF8PROC_CATEGORY_ZS,
                                       UTF8PROC_CATEGORY_ZL, UTF8PROC_CATEGORY_ZP);
}

#endif

static inline bool IsLowerCaseCharacterAscii(uint8_t ascii_character) {
  return (ascii_character >= 'a') && (ascii_character <= 'z');
}

static inline bool IsUpperCaseCharacterAscii(uint8_t ascii_character) {
  return (ascii_character >= 'A') && (ascii_character <= 'Z');
}

static inline bool IsCasedCharacterAscii(uint8_t ascii_character) {
  return IsLowerCaseCharacterAscii(ascii_character) ||
         IsUpperCaseCharacterAscii(ascii_character);
}

static inline bool IsAlphaCharacterAscii(uint8_t ascii_character) {
  return IsCasedCharacterAscii(ascii_character);  // same
}

static inline bool IsAlphaNumericCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= '0') && (ascii_character <= '9')) ||
         ((ascii_character >= 'a') && (ascii_character <= 'z')) ||
         ((ascii_character >= 'A') && (ascii_character <= 'Z'));
}

static inline bool IsDecimalCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= '0') && (ascii_character <= '9'));
}

static inline bool IsSpaceCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= 0x09) && (ascii_character <= 0x0D)) ||
         (ascii_character == ' ');
}

static inline bool IsPrintableCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= ' ') && (ascii_character <= '~'));
}

template <typename Derived, bool allow_empty = false>
struct CharacterPredicateUnicode {
  static bool Call(KernelContext* ctx, const uint8_t* input,
                   size_t input_string_ncodeunits) {
    if (allow_empty && input_string_ncodeunits == 0) {
      return true;
    }
    bool all;
    bool any = false;
    if (!ARROW_PREDICT_TRUE(arrow::util::UTF8AllOf(
            input, input + input_string_ncodeunits, &all, [&any](uint32_t codepoint) {
              any |= Derived::PredicateCharacterAny(codepoint);
              return Derived::PredicateCharacterAll(codepoint);
            }))) {
      ctx->SetStatus(Status::Invalid("Invalid UTF8 sequence in input"));
      return false;
    }
    return all & any;
  }

  static inline bool PredicateCharacterAny(uint32_t) {
    return true;  // default condition make sure there is at least 1 charachter
  }
};

template <typename Derived, bool allow_empty = false>
struct CharacterPredicateAscii {
  static bool Call(KernelContext* ctx, const uint8_t* input,
                   size_t input_string_ncodeunits) {
    if (allow_empty && input_string_ncodeunits == 0) {
      return true;
    }
    bool any = false;
    // MB: A simple for loops seems 8% faster on gcc 9.3, running the IsAlphaNumericAscii
    // benchmark. I don't consider that worth it.
    bool all = std::all_of(input, input + input_string_ncodeunits,
                           [&any](uint8_t ascii_character) {
                             any |= Derived::PredicateCharacterAny(ascii_character);
                             return Derived::PredicateCharacterAll(ascii_character);
                           });
    return all & any;
  }

  static inline bool PredicateCharacterAny(uint8_t) {
    return true;  // default condition make sure there is at least 1 charachter
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsAlphaNumericUnicode : CharacterPredicateUnicode<IsAlphaNumericUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsAlphaNumericCharacterUnicode(codepoint);
  }
};
#endif

struct IsAlphaNumericAscii : CharacterPredicateAscii<IsAlphaNumericAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsAlphaNumericCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsAlphaUnicode : CharacterPredicateUnicode<IsAlphaUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsAlphaCharacterUnicode(codepoint);
  }
};
#endif

struct IsAlphaAscii : CharacterPredicateAscii<IsAlphaAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsAlphaCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsDecimalUnicode : CharacterPredicateUnicode<IsDecimalUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsDecimalCharacterUnicode(codepoint);
  }
};
#endif

struct IsDecimalAscii : CharacterPredicateAscii<IsDecimalAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsDecimalCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsDigitUnicode : CharacterPredicateUnicode<IsDigitUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsDigitCharacterUnicode(codepoint);
  }
};

struct IsNumericUnicode : CharacterPredicateUnicode<IsNumericUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsNumericCharacterUnicode(codepoint);
  }
};
#endif

struct IsAscii {
  static bool Call(KernelContext* ctx, const uint8_t* input,
                   size_t input_string_nascii_characters) {
    return std::all_of(input, input + input_string_nascii_characters,
                       IsAsciiCharacter<uint8_t>);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsLowerUnicode : CharacterPredicateUnicode<IsLowerUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterUnicode(codepoint) || IsLowerCaseCharacterUnicode(codepoint);
  }
  static inline bool PredicateCharacterAny(uint32_t codepoint) {
    return IsCasedCharacterUnicode(codepoint);  // at least 1 cased character
  }
};
#endif

struct IsLowerAscii : CharacterPredicateAscii<IsLowerAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterAscii(ascii_character) ||
           IsLowerCaseCharacterAscii(ascii_character);
  }
  static inline bool PredicateCharacterAny(uint8_t ascii_character) {
    return IsCasedCharacterAscii(ascii_character);  // at least 1 cased character
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsPrintableUnicode
    : CharacterPredicateUnicode<IsPrintableUnicode, /*allow_empty=*/true> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return codepoint == ' ' || IsPrintableCharacterUnicode(codepoint);
  }
};
#endif

struct IsPrintableAscii
    : CharacterPredicateAscii<IsPrintableAscii, /*allow_empty=*/true> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsPrintableCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsSpaceUnicode : CharacterPredicateUnicode<IsSpaceUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsSpaceCharacterUnicode(codepoint);
  }
};
#endif

struct IsSpaceAscii : CharacterPredicateAscii<IsSpaceAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsSpaceCharacterAscii(ascii_character);
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsTitleUnicode {
  static bool Call(KernelContext* ctx, const uint8_t* input,
                   size_t input_string_ncodeunits) {
    // rules:
    // * 1: lower case follows cased
    // * 2: upper case follows uncased
    // * 3: at least 1 cased character (which logically should be upper/title)
    bool rules_1_and_2;
    bool previous_cased = false;  // in LL, LU or LT
    bool rule_3 = false;
    bool status =
        arrow::util::UTF8AllOf(input, input + input_string_ncodeunits, &rules_1_and_2,
                               [&previous_cased, &rule_3](uint32_t codepoint) {
                                 if (IsLowerCaseCharacterUnicode(codepoint)) {
                                   if (!previous_cased) return false;  // rule 1 broken
                                   previous_cased = true;
                                 } else if (IsCasedCharacterUnicode(codepoint)) {
                                   if (previous_cased) return false;  // rule 2 broken
                                   // next should be a lower case or uncased
                                   previous_cased = true;
                                   rule_3 = true;  // rule 3 obeyed
                                 } else {
                                   // a non-cased char, like _ or 1
                                   // next should be upper case or more uncased
                                   previous_cased = false;
                                 }
                                 return true;
                               });
    if (!ARROW_PREDICT_TRUE(status)) {
      ctx->SetStatus(Status::Invalid("Invalid UTF8 sequence in input"));
      return false;
    }
    return rules_1_and_2 & rule_3;
  }
};
#endif

struct IsTitleAscii {
  static bool Call(KernelContext* ctx, const uint8_t* input,
                   size_t input_string_ncodeunits) {
    // rules:
    // * 1: lower case follows cased
    // * 2: upper case follows uncased
    // * 3: at least 1 cased character (which logically should be upper/title)
    bool rules_1_and_2 = true;
    bool previous_cased = false;  // in LL, LU or LT
    bool rule_3 = false;
    // we cannot rely on std::all_of because we need guaranteed order
    for (const uint8_t* c = input; c < input + input_string_ncodeunits; ++c) {
      if (IsLowerCaseCharacterAscii(*c)) {
        if (!previous_cased) {
          // rule 1 broken
          rules_1_and_2 = false;
          break;
        }
        previous_cased = true;
      } else if (IsCasedCharacterAscii(*c)) {
        if (previous_cased) {
          // rule 2 broken
          rules_1_and_2 = false;
          break;
        }
        // next should be a lower case or uncased
        previous_cased = true;
        rule_3 = true;  // rule 3 obeyed
      } else {
        // a non-cased char, like _ or 1
        // next should be upper case or more uncased
        previous_cased = false;
      }
    }
    return rules_1_and_2 & rule_3;
  }
};

#ifdef ARROW_WITH_UTF8PROC
struct IsUpperUnicode : CharacterPredicateUnicode<IsUpperUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterUnicode(codepoint) || IsUpperCaseCharacterUnicode(codepoint);
  }
  static inline bool PredicateCharacterAny(uint32_t codepoint) {
    return IsCasedCharacterUnicode(codepoint);  // at least 1 cased character
  }
};
#endif

struct IsUpperAscii : CharacterPredicateAscii<IsUpperAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterAscii(ascii_character) ||
           IsUpperCaseCharacterAscii(ascii_character);
  }
  static inline bool PredicateCharacterAny(uint8_t ascii_character) {
    return IsCasedCharacterAscii(ascii_character);  // at least 1 cased character
  }
};

// splitting

template <typename Type, typename ListType, typename Options, typename Derived>
struct SplitBaseTransform {
  using string_offset_type = typename Type::offset_type;
  using list_offset_type = typename ListType::offset_type;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using ArrayListType = typename TypeTraits<ListType>::ArrayType;
  using ListScalarType = typename TypeTraits<ListType>::ScalarType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using ListOffsetsBuilderType = TypedBufferBuilder<list_offset_type>;
  using State = OptionsWrapper<Options>;

  std::vector<util::string_view> parts;
  Options options;

  explicit SplitBaseTransform(Options options) : options(options) {}

  Status Split(const util::string_view& s, BuilderType* builder) {
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(s.data());
    const uint8_t* end = begin + s.length();

    int64_t max_splits = options.max_splits;
    // if there is no max splits, reversing does not make sense (and is probably less
    // efficient), but is useful for testing
    if (options.reverse) {
      // note that i points 1 further than the 'current'
      const uint8_t* i = end;
      // we will record the parts in reverse order
      parts.clear();
      if (max_splits > -1) {
        parts.reserve(max_splits + 1);
      }
      while (max_splits != 0) {
        const uint8_t *separator_begin, *separator_end;
        // find with whatever algo the part we will 'cut out'
        if (static_cast<Derived&>(*this).FindReverse(begin, i, &separator_begin,
                                                     &separator_end, options)) {
          parts.emplace_back(reinterpret_cast<const char*>(separator_end),
                             i - separator_end);
          i = separator_begin;
          max_splits--;
        } else {
          // if we cannot find a separator, we're done
          break;
        }
      }
      parts.emplace_back(reinterpret_cast<const char*>(begin), i - begin);
      // now we do the copying
      for (auto it = parts.rbegin(); it != parts.rend(); ++it) {
        RETURN_NOT_OK(builder->Append(*it));
      }
    } else {
      const uint8_t* i = begin;
      while (max_splits != 0) {
        const uint8_t *separator_begin, *separator_end;
        // find with whatever algo the part we will 'cut out'
        if (static_cast<Derived&>(*this).Find(i, end, &separator_begin, &separator_end,
                                              options)) {
          // the part till the beginning of the 'cut'
          RETURN_NOT_OK(
              builder->Append(i, static_cast<string_offset_type>(separator_begin - i)));
          i = separator_end;
          max_splits--;
        } else {
          // if we cannot find a separator, we're done
          break;
        }
      }
      // trailing part
      RETURN_NOT_OK(builder->Append(i, static_cast<string_offset_type>(end - i)));
    }
    return Status::OK();
  }

  static Status CheckOptions(const Options& options) { return Status::OK(); }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    Options options = State::Get(ctx);
    Derived splitter(options);  // we make an instance to reuse the parts vectors
    splitter.Split(ctx, batch, out);
  }

  void Split(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    EnsureLookupTablesFilled();  // only needed for unicode
    KERNEL_RETURN_IF_ERROR(ctx, Derived::CheckOptions(options));

    if (batch[0].kind() == Datum::ARRAY) {
      const ArrayData& input = *batch[0].array();
      ArrayType input_boxed(batch[0].array());

      BuilderType builder(input.type, ctx->memory_pool());
      // a slight overestimate of the data needed
      KERNEL_RETURN_IF_ERROR(ctx, builder.ReserveData(input_boxed.total_values_length()));
      // the minimum amount of strings needed
      KERNEL_RETURN_IF_ERROR(ctx, builder.Resize(input.length));

      ArrayData* output_list = out->mutable_array();
      // list offsets were preallocated
      auto* list_offsets = output_list->GetMutableValues<list_offset_type>(1);
      DCHECK_NE(list_offsets, nullptr);
      // initial value
      *list_offsets++ = 0;
      KERNEL_RETURN_IF_ERROR(
          ctx,
          VisitArrayDataInline<Type>(
              input,
              [&](util::string_view s) {
                RETURN_NOT_OK(Split(s, &builder));
                if (ARROW_PREDICT_FALSE(builder.length() >
                                        std::numeric_limits<list_offset_type>::max())) {
                  return Status::CapacityError("List offset does not fit into 32 bit");
                }
                *list_offsets++ = static_cast<list_offset_type>(builder.length());
                return Status::OK();
              },
              [&]() {
                // null value is already taken from input
                *list_offsets++ = static_cast<list_offset_type>(builder.length());
                return Status::OK();
              }));
      // assign list child data
      std::shared_ptr<Array> string_array;
      KERNEL_RETURN_IF_ERROR(ctx, builder.Finish(&string_array));
      output_list->child_data.push_back(string_array->data());

    } else {
      const auto& input = checked_cast<const ScalarType&>(*batch[0].scalar());
      auto result = checked_pointer_cast<ListScalarType>(MakeNullScalar(out->type()));
      if (input.is_valid) {
        result->is_valid = true;
        BuilderType builder(input.type, ctx->memory_pool());
        util::string_view s(*input.value);
        KERNEL_RETURN_IF_ERROR(ctx, Split(s, &builder));
        KERNEL_RETURN_IF_ERROR(ctx, builder.Finish(&result->value));
      }
      out->value = result;
    }
  }
};

template <typename Type, typename ListType>
struct SplitPatternTransform : SplitBaseTransform<Type, ListType, SplitPatternOptions,
                                                  SplitPatternTransform<Type, ListType>> {
  using Base = SplitBaseTransform<Type, ListType, SplitPatternOptions,
                                  SplitPatternTransform<Type, ListType>>;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using string_offset_type = typename Type::offset_type;
  using Base::Base;

  static Status CheckOptions(const SplitPatternOptions& options) {
    if (options.pattern.length() == 0) {
      return Status::Invalid("Empty separator");
    }
    return Status::OK();
  }

  static bool Find(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitPatternOptions& options) {
    const uint8_t* pattern = reinterpret_cast<const uint8_t*>(options.pattern.c_str());
    const int64_t pattern_length = options.pattern.length();
    const uint8_t* i = begin;
    // this is O(n*m) complexity, we could use the Knuth-Morris-Pratt algorithm used in
    // the match kernel
    while ((i + pattern_length <= end)) {
      i = std::search(i, end, pattern, pattern + pattern_length);
      if (i != end) {
        *separator_begin = i;
        *separator_end = i + pattern_length;
        return true;
      }
    }
    return false;
  }

  static bool FindReverse(const uint8_t* begin, const uint8_t* end,
                          const uint8_t** separator_begin, const uint8_t** separator_end,
                          const SplitPatternOptions& options) {
    const uint8_t* pattern = reinterpret_cast<const uint8_t*>(options.pattern.c_str());
    const int64_t pattern_length = options.pattern.length();
    // this is O(n*m) complexity, we could use the Knuth-Morris-Pratt algorithm used in
    // the match kernel
    std::reverse_iterator<const uint8_t*> ri(end);
    std::reverse_iterator<const uint8_t*> rend(begin);
    std::reverse_iterator<const uint8_t*> pattern_rbegin(pattern + pattern_length);
    std::reverse_iterator<const uint8_t*> pattern_rend(pattern);
    while (begin <= ri.base() - pattern_length) {
      ri = std::search(ri, rend, pattern_rbegin, pattern_rend);
      if (ri != rend) {
        *separator_begin = ri.base() - pattern_length;
        *separator_end = ri.base();
        return true;
      }
    }
    return false;
  }
};

const FunctionDoc split_pattern_doc(
    "Split string according to separator",
    ("Split each string according to the exact `pattern` defined in\n"
     "SplitPatternOptions.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitPatternOptions."),
    {"strings"}, "SplitPatternOptions");

const FunctionDoc ascii_split_whitespace_doc(
    "Split string according to any ASCII whitespace",
    ("Split each string according any non-zero length sequence of ASCII\n"
     "whitespace characters.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitOptions."),
    {"strings"}, "SplitOptions");

const FunctionDoc utf8_split_whitespace_doc(
    "Split string according to any Unicode whitespace",
    ("Split each string according any non-zero length sequence of Unicode\n"
     "whitespace characters.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitOptions."),
    {"strings"}, "SplitOptions");

void AddSplitPattern(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("split_pattern", Arity::Unary(),
                                               &split_pattern_doc);
  using t32 = SplitPatternTransform<StringType, ListType>;
  using t64 = SplitPatternTransform<LargeStringType, ListType>;
  DCHECK_OK(func->AddKernel({utf8()}, {list(utf8())}, t32::Exec, t32::State::Init));
  DCHECK_OK(
      func->AddKernel({large_utf8()}, {list(large_utf8())}, t64::Exec, t64::State::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <typename Type, typename ListType>
struct SplitWhitespaceAsciiTransform
    : SplitBaseTransform<Type, ListType, SplitOptions,
                         SplitWhitespaceAsciiTransform<Type, ListType>> {
  using Base = SplitBaseTransform<Type, ListType, SplitOptions,
                                  SplitWhitespaceAsciiTransform<Type, ListType>>;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using string_offset_type = typename Type::offset_type;
  using Base::Base;
  static bool Find(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitOptions& options) {
    const uint8_t* i = begin;
    while ((i < end)) {
      if (IsSpaceCharacterAscii(*i)) {
        *separator_begin = i;
        do {
          i++;
        } while (IsSpaceCharacterAscii(*i) && i < end);
        *separator_end = i;
        return true;
      }
      i++;
    }
    return false;
  }
  static bool FindReverse(const uint8_t* begin, const uint8_t* end,
                          const uint8_t** separator_begin, const uint8_t** separator_end,
                          const SplitOptions& options) {
    const uint8_t* i = end - 1;
    while ((i >= begin)) {
      if (IsSpaceCharacterAscii(*i)) {
        *separator_end = i + 1;
        do {
          i--;
        } while (IsSpaceCharacterAscii(*i) && i >= begin);
        *separator_begin = i + 1;
        return true;
      }
      i--;
    }
    return false;
  }
};

void AddSplitWhitespaceAscii(FunctionRegistry* registry) {
  static const SplitOptions default_options{};
  auto func =
      std::make_shared<ScalarFunction>("ascii_split_whitespace", Arity::Unary(),
                                       &ascii_split_whitespace_doc, &default_options);
  using t32 = SplitWhitespaceAsciiTransform<StringType, ListType>;
  using t64 = SplitWhitespaceAsciiTransform<LargeStringType, ListType>;
  DCHECK_OK(func->AddKernel({utf8()}, {list(utf8())}, t32::Exec, t32::State::Init));
  DCHECK_OK(
      func->AddKernel({large_utf8()}, {list(large_utf8())}, t64::Exec, t64::State::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#ifdef ARROW_WITH_UTF8PROC
template <typename Type, typename ListType>
struct SplitWhitespaceUtf8Transform
    : SplitBaseTransform<Type, ListType, SplitOptions,
                         SplitWhitespaceUtf8Transform<Type, ListType>> {
  using Base = SplitBaseTransform<Type, ListType, SplitOptions,
                                  SplitWhitespaceUtf8Transform<Type, ListType>>;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using string_offset_type = typename Type::offset_type;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using Base::Base;
  static bool Find(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitOptions& options) {
    const uint8_t* i = begin;
    while ((i < end)) {
      uint32_t codepoint = 0;
      *separator_begin = i;
      if (ARROW_PREDICT_FALSE(!arrow::util::UTF8Decode(&i, &codepoint))) {
        return false;
      }
      if (IsSpaceCharacterUnicode(codepoint)) {
        do {
          *separator_end = i;
          if (ARROW_PREDICT_FALSE(!arrow::util::UTF8Decode(&i, &codepoint))) {
            return false;
          }
        } while (IsSpaceCharacterUnicode(codepoint) && i < end);
        return true;
      }
    }
    return false;
  }
  static bool FindReverse(const uint8_t* begin, const uint8_t* end,
                          const uint8_t** separator_begin, const uint8_t** separator_end,
                          const SplitOptions& options) {
    const uint8_t* i = end - 1;
    while ((i >= begin)) {
      uint32_t codepoint = 0;
      *separator_end = i + 1;
      if (ARROW_PREDICT_FALSE(!arrow::util::UTF8DecodeReverse(&i, &codepoint))) {
        return false;
      }
      if (IsSpaceCharacterUnicode(codepoint)) {
        do {
          *separator_begin = i + 1;
          if (ARROW_PREDICT_FALSE(!arrow::util::UTF8DecodeReverse(&i, &codepoint))) {
            return false;
          }
        } while (IsSpaceCharacterUnicode(codepoint) && i >= begin);
        return true;
      }
    }
    return false;
  }
};

void AddSplitWhitespaceUTF8(FunctionRegistry* registry) {
  static const SplitOptions default_options{};
  auto func =
      std::make_shared<ScalarFunction>("utf8_split_whitespace", Arity::Unary(),
                                       &utf8_split_whitespace_doc, &default_options);
  using t32 = SplitWhitespaceUtf8Transform<StringType, ListType>;
  using t64 = SplitWhitespaceUtf8Transform<LargeStringType, ListType>;
  DCHECK_OK(func->AddKernel({utf8()}, {list(utf8())}, t32::Exec, t32::State::Init));
  DCHECK_OK(
      func->AddKernel({large_utf8()}, {list(large_utf8())}, t64::Exec, t64::State::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}
#endif

void AddSplit(FunctionRegistry* registry) {
  AddSplitPattern(registry);
  AddSplitWhitespaceAscii(registry);
#ifdef ARROW_WITH_UTF8PROC
  AddSplitWhitespaceUTF8(registry);
#endif
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

const FunctionDoc strptime_doc(
    "Parse timestamps",
    ("For each string in `strings`, parse it as a timestamp.\n"
     "The timestamp unit and the expected string pattern must be given\n"
     "in StrptimeOptions.  Null inputs emit null.  If a non-null string\n"
     "fails parsing, an error is returned."),
    {"strings"}, "StrptimeOptions");

const FunctionDoc binary_length_doc(
    "Compute string lengths",
    ("For each string in `strings`, emit its length.  Null values emit null."),
    {"strings"});

void AddStrptime(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("strptime", Arity::Unary(), &strptime_doc);
  DCHECK_OK(func->AddKernel({utf8()}, OutputType(StrptimeResolve),
                            StrptimeExec<StringType>, StrptimeState::Init));
  DCHECK_OK(func->AddKernel({large_utf8()}, OutputType(StrptimeResolve),
                            StrptimeExec<LargeStringType>, StrptimeState::Init));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void AddBinaryLength(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("binary_length", Arity::Unary(),
                                               &binary_length_doc);
  ArrayKernelExec exec_offset_32 =
      applicator::ScalarUnaryNotNull<Int32Type, StringType, BinaryLength>::Exec;
  ArrayKernelExec exec_offset_64 =
      applicator::ScalarUnaryNotNull<Int64Type, LargeStringType, BinaryLength>::Exec;
  for (const auto& input_type : {binary(), utf8()}) {
    DCHECK_OK(func->AddKernel({input_type}, int32(), exec_offset_32));
  }
  for (const auto& input_type : {large_binary(), large_utf8()}) {
    DCHECK_OK(func->AddKernel({input_type}, int64(), exec_offset_64));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <template <typename> class ExecFunctor>
void MakeUnaryStringBatchKernel(
    std::string name, FunctionRegistry* registry, const FunctionDoc* doc,
    MemAllocation::type mem_allocation = MemAllocation::PREALLOCATE) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);
  {
    auto exec_32 = ExecFunctor<StringType>::Exec;
    ScalarKernel kernel{{utf8()}, utf8(), exec_32};
    kernel.mem_allocation = mem_allocation;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  {
    auto exec_64 = ExecFunctor<LargeStringType>::Exec;
    ScalarKernel kernel{{large_utf8()}, large_utf8(), exec_64};
    kernel.mem_allocation = mem_allocation;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#ifdef ARROW_WITH_UTF8PROC

template <template <typename> class Transformer>
void MakeUnaryStringUTF8TransformKernel(std::string name, FunctionRegistry* registry,
                                        const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);
  ArrayKernelExec exec_32 = Transformer<StringType>::Exec;
  ArrayKernelExec exec_64 = Transformer<LargeStringType>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, utf8(), exec_32));
  DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(), exec_64));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#endif

using StringPredicate = std::function<bool(KernelContext*, const uint8_t*, size_t)>;

template <typename Type>
void ApplyPredicate(KernelContext* ctx, const ExecBatch& batch, StringPredicate predicate,
                    Datum* out) {
  EnsureLookupTablesFilled();
  if (batch[0].kind() == Datum::ARRAY) {
    const ArrayData& input = *batch[0].array();
    ArrayIterator<Type> input_it(input);
    ArrayData* out_arr = out->mutable_array();
    ::arrow::internal::GenerateBitsUnrolled(
        out_arr->buffers[1]->mutable_data(), out_arr->offset, input.length,
        [&]() -> bool {
          util::string_view val = input_it();
          return predicate(ctx, reinterpret_cast<const uint8_t*>(val.data()), val.size());
        });
  } else {
    const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
    if (input.is_valid) {
      bool boolean_result =
          predicate(ctx, input.value->data(), static_cast<size_t>(input.value->size()));
      if (!ctx->status().ok()) {
        // UTF decoding can lead to issues
        return;
      }
      out->value = std::make_shared<BooleanScalar>(boolean_result);
    }
  }
}

template <typename Predicate>
void AddUnaryStringPredicate(std::string name, FunctionRegistry* registry,
                             const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);
  auto exec_32 = [](KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ApplyPredicate<StringType>(ctx, batch, Predicate::Call, out);
  };
  auto exec_64 = [](KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ApplyPredicate<LargeStringType>(ctx, batch, Predicate::Call, out);
  };
  DCHECK_OK(func->AddKernel({utf8()}, boolean(), std::move(exec_32)));
  DCHECK_OK(func->AddKernel({large_utf8()}, boolean(), std::move(exec_64)));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

FunctionDoc StringPredicateDoc(std::string summary, std::string description) {
  return FunctionDoc{std::move(summary), std::move(description), {"strings"}};
}

FunctionDoc StringClassifyDoc(std::string class_summary, std::string class_desc,
                              bool non_empty) {
  std::string summary, description;
  {
    std::stringstream ss;
    ss << "Classify strings as " << class_summary;
    summary = ss.str();
  }
  {
    std::stringstream ss;
    if (non_empty) {
      ss
          << ("For each string in `strings`, emit true iff the string is non-empty\n"
              "and consists only of ");
    } else {
      ss
          << ("For each string in `strings`, emit true iff the string consists only\n"
              "of ");
    }
    ss << class_desc << ".  Null strings emit null.";
    description = ss.str();
  }
  return StringPredicateDoc(std::move(summary), std::move(description));
}

const auto string_is_ascii_doc = StringClassifyDoc("ASCII", "ASCII characters", false);

const auto ascii_is_alnum_doc =
    StringClassifyDoc("ASCII alphanumeric", "alphanumeric ASCII characters", true);
const auto ascii_is_alpha_doc =
    StringClassifyDoc("ASCII alphabetic", "alphabetic ASCII characters", true);
const auto ascii_is_decimal_doc =
    StringClassifyDoc("ASCII decimal", "decimal ASCII characters", true);
const auto ascii_is_lower_doc =
    StringClassifyDoc("ASCII lowercase", "lowercase ASCII characters", true);
const auto ascii_is_printable_doc =
    StringClassifyDoc("ASCII printable", "printable ASCII characters", true);
const auto ascii_is_space_doc =
    StringClassifyDoc("ASCII whitespace", "whitespace ASCII characters", true);
const auto ascii_is_upper_doc =
    StringClassifyDoc("ASCII uppercase", "uppercase ASCII characters", true);

const auto ascii_is_title_doc = StringPredicateDoc(
    "Classify strings as ASCII titlecase",
    ("For each string in `strings`, emit true iff the string is title-cased,\n"
     "i.e. it has at least one cased character, each uppercase character\n"
     "follows a non-cased character, and each lowercase character follows\n"
     "an uppercase character.\n"));

const auto utf8_is_alnum_doc =
    StringClassifyDoc("alphanumeric", "alphanumeric Unicode characters", true);
const auto utf8_is_alpha_doc =
    StringClassifyDoc("alphabetic", "alphabetic Unicode characters", true);
const auto utf8_is_decimal_doc =
    StringClassifyDoc("decimal", "decimal Unicode characters", true);
const auto utf8_is_digit_doc = StringClassifyDoc("digits", "Unicode digits", true);
const auto utf8_is_lower_doc =
    StringClassifyDoc("lowercase", "lowercase Unicode characters", true);
const auto utf8_is_numeric_doc =
    StringClassifyDoc("numeric", "numeric Unicode characters", true);
const auto utf8_is_printable_doc =
    StringClassifyDoc("printable", "printable Unicode characters", true);
const auto utf8_is_space_doc =
    StringClassifyDoc("whitespace", "whitespace Unicode characters", true);
const auto utf8_is_upper_doc =
    StringClassifyDoc("uppercase", "uppercase Unicode characters", true);

const auto utf8_is_title_doc = StringPredicateDoc(
    "Classify strings as titlecase",
    ("For each string in `strings`, emit true iff the string is title-cased,\n"
     "i.e. it has at least one cased character, each uppercase character\n"
     "follows a non-cased character, and each lowercase character follows\n"
     "an uppercase character.\n"));

const FunctionDoc ascii_upper_doc(
    "Transform ASCII input to uppercase",
    ("For each string in `strings`, return an uppercase version.\n\n"
     "This function assumes the input is fully ASCII.  It it may contain\n"
     "non-ASCII characters, use \"utf8_upper\" instead."),
    {"strings"});

const FunctionDoc ascii_lower_doc(
    "Transform ASCII input to lowercase",
    ("For each string in `strings`, return a lowercase version.\n\n"
     "This function assumes the input is fully ASCII.  It it may contain\n"
     "non-ASCII characters, use \"utf8_lower\" instead."),
    {"strings"});

const FunctionDoc utf8_upper_doc(
    "Transform input to uppercase",
    ("For each string in `strings`, return an uppercase version."), {"strings"});

const FunctionDoc utf8_lower_doc(
    "Transform input to lowercase",
    ("For each string in `strings`, return a lowercase version."), {"strings"});

}  // namespace

void RegisterScalarStringAscii(FunctionRegistry* registry) {
  // ascii_upper and ascii_lower are able to reuse the original offsets buffer,
  // so don't preallocate them in the output.
  MakeUnaryStringBatchKernel<AsciiUpper>("ascii_upper", registry, &ascii_upper_doc,
                                         MemAllocation::NO_PREALLOCATE);
  MakeUnaryStringBatchKernel<AsciiLower>("ascii_lower", registry, &ascii_lower_doc,
                                         MemAllocation::NO_PREALLOCATE);

  AddUnaryStringPredicate<IsAscii>("string_is_ascii", registry, &string_is_ascii_doc);

  AddUnaryStringPredicate<IsAlphaNumericAscii>("ascii_is_alnum", registry,
                                               &ascii_is_alnum_doc);
  AddUnaryStringPredicate<IsAlphaAscii>("ascii_is_alpha", registry, &ascii_is_alpha_doc);
  AddUnaryStringPredicate<IsDecimalAscii>("ascii_is_decimal", registry,
                                          &ascii_is_decimal_doc);
  // no is_digit for ascii, since it is the same as is_decimal
  AddUnaryStringPredicate<IsLowerAscii>("ascii_is_lower", registry, &ascii_is_lower_doc);
  // no is_numeric for ascii, since it is the same as is_decimal
  AddUnaryStringPredicate<IsPrintableAscii>("ascii_is_printable", registry,
                                            &ascii_is_printable_doc);
  AddUnaryStringPredicate<IsSpaceAscii>("ascii_is_space", registry, &ascii_is_space_doc);
  AddUnaryStringPredicate<IsTitleAscii>("ascii_is_title", registry, &ascii_is_title_doc);
  AddUnaryStringPredicate<IsUpperAscii>("ascii_is_upper", registry, &ascii_is_upper_doc);

#ifdef ARROW_WITH_UTF8PROC
  MakeUnaryStringUTF8TransformKernel<UTF8Upper>("utf8_upper", registry, &utf8_upper_doc);
  MakeUnaryStringUTF8TransformKernel<UTF8Lower>("utf8_lower", registry, &utf8_lower_doc);

  AddUnaryStringPredicate<IsAlphaNumericUnicode>("utf8_is_alnum", registry,
                                                 &utf8_is_alnum_doc);
  AddUnaryStringPredicate<IsAlphaUnicode>("utf8_is_alpha", registry, &utf8_is_alpha_doc);
  AddUnaryStringPredicate<IsDecimalUnicode>("utf8_is_decimal", registry,
                                            &utf8_is_decimal_doc);
  AddUnaryStringPredicate<IsDigitUnicode>("utf8_is_digit", registry, &utf8_is_digit_doc);
  AddUnaryStringPredicate<IsLowerUnicode>("utf8_is_lower", registry, &utf8_is_lower_doc);
  AddUnaryStringPredicate<IsNumericUnicode>("utf8_is_numeric", registry,
                                            &utf8_is_numeric_doc);
  AddUnaryStringPredicate<IsPrintableUnicode>("utf8_is_printable", registry,
                                              &utf8_is_printable_doc);
  AddUnaryStringPredicate<IsSpaceUnicode>("utf8_is_space", registry, &utf8_is_space_doc);
  AddUnaryStringPredicate<IsTitleUnicode>("utf8_is_title", registry, &utf8_is_title_doc);
  AddUnaryStringPredicate<IsUpperUnicode>("utf8_is_upper", registry, &utf8_is_upper_doc);
#endif

  AddSplit(registry);
  AddBinaryLength(registry);
  AddMatchSubstring(registry);
  AddStrptime(registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
