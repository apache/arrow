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
#include <iterator>
#include <string>

#ifdef ARROW_WITH_UTF8PROC
#include <utf8proc.h>
#endif

#ifdef ARROW_WITH_RE2
#include <re2/re2.h>
#endif

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/buffer_builder.h"

#include "arrow/builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

namespace {

#ifdef ARROW_WITH_RE2
util::string_view ToStringView(re2::StringPiece piece) {
  return {piece.data(), piece.length()};
}

re2::StringPiece ToStringPiece(util::string_view view) {
  return {view.data(), view.length()};
}

Status RegexStatus(const RE2& regex) {
  if (!regex.ok()) {
    return Status::Invalid("Invalid regular expression: ", regex.error());
  }
  return Status::OK();
}
#endif

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

struct Utf8Length {
  template <typename OutValue, typename Arg0Value = util::string_view>
  static OutValue Call(KernelContext*, Arg0Value val) {
    auto str = reinterpret_cast<const uint8_t*>(val.data());
    auto strlen = val.size();

    OutValue length = 0;
    while (strlen > 0) {
      length += ((*str & 0xc0) != 0x80);
      ++str;
      --strlen;
    }
    return length;
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

#endif  // ARROW_WITH_UTF8PROC

/// Transform string -> string with a reasonable guess on the maximum number of codepoints
template <typename Type, typename Derived>
struct StringTransform {
  using offset_type = typename Type::offset_type;
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  static int64_t MaxCodeunits(offset_type input_ncodeunits) { return input_ncodeunits; }
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    Derived().Execute(ctx, batch, out);
  }
  void Execute(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch[0].kind() == Datum::ARRAY) {
      const ArrayData& input = *batch[0].array();
      ArrayType input_boxed(batch[0].array());
      ArrayData* output = out->mutable_array();

      offset_type input_ncodeunits = input_boxed.total_values_length();
      offset_type input_nstrings = static_cast<offset_type>(input.length);

      int64_t output_ncodeunits_max = Derived::MaxCodeunits(input_ncodeunits);
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
        if (ARROW_PREDICT_FALSE(!static_cast<Derived&>(*this).Transform(
                input_string, input_string_ncodeunits, output_str + output_ncodeunits,
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

        int64_t output_ncodeunits_max = Derived::MaxCodeunits(data_nbytes);
        if (output_ncodeunits_max > std::numeric_limits<offset_type>::max()) {
          ctx->SetStatus(Status::CapacityError(
              "Result might not fit in a 32bit utf8 array, convert to large_utf8"));
          return;
        }
        KERNEL_ASSIGN_OR_RAISE(auto value_buffer, ctx,
                               ctx->Allocate(output_ncodeunits_max));
        result->value = value_buffer;
        offset_type encoded_nbytes = 0;
        if (ARROW_PREDICT_FALSE(!static_cast<Derived&>(*this).Transform(
                input.value->data(), data_nbytes, value_buffer->mutable_data(),
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

#ifdef ARROW_WITH_UTF8PROC

// transforms per codepoint
template <typename Type, typename Derived>
struct StringTransformCodepoint : StringTransform<Type, Derived> {
  using Base = StringTransform<Type, Derived>;
  using offset_type = typename Base::offset_type;

  bool Transform(const uint8_t* input, offset_type input_string_ncodeunits,
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
  static int64_t MaxCodeunits(offset_type input_ncodeunits) {
    // Section 5.18 of the Unicode spec claim that the number of codepoints for case
    // mapping can grow by a factor of 3. This means grow by a factor of 3 in bytes
    // However, since we don't support all casings (SpecialCasing.txt) the growth
    // in bytes iss actually only at max 3/2 (as covered by the unittest).
    // Note that rounding down the 3/2 is ok, since only codepoints encoded by
    // two code units (even) can grow to 3 code units.
    return static_cast<int64_t>(input_ncodeunits) * 3 / 2;
  }
  void Execute(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    EnsureLookupTablesFilled();
    Base::Execute(ctx, batch, out);
  }
};

template <typename Type>
struct UTF8Upper : StringTransformCodepoint<Type, UTF8Upper<Type>> {
  inline static uint32_t TransformCodepoint(uint32_t codepoint) {
    return codepoint <= kMaxCodepointLookup ? lut_upper_codepoint[codepoint]
                                            : utf8proc_toupper(codepoint);
  }
};

template <typename Type>
struct UTF8Lower : StringTransformCodepoint<Type, UTF8Lower<Type>> {
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

using MatchSubstringState = OptionsWrapper<MatchSubstringOptions>;

template <typename Type, typename Matcher>
struct MatchSubstring {
  using offset_type = typename Type::offset_type;
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // TODO Cache matcher across invocations (for regex compilation)
    Matcher matcher(ctx, MatchSubstringState::Get(ctx));
    if (ctx->HasError()) return;
    StringBoolTransform<Type>(
        ctx, batch,
        [&matcher](const void* raw_offsets, const uint8_t* data, int64_t length,
                   int64_t output_offset, uint8_t* output) {
          const offset_type* offsets = reinterpret_cast<const offset_type*>(raw_offsets);
          FirstTimeBitmapWriter bitmap_writer(output, output_offset, length);
          for (int64_t i = 0; i < length; ++i) {
            const char* current_data = reinterpret_cast<const char*>(data + offsets[i]);
            int64_t current_length = offsets[i + 1] - offsets[i];
            if (matcher.Match(util::string_view(current_data, current_length))) {
              bitmap_writer.Set();
            }
            bitmap_writer.Next();
          }
          bitmap_writer.Finish();
        },
        out);
  }
};

// This is an implementation of the Knuth-Morris-Pratt algorithm
struct PlainSubstringMatcher {
  const MatchSubstringOptions& options_;
  std::vector<int64_t> prefix_table;

  PlainSubstringMatcher(KernelContext* ctx, const MatchSubstringOptions& options)
      : options_(options) {
    // Phase 1: Build the prefix table
    const auto pattern_length = options_.pattern.size();
    prefix_table.resize(pattern_length + 1, /*value=*/0);
    int64_t prefix_length = -1;
    prefix_table[0] = -1;
    for (size_t pos = 0; pos < pattern_length; ++pos) {
      // The prefix cannot be expanded, reset.
      while (prefix_length >= 0 &&
             options_.pattern[pos] != options_.pattern[prefix_length]) {
        prefix_length = prefix_table[prefix_length];
      }
      prefix_length++;
      prefix_table[pos + 1] = prefix_length;
    }
  }

  bool Match(util::string_view current) {
    // Phase 2: Find the prefix in the data
    const auto pattern_length = options_.pattern.size();
    int64_t pattern_pos = 0;
    for (const auto c : current) {
      while ((pattern_pos >= 0) && (options_.pattern[pattern_pos] != c)) {
        pattern_pos = prefix_table[pattern_pos];
      }
      pattern_pos++;
      if (static_cast<size_t>(pattern_pos) == pattern_length) {
        return true;
      }
    }
    return false;
  }
};

const FunctionDoc match_substring_doc(
    "Match strings against literal pattern",
    ("For each string in `strings`, emit true iff it contains a given pattern.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions");

#ifdef ARROW_WITH_RE2
struct RegexSubstringMatcher {
  const MatchSubstringOptions& options_;
  const RE2 regex_match_;

  RegexSubstringMatcher(KernelContext* ctx, const MatchSubstringOptions& options)
      : options_(options), regex_match_(options_.pattern, RE2::Quiet) {
    KERNEL_RETURN_IF_ERROR(ctx, RegexStatus(regex_match_));
  }

  bool Match(util::string_view current) {
    auto piece = re2::StringPiece(current.data(), current.length());
    return re2::RE2::PartialMatch(piece, regex_match_);
  }
};

const FunctionDoc match_substring_regex_doc(
    "Match strings against regex pattern",
    ("For each string in `strings`, emit true iff it matches a given pattern at any "
     "position.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions");
#endif

void AddMatchSubstring(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("match_substring", Arity::Unary(),
                                                 &match_substring_doc);
    auto exec_32 = MatchSubstring<StringType, PlainSubstringMatcher>::Exec;
    auto exec_64 = MatchSubstring<LargeStringType, PlainSubstringMatcher>::Exec;
    DCHECK_OK(func->AddKernel({utf8()}, boolean(), exec_32, MatchSubstringState::Init));
    DCHECK_OK(
        func->AddKernel({large_utf8()}, boolean(), exec_64, MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#ifdef ARROW_WITH_RE2
  {
    auto func = std::make_shared<ScalarFunction>("match_substring_regex", Arity::Unary(),
                                                 &match_substring_regex_doc);
    auto exec_32 = MatchSubstring<StringType, RegexSubstringMatcher>::Exec;
    auto exec_64 = MatchSubstring<LargeStringType, RegexSubstringMatcher>::Exec;
    DCHECK_OK(func->AddKernel({utf8()}, boolean(), exec_32, MatchSubstringState::Init));
    DCHECK_OK(
        func->AddKernel({large_utf8()}, boolean(), exec_64, MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#endif
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
// Replace substring (plain, regex)

template <typename Type, typename Replacer>
struct ReplaceSubString {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using offset_type = typename Type::offset_type;
  using ValueDataBuilder = TypedBufferBuilder<uint8_t>;
  using OffsetBuilder = TypedBufferBuilder<offset_type>;
  using State = OptionsWrapper<ReplaceSubstringOptions>;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // TODO Cache replacer across invocations (for regex compilation)
    Replacer replacer{ctx, State::Get(ctx)};
    if (!ctx->HasError()) {
      Replace(ctx, batch, &replacer, out);
    }
  }

  static void Replace(KernelContext* ctx, const ExecBatch& batch, Replacer* replacer,
                      Datum* out) {
    ValueDataBuilder value_data_builder(ctx->memory_pool());
    OffsetBuilder offset_builder(ctx->memory_pool());

    if (batch[0].kind() == Datum::ARRAY) {
      // We already know how many strings we have, so we can use Reserve/UnsafeAppend
      KERNEL_RETURN_IF_ERROR(ctx, offset_builder.Reserve(batch[0].array()->length));
      offset_builder.UnsafeAppend(0);  // offsets start at 0

      const ArrayData& input = *batch[0].array();
      KERNEL_RETURN_IF_ERROR(
          ctx, VisitArrayDataInline<Type>(
                   input,
                   [&](util::string_view s) {
                     RETURN_NOT_OK(replacer->ReplaceString(s, &value_data_builder));
                     offset_builder.UnsafeAppend(
                         static_cast<offset_type>(value_data_builder.length()));
                     return Status::OK();
                   },
                   [&]() {
                     // offset for null value
                     offset_builder.UnsafeAppend(
                         static_cast<offset_type>(value_data_builder.length()));
                     return Status::OK();
                   }));
      ArrayData* output = out->mutable_array();
      KERNEL_RETURN_IF_ERROR(ctx, value_data_builder.Finish(&output->buffers[2]));
      KERNEL_RETURN_IF_ERROR(ctx, offset_builder.Finish(&output->buffers[1]));
    } else {
      const auto& input = checked_cast<const ScalarType&>(*batch[0].scalar());
      auto result = std::make_shared<ScalarType>();
      if (input.is_valid) {
        util::string_view s = static_cast<util::string_view>(*input.value);
        KERNEL_RETURN_IF_ERROR(ctx, replacer->ReplaceString(s, &value_data_builder));
        KERNEL_RETURN_IF_ERROR(ctx, value_data_builder.Finish(&result->value));
        result->is_valid = true;
      }
      out->value = result;
    }
  }
};

struct PlainSubStringReplacer {
  const ReplaceSubstringOptions& options_;

  PlainSubStringReplacer(KernelContext* ctx, const ReplaceSubstringOptions& options)
      : options_(options) {}

  Status ReplaceString(util::string_view s, TypedBufferBuilder<uint8_t>* builder) {
    const char* i = s.begin();
    const char* end = s.end();
    int64_t max_replacements = options_.max_replacements;
    while ((i < end) && (max_replacements != 0)) {
      const char* pos =
          std::search(i, end, options_.pattern.begin(), options_.pattern.end());
      if (pos == end) {
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                      static_cast<int64_t>(end - i)));
        i = end;
      } else {
        // the string before the pattern
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                      static_cast<int64_t>(pos - i)));
        // the replacement
        RETURN_NOT_OK(
            builder->Append(reinterpret_cast<const uint8_t*>(options_.replacement.data()),
                            options_.replacement.length()));
        // skip pattern
        i = pos + options_.pattern.length();
        max_replacements--;
      }
    }
    // if we exited early due to max_replacements, add the trailing part
    RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                  static_cast<int64_t>(end - i)));
    return Status::OK();
  }
};

#ifdef ARROW_WITH_RE2
struct RegexSubStringReplacer {
  const ReplaceSubstringOptions& options_;
  const RE2 regex_find_;
  const RE2 regex_replacement_;

  // Using RE2::FindAndConsume we can only find the pattern if it is a group, therefore
  // we have 2 regexes, one with () around it, one without.
  RegexSubStringReplacer(KernelContext* ctx, const ReplaceSubstringOptions& options)
      : options_(options),
        regex_find_("(" + options_.pattern + ")", RE2::Quiet),
        regex_replacement_(options_.pattern, RE2::Quiet) {
    KERNEL_RETURN_IF_ERROR(ctx, RegexStatus(regex_find_));
    KERNEL_RETURN_IF_ERROR(ctx, RegexStatus(regex_replacement_));
    std::string replacement_error;
    if (!regex_replacement_.CheckRewriteString(options_.replacement,
                                               &replacement_error)) {
      ctx->SetStatus(
          Status::Invalid("Invalid replacement string: ", std::move(replacement_error)));
    }
  }

  Status ReplaceString(util::string_view s, TypedBufferBuilder<uint8_t>* builder) {
    re2::StringPiece replacement(options_.replacement);

    if (options_.max_replacements == -1) {
      std::string s_copy(s.to_string());
      re2::RE2::GlobalReplace(&s_copy, regex_replacement_, replacement);
      RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(s_copy.data()),
                                    s_copy.length()));
      return Status::OK();
    }

    // Since RE2 does not have the concept of max_replacements, we have to do some work
    // ourselves.
    // We might do this faster similar to RE2::GlobalReplace using Match and Rewrite
    const char* i = s.begin();
    const char* end = s.end();
    re2::StringPiece piece(s.data(), s.length());

    int64_t max_replacements = options_.max_replacements;
    while ((i < end) && (max_replacements != 0)) {
      std::string found;
      if (!re2::RE2::FindAndConsume(&piece, regex_find_, &found)) {
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                      static_cast<int64_t>(end - i)));
        i = end;
      } else {
        // wind back to the beginning of the match
        const char* pos = piece.begin() - found.length();
        // the string before the pattern
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                      static_cast<int64_t>(pos - i)));
        // replace the pattern in what we found
        if (!re2::RE2::Replace(&found, regex_replacement_, replacement)) {
          return Status::Invalid("Regex found, but replacement failed");
        }
        RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(found.data()),
                                      static_cast<int64_t>(found.length())));
        // skip pattern
        i = piece.begin();
        max_replacements--;
      }
    }
    // If we exited early due to max_replacements, add the trailing part
    RETURN_NOT_OK(builder->Append(reinterpret_cast<const uint8_t*>(i),
                                  static_cast<int64_t>(end - i)));
    return Status::OK();
  }
};
#endif

template <typename Type>
using ReplaceSubStringPlain = ReplaceSubString<Type, PlainSubStringReplacer>;

const FunctionDoc replace_substring_doc(
    "Replace non-overlapping substrings that match pattern by replacement",
    ("For each string in `strings`, replace non-overlapping substrings that match\n"
     "`pattern` by `replacement`. If `max_replacements != -1`, it determines the\n"
     "maximum amount of replacements made, counting from the left. Null values emit\n"
     "null."),
    {"strings"}, "ReplaceSubstringOptions");

#ifdef ARROW_WITH_RE2
template <typename Type>
using ReplaceSubStringRegex = ReplaceSubString<Type, RegexSubStringReplacer>;

const FunctionDoc replace_substring_regex_doc(
    "Replace non-overlapping substrings that match regex `pattern` by `replacement`",
    ("For each string in `strings`, replace non-overlapping substrings that match the\n"
     "regular expression `pattern` by `replacement` using the Google RE2 library.\n"
     "If `max_replacements != -1`, it determines the maximum amount of replacements\n"
     "made, counting from the left. Note that if the pattern contains groups,\n"
     "backreferencing macan be used. Null values emit null."),
    {"strings"}, "ReplaceSubstringOptions");
#endif

// ----------------------------------------------------------------------
// Extract with regex

#ifdef ARROW_WITH_RE2

// TODO cache this once per ExtractRegexOptions
struct ExtractRegexData {
  // Use unique_ptr<> because RE2 is non-movable
  std::unique_ptr<RE2> regex;
  std::vector<std::string> group_names;

  static Result<ExtractRegexData> Make(const ExtractRegexOptions& options) {
    ExtractRegexData data(options.pattern);
    RETURN_NOT_OK(RegexStatus(*data.regex));

    const int group_count = data.regex->NumberOfCapturingGroups();
    const auto& name_map = data.regex->CapturingGroupNames();
    data.group_names.reserve(group_count);

    for (int i = 0; i < group_count; i++) {
      auto item = name_map.find(i + 1);  // re2 starts counting from 1
      if (item == name_map.end()) {
        // XXX should we instead just create fields with an empty name?
        return Status::Invalid("Regular expression contains unnamed groups");
      }
      data.group_names.emplace_back(item->second);
    }
    return std::move(data);
  }

  Result<ValueDescr> ResolveOutputType(const std::vector<ValueDescr>& args) const {
    const auto& input_type = args[0].type;
    if (input_type == nullptr) {
      // No input type specified => propagate shape
      return args[0];
    }
    // Input type is either String or LargeString and is also the type of each
    // field in the output struct type.
    DCHECK(input_type->id() == Type::STRING || input_type->id() == Type::LARGE_STRING);
    FieldVector fields;
    fields.reserve(group_names.size());
    std::transform(group_names.begin(), group_names.end(), std::back_inserter(fields),
                   [&](const std::string& name) { return field(name, input_type); });
    return struct_(std::move(fields));
  }

 private:
  explicit ExtractRegexData(const std::string& pattern)
      : regex(new RE2(pattern, RE2::Quiet)) {}
};

Result<ValueDescr> ResolveExtractRegexOutput(KernelContext* ctx,
                                             const std::vector<ValueDescr>& args) {
  using State = OptionsWrapper<ExtractRegexOptions>;
  ExtractRegexOptions options = State::Get(ctx);
  ARROW_ASSIGN_OR_RAISE(auto data, ExtractRegexData::Make(options));
  return data.ResolveOutputType(args);
}

struct ExtractRegexBase {
  const ExtractRegexData& data;
  const int group_count;
  std::vector<re2::StringPiece> found_values;
  std::vector<re2::RE2::Arg> args;
  std::vector<const re2::RE2::Arg*> args_pointers;
  const re2::RE2::Arg** args_pointers_start;
  const re2::RE2::Arg* null_arg = nullptr;

  explicit ExtractRegexBase(const ExtractRegexData& data)
      : data(data),
        group_count(static_cast<int>(data.group_names.size())),
        found_values(group_count) {
    args.reserve(group_count);
    args_pointers.reserve(group_count);

    for (int i = 0; i < group_count; i++) {
      args.emplace_back(&found_values[i]);
      // Since we reserved capacity, we're guaranteed the pointer remains valid
      args_pointers.push_back(&args[i]);
    }
    // Avoid null pointer if there is no capture group
    args_pointers_start = (group_count > 0) ? args_pointers.data() : &null_arg;
  }

  bool Match(util::string_view s) {
    return re2::RE2::PartialMatchN(ToStringPiece(s), *data.regex, args_pointers_start,
                                   group_count);
  }
};

template <typename Type>
struct ExtractRegex : public ExtractRegexBase {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using State = OptionsWrapper<ExtractRegexOptions>;

  using ExtractRegexBase::ExtractRegexBase;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ExtractRegexOptions options = State::Get(ctx);
    KERNEL_ASSIGN_OR_RAISE(auto data, ctx, ExtractRegexData::Make(options));
    ExtractRegex{data}.Extract(ctx, batch, out);
  }

  void Extract(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    KERNEL_ASSIGN_OR_RAISE(auto descr, ctx,
                           data.ResolveOutputType(batch.GetDescriptors()));
    DCHECK_NE(descr.type, nullptr);
    const auto& type = descr.type;

    if (batch[0].kind() == Datum::ARRAY) {
      std::unique_ptr<ArrayBuilder> array_builder;
      KERNEL_RETURN_IF_ERROR(ctx, MakeBuilder(ctx->memory_pool(), type, &array_builder));
      StructBuilder* struct_builder = checked_cast<StructBuilder*>(array_builder.get());

      std::vector<BuilderType*> field_builders;
      field_builders.reserve(group_count);
      for (int i = 0; i < group_count; i++) {
        field_builders.push_back(
            checked_cast<BuilderType*>(struct_builder->field_builder(i)));
      }

      auto visit_null = [&]() {
        for (int i = 0; i < group_count; i++) {
          RETURN_NOT_OK(field_builders[i]->AppendEmptyValue());
        }
        return struct_builder->AppendNull();
      };
      auto visit_value = [&](util::string_view s) {
        if (Match(s)) {
          for (int i = 0; i < group_count; i++) {
            RETURN_NOT_OK(field_builders[i]->Append(ToStringView(found_values[i])));
          }
          return struct_builder->Append();
        } else {
          return visit_null();
        }
      };
      const ArrayData& input = *batch[0].array();
      KERNEL_RETURN_IF_ERROR(ctx,
                             VisitArrayDataInline<Type>(input, visit_value, visit_null));

      std::shared_ptr<Array> out_array;
      KERNEL_RETURN_IF_ERROR(ctx, struct_builder->Finish(&out_array));
      *out = std::move(out_array);
    } else {
      const auto& input = checked_cast<const ScalarType&>(*batch[0].scalar());
      auto result = std::make_shared<StructScalar>(type);
      if (input.is_valid && Match(util::string_view(*input.value))) {
        result->value.reserve(group_count);
        for (int i = 0; i < group_count; i++) {
          result->value.push_back(
              std::make_shared<ScalarType>(found_values[i].as_string()));
        }
        result->is_valid = true;
      } else {
        result->is_valid = false;
      }
      out->value = std::move(result);
    }
  }
};

const FunctionDoc extract_regex_doc(
    "Extract substrings captured by a regex pattern",
    ("For each string in `strings`, match the regular expression and, if\n"
     "successful, emit a struct with field names and values coming from the\n"
     "regular expression's named capture groups. If the input is null or the\n"
     "regular expression fails matching, a null output value is emitted.\n"
     "\n"
     "Regular expression matching is done using the Google RE2 library."),
    {"strings"}, "ExtractRegexOptions");

void AddExtractRegex(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("extract_regex", Arity::Unary(),
                                               &extract_regex_doc);
  using t32 = ExtractRegex<StringType>;
  using t64 = ExtractRegex<LargeStringType>;
  OutputType out_ty(ResolveExtractRegexOutput);
  ScalarKernel kernel;

  // Null values will be computed based on regex match or not
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  kernel.signature.reset(new KernelSignature({utf8()}, out_ty));
  kernel.exec = t32::Exec;
  kernel.init = t32::State::Init;
  DCHECK_OK(func->AddKernel(kernel));
  kernel.signature.reset(new KernelSignature({large_utf8()}, out_ty));
  kernel.exec = t64::Exec;
  kernel.init = t64::State::Init;
  DCHECK_OK(func->AddKernel(kernel));

  DCHECK_OK(registry->AddFunction(std::move(func)));
}
#endif  // ARROW_WITH_RE2

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
      ctx->SetStatus(Status::Invalid("Failed to parse string: '", val,
                                     "' as a scalar of type ",
                                     TimestampType(unit).ToString()));
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

#ifdef ARROW_WITH_UTF8PROC

template <typename Type, bool left, bool right, typename Derived>
struct UTF8TrimWhitespaceBase : StringTransform<Type, Derived> {
  using Base = StringTransform<Type, Derived>;
  using offset_type = typename Base::offset_type;
  bool Transform(const uint8_t* input, offset_type input_string_ncodeunits,
                 uint8_t* output, offset_type* output_written) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;

    auto predicate = [](uint32_t c) { return !IsSpaceCharacterUnicode(c); };
    if (left && !ARROW_PREDICT_TRUE(
                    arrow::util::UTF8FindIf(begin, end, predicate, &begin_trimmed))) {
      return false;
    }
    if (right && (begin_trimmed < end)) {
      if (!ARROW_PREDICT_TRUE(arrow::util::UTF8FindIfReverse(begin_trimmed, end,
                                                             predicate, &end_trimmed))) {
        return false;
      }
    }
    std::copy(begin_trimmed, end_trimmed, output);
    *output_written = static_cast<offset_type>(end_trimmed - begin_trimmed);
    return true;
  }
  void Execute(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    EnsureLookupTablesFilled();
    Base::Execute(ctx, batch, out);
  }
};

template <typename Type>
struct UTF8TrimWhitespace
    : UTF8TrimWhitespaceBase<Type, true, true, UTF8TrimWhitespace<Type>> {};

template <typename Type>
struct UTF8LTrimWhitespace
    : UTF8TrimWhitespaceBase<Type, true, false, UTF8LTrimWhitespace<Type>> {};

template <typename Type>
struct UTF8RTrimWhitespace
    : UTF8TrimWhitespaceBase<Type, false, true, UTF8RTrimWhitespace<Type>> {};

struct TrimStateUTF8 {
  TrimOptions options_;
  std::vector<bool> codepoints_;
  explicit TrimStateUTF8(KernelContext* ctx, TrimOptions options)
      : options_(std::move(options)) {
    if (!ARROW_PREDICT_TRUE(
            arrow::util::UTF8ForEach(options_.characters, [&](uint32_t c) {
              codepoints_.resize(
                  std::max(c + 1, static_cast<uint32_t>(codepoints_.size())));
              codepoints_.at(c) = true;
            }))) {
      ctx->SetStatus(Status::Invalid("Invalid UTF8 sequence in input"));
    }
  }
};

template <typename Type, bool left, bool right, typename Derived>
struct UTF8TrimBase : StringTransform<Type, Derived> {
  using Base = StringTransform<Type, Derived>;
  using offset_type = typename Base::offset_type;
  using State = KernelStateFromFunctionOptions<TrimStateUTF8, TrimOptions>;
  TrimStateUTF8 state_;

  explicit UTF8TrimBase(TrimStateUTF8 state) : state_(std::move(state)) {}

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    TrimStateUTF8 state = State::Get(ctx);
    Derived(state).Execute(ctx, batch, out);
  }

  void Execute(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    EnsureLookupTablesFilled();
    Base::Execute(ctx, batch, out);
  }

  bool Transform(const uint8_t* input, offset_type input_string_ncodeunits,
                 uint8_t* output, offset_type* output_written) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;

    auto predicate = [&](uint32_t c) {
      bool contains = state_.codepoints_[c];
      return !contains;
    };
    if (left && !ARROW_PREDICT_TRUE(
                    arrow::util::UTF8FindIf(begin, end, predicate, &begin_trimmed))) {
      return false;
    }
    if (right && (begin_trimmed < end)) {
      if (!ARROW_PREDICT_TRUE(arrow::util::UTF8FindIfReverse(begin_trimmed, end,
                                                             predicate, &end_trimmed))) {
        return false;
      }
    }
    std::copy(begin_trimmed, end_trimmed, output);
    *output_written = static_cast<offset_type>(end_trimmed - begin_trimmed);
    return true;
  }
};
template <typename Type>
struct UTF8Trim : UTF8TrimBase<Type, true, true, UTF8Trim<Type>> {
  using Base = UTF8TrimBase<Type, true, true, UTF8Trim<Type>>;
  using Base::Base;
};

template <typename Type>
struct UTF8LTrim : UTF8TrimBase<Type, true, false, UTF8LTrim<Type>> {
  using Base = UTF8TrimBase<Type, true, false, UTF8LTrim<Type>>;
  using Base::Base;
};

template <typename Type>
struct UTF8RTrim : UTF8TrimBase<Type, false, true, UTF8RTrim<Type>> {
  using Base = UTF8TrimBase<Type, false, true, UTF8RTrim<Type>>;
  using Base::Base;
};

#endif

template <typename Type, bool left, bool right, typename Derived>
struct AsciiTrimWhitespaceBase : StringTransform<Type, Derived> {
  using offset_type = typename Type::offset_type;
  bool Transform(const uint8_t* input, offset_type input_string_ncodeunits,
                 uint8_t* output, offset_type* output_written) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;

    auto predicate = [](unsigned char c) { return !IsSpaceCharacterAscii(c); };
    const uint8_t* begin_trimmed = left ? std::find_if(begin, end, predicate) : begin;
    if (right & (begin_trimmed < end)) {
      std::reverse_iterator<const uint8_t*> rbegin(end);
      std::reverse_iterator<const uint8_t*> rend(begin_trimmed);
      end_trimmed = std::find_if(rbegin, rend, predicate).base();
    }
    std::copy(begin_trimmed, end_trimmed, output);
    *output_written = static_cast<offset_type>(end_trimmed - begin_trimmed);
    return true;
  }
};

template <typename Type>
struct AsciiTrimWhitespace
    : AsciiTrimWhitespaceBase<Type, true, true, AsciiTrimWhitespace<Type>> {};

template <typename Type>
struct AsciiLTrimWhitespace
    : AsciiTrimWhitespaceBase<Type, true, false, AsciiLTrimWhitespace<Type>> {};

template <typename Type>
struct AsciiRTrimWhitespace
    : AsciiTrimWhitespaceBase<Type, false, true, AsciiRTrimWhitespace<Type>> {};

template <typename Type, bool left, bool right, typename Derived>
struct AsciiTrimBase : StringTransform<Type, Derived> {
  using Base = StringTransform<Type, Derived>;
  using offset_type = typename Base::offset_type;
  using State = OptionsWrapper<TrimOptions>;
  TrimOptions options_;
  std::vector<bool> characters_;

  explicit AsciiTrimBase(TrimOptions options)
      : options_(std::move(options)), characters_(256) {
    std::for_each(options_.characters.begin(), options_.characters.end(),
                  [&](char c) { characters_[static_cast<unsigned char>(c)] = true; });
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    TrimOptions options = State::Get(ctx);
    Derived(options).Execute(ctx, batch, out);
  }

  bool Transform(const uint8_t* input, offset_type input_string_ncodeunits,
                 uint8_t* output, offset_type* output_written) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed;

    auto predicate = [&](unsigned char c) {
      bool contains = characters_[c];
      return !contains;
    };

    begin_trimmed = left ? std::find_if(begin, end, predicate) : begin;
    if (right & (begin_trimmed < end)) {
      std::reverse_iterator<const uint8_t*> rbegin(end);
      std::reverse_iterator<const uint8_t*> rend(begin_trimmed);
      end_trimmed = std::find_if(rbegin, rend, predicate).base();
    }
    std::copy(begin_trimmed, end_trimmed, output);
    *output_written = static_cast<offset_type>(end_trimmed - begin_trimmed);
    return true;
  }
};

template <typename Type>
struct AsciiTrim : AsciiTrimBase<Type, true, true, AsciiTrim<Type>> {
  using Base = AsciiTrimBase<Type, true, true, AsciiTrim<Type>>;
  using Base::Base;
};

template <typename Type>
struct AsciiLTrim : AsciiTrimBase<Type, true, false, AsciiLTrim<Type>> {
  using Base = AsciiTrimBase<Type, true, false, AsciiLTrim<Type>>;
  using Base::Base;
};

template <typename Type>
struct AsciiRTrim : AsciiTrimBase<Type, false, true, AsciiRTrim<Type>> {
  using Base = AsciiTrimBase<Type, false, true, AsciiRTrim<Type>>;
  using Base::Base;
};

const FunctionDoc utf8_trim_whitespace_doc(
    "Trim leading and trailing whitespace characters",
    ("For each string in `strings`, emit a string with leading and trailing whitespace\n"
     "characters removed, where whitespace characters are defined by the Unicode\n"
     "standard.  Null values emit null."),
    {"strings"});

const FunctionDoc utf8_ltrim_whitespace_doc(
    "Trim leading whitespace characters",
    ("For each string in `strings`, emit a string with leading whitespace\n"
     "characters removed, where whitespace characters are defined by the Unicode\n"
     "standard.  Null values emit null."),
    {"strings"});

const FunctionDoc utf8_rtrim_whitespace_doc(
    "Trim trailing whitespace characters",
    ("For each string in `strings`, emit a string with trailing whitespace\n"
     "characters removed, where whitespace characters are defined by the Unicode\n"
     "standard.  Null values emit null."),
    {"strings"});

const FunctionDoc ascii_trim_whitespace_doc(
    "Trim leading and trailing ASCII whitespace characters",
    ("For each string in `strings`, emit a string with leading and trailing ASCII\n"
     "whitespace characters removed. Use `utf8_trim_whitespace` to trim Unicode\n"
     "whitespace characters. Null values emit null."),
    {"strings"});

const FunctionDoc ascii_ltrim_whitespace_doc(
    "Trim leading ASCII whitespace characters",
    ("For each string in `strings`, emit a string with leading ASCII whitespace\n"
     "characters removed.  Use `utf8_ltrim_whitespace` to trim leading Unicode\n"
     "whitespace characters. Null values emit null."),
    {"strings"});

const FunctionDoc ascii_rtrim_whitespace_doc(
    "Trim trailing ASCII whitespace characters",
    ("For each string in `strings`, emit a string with trailing ASCII whitespace\n"
     "characters removed. Use `utf8_rtrim_whitespace` to trim trailing Unicode\n"
     "whitespace characters. Null values emit null."),
    {"strings"});

const FunctionDoc utf8_trim_doc(
    "Trim leading and trailing characters present in the `characters` arguments",
    ("For each string in `strings`, emit a string with leading and trailing\n"
     "characters removed that are present in the `characters` argument.  Null values\n"
     "emit null."),
    {"strings"}, "TrimOptions");

const FunctionDoc utf8_ltrim_doc(
    "Trim leading characters present in the `characters` arguments",
    ("For each string in `strings`, emit a string with leading\n"
     "characters removed that are present in the `characters` argument.  Null values\n"
     "emit null."),
    {"strings"}, "TrimOptions");

const FunctionDoc utf8_rtrim_doc(
    "Trim trailing characters present in the `characters` arguments",
    ("For each string in `strings`, emit a string with leading "
     "characters removed that are present in the `characters` argument.  Null values\n"
     "emit null."),
    {"strings"}, "TrimOptions");

const FunctionDoc ascii_trim_doc(
    utf8_trim_doc.summary + "",
    utf8_trim_doc.description +
        ("\nBoth the input string as the `characters` argument are interepreted as\n"
         "ASCII characters, to trim non-ASCII characters, use `utf8_trim`."),
    {"strings"}, "TrimOptions");

const FunctionDoc ascii_ltrim_doc(
    utf8_ltrim_doc.summary + "",
    utf8_ltrim_doc.description +
        ("\nBoth the input string as the `characters` argument are interepreted as\n"
         "ASCII characters, to trim non-ASCII characters, use `utf8_trim`."),
    {"strings"}, "TrimOptions");

const FunctionDoc ascii_rtrim_doc(
    utf8_rtrim_doc.summary + "",
    utf8_rtrim_doc.description +
        ("\nBoth the input string as the `characters` argument are interepreted as\n"
         "ASCII characters, to trim non-ASCII characters, use `utf8_trim`."),
    {"strings"}, "TrimOptions");

const FunctionDoc strptime_doc(
    "Parse timestamps",
    ("For each string in `strings`, parse it as a timestamp.\n"
     "The timestamp unit and the expected string pattern must be given\n"
     "in StrptimeOptions.  Null inputs emit null.  If a non-null string\n"
     "fails parsing, an error is returned."),
    {"strings"}, "StrptimeOptions");

const FunctionDoc binary_length_doc(
    "Compute string lengths",
    ("For each string in `strings`, emit the number of bytes.  Null values emit null."),
    {"strings"});

const FunctionDoc utf8_length_doc("Compute UTF8 string lengths",
                                  ("For each string in `strings`, emit the number of "
                                   "UTF8 characters.  Null values emit null."),
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

void AddUtf8Length(FunctionRegistry* registry) {
  auto func =
      std::make_shared<ScalarFunction>("utf8_length", Arity::Unary(), &utf8_length_doc);

  ArrayKernelExec exec_offset_32 =
      applicator::ScalarUnaryNotNull<Int32Type, StringType, Utf8Length>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, int32(), std::move(exec_offset_32)));

  ArrayKernelExec exec_offset_64 =
      applicator::ScalarUnaryNotNull<Int64Type, LargeStringType, Utf8Length>::Exec;
  DCHECK_OK(func->AddKernel({large_utf8()}, int64(), std::move(exec_offset_64)));

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

template <template <typename> class ExecFunctor>
void MakeUnaryStringBatchKernelWithState(
    std::string name, FunctionRegistry* registry, const FunctionDoc* doc,
    MemAllocation::type mem_allocation = MemAllocation::PREALLOCATE) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);
  {
    using t32 = ExecFunctor<StringType>;
    ScalarKernel kernel{{utf8()}, utf8(), t32::Exec, t32::State::Init};
    kernel.mem_allocation = mem_allocation;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  {
    using t64 = ExecFunctor<LargeStringType>;
    ScalarKernel kernel{{large_utf8()}, large_utf8(), t64::Exec, t64::State::Init};
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
  MakeUnaryStringBatchKernel<AsciiTrimWhitespace>("ascii_trim_whitespace", registry,
                                                  &ascii_trim_whitespace_doc);
  MakeUnaryStringBatchKernel<AsciiLTrimWhitespace>("ascii_ltrim_whitespace", registry,
                                                   &ascii_ltrim_whitespace_doc);
  MakeUnaryStringBatchKernel<AsciiRTrimWhitespace>("ascii_rtrim_whitespace", registry,
                                                   &ascii_rtrim_whitespace_doc);
  MakeUnaryStringBatchKernelWithState<AsciiTrim>("ascii_trim", registry,
                                                 &ascii_lower_doc);
  MakeUnaryStringBatchKernelWithState<AsciiLTrim>("ascii_ltrim", registry,
                                                  &ascii_lower_doc);
  MakeUnaryStringBatchKernelWithState<AsciiRTrim>("ascii_rtrim", registry,
                                                  &ascii_lower_doc);

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
  MakeUnaryStringBatchKernel<UTF8TrimWhitespace>("utf8_trim_whitespace", registry,
                                                 &utf8_trim_whitespace_doc);
  MakeUnaryStringBatchKernel<UTF8LTrimWhitespace>("utf8_ltrim_whitespace", registry,
                                                  &utf8_ltrim_whitespace_doc);
  MakeUnaryStringBatchKernel<UTF8RTrimWhitespace>("utf8_rtrim_whitespace", registry,
                                                  &utf8_rtrim_whitespace_doc);
  MakeUnaryStringBatchKernelWithState<UTF8Trim>("utf8_trim", registry, &utf8_trim_doc);
  MakeUnaryStringBatchKernelWithState<UTF8LTrim>("utf8_ltrim", registry, &utf8_ltrim_doc);
  MakeUnaryStringBatchKernelWithState<UTF8RTrim>("utf8_rtrim", registry, &utf8_rtrim_doc);

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
  AddUtf8Length(registry);
  AddMatchSubstring(registry);
  MakeUnaryStringBatchKernelWithState<ReplaceSubStringPlain>(
      "replace_substring", registry, &replace_substring_doc,
      MemAllocation::NO_PREALLOCATE);
#ifdef ARROW_WITH_RE2
  MakeUnaryStringBatchKernelWithState<ReplaceSubStringRegex>(
      "replace_substring_regex", registry, &replace_substring_regex_doc,
      MemAllocation::NO_PREALLOCATE);
  AddExtractRegex(registry);
#endif
  AddStrptime(registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
