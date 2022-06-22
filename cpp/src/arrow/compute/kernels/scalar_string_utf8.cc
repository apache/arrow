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
#include <mutex>
#include <string>

#ifdef ARROW_WITH_UTF8PROC
#include <utf8proc.h>
#endif

#include "arrow/compute/kernels/scalar_string_internal.h"
#include "arrow/util/utf8.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// String transformation base classes

#ifdef ARROW_WITH_UTF8PROC

template <template <typename> class Transformer>
void MakeUnaryStringUTF8TransformKernel(std::string name, FunctionRegistry* registry,
                                        FunctionDoc doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), std::move(doc));
  for (const auto& ty : StringTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<Transformer>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, std::move(exec)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#endif  // ARROW_WITH_UTF8PROC

// ----------------------------------------------------------------------
// Predicates and classification

#ifdef ARROW_WITH_UTF8PROC

// Direct lookup tables for unicode properties
constexpr uint32_t kMaxCodepointLookup =
    0xffff;  // up to this codepoint is in a lookup table
std::vector<uint32_t> lut_upper_codepoint;
std::vector<uint32_t> lut_lower_codepoint;
std::vector<uint32_t> lut_swapcase_codepoint;
std::vector<utf8proc_category_t> lut_category;
std::once_flag flag_case_luts;

// IsAlpha/Digit etc

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

template <typename Derived, bool allow_empty = false>
struct CharacterPredicateUnicode {
  static bool Call(KernelContext*, const uint8_t* input, size_t input_string_ncodeunits,
                   Status* st) {
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
      *st = Status::Invalid("Invalid UTF8 sequence in input");
      return false;
    }
    return all & any;
  }

  static inline bool PredicateCharacterAny(uint32_t) {
    return true;  // default condition make sure there is at least 1 character
  }
};

struct IsAlphaNumericUnicode : CharacterPredicateUnicode<IsAlphaNumericUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsAlphaNumericCharacterUnicode(codepoint);
  }
};

struct IsAlphaUnicode : CharacterPredicateUnicode<IsAlphaUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsAlphaCharacterUnicode(codepoint);
  }
};

struct IsDecimalUnicode : CharacterPredicateUnicode<IsDecimalUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsDecimalCharacterUnicode(codepoint);
  }
};

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

struct IsLowerUnicode : CharacterPredicateUnicode<IsLowerUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterUnicode(codepoint) || IsLowerCaseCharacterUnicode(codepoint);
  }
  static inline bool PredicateCharacterAny(uint32_t codepoint) {
    return IsCasedCharacterUnicode(codepoint);  // at least 1 cased character
  }
};

struct IsPrintableUnicode
    : CharacterPredicateUnicode<IsPrintableUnicode, /*allow_empty=*/true> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return codepoint == ' ' || IsPrintableCharacterUnicode(codepoint);
  }
};

struct IsSpaceUnicode : CharacterPredicateUnicode<IsSpaceUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    return IsSpaceCharacterUnicode(codepoint);
  }
};

struct IsTitleUnicode {
  static bool Call(KernelContext*, const uint8_t* input, size_t input_string_ncodeunits,
                   Status* st) {
    // rules:
    //   1. lower case follows cased
    //   2. upper case follows uncased
    //   3. at least 1 cased character (which logically should be upper/title)
    bool rules_1_and_2;
    bool previous_cased = false;  // in LL, LU or LT
    bool rule_3 = false;
    bool status =
        arrow::util::UTF8AllOf(input, input + input_string_ncodeunits, &rules_1_and_2,
                               [&previous_cased, &rule_3](uint32_t codepoint) {
                                 if (IsLowerCaseCharacterUnicode(codepoint)) {
                                   if (!previous_cased) return false;  // rule 1 broken
                                   // next should be more lower case or uncased
                                   previous_cased = true;
                                 } else if (IsCasedCharacterUnicode(codepoint)) {
                                   if (previous_cased) return false;  // rule 2 broken
                                   // next should be a lower case or uncased
                                   previous_cased = true;
                                   rule_3 = true;  // rule 3 obeyed
                                 } else {
                                   // an uncased char, like _ or 1
                                   // next should be upper case or more uncased
                                   previous_cased = false;
                                 }
                                 return true;
                               });
    if (!ARROW_PREDICT_TRUE(status)) {
      *st = Status::Invalid("Invalid UTF8 sequence in input");
      return false;
    }
    return rules_1_and_2 & rule_3;
  }
};

struct IsUpperUnicode : CharacterPredicateUnicode<IsUpperUnicode> {
  static inline bool PredicateCharacterAll(uint32_t codepoint) {
    // Only for cased character it needs to be lower case
    return !IsCasedCharacterUnicode(codepoint) || IsUpperCaseCharacterUnicode(codepoint);
  }
  static inline bool PredicateCharacterAny(uint32_t codepoint) {
    return IsCasedCharacterUnicode(codepoint);  // at least 1 cased character
  }
};

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
     "follows an uncased character, and each lowercase character follows\n"
     "an uppercase character."));

void AddUtf8StringPredicates(FunctionRegistry* registry) {
  AddUnaryStringPredicate<IsAlphaNumericUnicode>("utf8_is_alnum", registry,
                                                 utf8_is_alnum_doc);
  AddUnaryStringPredicate<IsAlphaUnicode>("utf8_is_alpha", registry, utf8_is_alpha_doc);
  AddUnaryStringPredicate<IsDecimalUnicode>("utf8_is_decimal", registry,
                                            utf8_is_decimal_doc);
  AddUnaryStringPredicate<IsDigitUnicode>("utf8_is_digit", registry, utf8_is_digit_doc);
  AddUnaryStringPredicate<IsNumericUnicode>("utf8_is_numeric", registry,
                                            utf8_is_numeric_doc);
  AddUnaryStringPredicate<IsLowerUnicode>("utf8_is_lower", registry, utf8_is_lower_doc);
  AddUnaryStringPredicate<IsPrintableUnicode>("utf8_is_printable", registry,
                                              utf8_is_printable_doc);
  AddUnaryStringPredicate<IsSpaceUnicode>("utf8_is_space", registry, utf8_is_space_doc);
  AddUnaryStringPredicate<IsTitleUnicode>("utf8_is_title", registry, utf8_is_title_doc);
  AddUnaryStringPredicate<IsUpperUnicode>("utf8_is_upper", registry, utf8_is_upper_doc);
}

#endif  // ARROW_WITH_UTF8PROC

// ----------------------------------------------------------------------
// Case conversion

#ifdef ARROW_WITH_UTF8PROC

struct FunctionalCaseMappingTransform : public StringTransformBase {
  Status PreExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) override {
    EnsureUtf8LookupTablesFilled();
    return Status::OK();
  }

  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    // Section 5.18 of the Unicode spec claims that the number of codepoints for case
    // mapping can grow by a factor of 3. This means grow by a factor of 3 in bytes
    // However, since we don't support all casings (SpecialCasing.txt) the growth
    // in bytes is actually only at max 3/2 (as covered by the unittest).
    // Note that rounding down the 3/2 is ok, since only codepoints encoded by
    // two code units (even) can grow to 3 code units.
    return input_ncodeunits * 3 / 2;
  }
};

template <typename CodepointTransform>
struct StringTransformCodepoint : public FunctionalCaseMappingTransform {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    uint8_t* output_start = output;
    if (ARROW_PREDICT_FALSE(
            !arrow::util::UTF8Transform(input, input + input_string_ncodeunits, &output,
                                        CodepointTransform::TransformCodepoint))) {
      return kStringTransformError;
    }
    return output - output_start;
  }
};

struct UTF8UpperTransform : public FunctionalCaseMappingTransform {
  static uint32_t TransformCodepoint(uint32_t codepoint) {
    return codepoint <= kMaxCodepointLookup ? lut_upper_codepoint[codepoint]
                                            : utf8proc_toupper(codepoint);
  }
};

template <typename Type>
using UTF8Upper = StringTransformExec<Type, StringTransformCodepoint<UTF8UpperTransform>>;

struct UTF8LowerTransform : public FunctionalCaseMappingTransform {
  static uint32_t TransformCodepoint(uint32_t codepoint) {
    return codepoint <= kMaxCodepointLookup ? lut_lower_codepoint[codepoint]
                                            : utf8proc_tolower(codepoint);
  }
};

template <typename Type>
using UTF8Lower = StringTransformExec<Type, StringTransformCodepoint<UTF8LowerTransform>>;

struct UTF8SwapCaseTransform : public FunctionalCaseMappingTransform {
  static uint32_t TransformCodepoint(uint32_t codepoint) {
    if (codepoint <= kMaxCodepointLookup) {
      return lut_swapcase_codepoint[codepoint];
    } else {
      if (IsLowerCaseCharacterUnicode(codepoint)) {
        return utf8proc_toupper(codepoint);
      } else if (IsUpperCaseCharacterUnicode(codepoint)) {
        return utf8proc_tolower(codepoint);
      }
    }

    return codepoint;
  }
};

template <typename Type>
using UTF8SwapCase =
    StringTransformExec<Type, StringTransformCodepoint<UTF8SwapCaseTransform>>;

struct Utf8CapitalizeTransform : public FunctionalCaseMappingTransform {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    uint8_t* output_start = output;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* next = input;
    if (input_string_ncodeunits > 0) {
      if (ARROW_PREDICT_FALSE(!util::UTF8AdvanceCodepoints(input, end, &next, 1))) {
        return kStringTransformError;
      }
      if (ARROW_PREDICT_FALSE(!util::UTF8Transform(
              input, next, &output, UTF8UpperTransform::TransformCodepoint))) {
        return kStringTransformError;
      }
      if (ARROW_PREDICT_FALSE(!util::UTF8Transform(
              next, end, &output, UTF8LowerTransform::TransformCodepoint))) {
        return kStringTransformError;
      }
    }
    return output - output_start;
  }
};

template <typename Type>
using Utf8Capitalize = StringTransformExec<Type, Utf8CapitalizeTransform>;

struct Utf8TitleTransform : public FunctionalCaseMappingTransform {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    uint8_t* output_start = output;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* next = input;
    bool is_next_upper = true;
    while ((input = next) < end) {
      uint32_t codepoint;
      if (ARROW_PREDICT_FALSE(!util::UTF8Decode(&next, &codepoint))) {
        return kStringTransformError;
      }
      if (IsCasedCharacterUnicode(codepoint)) {
        // Lower/uppercase current codepoint and
        // prepare to lowercase next consecutive cased codepoints
        output = is_next_upper
                     ? util::UTF8Encode(output,
                                        UTF8UpperTransform::TransformCodepoint(codepoint))
                     : util::UTF8Encode(
                           output, UTF8LowerTransform::TransformCodepoint(codepoint));
        is_next_upper = false;
      } else {
        // Copy current uncased codepoint and
        // prepare to uppercase next cased codepoint
        std::memcpy(output, input, next - input);
        output += next - input;
        is_next_upper = true;
      }
    }
    return output - output_start;
  }
};

template <typename Type>
using Utf8Title = StringTransformExec<Type, Utf8TitleTransform>;

const FunctionDoc utf8_upper_doc(
    "Transform input to uppercase",
    ("For each string in `strings`, return an uppercase version."), {"strings"});

const FunctionDoc utf8_lower_doc(
    "Transform input to lowercase",
    ("For each string in `strings`, return a lowercase version."), {"strings"});

const FunctionDoc utf8_swapcase_doc(
    "Transform input lowercase characters to uppercase and uppercase characters to "
    "lowercase",
    ("For each string in `strings`, return an opposite case version."), {"strings"});

const FunctionDoc utf8_capitalize_doc(
    "Capitalize the first character of input",
    ("For each string in `strings`, return a capitalized version,\n"
     "with the first character uppercased and the others lowercased."),
    {"strings"});

const FunctionDoc utf8_title_doc(
    "Titlecase each word of input",
    ("For each string in `strings`, return a titlecased version.\n"
     "Each word in the output will start with an uppercase character and its\n"
     "remaining characters will be lowercase."),
    {"strings"});

void AddUtf8StringCaseConversion(FunctionRegistry* registry) {
  MakeUnaryStringUTF8TransformKernel<UTF8Upper>("utf8_upper", registry, utf8_upper_doc);
  MakeUnaryStringUTF8TransformKernel<UTF8Lower>("utf8_lower", registry, utf8_lower_doc);
  MakeUnaryStringUTF8TransformKernel<UTF8SwapCase>("utf8_swapcase", registry,
                                                   utf8_swapcase_doc);
  MakeUnaryStringBatchKernel<Utf8Capitalize>("utf8_capitalize", registry,
                                             utf8_capitalize_doc);
  MakeUnaryStringBatchKernel<Utf8Title>("utf8_title", registry, utf8_title_doc);
}

#endif  // ARROW_WITH_UTF8PROC

// ----------------------------------------------------------------------
// Code point normalization

#ifdef ARROW_WITH_UTF8PROC

struct Utf8NormalizeBase {
  // Pre-size scratch space
  explicit Utf8NormalizeBase(const Utf8NormalizeOptions& options)
      : decompose_options_(MakeDecomposeOptions(options.form)), codepoints_(32) {}

  // Try to decompose the given UTF8 string into the codepoints space,
  // returning the number of codepoints output.
  Result<int64_t> DecomposeIntoScratch(util::string_view v) {
    auto decompose = [&]() {
      return utf8proc_decompose(reinterpret_cast<const utf8proc_uint8_t*>(v.data()),
                                v.size(),
                                reinterpret_cast<utf8proc_int32_t*>(codepoints_.data()),
                                codepoints_.capacity(), decompose_options_);
    };
    auto res = decompose();
    if (res > static_cast<int64_t>(codepoints_.capacity())) {
      // Codepoints buffer not large enough, reallocate and try again
      codepoints_.assign(res, 0);
      res = decompose();
      DCHECK_EQ(res, static_cast<int64_t>(codepoints_.capacity()));
    }
    if (res < 0) {
      return Status::Invalid("Cannot normalize utf8 string: ", utf8proc_errmsg(res));
    }
    return res;
  }

  Result<int64_t> Decompose(util::string_view v, BufferBuilder* data_builder) {
    if (::arrow::util::ValidateAscii(v)) {
      // Fast path: normalization is a no-op
      RETURN_NOT_OK(data_builder->Append(v.data(), v.size()));
      return v.size();
    }
    // NOTE: we may be able to find more shortcuts using a precomputed table?
    // Out of 1114112 valid unicode codepoints, 1097203 don't change when
    // any normalization is applied.  Precomputing a table of such
    // "no-op" codepoints would help expand the fast path.

    ARROW_ASSIGN_OR_RAISE(const auto n_codepoints, DecomposeIntoScratch(v));
    // Encode normalized codepoints directly into the output
    int64_t n_bytes = 0;
    for (int64_t i = 0; i < n_codepoints; ++i) {
      n_bytes += ::arrow::util::UTF8EncodedLength(codepoints_[i]);
    }
    RETURN_NOT_OK(data_builder->Reserve(n_bytes));
    uint8_t* out = data_builder->mutable_data() + data_builder->length();
    for (int64_t i = 0; i < n_codepoints; ++i) {
      out = ::arrow::util::UTF8Encode(out, codepoints_[i]);
    }
    DCHECK_EQ(out - data_builder->mutable_data(), data_builder->length() + n_bytes);
    data_builder->UnsafeAdvance(n_bytes);
    return n_bytes;
  }

 protected:
  static utf8proc_option_t MakeDecomposeOptions(Utf8NormalizeOptions::Form form) {
    switch (form) {
      case Utf8NormalizeOptions::Form::NFKC:
        return static_cast<utf8proc_option_t>(UTF8PROC_STABLE | UTF8PROC_COMPOSE |
                                              UTF8PROC_COMPAT);
      case Utf8NormalizeOptions::Form::NFD:
        return static_cast<utf8proc_option_t>(UTF8PROC_STABLE | UTF8PROC_DECOMPOSE);
      case Utf8NormalizeOptions::Form::NFKD:
        return static_cast<utf8proc_option_t>(UTF8PROC_STABLE | UTF8PROC_DECOMPOSE |
                                              UTF8PROC_COMPAT);
      case Utf8NormalizeOptions::Form::NFC:
      default:
        return static_cast<utf8proc_option_t>(UTF8PROC_STABLE | UTF8PROC_COMPOSE);
    }
  }

  const utf8proc_option_t decompose_options_;
  // UTF32 scratch space for decomposition
  std::vector<uint32_t> codepoints_;
};

template <typename Type>
struct Utf8NormalizeExec : public Utf8NormalizeBase {
  using State = OptionsWrapper<Utf8NormalizeOptions>;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using offset_type = typename Type::offset_type;
  using OffsetBuilder = TypedBufferBuilder<offset_type>;

  using Utf8NormalizeBase::Utf8NormalizeBase;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = State::Get(ctx);
    Utf8NormalizeExec exec{options};
    if (batch[0].is_array()) {
      return exec.ExecArray(ctx, batch[0].array, out);
    } else {
      DCHECK(batch[0].is_scalar());
      return exec.ExecScalar(ctx, *batch[0].scalar, out);
    }
  }

  Status ExecArray(KernelContext* ctx, const ArraySpan& array, ExecResult* out) {
    BufferBuilder data_builder(ctx->memory_pool());

    const offset_type* in_offsets = array.GetValues<offset_type>(1);
    if (array.length > 0) {
      RETURN_NOT_OK(data_builder.Reserve(in_offsets[array.length] - in_offsets[0]));
    }

    // Output offsets are preallocated
    ArrayData* output = out->array_data().get();
    offset_type* out_offsets = output->GetMutableValues<offset_type>(1);

    int64_t offset = 0;
    *out_offsets++ = static_cast<offset_type>(offset);

    RETURN_NOT_OK(VisitArraySpanInline<Type>(
        array,
        [&](util::string_view v) {
          ARROW_ASSIGN_OR_RAISE(auto n_bytes, Decompose(v, &data_builder));
          offset += n_bytes;
          *out_offsets++ = static_cast<offset_type>(offset);
          return Status::OK();
        },
        [&]() {
          *out_offsets++ = static_cast<offset_type>(offset);
          return Status::OK();
        }));

    return data_builder.Finish(&output->buffers[2]);
  }

  Status ExecScalar(KernelContext* ctx, const Scalar& scalar, ExecResult* out) {
    if (scalar.is_valid) {
      const auto& string_scalar = checked_cast<const ScalarType&>(scalar);
      auto* out_scalar = checked_cast<ScalarType*>(out->scalar().get());

      BufferBuilder data_builder(ctx->memory_pool());
      RETURN_NOT_OK(Decompose(string_scalar.view(), &data_builder));
      RETURN_NOT_OK(data_builder.Finish(&out_scalar->value));
      out_scalar->is_valid = true;
    }
    return Status::OK();
  }
};

const FunctionDoc utf8_normalize_doc(
    "Utf8-normalize input",
    ("For each string in `strings`, return the normal form.\n\n"
     "The normalization form must be given in the options.\n"
     "Null inputs emit null."),
    {"strings"}, "Utf8NormalizeOptions", /*options_required=*/true);

void AddUtf8StringNormalize(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernelWithState<Utf8NormalizeExec>("utf8_normalize", registry,
                                                         utf8_normalize_doc);
}

#endif  // ARROW_WITH_UTF8PROC

// ----------------------------------------------------------------------
// String length

struct Utf8Length {
  template <typename OutValue, typename Arg0Value = util::string_view>
  static OutValue Call(KernelContext*, Arg0Value val, Status*) {
    auto str = reinterpret_cast<const uint8_t*>(val.data());
    auto strlen = val.size();
    return static_cast<OutValue>(util::UTF8Length(str, str + strlen));
  }
};

const FunctionDoc utf8_length_doc(
    "Compute UTF8 string lengths",
    ("For each string in `strings`, emit its length in UTF8 characters.\n"
     "Null values emit null."),
    {"strings"});

void AddUtf8StringLength(FunctionRegistry* registry) {
  auto func =
      std::make_shared<ScalarFunction>("utf8_length", Arity::Unary(), utf8_length_doc);
  {
    auto exec = applicator::ScalarUnaryNotNull<Int32Type, StringType, Utf8Length>::Exec;
    DCHECK_OK(func->AddKernel({utf8()}, int32(), std::move(exec)));
  }
  {
    auto exec =
        applicator::ScalarUnaryNotNull<Int64Type, LargeStringType, Utf8Length>::Exec;
    DCHECK_OK(func->AddKernel({large_utf8()}, int64(), std::move(exec)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// String reversal

struct Utf8ReverseTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    int64_t i = 0;
    while (i < input_string_ncodeunits) {
      int64_t char_end = std::min(i + util::ValidUtf8CodepointByteSize(input + i),
                                  input_string_ncodeunits);
      std::copy(input + i, input + char_end, output + input_string_ncodeunits - char_end);
      i = char_end;
    }
    return input_string_ncodeunits;
  }
};

template <typename Type>
using Utf8Reverse = StringTransformExec<Type, Utf8ReverseTransform>;

const FunctionDoc utf8_reverse_doc(
    "Reverse input",
    ("For each string in `strings`, return a reversed version.\n\n"
     "This function operates on Unicode codepoints, not grapheme\n"
     "clusters. Hence, it will not correctly reverse grapheme clusters\n"
     "composed of multiple codepoints."),
    {"strings"});

void AddUtf8StringReverse(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernel<Utf8Reverse>("utf8_reverse", registry, utf8_reverse_doc);
}

// ----------------------------------------------------------------------
// String trimming

struct UTF8TrimState {
  TrimOptions options_;
  std::vector<bool> codepoints_;
  Status status_ = Status::OK();

  explicit UTF8TrimState(KernelContext* ctx, TrimOptions options)
      : options_(std::move(options)) {
    if (!ARROW_PREDICT_TRUE(
            arrow::util::UTF8ForEach(options_.characters, [&](uint32_t c) {
              codepoints_.resize(
                  std::max(c + 1, static_cast<uint32_t>(codepoints_.size())));
              codepoints_.at(c) = true;
            }))) {
      status_ = Status::Invalid("Invalid UTF8 sequence in input");
    }
  }
};

template <bool TrimLeft, bool TrimRight>
struct UTF8TrimTransform : public StringTransformBase {
  using State = KernelStateFromFunctionOptions<UTF8TrimState, TrimOptions>;

  const UTF8TrimState& state_;

  explicit UTF8TrimTransform(const UTF8TrimState& state) : state_(state) {}

  Status PreExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) override {
    return state_.status_;
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;
    const auto& codepoints = state_.codepoints_;

    auto predicate = [&](uint32_t c) { return c >= codepoints.size() || !codepoints[c]; };
    if (TrimLeft && !ARROW_PREDICT_TRUE(
                        arrow::util::UTF8FindIf(begin, end, predicate, &begin_trimmed))) {
      return kStringTransformError;
    }
    if (TrimRight && begin_trimmed < end) {
      if (!ARROW_PREDICT_TRUE(arrow::util::UTF8FindIfReverse(begin_trimmed, end,
                                                             predicate, &end_trimmed))) {
        return kStringTransformError;
      }
    }
    std::copy(begin_trimmed, end_trimmed, output);
    return end_trimmed - begin_trimmed;
  }
};

template <typename Type>
using UTF8Trim = StringTransformExecWithState<Type, UTF8TrimTransform<true, true>>;

template <typename Type>
using UTF8LTrim = StringTransformExecWithState<Type, UTF8TrimTransform<true, false>>;

template <typename Type>
using UTF8RTrim = StringTransformExecWithState<Type, UTF8TrimTransform<false, true>>;

#ifdef ARROW_WITH_UTF8PROC

template <bool TrimLeft, bool TrimRight>
struct UTF8TrimWhitespaceTransform : public StringTransformBase {
  Status PreExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) override {
    EnsureUtf8LookupTablesFilled();
    return Status::OK();
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;

    auto predicate = [](uint32_t c) { return !IsSpaceCharacterUnicode(c); };
    if (TrimLeft && !ARROW_PREDICT_TRUE(
                        arrow::util::UTF8FindIf(begin, end, predicate, &begin_trimmed))) {
      return kStringTransformError;
    }
    if (TrimRight && begin_trimmed < end) {
      if (!ARROW_PREDICT_TRUE(arrow::util::UTF8FindIfReverse(begin_trimmed, end,
                                                             predicate, &end_trimmed))) {
        return kStringTransformError;
      }
    }
    std::copy(begin_trimmed, end_trimmed, output);
    return end_trimmed - begin_trimmed;
  }
};

template <typename Type>
using UTF8TrimWhitespace =
    StringTransformExec<Type, UTF8TrimWhitespaceTransform<true, true>>;

template <typename Type>
using UTF8LTrimWhitespace =
    StringTransformExec<Type, UTF8TrimWhitespaceTransform<true, false>>;

template <typename Type>
using UTF8RTrimWhitespace =
    StringTransformExec<Type, UTF8TrimWhitespaceTransform<false, true>>;

#endif

const FunctionDoc utf8_trim_doc(
    "Trim leading and trailing characters",
    ("For each string in `strings`, remove any leading or trailing characters\n"
     "from the `characters` option (as given in TrimOptions).\n"
     "Null values emit null."),
    {"strings"}, "TrimOptions", /*options_required=*/true);

const FunctionDoc utf8_ltrim_doc(
    "Trim leading characters",
    ("For each string in `strings`, remove any leading characters\n"
     "from the `characters` option (as given in TrimOptions).\n"
     "Null values emit null."),
    {"strings"}, "TrimOptions", /*options_required=*/true);

const FunctionDoc utf8_rtrim_doc(
    "Trim trailing characters",
    ("For each string in `strings`, remove any trailing characters\n"
     "from the `characters` option (as given in TrimOptions).\n"
     "Null values emit null."),
    {"strings"}, "TrimOptions", /*options_required=*/true);

#ifdef ARROW_WITH_UTF8PROC

const FunctionDoc utf8_trim_whitespace_doc(
    "Trim leading and trailing whitespace characters",
    ("For each string in `strings`, emit a string with leading and trailing\n"
     "whitespace characters removed, where whitespace characters are defined\n"
     "by the Unicode standard.  Null values emit null."),
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

#endif  // ARROW_WITH_UTF8PROC

void AddUtf8StringTrim(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernelWithState<UTF8Trim>("utf8_trim", registry, utf8_trim_doc);
  MakeUnaryStringBatchKernelWithState<UTF8LTrim>("utf8_ltrim", registry, utf8_ltrim_doc);
  MakeUnaryStringBatchKernelWithState<UTF8RTrim>("utf8_rtrim", registry, utf8_rtrim_doc);
#ifdef ARROW_WITH_UTF8PROC
  MakeUnaryStringBatchKernel<UTF8TrimWhitespace>("utf8_trim_whitespace", registry,
                                                 utf8_trim_whitespace_doc);
  MakeUnaryStringBatchKernel<UTF8LTrimWhitespace>("utf8_ltrim_whitespace", registry,
                                                  utf8_ltrim_whitespace_doc);
  MakeUnaryStringBatchKernel<UTF8RTrimWhitespace>("utf8_rtrim_whitespace", registry,
                                                  utf8_rtrim_whitespace_doc);
#endif  // ARROW_WITH_UTF8PROC
}

// ----------------------------------------------------------------------
// String padding

template <bool PadLeft, bool PadRight>
struct Utf8PadTransform : public StringTransformBase {
  using State = OptionsWrapper<PadOptions>;

  const PadOptions& options_;

  explicit Utf8PadTransform(const PadOptions& options) : options_(options) {}

  Status PreExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) override {
    auto str = reinterpret_cast<const uint8_t*>(options_.padding.data());
    auto strlen = options_.padding.size();
    if (util::UTF8Length(str, str + strlen) != 1) {
      return Status::Invalid("Padding must be one codepoint, got '", options_.padding,
                             "'");
    }
    return Status::OK();
  }

  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    // This is likely very overallocated but hard to do better without
    // actually looking at each string (because of strings that may be
    // longer than the given width)
    // One codepoint may be up to 4 bytes
    return input_ncodeunits + 4 * ninputs * options_.width;
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const int64_t input_width = util::UTF8Length(input, input + input_string_ncodeunits);
    if (input_width >= options_.width) {
      std::copy(input, input + input_string_ncodeunits, output);
      return input_string_ncodeunits;
    }
    const int64_t spaces = options_.width - input_width;
    int64_t left = 0;
    int64_t right = 0;
    if (PadLeft && PadRight) {
      // If odd number of spaces, put the extra space on the right
      left = spaces / 2;
      right = spaces - left;
    } else if (PadLeft) {
      left = spaces;
    } else if (PadRight) {
      right = spaces;
    } else {
      DCHECK(false) << "unreachable";
      return 0;
    }
    uint8_t* start = output;
    while (left) {
      output = std::copy(options_.padding.begin(), options_.padding.end(), output);
      left--;
    }
    output = std::copy(input, input + input_string_ncodeunits, output);
    while (right) {
      output = std::copy(options_.padding.begin(), options_.padding.end(), output);
      right--;
    }
    return output - start;
  }
};

template <typename Type>
using Utf8LPad = StringTransformExecWithState<Type, Utf8PadTransform<true, false>>;

template <typename Type>
using Utf8RPad = StringTransformExecWithState<Type, Utf8PadTransform<false, true>>;

template <typename Type>
using Utf8Center = StringTransformExecWithState<Type, Utf8PadTransform<true, true>>;

const FunctionDoc utf8_center_doc(
    "Center strings by padding with a given character",
    ("For each string in `strings`, emit a centered string by padding both sides \n"
     "with the given UTF8 codeunit.\nNull values emit null."),
    {"strings"}, "PadOptions", /*options_required=*/true);

const FunctionDoc utf8_lpad_doc(
    "Right-align strings by padding with a given character",
    ("For each string in `strings`, emit a right-aligned string by prepending \n"
     "the given UTF8 codeunit.\nNull values emit null."),
    {"strings"}, "PadOptions", /*options_required=*/true);

const FunctionDoc utf8_rpad_doc(
    "Left-align strings by padding with a given character",
    ("For each string in `strings`, emit a left-aligned string by appending \n"
     "the given UTF8 codeunit.\nNull values emit null."),
    {"strings"}, "PadOptions", /*options_required=*/true);

void AddUtf8StringPad(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernelWithState<Utf8LPad>("utf8_lpad", registry, utf8_lpad_doc);
  MakeUnaryStringBatchKernelWithState<Utf8RPad>("utf8_rpad", registry, utf8_rpad_doc);
  MakeUnaryStringBatchKernelWithState<Utf8Center>("utf8_center", registry,
                                                  utf8_center_doc);
}

// ----------------------------------------------------------------------
// Replace slice

struct Utf8ReplaceSliceTransform : ReplaceStringSliceTransformBase {
  using ReplaceStringSliceTransformBase::ReplaceStringSliceTransformBase;
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const auto& opts = *options;
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t *begin_sliced, *end_sliced;
    uint8_t* output_start = output;

    // Mimic Pandas: if stop would be before start, treat as 0-length slice
    if (opts.start >= 0) {
      // Count from left
      if (!arrow::util::UTF8AdvanceCodepoints(begin, end, &begin_sliced, opts.start)) {
        return kStringTransformError;
      }
      if (opts.stop > options->start) {
        // Continue counting from left
        const int64_t length = opts.stop - options->start;
        if (!arrow::util::UTF8AdvanceCodepoints(begin_sliced, end, &end_sliced, length)) {
          return kStringTransformError;
        }
      } else if (opts.stop < 0) {
        // Count from right
        if (!arrow::util::UTF8AdvanceCodepointsReverse(begin_sliced, end, &end_sliced,
                                                       -opts.stop)) {
          return kStringTransformError;
        }
      } else {
        // Zero-length slice
        end_sliced = begin_sliced;
      }
    } else {
      // Count from right
      if (!arrow::util::UTF8AdvanceCodepointsReverse(begin, end, &begin_sliced,
                                                     -opts.start)) {
        return kStringTransformError;
      }
      if (opts.stop >= 0) {
        // Restart counting from left
        if (!arrow::util::UTF8AdvanceCodepoints(begin, end, &end_sliced, opts.stop)) {
          return kStringTransformError;
        }
        if (end_sliced <= begin_sliced) {
          // Zero-length slice
          end_sliced = begin_sliced;
        }
      } else if ((opts.stop < 0) && (options->stop > options->start)) {
        // Count from right
        if (!arrow::util::UTF8AdvanceCodepointsReverse(begin_sliced, end, &end_sliced,
                                                       -opts.stop)) {
          return kStringTransformError;
        }
      } else {
        // Zero-length slice
        end_sliced = begin_sliced;
      }
    }
    output = std::copy(begin, begin_sliced, output);
    output = std::copy(opts.replacement.begin(), options->replacement.end(), output);
    output = std::copy(end_sliced, end, output);
    return output - output_start;
  }
};

template <typename Type>
using Utf8ReplaceSlice = StringTransformExecWithState<Type, Utf8ReplaceSliceTransform>;

const FunctionDoc utf8_replace_slice_doc(
    "Replace a slice of a string",
    ("For each string in `strings`, replace a slice of the string defined by `start`\n"
     "and `stop` indices with the given `replacement`. `start` is inclusive\n"
     "and `stop` is exclusive, and both are measured in UTF8 characters.\n"
     "Null values emit null."),
    {"strings"}, "ReplaceSliceOptions", /*options_required=*/true);

void AddUtf8StringReplaceSlice(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("utf8_replace_slice", Arity::Unary(),
                                               utf8_replace_slice_doc);

  for (const auto& ty : StringTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<Utf8ReplaceSlice>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, std::move(exec),
                              ReplaceStringSliceTransformBase::State::Init));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// Slicing

struct SliceCodeunitsTransform : StringSliceTransformBase {
  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    const SliceOptions& opt = *this->options;
    if ((opt.start >= 0) != (opt.stop >= 0)) {
      // If start and stop don't have the same sign, we can't guess an upper bound
      // on the resulting slice lengths, so return a worst case estimate.
      return input_ncodeunits;
    }
    int64_t max_slice_codepoints = (opt.stop - opt.start + opt.step - 1) / opt.step;
    // The maximum UTF8 byte size of a codepoint is 4
    return std::min(input_ncodeunits,
                    4 * ninputs * std::max<int64_t>(0, max_slice_codepoints));
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    if (options->step >= 1) {
      return SliceForward(input, input_string_ncodeunits, output);
    }
    return SliceBackward(input, input_string_ncodeunits, output);
  }

#define RETURN_IF_UTF8_ERROR(expr)    \
  do {                                \
    if (ARROW_PREDICT_FALSE(!expr)) { \
      return kStringTransformError;   \
    }                                 \
  } while (0)

  int64_t SliceForward(const uint8_t* input, int64_t input_string_ncodeunits,
                       uint8_t* output) {
    // Slice in forward order (step > 0)
    const SliceOptions& opt = *this->options;
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* begin_sliced = begin;
    const uint8_t* end_sliced = end;

    // First, compute begin_sliced and end_sliced
    if (opt.start >= 0) {
      // start counting from the left
      RETURN_IF_UTF8_ERROR(
          arrow::util::UTF8AdvanceCodepoints(begin, end, &begin_sliced, opt.start));
      if (opt.stop > opt.start) {
        // continue counting from begin_sliced
        const int64_t length = opt.stop - opt.start;
        RETURN_IF_UTF8_ERROR(
            arrow::util::UTF8AdvanceCodepoints(begin_sliced, end, &end_sliced, length));
      } else if (opt.stop < 0) {
        // or from the end (but we will never need to < begin_sliced)
        RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
            begin_sliced, end, &end_sliced, -opt.stop));
      } else {
        // zero length slice
        return 0;
      }
    } else {
      // start counting from the right
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
          begin, end, &begin_sliced, -opt.start));
      if (opt.stop > 0) {
        // continue counting from the left, we cannot start from begin_sliced because we
        // don't know how many codepoints are between begin and begin_sliced
        RETURN_IF_UTF8_ERROR(
            arrow::util::UTF8AdvanceCodepoints(begin, end, &end_sliced, opt.stop));
        // and therefore we also needs this
        if (end_sliced <= begin_sliced) {
          // zero length slice
          return 0;
        }
      } else if ((opt.stop < 0) && (opt.stop > opt.start)) {
        // stop is negative, but larger than start, so we count again from the right
        // in some cases we can optimize this, depending on the shortest path (from end
        // or begin_sliced), but begin_sliced and opt.start can be 'out of sync',
        // for instance when start=-100, when the string length is only 10.
        RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
            begin_sliced, end, &end_sliced, -opt.stop));
      } else {
        // zero length slice
        return 0;
      }
    }

    // Second, copy computed slice to output
    DCHECK(begin_sliced <= end_sliced);
    if (opt.step == 1) {
      // fast case, where we simply can finish with a memcpy
      std::copy(begin_sliced, end_sliced, output);
      return end_sliced - begin_sliced;
    }
    uint8_t* dest = output;
    const uint8_t* i = begin_sliced;

    while (i < end_sliced) {
      uint32_t codepoint = 0;
      // write a single codepoint
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8Decode(&i, &codepoint));
      dest = arrow::util::UTF8Encode(dest, codepoint);
      // and skip the remainder
      int64_t skips = opt.step - 1;
      while ((skips--) && (i < end_sliced)) {
        RETURN_IF_UTF8_ERROR(arrow::util::UTF8Decode(&i, &codepoint));
      }
    }
    return dest - output;
  }

  int64_t SliceBackward(const uint8_t* input, int64_t input_string_ncodeunits,
                        uint8_t* output) {
    // Slice in reverse order (step < 0)
    const SliceOptions& opt = *this->options;
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* begin_sliced = begin;
    const uint8_t* end_sliced = end;

    // Serious +1 -1 kung fu because begin_sliced and end_sliced act like
    // reverse iterators.
    if (opt.start >= 0) {
      // +1 because begin_sliced acts as as the end of a reverse iterator
      RETURN_IF_UTF8_ERROR(
          arrow::util::UTF8AdvanceCodepoints(begin, end, &begin_sliced, opt.start + 1));
    } else {
      // -1 because start=-1 means the last codeunit, which is 0 advances
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
          begin, end, &begin_sliced, -opt.start - 1));
    }
    // make it point at the last codeunit of the previous codeunit
    begin_sliced--;

    // similar to opt.start
    if (opt.stop >= 0) {
      RETURN_IF_UTF8_ERROR(
          arrow::util::UTF8AdvanceCodepoints(begin, end, &end_sliced, opt.stop + 1));
    } else {
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8AdvanceCodepointsReverse(
          begin, end, &end_sliced, -opt.stop - 1));
    }
    end_sliced--;

    // Copy computed slice to output
    uint8_t* dest = output;
    const uint8_t* i = begin_sliced;
    while (i > end_sliced) {
      uint32_t codepoint = 0;
      // write a single codepoint
      RETURN_IF_UTF8_ERROR(arrow::util::UTF8DecodeReverse(&i, &codepoint));
      dest = arrow::util::UTF8Encode(dest, codepoint);
      // and skip the remainder
      int64_t skips = -opt.step - 1;
      while ((skips--) && (i > end_sliced)) {
        RETURN_IF_UTF8_ERROR(arrow::util::UTF8DecodeReverse(&i, &codepoint));
      }
    }
    return dest - output;
  }

#undef RETURN_IF_UTF8_ERROR
};

template <typename Type>
using SliceCodeunits = StringTransformExec<Type, SliceCodeunitsTransform>;

const FunctionDoc utf8_slice_codeunits_doc(
    "Slice string",
    ("For each string in `strings`, emit the substring defined by\n"
     "(`start`, `stop`, `step`) as given by `SliceOptions` where `start` is\n"
     "inclusive and `stop` is exclusive. All three values are measured in\n"
     "UTF8 codeunits.\n"
     "If `step` is negative, the string will be advanced in reversed order.\n"
     "An error is raised if `step` is zero.\n"
     "Null inputs emit null."),
    {"strings"}, "SliceOptions", /*options_required=*/true);

void AddUtf8StringSlice(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("utf8_slice_codeunits", Arity::Unary(),
                                               utf8_slice_codeunits_doc);
  for (const auto& ty : StringTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<SliceCodeunits>(ty);
    DCHECK_OK(
        func->AddKernel({ty}, ty, std::move(exec), SliceCodeunitsTransform::State::Init));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// Split by whitespace

#ifdef ARROW_WITH_UTF8PROC

struct SplitWhitespaceUtf8Finder : public StringSplitFinderBase<SplitOptions> {
  using Options = SplitOptions;

  Status PreExec(const SplitOptions& options) override {
    EnsureUtf8LookupTablesFilled();
    return Status::OK();
  }

  bool Find(const uint8_t* begin, const uint8_t* end, const uint8_t** separator_begin,
            const uint8_t** separator_end, const SplitOptions& options) {
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

  bool FindReverse(const uint8_t* begin, const uint8_t* end,
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

template <typename Type, typename ListType>
using SplitWhitespaceUtf8Exec =
    StringSplitExec<Type, ListType, SplitWhitespaceUtf8Finder>;

const FunctionDoc utf8_split_whitespace_doc(
    "Split string according to any Unicode whitespace",
    ("Split each string according any non-zero length sequence of Unicode\n"
     "whitespace characters.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitOptions."),
    {"strings"}, "SplitOptions");

void AddUtf8StringSplitWhitespace(FunctionRegistry* registry) {
  static const SplitOptions default_options;
  auto func =
      std::make_shared<ScalarFunction>("utf8_split_whitespace", Arity::Unary(),
                                       utf8_split_whitespace_doc, &default_options);
  for (const auto& ty : StringTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<SplitWhitespaceUtf8Exec, ListType>(ty);
    DCHECK_OK(func->AddKernel({ty}, {list(ty)}, std::move(exec), StringSplitState::Init));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

#endif  // ARROW_WITH_UTF8PROC

// ----------------------------------------------------------------------

}  // namespace

void EnsureUtf8LookupTablesFilled() {
#ifdef ARROW_WITH_UTF8PROC
  std::call_once(flag_case_luts, []() {
    lut_upper_codepoint.reserve(kMaxCodepointLookup + 1);
    lut_lower_codepoint.reserve(kMaxCodepointLookup + 1);
    lut_swapcase_codepoint.reserve(kMaxCodepointLookup + 1);
    for (uint32_t i = 0; i <= kMaxCodepointLookup; i++) {
      lut_upper_codepoint.push_back(utf8proc_toupper(i));
      lut_lower_codepoint.push_back(utf8proc_tolower(i));
      lut_category.push_back(utf8proc_category(i));

      if (IsLowerCaseCharacterUnicode(i)) {
        lut_swapcase_codepoint.push_back(utf8proc_toupper(i));
      } else if (IsUpperCaseCharacterUnicode(i)) {
        lut_swapcase_codepoint.push_back(utf8proc_tolower(i));
      } else {
        lut_swapcase_codepoint.push_back(i);
      }
    }
  });
#endif
}

void RegisterScalarStringUtf8(FunctionRegistry* registry) {
#ifdef ARROW_WITH_UTF8PROC
  AddUtf8StringPredicates(registry);
  AddUtf8StringCaseConversion(registry);
  AddUtf8StringNormalize(registry);
#endif
  AddUtf8StringLength(registry);
  AddUtf8StringReverse(registry);
  AddUtf8StringTrim(registry);
  AddUtf8StringPad(registry);
  AddUtf8StringReplaceSlice(registry);
  AddUtf8StringSlice(registry);
#ifdef ARROW_WITH_UTF8PROC
  AddUtf8StringSplitWhitespace(registry);
#endif
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
