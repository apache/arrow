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
#include <memory>
#include <string>

#ifdef ARROW_WITH_RE2
#include <re2/re2.h>
#endif

#include "arrow/array/builder_nested.h"
#include "arrow/compute/kernels/scalar_string_internal.h"
#include "arrow/result.h"
#include "arrow/util/macros.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::EndsWith;
using internal::StartsWith;

namespace compute {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// re2 utilities

#ifdef ARROW_WITH_RE2
std::string_view ToStringView(re2::StringPiece piece) {
  return {piece.data(), piece.length()};
}

re2::StringPiece ToStringPiece(std::string_view view) {
  return {view.data(), view.length()};
}

Status RegexStatus(const RE2& regex) {
  if (!regex.ok()) {
    return Status::Invalid("Invalid regular expression: ", regex.error());
  }
  return Status::OK();
}

RE2::Options MakeRE2Options(bool is_utf8, bool ignore_case = false,
                            bool literal = false) {
  RE2::Options options(RE2::Quiet);
  options.set_encoding(is_utf8 ? RE2::Options::EncodingUTF8
                               : RE2::Options::EncodingLatin1);
  options.set_case_sensitive(!ignore_case);
  options.set_literal(literal);
  return options;
}

// Set RE2 encoding based on input type: Latin-1 for BinaryTypes and UTF-8 for StringTypes
template <typename T>
RE2::Options MakeRE2Options(bool ignore_case = false, bool literal = false) {
  return MakeRE2Options(T::is_utf8, ignore_case, literal);
}
#endif

// ----------------------------------------------------------------------
// String transformation base classes

template <typename StringTransform>
struct FixedSizeBinaryTransformExecBase {
  static Status Execute(KernelContext* ctx, StringTransform* transform,
                        const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& input = batch[0].array;
    ArrayData* output = out->array_data().get();

    const int32_t input_width = input.type->byte_width();
    const int32_t output_width = out->type()->byte_width();
    const int64_t input_nstrings = input.length;
    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          ctx->Allocate(output_width * input_nstrings));
    uint8_t* output_str = values_buffer->mutable_data();

    const uint8_t* input_data = input.GetValues<uint8_t>(1);
    for (int64_t i = 0; i < input_nstrings; i++) {
      if (!input.IsNull(i)) {
        const uint8_t* input_string = input_data + i * input_width;
        auto encoded_nbytes = static_cast<int32_t>(
            transform->Transform(input_string, input_width, output_str));
        if (encoded_nbytes != output_width) {
          return transform->InvalidInputSequence();
        }
      } else {
        std::memset(output_str, 0x00, output_width);
      }
      output_str += output_width;
    }

    output->buffers[1] = std::move(values_buffer);
    return Status::OK();
  }
};

template <typename StringTransform>
struct FixedSizeBinaryTransformExecWithState
    : public FixedSizeBinaryTransformExecBase<StringTransform> {
  using State = typename StringTransform::State;
  using FixedSizeBinaryTransformExecBase<StringTransform>::Execute;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    StringTransform transform(State::Get(ctx));
    RETURN_NOT_OK(transform.PreExec(ctx, batch, out));
    return Execute(ctx, &transform, batch, out);
  }

  static Result<TypeHolder> OutputType(KernelContext* ctx,
                                       const std::vector<TypeHolder>& types) {
    DCHECK_EQ(1, types.size());
    const auto& options = State::Get(ctx);
    const int32_t input_width = types[0].type->byte_width();
    const int32_t output_width = StringTransform::FixedOutputSize(options, input_width);
    return fixed_size_binary(output_width);
  }
};

template <typename Type1, typename Type2>
struct StringBinaryTransformBase {
  using ViewType2 = typename GetViewType<Type2>::T;
  using ArrayType1 = typename TypeTraits<Type1>::ArrayType;
  using offset_type = typename ArrayType1::offset_type;

  virtual ~StringBinaryTransformBase() = default;

  virtual Status PreExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return Status::OK();
  }

  virtual Status InvalidInputSequence() {
    return Status::Invalid("Invalid UTF8 sequence in input");
  }

  // Return the maximum total size of the output in codeunits (i.e. bytes)
  // given input characteristics for different input shapes.
  // The Status parameter should only be set if an error needs to be signaled.

  // Scalar-Array
  virtual Result<int64_t> MaxCodeunits(const int64_t input1_ncodeunits,
                                       const ArraySpan&) {
    return input1_ncodeunits;
  }

  // Array-Scalar
  virtual Result<int64_t> MaxCodeunits(const ArraySpan& input1, const ViewType2) {
    return GetVarBinaryValuesLength<offset_type>(input1);
  }

  // Array-Array
  virtual Result<int64_t> MaxCodeunits(const ArraySpan& input1, const ArraySpan&) {
    return GetVarBinaryValuesLength<offset_type>(input1);
  }

  // Not all combinations of input shapes are meaningful to string binary
  // transforms, so these flags serve as control toggles for enabling/disabling
  // the corresponding ones. These flags should be set in the PreExec() method.
  //
  // This is an example of a StringTransform that disables support for arguments
  // with mixed Scalar/Array shapes.
  //
  // template <typename Type1, typename Type2>
  // struct MyStringTransform : public StringBinaryTransformBase<Type1, Type2> {
  //   Status PreExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) override
  //   {
  //     enable_scalar_array_ = false;
  //     enable_array_scalar_ = false;
  //     return StringBinaryTransformBase::PreExec(ctx, batch, out);
  //   }
  //   ...
  // };
  bool enable_scalar_array_ = true;
  bool enable_array_scalar_ = true;
  bool enable_array_array_ = true;
};

/// Kernel exec generator for binary (two parameters) string transforms.
/// The first parameter is expected to always be a Binary/StringType while the
/// second parameter is generic. Types of template parameter StringTransform
/// need to define a transform method with the following signature:
///
/// Result<int64_t> Transform(
///    const uint8_t* input, const int64_t input_string_ncodeunits,
///    const ViewType2 value2, uint8_t* output);
///
/// where
///   * `input` - input sequence (binary or string)
///   * `input_string_ncodeunits` - length of input sequence in codeunits
///   * `value2` - second argument to the string transform
///   * `output` - output sequence (binary or string)
///
/// and returns the number of codeunits of the `output` sequence or a negative
/// value if an invalid input sequence is detected.
template <typename Type1, typename Type2, typename StringTransform>
struct StringBinaryTransformExecBase {
  using offset_type = typename Type1::offset_type;
  using ViewType2 = typename GetViewType<Type2>::T;
  using ArrayType1 = typename TypeTraits<Type1>::ArrayType;
  using ArrayType2 = typename TypeTraits<Type2>::ArrayType;

  static Status Execute(KernelContext* ctx, StringTransform* transform,
                        const ExecSpan& batch, ExecResult* out) {
    if (batch[0].is_scalar()) {
      if (batch[1].is_array()) {
        if (transform->enable_scalar_array_) {
          return ExecScalarArray(ctx, transform, batch[0].scalar, batch[1].array, out);
        }
      }
    } else if (batch[0].is_array()) {
      if (batch[1].is_scalar()) {
        if (transform->enable_array_scalar_) {
          return ExecArrayScalar(ctx, transform, batch[0].array, batch[1].scalar, out);
        }
      } else if (batch[1].is_array()) {
        if (transform->enable_array_array_) {
          return ExecArrayArray(ctx, transform, batch[0].array, batch[1].array, out);
        }
      }
    }
    return Status::Invalid(
        "Binary string transform has no combination of operand kinds enabled.");
  }

  static Status ExecArrayScalar(KernelContext* ctx, StringTransform* transform,
                                const ArraySpan& data1, const Scalar* scalar2,
                                ExecResult* out) {
    if (!scalar2->is_valid) {
      return Status::OK();
    }
    const auto value2 = UnboxScalar<Type2>::Unbox(*scalar2);

    // Calculate max number of output codeunits
    ARROW_ASSIGN_OR_RAISE(const auto max_output_ncodeunits,
                          transform->MaxCodeunits(data1, value2));
    RETURN_NOT_OK(CheckOutputCapacity(max_output_ncodeunits));

    // Allocate output strings
    ArrayData* output = out->array_data().get();
    ARROW_ASSIGN_OR_RAISE(auto values_buffer, ctx->Allocate(max_output_ncodeunits));
    output->buffers[2] = values_buffer;
    const auto output_string = output->buffers[2]->mutable_data();

    // String offsets are preallocated
    auto output_offsets = output->GetMutableValues<offset_type>(1);
    output_offsets[0] = 0;
    offset_type output_ncodeunits = 0;

    // Apply transform
    RETURN_NOT_OK(VisitArraySpanInline<Type1>(
        data1,
        [&](std::string_view input_string_view) {
          auto input_ncodeunits = static_cast<offset_type>(input_string_view.length());
          auto input_string = reinterpret_cast<const uint8_t*>(input_string_view.data());
          ARROW_ASSIGN_OR_RAISE(
              auto encoded_nbytes_,
              transform->Transform(input_string, input_ncodeunits, value2,
                                   output_string + output_ncodeunits));
          auto encoded_nbytes = static_cast<offset_type>(encoded_nbytes_);
          if (encoded_nbytes < 0) {
            return transform->InvalidInputSequence();
          }
          output_ncodeunits += encoded_nbytes;
          *(++output_offsets) = output_ncodeunits;
          return Status::OK();
        },
        [&]() {
          *(++output_offsets) = output_ncodeunits;
          return Status::OK();
        }));
    DCHECK_LE(output_ncodeunits, max_output_ncodeunits);

    // Trim the codepoint buffer, since we may have allocated too much
    return values_buffer->Resize(output_ncodeunits, /*shrink_to_fit=*/true);
  }

  static Status ExecScalarArray(KernelContext* ctx, StringTransform* transform,
                                const Scalar* scalar1, const ArraySpan& data2,
                                ExecResult* out) {
    if (!scalar1->is_valid) {
      return Status::OK();
    }
    const auto& binary_scalar1 = checked_cast<const BaseBinaryScalar&>(*scalar1);
    const auto input_string = binary_scalar1.value->data();
    const auto input_ncodeunits = binary_scalar1.value->size();

    // Calculate max number of output codeunits
    ARROW_ASSIGN_OR_RAISE(const auto max_output_ncodeunits,
                          transform->MaxCodeunits(input_ncodeunits, data2));
    RETURN_NOT_OK(CheckOutputCapacity(max_output_ncodeunits));

    // Allocate output strings
    ArrayData* output = out->array_data().get();
    ARROW_ASSIGN_OR_RAISE(auto values_buffer, ctx->Allocate(max_output_ncodeunits));
    output->buffers[2] = values_buffer;
    const auto output_string = output->buffers[2]->mutable_data();

    // String offsets are preallocated
    auto output_offsets = output->GetMutableValues<offset_type>(1);
    output_offsets[0] = 0;
    offset_type output_ncodeunits = 0;

    // TODO(wesm): rewrite to not require boxing
    const ArrayType2 array2(data2.ToArrayData());

    // Apply transform
    RETURN_NOT_OK(arrow::internal::VisitBitBlocks(
        data2.buffers[0].data, data2.offset, data2.length,
        [&](int64_t i) {
          ViewType2 value2 = array2.GetView(i);
          ARROW_ASSIGN_OR_RAISE(
              auto encoded_nbytes_,
              transform->Transform(input_string, input_ncodeunits, value2,
                                   output_string + output_ncodeunits));
          auto encoded_nbytes = static_cast<offset_type>(encoded_nbytes_);
          if (encoded_nbytes < 0) {
            return transform->InvalidInputSequence();
          }
          output_ncodeunits += encoded_nbytes;
          *(++output_offsets) = output_ncodeunits;
          return Status::OK();
        },
        [&]() {
          *(++output_offsets) = output_ncodeunits;
          return Status::OK();
        }));
    DCHECK_LE(output_ncodeunits, max_output_ncodeunits);

    // Trim the codepoint buffer, since we may have allocated too much
    return values_buffer->Resize(output_ncodeunits, /*shrink_to_fit=*/true);
  }

  static Status ExecArrayArray(KernelContext* ctx, StringTransform* transform,
                               const ArraySpan& data1, const ArraySpan& data2,
                               ExecResult* out) {
    // Calculate max number of output codeunits
    ARROW_ASSIGN_OR_RAISE(const auto max_output_ncodeunits,
                          transform->MaxCodeunits(data1, data2));
    RETURN_NOT_OK(CheckOutputCapacity(max_output_ncodeunits));

    // Allocate output strings
    ArrayData* output = out->array_data().get();
    ARROW_ASSIGN_OR_RAISE(auto values_buffer, ctx->Allocate(max_output_ncodeunits));
    output->buffers[2] = values_buffer;
    const auto output_string = output->buffers[2]->mutable_data();

    // String offsets are preallocated
    auto output_offsets = output->GetMutableValues<offset_type>(1);
    output_offsets[0] = 0;
    offset_type output_ncodeunits = 0;

    const offset_type* data1_offsets = data1.GetValues<offset_type>(1);
    const uint8_t* data1_data = data1.GetValues<uint8_t>(2, /*offset=*/0);

    // TODO(wesm): rewrite to not require boxing
    const ArrayType2 array2(data2.ToArrayData());

    // Apply transform
    RETURN_NOT_OK(arrow::internal::VisitTwoBitBlocks(
        data1.buffers[0].data, data1.offset, data2.buffers[0].data, data2.offset,
        data1.length,
        [&](int64_t i) {
          const offset_type input_ncodeunits = data1_offsets[i + 1] - data1_offsets[i];
          const uint8_t* input_string = data1_data + data1_offsets[i];
          ViewType2 value2 = array2.GetView(i);
          ARROW_ASSIGN_OR_RAISE(
              auto encoded_nbytes_,
              transform->Transform(input_string, input_ncodeunits, value2,
                                   output_string + output_ncodeunits));
          auto encoded_nbytes = static_cast<offset_type>(encoded_nbytes_);
          if (encoded_nbytes < 0) {
            return transform->InvalidInputSequence();
          }
          output_ncodeunits += encoded_nbytes;
          *(++output_offsets) = output_ncodeunits;
          return Status::OK();
        },
        [&]() {
          *(++output_offsets) = output_ncodeunits;
          return Status::OK();
        }));
    DCHECK_LE(output_ncodeunits, max_output_ncodeunits);

    // Trim the codepoint buffer, since we may have allocated too much
    return values_buffer->Resize(output_ncodeunits, /*shrink_to_fit=*/true);
  }

  static Status CheckOutputCapacity(int64_t ncodeunits) {
    if (ncodeunits > std::numeric_limits<offset_type>::max()) {
      return Status::CapacityError(
          "Result might not fit in requested binary/string array. "
          "If possible, convert to a large binary/string.");
    }
    return Status::OK();
  }
};

template <typename Type1, typename Type2, typename StringTransform>
struct StringBinaryTransformExec
    : public StringBinaryTransformExecBase<Type1, Type2, StringTransform> {
  using StringBinaryTransformExecBase<Type1, Type2, StringTransform>::Execute;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    StringTransform transform;
    RETURN_NOT_OK(transform.PreExec(ctx, batch, out));
    return Execute(ctx, &transform, batch, out);
  }
};

template <typename Type1, typename Type2, typename StringTransform>
struct StringBinaryTransformExecWithState
    : public StringBinaryTransformExecBase<Type1, Type2, StringTransform> {
  using State = typename StringTransform::State;
  using StringBinaryTransformExecBase<Type1, Type2, StringTransform>::Execute;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    StringTransform transform(State::Get(ctx));
    RETURN_NOT_OK(transform.PreExec(ctx, batch, out));
    return Execute(ctx, &transform, batch, out);
  }
};

using TransformFunc = std::function<void(const uint8_t*, int64_t, uint8_t*)>;

// Apply `transform` to input character data- this function cannot change the
// length
template <typename Type>
Status StringDataTransform(KernelContext* ctx, const ExecSpan& batch,
                           TransformFunc transform, ExecResult* out) {
  using offset_type = typename Type::offset_type;

  const ArraySpan& input = batch[0].array;
  ArrayData* out_arr = out->array_data().get();

  const auto offsets = input.GetValues<offset_type>(1);
  int64_t offset_nbytes = (input.length + 1) * sizeof(offset_type);
  if (input.offset == 0) {
    // We can reuse offsets from input if the input owns it
    if (input.buffers[1].owner != nullptr) {
      out_arr->buffers[1] = input.GetBuffer(1);
    } else {
      RETURN_NOT_OK(ctx->Allocate(offset_nbytes).Value(&out_arr->buffers[1]));
      std::memcpy(out_arr->buffers[1]->mutable_data(), input.buffers[1].data,
                  offset_nbytes);
    }
  } else {
    // We must allocate new space for the offsets and shift the existing offsets
    RETURN_NOT_OK(ctx->Allocate(offset_nbytes).Value(&out_arr->buffers[1]));
    auto out_offsets =
        reinterpret_cast<offset_type*>(out_arr->buffers[1]->mutable_data());
    offset_type first_offset = offsets[0];
    for (int64_t i = 0; i < input.length; ++i) {
      *out_offsets++ = offsets[i] - first_offset;
    }
    *out_offsets = offsets[input.length] - first_offset;
  }

  int64_t data_nbytes = GetVarBinaryValuesLength<offset_type>(input);
  if (input.length > 0) {
    // Allocate space for output data
    if (data_nbytes > 0) {
      RETURN_NOT_OK(ctx->Allocate(data_nbytes).Value(&out_arr->buffers[2]));
      transform(input.buffers[2].data + offsets[0], data_nbytes,
                out_arr->buffers[2]->mutable_data());
    } else {
      // Empty buffer
      out_arr->buffers[2] = Buffer::FromString("");
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Predicates and classification

// Code units in the range [a-z] can only be an encoding of an ASCII
// character/codepoint, not the 2nd, 3rd or 4th code unit (byte) of a different
// codepoint. This is guaranteed by the non-overlap design of the Unicode
// standard. (see section 2.5 of Unicode Standard Core Specification v13.0)

// IsAlpha/Digit etc

static inline bool IsAsciiCharacter(uint8_t character) { return character < 128; }

static inline bool IsLowerCaseCharacterAscii(uint8_t ascii_character) {
  return (ascii_character >= 'a') && (ascii_character <= 'z');
}

static inline bool IsUpperCaseCharacterAscii(uint8_t ascii_character) {
  return (ascii_character >= 'A') && (ascii_character <= 'Z');
}

static inline bool IsCasedCharacterAscii(uint8_t ascii_character) {
  // Note: Non-ASCII characters are seen as uncased.
  return IsLowerCaseCharacterAscii(ascii_character) ||
         IsUpperCaseCharacterAscii(ascii_character);
}

static inline bool IsAlphaCharacterAscii(uint8_t ascii_character) {
  return IsCasedCharacterAscii(ascii_character);
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
  return ((ascii_character >= 9) && (ascii_character <= 13)) || (ascii_character == ' ');
}

static inline bool IsPrintableCharacterAscii(uint8_t ascii_character) {
  return ((ascii_character >= ' ') && (ascii_character <= '~'));
}

struct IsAscii {
  static bool Call(KernelContext*, const uint8_t* input,
                   size_t input_string_nascii_characters, Status*) {
    return std::all_of(input, input + input_string_nascii_characters, IsAsciiCharacter);
  }
};

template <typename Derived, bool allow_empty = false>
struct CharacterPredicateAscii {
  static bool Call(KernelContext*, const uint8_t* input, size_t input_string_ncodeunits,
                   Status*) {
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
    return true;  // default condition make sure there is at least 1 character
  }
};

struct IsAlphaNumericAscii : CharacterPredicateAscii<IsAlphaNumericAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsAlphaNumericCharacterAscii(ascii_character);
  }
};

struct IsAlphaAscii : CharacterPredicateAscii<IsAlphaAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsAlphaCharacterAscii(ascii_character);
  }
};

struct IsDecimalAscii : CharacterPredicateAscii<IsDecimalAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsDecimalCharacterAscii(ascii_character);
  }
};

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

struct IsPrintableAscii
    : CharacterPredicateAscii<IsPrintableAscii, /*allow_empty=*/true> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsPrintableCharacterAscii(ascii_character);
  }
};

struct IsSpaceAscii : CharacterPredicateAscii<IsSpaceAscii> {
  static inline bool PredicateCharacterAll(uint8_t ascii_character) {
    return IsSpaceCharacterAscii(ascii_character);
  }
};

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

struct IsTitleAscii {
  static bool Call(KernelContext*, const uint8_t* input, size_t input_string_ncodeunits,
                   Status*) {
    // Rules:
    //   1. lower case follows cased
    //   2. upper case follows uncased
    //   3. at least 1 cased character (which logically should be upper/title)
    bool rules_1_and_2 = true;
    bool previous_cased = false;  // in LL, LU or LT
    bool rule_3 = false;
    for (const uint8_t* c = input; c < input + input_string_ncodeunits; ++c) {
      if (IsLowerCaseCharacterAscii(*c)) {
        if (!previous_cased) {
          // rule 1 broken
          rules_1_and_2 = false;
          break;
        }
        // next should be more lower case or uncased
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
        // an uncased character, like _ or 1
        // next should be upper case or more uncased
        previous_cased = false;
      }
    }
    return rules_1_and_2 & rule_3;
  }
};

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
     "follows an uncased character, and each lowercase character follows\n"
     "an uppercase character."));

void AddAsciiStringPredicates(FunctionRegistry* registry) {
  AddUnaryStringPredicate<IsAscii>("string_is_ascii", registry, string_is_ascii_doc);

  AddUnaryStringPredicate<IsAlphaNumericAscii>("ascii_is_alnum", registry,
                                               ascii_is_alnum_doc);
  AddUnaryStringPredicate<IsAlphaAscii>("ascii_is_alpha", registry, ascii_is_alpha_doc);
  AddUnaryStringPredicate<IsDecimalAscii>("ascii_is_decimal", registry,
                                          ascii_is_decimal_doc);
  // no is_digit for ascii, since it is the same as is_decimal
  AddUnaryStringPredicate<IsLowerAscii>("ascii_is_lower", registry, ascii_is_lower_doc);
  // no is_numeric for ascii, since it is the same as is_decimal
  AddUnaryStringPredicate<IsPrintableAscii>("ascii_is_printable", registry,
                                            ascii_is_printable_doc);
  AddUnaryStringPredicate<IsSpaceAscii>("ascii_is_space", registry, ascii_is_space_doc);
  AddUnaryStringPredicate<IsUpperAscii>("ascii_is_upper", registry, ascii_is_upper_doc);
  AddUnaryStringPredicate<IsTitleAscii>("ascii_is_title", registry, ascii_is_title_doc);
}

// ----------------------------------------------------------------------
// Case conversion

static inline uint8_t ascii_tolower(uint8_t utf8_code_unit) {
  return ((utf8_code_unit >= 'A') && (utf8_code_unit <= 'Z')) ? (utf8_code_unit + 32)
                                                              : utf8_code_unit;
}

static inline uint8_t ascii_toupper(uint8_t utf8_code_unit) {
  return ((utf8_code_unit >= 'a') && (utf8_code_unit <= 'z')) ? (utf8_code_unit - 32)
                                                              : utf8_code_unit;
}

static inline uint8_t ascii_swapcase(uint8_t utf8_code_unit) {
  if (IsLowerCaseCharacterAscii(utf8_code_unit)) {
    utf8_code_unit -= 32;
  } else if (IsUpperCaseCharacterAscii(utf8_code_unit)) {
    utf8_code_unit += 32;
  }
  return utf8_code_unit;
}

void TransformAsciiUpper(const uint8_t* input, int64_t length, uint8_t* output) {
  std::transform(input, input + length, output, ascii_toupper);
}

template <typename Type>
struct AsciiUpper {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return StringDataTransform<Type>(ctx, batch, TransformAsciiUpper, out);
  }
};

void TransformAsciiLower(const uint8_t* input, int64_t length, uint8_t* output) {
  std::transform(input, input + length, output, ascii_tolower);
}

template <typename Type>
struct AsciiLower {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return StringDataTransform<Type>(ctx, batch, TransformAsciiLower, out);
  }
};

void TransformAsciiSwapCase(const uint8_t* input, int64_t length, uint8_t* output) {
  std::transform(input, input + length, output, ascii_swapcase);
}

template <typename Type>
struct AsciiSwapCase {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return StringDataTransform<Type>(ctx, batch, TransformAsciiSwapCase, out);
  }
};

struct AsciiCapitalizeTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    if (input_string_ncodeunits > 0) {
      *output++ = ascii_toupper(*input++);
      TransformAsciiLower(input, input_string_ncodeunits - 1, output);
    }
    return input_string_ncodeunits;
  }
};

template <typename Type>
using AsciiCapitalize = StringTransformExec<Type, AsciiCapitalizeTransform>;

struct AsciiTitleTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* next = input;
    bool is_next_upper = true;
    while ((input = next++) < end) {
      if (IsCasedCharacterAscii(*input)) {
        // Lower/uppercase current character and
        // prepare to lowercase next consecutive cased characters
        *output++ = is_next_upper ? ascii_toupper(*input) : ascii_tolower(*input);
        is_next_upper = false;
      } else {
        // Copy current uncased character and
        // prepare to uppercase next cased character
        *output++ = *input;
        is_next_upper = true;
      }
    }
    return input_string_ncodeunits;
  }
};

template <typename Type>
using AsciiTitle = StringTransformExec<Type, AsciiTitleTransform>;

const FunctionDoc ascii_upper_doc(
    "Transform ASCII input to uppercase",
    ("For each string in `strings`, return an uppercase version.\n\n"
     "This function assumes the input is fully ASCII.  It it may contain\n"
     "non-ASCII characters, use \"utf8_upper\" instead."),
    {"strings"});

const FunctionDoc ascii_lower_doc(
    "Transform ASCII input to lowercase",
    ("For each string in `strings`, return a lowercase version.\n\n"
     "This function assumes the input is fully ASCII.  If it may contain\n"
     "non-ASCII characters, use \"utf8_lower\" instead."),
    {"strings"});

const FunctionDoc ascii_swapcase_doc(
    "Transform ASCII input by inverting casing",
    ("For each string in `strings`, return a string with opposite casing.\n\n"
     "This function assumes the input is fully ASCII.  If it may contain\n"
     "non-ASCII characters, use \"utf8_swapcase\" instead."),
    {"strings"});

const FunctionDoc ascii_capitalize_doc(
    "Capitalize the first character of ASCII input",
    ("For each string in `strings`, return a capitalized version.\n\n"
     "This function assumes the input is fully ASCII.  If it may contain\n"
     "non-ASCII characters, use \"utf8_capitalize\" instead."),
    {"strings"});

const FunctionDoc ascii_title_doc(
    "Titlecase each word of ASCII input",
    ("For each string in `strings`, return a titlecased version.\n"
     "Each word in the output will start with an uppercase character and its\n"
     "remaining characters will be lowercase.\n\n"
     "This function assumes the input is fully ASCII.  If it may contain\n"
     "non-ASCII characters, use \"utf8_title\" instead."),
    {"strings"});

void AddAsciiStringCaseConversion(FunctionRegistry* registry) {
  // Some kernels are able to reuse the original offsets buffer, so don't
  // preallocate them in the output. Only kernels that invoke
  // "StringDataTransform" support no preallocation.
  MakeUnaryStringBatchKernel<AsciiUpper>("ascii_upper", registry, ascii_upper_doc,
                                         MemAllocation::NO_PREALLOCATE);
  MakeUnaryStringBatchKernel<AsciiLower>("ascii_lower", registry, ascii_lower_doc,
                                         MemAllocation::NO_PREALLOCATE);
  MakeUnaryStringBatchKernel<AsciiSwapCase>(
      "ascii_swapcase", registry, ascii_swapcase_doc, MemAllocation::NO_PREALLOCATE);
  MakeUnaryStringBatchKernel<AsciiCapitalize>("ascii_capitalize", registry,
                                              ascii_capitalize_doc);
  MakeUnaryStringBatchKernel<AsciiTitle>("ascii_title", registry, ascii_title_doc);
}

// ----------------------------------------------------------------------
// Binary string length

struct BinaryLength {
  template <typename OutValue, typename Arg0Value = std::string_view>
  static OutValue Call(KernelContext*, Arg0Value val, Status*) {
    return static_cast<OutValue>(val.size());
  }

  static Status FixedSizeExec(KernelContext*, const ExecSpan& batch, ExecResult* out) {
    // Output is preallocated and validity buffer is precomputed
    const int32_t width = batch[0].type()->byte_width();
    int32_t* buffer = out->array_span_mutable()->GetValues<int32_t>(1);
    std::fill(buffer, buffer + batch.length, width);
    return Status::OK();
  }
};

const FunctionDoc binary_length_doc(
    "Compute string lengths",
    ("For each string in `strings`, emit its length of bytes.\n"
     "Null values emit null."),
    {"strings"});

void AddAsciiStringLength(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("binary_length", Arity::Unary(),
                                               binary_length_doc);
  for (const auto& ty : {binary(), utf8()}) {
    auto exec =
        GenerateVarBinaryBase<applicator::ScalarUnaryNotNull, Int32Type, BinaryLength>(
            ty);
    DCHECK_OK(func->AddKernel({ty}, int32(), std::move(exec)));
  }
  for (const auto& ty : {large_binary(), large_utf8()}) {
    auto exec =
        GenerateVarBinaryBase<applicator::ScalarUnaryNotNull, Int64Type, BinaryLength>(
            ty);
    DCHECK_OK(func->AddKernel({ty}, int64(), std::move(exec)));
  }
  DCHECK_OK(func->AddKernel({InputType(Type::FIXED_SIZE_BINARY)}, int32(),
                            BinaryLength::FixedSizeExec));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// String reversal

struct BinaryReverseTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    for (int64_t i = 0; i < input_string_ncodeunits; i++) {
      output[input_string_ncodeunits - i - 1] = input[i];
    }
    return input_string_ncodeunits;
  }
};

template <typename Type>
using BinaryReverse = StringTransformExec<Type, BinaryReverseTransform>;

struct AsciiReverseTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    uint8_t utf8_char_found = 0;
    for (int64_t i = 0; i < input_string_ncodeunits; i++) {
      // if a utf8 char is found, report to utf8_char_found
      utf8_char_found |= input[i] & 0x80;
      output[input_string_ncodeunits - i - 1] = input[i];
    }
    return utf8_char_found ? kStringTransformError : input_string_ncodeunits;
  }

  Status InvalidInputSequence() override {
    return Status::Invalid("Non-ASCII sequence in input");
  }
};

template <typename Type>
using AsciiReverse = StringTransformExec<Type, AsciiReverseTransform>;

const FunctionDoc binary_reverse_doc(
    "Reverse binary input",
    ("For each binary string in `strings`, return a reversed version.\n\n"
     "This function reverses the binary data at a byte-level."),
    {"strings"});

const FunctionDoc ascii_reverse_doc(
    "Reverse ASCII input",
    ("For each ASCII string in `strings`, return a reversed version.\n\n"
     "This function assumes the input is fully ASCII.  If it may contain\n"
     "non-ASCII characters, use \"utf8_reverse\" instead."),
    {"strings"});

void AddAsciiStringReverse(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("binary_reverse", Arity::Unary(),
                                                 binary_reverse_doc);
    for (const auto& ty : BinaryTypes()) {
      DCHECK_OK(
          func->AddKernel({ty}, ty, GenerateVarBinaryToVarBinary<BinaryReverse>(ty)));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  MakeUnaryStringBatchKernel<AsciiReverse>("ascii_reverse", registry, ascii_reverse_doc);
}

// ----------------------------------------------------------------------
// String trimming

struct AsciiTrimState {
  TrimOptions options_;
  std::vector<bool> characters_;

  explicit AsciiTrimState(KernelContext* ctx, TrimOptions options)
      : options_(std::move(options)), characters_(256) {
    for (const auto c : options_.characters) {
      characters_[static_cast<unsigned char>(c)] = true;
    }
  }
};

template <bool TrimLeft, bool TrimRight>
struct AsciiTrimTransform : public StringTransformBase {
  using State = KernelStateFromFunctionOptions<AsciiTrimState, TrimOptions>;

  const AsciiTrimState& state_;

  explicit AsciiTrimTransform(const AsciiTrimState& state) : state_(state) {}

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;
    const auto& characters = state_.characters_;

    auto predicate = [&](uint8_t c) { return !characters[c]; };
    if (TrimLeft) {
      begin_trimmed = std::find_if(begin, end, predicate);
    }
    if (TrimRight && begin_trimmed < end) {
      std::reverse_iterator<const uint8_t*> rbegin(end);
      std::reverse_iterator<const uint8_t*> rend(begin_trimmed);
      end_trimmed = std::find_if(rbegin, rend, predicate).base();
    }
    std::copy(begin_trimmed, end_trimmed, output);
    return end_trimmed - begin_trimmed;
  }
};

template <typename Type>
using AsciiTrim = StringTransformExecWithState<Type, AsciiTrimTransform<true, true>>;

template <typename Type>
using AsciiLTrim = StringTransformExecWithState<Type, AsciiTrimTransform<true, false>>;

template <typename Type>
using AsciiRTrim = StringTransformExecWithState<Type, AsciiTrimTransform<false, true>>;

template <bool TrimLeft, bool TrimRight>
struct AsciiTrimWhitespaceTransform : public StringTransformBase {
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_ncodeunits;
    const uint8_t* end_trimmed = end;
    const uint8_t* begin_trimmed = begin;

    auto predicate = [](unsigned char c) { return !IsSpaceCharacterAscii(c); };
    if (TrimLeft) {
      begin_trimmed = std::find_if(begin, end, predicate);
    }
    if (TrimRight && begin_trimmed < end) {
      std::reverse_iterator<const uint8_t*> rbegin(end);
      std::reverse_iterator<const uint8_t*> rend(begin_trimmed);
      end_trimmed = std::find_if(rbegin, rend, predicate).base();
    }
    std::copy(begin_trimmed, end_trimmed, output);
    return end_trimmed - begin_trimmed;
  }
};

template <typename Type>
using AsciiTrimWhitespace =
    StringTransformExec<Type, AsciiTrimWhitespaceTransform<true, true>>;

template <typename Type>
using AsciiLTrimWhitespace =
    StringTransformExec<Type, AsciiTrimWhitespaceTransform<true, false>>;

template <typename Type>
using AsciiRTrimWhitespace =
    StringTransformExec<Type, AsciiTrimWhitespaceTransform<false, true>>;

const FunctionDoc ascii_trim_doc(
    "Trim leading and trailing characters",
    ("For each string in `strings`, remove any leading or trailing characters\n"
     "from the `characters` option (as given in TrimOptions).\n"
     "Null values emit null.\n"
     "Both the `strings` and the `characters` are interpreted as\n"
     "ASCII; to trim non-ASCII characters, use `utf8_trim`."),
    {"strings"}, "TrimOptions", /*options_required=*/true);

const FunctionDoc ascii_ltrim_doc(
    "Trim leading characters",
    ("For each string in `strings`, remove any leading characters\n"
     "from the `characters` option (as given in TrimOptions).\n"
     "Null values emit null.\n"
     "Both the `strings` and the `characters` are interpreted as\n"
     "ASCII; to trim non-ASCII characters, use `utf8_ltrim`."),
    {"strings"}, "TrimOptions", /*options_required=*/true);

const FunctionDoc ascii_rtrim_doc(
    "Trim trailing characters",
    ("For each string in `strings`, remove any trailing characters\n"
     "from the `characters` option (as given in TrimOptions).\n"
     "Null values emit null.\n"
     "Both the `strings` and the `characters` are interpreted as\n"
     "ASCII; to trim non-ASCII characters, use `utf8_rtrim`."),
    {"strings"}, "TrimOptions", /*options_required=*/true);

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

void AddAsciiStringTrim(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernelWithState<AsciiTrim>("ascii_trim", registry, ascii_trim_doc);
  MakeUnaryStringBatchKernelWithState<AsciiLTrim>("ascii_ltrim", registry,
                                                  ascii_ltrim_doc);
  MakeUnaryStringBatchKernelWithState<AsciiRTrim>("ascii_rtrim", registry,
                                                  ascii_rtrim_doc);
  MakeUnaryStringBatchKernel<AsciiTrimWhitespace>("ascii_trim_whitespace", registry,
                                                  ascii_trim_whitespace_doc);
  MakeUnaryStringBatchKernel<AsciiLTrimWhitespace>("ascii_ltrim_whitespace", registry,
                                                   ascii_ltrim_whitespace_doc);
  MakeUnaryStringBatchKernel<AsciiRTrimWhitespace>("ascii_rtrim_whitespace", registry,
                                                   ascii_rtrim_whitespace_doc);
}

// ----------------------------------------------------------------------
// String padding

template <bool PadLeft, bool PadRight>
struct AsciiPadTransform : public StringTransformBase {
  using State = OptionsWrapper<PadOptions>;

  const PadOptions& options_;

  explicit AsciiPadTransform(const PadOptions& options) : options_(options) {}

  Status PreExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) override {
    if (options_.padding.size() != 1) {
      return Status::Invalid("Padding must be one byte, got '", options_.padding, "'");
    }
    return Status::OK();
  }

  int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) override {
    // This is likely very overallocated but hard to do better without
    // actually looking at each string (because of strings that may be
    // longer than the given width)
    return input_ncodeunits + ninputs * options_.width;
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    if (input_string_ncodeunits >= options_.width) {
      std::copy(input, input + input_string_ncodeunits, output);
      return input_string_ncodeunits;
    }
    const int64_t spaces = options_.width - input_string_ncodeunits;
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
    std::fill(output, output + left, options_.padding[0]);
    output += left;
    output = std::copy(input, input + input_string_ncodeunits, output);
    std::fill(output, output + right, options_.padding[0]);
    return options_.width;
  }
};

template <typename Type>
using AsciiLPad = StringTransformExecWithState<Type, AsciiPadTransform<true, false>>;

template <typename Type>
using AsciiRPad = StringTransformExecWithState<Type, AsciiPadTransform<false, true>>;

template <typename Type>
using AsciiCenter = StringTransformExecWithState<Type, AsciiPadTransform<true, true>>;

const FunctionDoc ascii_lpad_doc(
    "Right-align strings by padding with a given character",
    ("For each string in `strings`, emit a right-aligned string by prepending \n"
     "the given ASCII character.\nNull values emit null."),
    {"strings"}, "PadOptions", /*options_required=*/true);

const FunctionDoc ascii_rpad_doc(
    "Left-align strings by padding with a given character",
    ("For each string in `strings`, emit a left-aligned string by appending \n"
     "the given ASCII character.\nNull values emit null."),
    {"strings"}, "PadOptions", /*options_required=*/true);

const FunctionDoc ascii_center_doc(
    "Center strings by padding with a given character",
    ("For each string in `strings`, emit a centered string by padding both sides \n"
     "with the given ASCII character.\nNull values emit null."),
    {"strings"}, "PadOptions", /*options_required=*/true);

void AddAsciiStringPad(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernelWithState<AsciiLPad>("ascii_lpad", registry, ascii_lpad_doc);
  MakeUnaryStringBatchKernelWithState<AsciiRPad>("ascii_rpad", registry, ascii_rpad_doc);
  MakeUnaryStringBatchKernelWithState<AsciiCenter>("ascii_center", registry,
                                                   ascii_center_doc);
}

// ----------------------------------------------------------------------
// Exact pattern detection

using StrToBoolTransformFunc =
    std::function<void(const void*, const uint8_t*, int64_t, int64_t, uint8_t*)>;

// Apply `transform` to input character data- this function cannot change the
// length
template <typename Type>
void StringBoolTransform(KernelContext* ctx, const ExecSpan& batch,
                         StrToBoolTransformFunc transform, ExecResult* out) {
  using offset_type = typename Type::offset_type;
  const ArraySpan& input = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  if (input.length > 0) {
    transform(reinterpret_cast<const offset_type*>(input.buffers[1].data) + input.offset,
              input.buffers[2].data, input.length, out_arr->offset,
              out_arr->buffers[1].data);
  }
}

using MatchSubstringState = OptionsWrapper<MatchSubstringOptions>;

// This is an implementation of the Knuth-Morris-Pratt algorithm
struct PlainSubstringMatcher {
  const MatchSubstringOptions& options_;
  std::vector<int64_t> prefix_table;

  static Result<std::unique_ptr<PlainSubstringMatcher>> Make(
      const MatchSubstringOptions& options) {
    // Should be handled by partial template specialization below
    DCHECK(!options.ignore_case);
    return std::make_unique<PlainSubstringMatcher>(options);
  }

  explicit PlainSubstringMatcher(const MatchSubstringOptions& options)
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

  int64_t Find(std::string_view current) const {
    // Phase 2: Find the prefix in the data
    const auto pattern_length = options_.pattern.size();
    int64_t pattern_pos = 0;
    int64_t pos = 0;
    if (pattern_length == 0) return 0;
    for (const auto c : current) {
      while ((pattern_pos >= 0) && (options_.pattern[pattern_pos] != c)) {
        pattern_pos = prefix_table[pattern_pos];
      }
      pattern_pos++;
      if (static_cast<size_t>(pattern_pos) == pattern_length) {
        return pos + 1 - pattern_length;
      }
      pos++;
    }
    return -1;
  }

  bool Match(std::string_view current) const { return Find(current) >= 0; }
};

struct PlainStartsWithMatcher {
  const MatchSubstringOptions& options_;

  explicit PlainStartsWithMatcher(const MatchSubstringOptions& options)
      : options_(options) {}

  static Result<std::unique_ptr<PlainStartsWithMatcher>> Make(
      const MatchSubstringOptions& options) {
    // Should be handled by partial template specialization below
    DCHECK(!options.ignore_case);
    return std::make_unique<PlainStartsWithMatcher>(options);
  }

  bool Match(std::string_view current) const {
    return StartsWith(current, options_.pattern);
  }
};

struct PlainEndsWithMatcher {
  const MatchSubstringOptions& options_;

  explicit PlainEndsWithMatcher(const MatchSubstringOptions& options)
      : options_(options) {}

  static Result<std::unique_ptr<PlainEndsWithMatcher>> Make(
      const MatchSubstringOptions& options) {
    // Should be handled by partial template specialization below
    DCHECK(!options.ignore_case);
    return std::make_unique<PlainEndsWithMatcher>(options);
  }

  bool Match(std::string_view current) const {
    return EndsWith(current, options_.pattern);
  }
};

#ifdef ARROW_WITH_RE2
struct RegexSubstringMatcher {
  const MatchSubstringOptions& options_;
  const RE2 regex_match_;

  static Result<std::unique_ptr<RegexSubstringMatcher>> Make(
      const MatchSubstringOptions& options, bool is_utf8 = true, bool literal = false) {
    auto matcher = std::make_unique<RegexSubstringMatcher>(options, is_utf8, literal);
    RETURN_NOT_OK(RegexStatus(matcher->regex_match_));
    return std::move(matcher);
  }

  explicit RegexSubstringMatcher(const MatchSubstringOptions& options,
                                 bool is_utf8 = true, bool literal = false)
      : options_(options),
        regex_match_(options_.pattern,
                     MakeRE2Options(is_utf8, options.ignore_case, literal)) {}

  bool Match(std::string_view current) const {
    auto piece = re2::StringPiece(current.data(), current.length());
    return RE2::PartialMatch(piece, regex_match_);
  }
};
#endif

template <typename Type, typename Matcher>
struct MatchSubstringImpl {
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out,
                     const Matcher* matcher) {
    StringBoolTransform<Type>(
        ctx, batch,
        [&matcher](const void* raw_offsets, const uint8_t* data, int64_t length,
                   int64_t output_offset, uint8_t* output) {
          const offset_type* offsets = reinterpret_cast<const offset_type*>(raw_offsets);
          FirstTimeBitmapWriter bitmap_writer(output, output_offset, length);
          for (int64_t i = 0; i < length; ++i) {
            const char* current_data = reinterpret_cast<const char*>(data + offsets[i]);
            int64_t current_length = offsets[i + 1] - offsets[i];
            if (matcher->Match(std::string_view(current_data, current_length))) {
              bitmap_writer.Set();
            }
            bitmap_writer.Next();
          }
          bitmap_writer.Finish();
        },
        out);
    return Status::OK();
  }
};

template <typename Type, typename Matcher>
struct MatchSubstring {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // TODO Cache matcher across invocations (for regex compilation)
    ARROW_ASSIGN_OR_RAISE(auto matcher, Matcher::Make(MatchSubstringState::Get(ctx)));
    return MatchSubstringImpl<Type, Matcher>::Exec(ctx, batch, out, matcher.get());
  }
};

#ifdef ARROW_WITH_RE2
template <typename Type>
struct MatchSubstring<Type, RegexSubstringMatcher> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // TODO Cache matcher across invocations (for regex compilation)
    ARROW_ASSIGN_OR_RAISE(auto matcher,
                          RegexSubstringMatcher::Make(MatchSubstringState::Get(ctx),
                                                      /*is_utf8=*/Type::is_utf8));
    return MatchSubstringImpl<Type, RegexSubstringMatcher>::Exec(ctx, batch, out,
                                                                 matcher.get());
  }
};
#endif

template <typename Type>
struct MatchSubstring<Type, PlainSubstringMatcher> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    auto options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      ARROW_ASSIGN_OR_RAISE(
          auto matcher, RegexSubstringMatcher::Make(options, /*is_utf8=*/Type::is_utf8,
                                                    /*literal=*/true));
      return MatchSubstringImpl<Type, RegexSubstringMatcher>::Exec(ctx, batch, out,
                                                                   matcher.get());
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    ARROW_ASSIGN_OR_RAISE(auto matcher, PlainSubstringMatcher::Make(options));
    return MatchSubstringImpl<Type, PlainSubstringMatcher>::Exec(ctx, batch, out,
                                                                 matcher.get());
  }
};

template <typename Type>
struct MatchSubstring<Type, PlainStartsWithMatcher> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    auto options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      MatchSubstringOptions converted_options = options;
      converted_options.pattern = "^" + RE2::QuoteMeta(options.pattern);
      ARROW_ASSIGN_OR_RAISE(
          auto matcher,
          RegexSubstringMatcher::Make(converted_options, /*is_utf8=*/Type::is_utf8));
      return MatchSubstringImpl<Type, RegexSubstringMatcher>::Exec(ctx, batch, out,
                                                                   matcher.get());
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    ARROW_ASSIGN_OR_RAISE(auto matcher, PlainStartsWithMatcher::Make(options));
    return MatchSubstringImpl<Type, PlainStartsWithMatcher>::Exec(ctx, batch, out,
                                                                  matcher.get());
  }
};

template <typename Type>
struct MatchSubstring<Type, PlainEndsWithMatcher> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    auto options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      MatchSubstringOptions converted_options = options;
      converted_options.pattern = RE2::QuoteMeta(options.pattern) + "$";
      ARROW_ASSIGN_OR_RAISE(
          auto matcher,
          RegexSubstringMatcher::Make(converted_options, /*is_utf8=*/Type::is_utf8));
      return MatchSubstringImpl<Type, RegexSubstringMatcher>::Exec(ctx, batch, out,
                                                                   matcher.get());
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    ARROW_ASSIGN_OR_RAISE(auto matcher, PlainEndsWithMatcher::Make(options));
    return MatchSubstringImpl<Type, PlainEndsWithMatcher>::Exec(ctx, batch, out,
                                                                matcher.get());
  }
};

#ifdef ARROW_WITH_RE2

// SQL LIKE match

/// Convert a SQL-style LIKE pattern (using '%' and '_') into a regex pattern
std::string MakeLikeRegex(const MatchSubstringOptions& options) {
  // Allow . to match \n
  std::string like_pattern = "(?s:^";
  like_pattern.reserve(options.pattern.size() + 7);
  bool escaped = false;
  for (const char c : options.pattern) {
    if (!escaped && c == '%') {
      like_pattern.append(".*");
    } else if (!escaped && c == '_') {
      like_pattern.append(".");
    } else if (!escaped && c == '\\') {
      escaped = true;
    } else {
      switch (c) {
        case '.':
        case '?':
        case '+':
        case '*':
        case '^':
        case '$':
        case '\\':
        case '[':
        case '{':
        case '(':
        case ')':
        case '|': {
          like_pattern.push_back('\\');
          like_pattern.push_back(c);
          escaped = false;
          break;
        }
        default: {
          like_pattern.push_back(c);
          escaped = false;
          break;
        }
      }
    }
  }
  like_pattern.append("$)");
  return like_pattern;
}

// Evaluate a SQL-like LIKE pattern by translating it to a regexp or
// substring search as appropriate. See what Apache Impala does:
// https://github.com/apache/impala/blob/9c38568657d62b6f6d7b10aa1c721ba843374dd8/be/src/exprs/like-predicate.cc
template <typename StringType>
struct MatchLike {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // NOTE: avoid making those constants global to avoid compiling regexes at startup
    static const RE2::Options kRE2Options = MakeRE2Options<StringType>();
    // A LIKE pattern matching this regex can be translated into a substring search.
    static const RE2 kLikePatternIsSubstringMatch(R"(%+([^%_]*[^\\%_])?%+)", kRE2Options);
    // A LIKE pattern matching this regex can be translated into a prefix search.
    static const RE2 kLikePatternIsStartsWith(R"(([^%_]*[^\\%_])?%+)", kRE2Options);
    // A LIKE pattern matching this regex can be translated into a suffix search.
    static const RE2 kLikePatternIsEndsWith(R"(%+([^%_]*))", kRE2Options);
    static bool global_checked = false;
    if (ARROW_PREDICT_FALSE(!global_checked)) {
      RETURN_NOT_OK(RegexStatus(kLikePatternIsSubstringMatch));
      RETURN_NOT_OK(RegexStatus(kLikePatternIsStartsWith));
      RETURN_NOT_OK(RegexStatus(kLikePatternIsEndsWith));
      global_checked = true;
    }

    auto original_options = MatchSubstringState::Get(ctx);
    auto original_state = ctx->state();

    Status status;
    std::string pattern;
    bool matched = false;
    if (!original_options.ignore_case) {
      if ((matched = RE2::FullMatch(original_options.pattern,
                                    kLikePatternIsSubstringMatch, &pattern))) {
        MatchSubstringOptions converted_options{pattern, original_options.ignore_case};
        MatchSubstringState converted_state(converted_options);
        ctx->SetState(&converted_state);
        status = MatchSubstring<StringType, PlainSubstringMatcher>::Exec(ctx, batch, out);
      } else if ((matched = RE2::FullMatch(original_options.pattern,
                                           kLikePatternIsStartsWith, &pattern))) {
        MatchSubstringOptions converted_options{pattern, original_options.ignore_case};
        MatchSubstringState converted_state(converted_options);
        ctx->SetState(&converted_state);
        status =
            MatchSubstring<StringType, PlainStartsWithMatcher>::Exec(ctx, batch, out);
      } else if ((matched = RE2::FullMatch(original_options.pattern,
                                           kLikePatternIsEndsWith, &pattern))) {
        MatchSubstringOptions converted_options{pattern, original_options.ignore_case};
        MatchSubstringState converted_state(converted_options);
        ctx->SetState(&converted_state);
        status = MatchSubstring<StringType, PlainEndsWithMatcher>::Exec(ctx, batch, out);
      }
    }

    if (!matched) {
      MatchSubstringOptions converted_options{MakeLikeRegex(original_options),
                                              original_options.ignore_case};
      MatchSubstringState converted_state(converted_options);
      ctx->SetState(&converted_state);
      status = MatchSubstring<StringType, RegexSubstringMatcher>::Exec(ctx, batch, out);
    }
    ctx->SetState(original_state);
    return status;
  }
};

#endif

const FunctionDoc match_substring_doc(
    "Match strings against literal pattern",
    ("For each string in `strings`, emit true iff it contains a given pattern.\n"
     "Null inputs emit null.\n"
     "The pattern must be given in MatchSubstringOptions.\n"
     "If ignore_case is set, only simple case folding is performed."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);

const FunctionDoc starts_with_doc(
    "Check if strings start with a literal pattern",
    ("For each string in `strings`, emit true iff it starts with a given pattern.\n"
     "The pattern must be given in MatchSubstringOptions.\n"
     "If ignore_case is set, only simple case folding is performed.\n"
     "\n"
     "Null inputs emit null."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);

const FunctionDoc ends_with_doc(
    "Check if strings end with a literal pattern",
    ("For each string in `strings`, emit true iff it ends with a given pattern.\n"
     "The pattern must be given in MatchSubstringOptions.\n"
     "If ignore_case is set, only simple case folding is performed.\n"
     "\n"
     "Null inputs emit null."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);

#ifdef ARROW_WITH_RE2
const FunctionDoc match_substring_regex_doc(
    "Match strings against regex pattern",
    ("For each string in `strings`, emit true iff it matches a given pattern\n"
     "at any position. The pattern must be given in MatchSubstringOptions.\n"
     "If ignore_case is set, only simple case folding is performed.\n"
     "\n"
     "Null inputs emit null."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);

const FunctionDoc match_like_doc(
    "Match strings against SQL-style LIKE pattern",
    ("For each string in `strings`, emit true iff it matches a given pattern\n"
     "at any position. '%' will match any number of characters, '_' will\n"
     "match exactly one character, and any other character matches itself.\n"
     "To match a literal '%', '_', or '\\', precede the character with a backslash.\n"
     "Null inputs emit null.  The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);
#endif

void AddAsciiStringMatchSubstring(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("match_substring", Arity::Unary(),
                                                 match_substring_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto exec = GenerateVarBinaryToVarBinary<MatchSubstring, PlainSubstringMatcher>(ty);
      DCHECK_OK(
          func->AddKernel({ty}, boolean(), std::move(exec), MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func =
        std::make_shared<ScalarFunction>("starts_with", Arity::Unary(), starts_with_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto exec =
          GenerateVarBinaryToVarBinary<MatchSubstring, PlainStartsWithMatcher>(ty);
      DCHECK_OK(
          func->AddKernel({ty}, boolean(), std::move(exec), MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func =
        std::make_shared<ScalarFunction>("ends_with", Arity::Unary(), ends_with_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto exec = GenerateVarBinaryToVarBinary<MatchSubstring, PlainEndsWithMatcher>(ty);
      DCHECK_OK(
          func->AddKernel({ty}, boolean(), std::move(exec), MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#ifdef ARROW_WITH_RE2
  {
    auto func = std::make_shared<ScalarFunction>("match_substring_regex", Arity::Unary(),
                                                 match_substring_regex_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto exec = GenerateVarBinaryToVarBinary<MatchSubstring, RegexSubstringMatcher>(ty);
      DCHECK_OK(
          func->AddKernel({ty}, boolean(), std::move(exec), MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func =
        std::make_shared<ScalarFunction>("match_like", Arity::Unary(), match_like_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto exec = GenerateVarBinaryToVarBinary<MatchLike>(ty);
      DCHECK_OK(
          func->AddKernel({ty}, boolean(), std::move(exec), MatchSubstringState::Init));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#endif
}

// ----------------------------------------------------------------------
// Substring find - lfind/index/etc.

struct FindSubstring {
  const PlainSubstringMatcher matcher_;

  explicit FindSubstring(PlainSubstringMatcher matcher) : matcher_(std::move(matcher)) {}

  template <typename OutValue, typename... Ignored>
  OutValue Call(KernelContext*, std::string_view val, Status*) const {
    return static_cast<OutValue>(matcher_.Find(val));
  }
};

#ifdef ARROW_WITH_RE2
struct FindSubstringRegex {
  std::unique_ptr<RE2> regex_match_;

  static Result<FindSubstringRegex> Make(const MatchSubstringOptions& options,
                                         bool is_utf8 = true, bool literal = false) {
    auto matcher = FindSubstringRegex(options, is_utf8, literal);
    RETURN_NOT_OK(RegexStatus(*matcher.regex_match_));
    return std::move(matcher);
  }

  explicit FindSubstringRegex(const MatchSubstringOptions& options, bool is_utf8 = true,
                              bool literal = false) {
    std::string regex = "(";
    regex.reserve(options.pattern.length() + 2);
    regex += literal ? RE2::QuoteMeta(options.pattern) : options.pattern;
    regex += ")";
    regex_match_ = std::make_unique<RE2>(
        regex, MakeRE2Options(is_utf8, options.ignore_case, /*literal=*/false));
  }

  template <typename OutValue, typename... Ignored>
  OutValue Call(KernelContext*, std::string_view val, Status*) const {
    re2::StringPiece piece(val.data(), val.length());
    re2::StringPiece match;
    if (RE2::PartialMatch(piece, *regex_match_, &match)) {
      return static_cast<OutValue>(match.data() - piece.data());
    }
    return -1;
  }
};
#endif

template <typename InputType>
struct FindSubstringExec {
  using OffsetType = typename TypeTraits<InputType>::OffsetType;
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const MatchSubstringOptions& options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      ARROW_ASSIGN_OR_RAISE(auto matcher,
                            FindSubstringRegex::Make(options, InputType::is_utf8, true));
      applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, FindSubstringRegex>
          kernel{std::move(matcher)};
      return kernel.Exec(ctx, batch, out);
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, FindSubstring> kernel{
        FindSubstring(PlainSubstringMatcher(options))};
    return kernel.Exec(ctx, batch, out);
  }
};

const FunctionDoc find_substring_doc(
    "Find first occurrence of substring",
    ("For each string in `strings`, emit the index in bytes of the first occurrence\n"
     "of the given literal pattern, or -1 if not found.\n"
     "Null inputs emit null. The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);

#ifdef ARROW_WITH_RE2
template <typename InputType>
struct FindSubstringRegexExec {
  using OffsetType = typename TypeTraits<InputType>::OffsetType;
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const MatchSubstringOptions& options = MatchSubstringState::Get(ctx);
    ARROW_ASSIGN_OR_RAISE(auto matcher, FindSubstringRegex::Make(options, false));
    applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, FindSubstringRegex>
        kernel{std::move(matcher)};
    return kernel.Exec(ctx, batch, out);
  }
};

const FunctionDoc find_substring_regex_doc(
    "Find location of first match of regex pattern",
    ("For each string in `strings`, emit the index in bytes of the first occurrence\n"
     "of the given literal pattern, or -1 if not found.\n"
     "Null inputs emit null. The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);
#endif

void AddAsciiStringFindSubstring(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("find_substring", Arity::Unary(),
                                                 find_substring_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto offset_type = offset_bit_width(ty->id()) == 64 ? int64() : int32();
      DCHECK_OK(func->AddKernel({ty}, offset_type,
                                GenerateVarBinaryToVarBinary<FindSubstringExec>(ty),
                                MatchSubstringState::Init));
    }
    DCHECK_OK(func->AddKernel({InputType(Type::FIXED_SIZE_BINARY)}, int32(),
                              FindSubstringExec<FixedSizeBinaryType>::Exec,
                              MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#ifdef ARROW_WITH_RE2
  {
    auto func = std::make_shared<ScalarFunction>("find_substring_regex", Arity::Unary(),
                                                 find_substring_regex_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto offset_type = offset_bit_width(ty->id()) == 64 ? int64() : int32();
      DCHECK_OK(func->AddKernel({ty}, offset_type,
                                GenerateVarBinaryToVarBinary<FindSubstringRegexExec>(ty),
                                MatchSubstringState::Init));
    }
    DCHECK_OK(func->AddKernel({InputType(Type::FIXED_SIZE_BINARY)}, int32(),
                              FindSubstringRegexExec<FixedSizeBinaryType>::Exec,
                              MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#endif
}

// ----------------------------------------------------------------------
// Substring count

struct CountSubstring {
  const PlainSubstringMatcher matcher_;

  explicit CountSubstring(PlainSubstringMatcher matcher) : matcher_(std::move(matcher)) {}

  template <typename OutValue, typename... Ignored>
  OutValue Call(KernelContext*, std::string_view val, Status*) const {
    OutValue count = 0;
    uint64_t start = 0;
    const auto pattern_size = std::max<uint64_t>(1, matcher_.options_.pattern.size());
    while (start <= val.size()) {
      const int64_t index = matcher_.Find(val.substr(start));
      if (index >= 0) {
        count++;
        start += index + pattern_size;
      } else {
        break;
      }
    }
    return count;
  }
};

#ifdef ARROW_WITH_RE2
struct CountSubstringRegex {
  std::unique_ptr<RE2> regex_match_;

  explicit CountSubstringRegex(const MatchSubstringOptions& options, bool is_utf8 = true,
                               bool literal = false)
      : regex_match_(new RE2(options.pattern,
                             MakeRE2Options(is_utf8, options.ignore_case, literal))) {}

  static Result<CountSubstringRegex> Make(const MatchSubstringOptions& options,
                                          bool is_utf8 = true, bool literal = false) {
    CountSubstringRegex counter(options, is_utf8, literal);
    RETURN_NOT_OK(RegexStatus(*counter.regex_match_));
    return std::move(counter);
  }

  template <typename OutValue, typename... Ignored>
  OutValue Call(KernelContext*, std::string_view val, Status*) const {
    OutValue count = 0;
    re2::StringPiece input(val.data(), val.size());
    auto last_size = input.size();
    while (RE2::FindAndConsume(&input, *regex_match_)) {
      count++;
      if (last_size == input.size()) {
        // 0-length match
        if (input.size() > 0) {
          input.remove_prefix(1);
        } else {
          break;
        }
      }
      last_size = input.size();
    }
    return count;
  }
};

template <typename InputType>
struct CountSubstringRegexExec {
  using OffsetType = typename TypeTraits<InputType>::OffsetType;
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const MatchSubstringOptions& options = MatchSubstringState::Get(ctx);
    ARROW_ASSIGN_OR_RAISE(
        auto counter, CountSubstringRegex::Make(options, /*is_utf8=*/InputType::is_utf8));
    applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, CountSubstringRegex>
        kernel{std::move(counter)};
    return kernel.Exec(ctx, batch, out);
  }
};
#endif

template <typename InputType>
struct CountSubstringExec {
  using OffsetType = typename TypeTraits<InputType>::OffsetType;
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const MatchSubstringOptions& options = MatchSubstringState::Get(ctx);
    if (options.ignore_case) {
#ifdef ARROW_WITH_RE2
      ARROW_ASSIGN_OR_RAISE(
          auto counter, CountSubstringRegex::Make(options, /*is_utf8=*/InputType::is_utf8,
                                                  /*literal=*/true));
      applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, CountSubstringRegex>
          kernel{std::move(counter)};
      return kernel.Exec(ctx, batch, out);
#else
      return Status::NotImplemented("ignore_case requires RE2");
#endif
    }
    applicator::ScalarUnaryNotNullStateful<OffsetType, InputType, CountSubstring> kernel{
        CountSubstring(PlainSubstringMatcher(options))};
    return kernel.Exec(ctx, batch, out);
  }
};

const FunctionDoc count_substring_doc(
    "Count occurrences of substring",
    ("For each string in `strings`, emit the number of occurrences of the given\n"
     "literal pattern.\n"
     "Null inputs emit null. The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);

#ifdef ARROW_WITH_RE2
const FunctionDoc count_substring_regex_doc(
    "Count occurrences of substring",
    ("For each string in `strings`, emit the number of occurrences of the given\n"
     "regular expression pattern.\n"
     "Null inputs emit null. The pattern must be given in MatchSubstringOptions."),
    {"strings"}, "MatchSubstringOptions", /*options_required=*/true);
#endif

void AddAsciiStringCountSubstring(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("count_substring", Arity::Unary(),
                                                 count_substring_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto offset_type = offset_bit_width(ty->id()) == 64 ? int64() : int32();
      DCHECK_OK(func->AddKernel({ty}, offset_type,
                                GenerateVarBinaryToVarBinary<CountSubstringExec>(ty),
                                MatchSubstringState::Init));
    }
    DCHECK_OK(func->AddKernel({InputType(Type::FIXED_SIZE_BINARY)}, int32(),
                              CountSubstringExec<FixedSizeBinaryType>::Exec,
                              MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#ifdef ARROW_WITH_RE2
  {
    auto func = std::make_shared<ScalarFunction>("count_substring_regex", Arity::Unary(),
                                                 count_substring_regex_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto offset_type = offset_bit_width(ty->id()) == 64 ? int64() : int32();
      DCHECK_OK(func->AddKernel({ty}, offset_type,
                                GenerateVarBinaryToVarBinary<CountSubstringRegexExec>(ty),
                                MatchSubstringState::Init));
    }
    DCHECK_OK(func->AddKernel({InputType(Type::FIXED_SIZE_BINARY)}, int32(),
                              CountSubstringRegexExec<FixedSizeBinaryType>::Exec,
                              MatchSubstringState::Init));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#endif
}

// ----------------------------------------------------------------------
// Replace substring (plain, regex)

using ReplaceState = OptionsWrapper<ReplaceSubstringOptions>;

template <typename Type, typename Replacer>
struct ReplaceSubstring {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using offset_type = typename Type::offset_type;
  using ValueDataBuilder = TypedBufferBuilder<uint8_t>;
  using OffsetBuilder = TypedBufferBuilder<offset_type>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // TODO Cache replacer across invocations (for regex compilation)
    ARROW_ASSIGN_OR_RAISE(auto replacer, Replacer::Make(ReplaceState::Get(ctx)));
    return Replace(ctx, batch, *replacer, out);
  }

  static Status Replace(KernelContext* ctx, const ExecSpan& batch,
                        const Replacer& replacer, ExecResult* out) {
    ValueDataBuilder value_data_builder(ctx->memory_pool());
    OffsetBuilder offset_builder(ctx->memory_pool());

    // We already know how many strings we have, so we can use Reserve/UnsafeAppend
    RETURN_NOT_OK(offset_builder.Reserve(batch.length + 1));
    offset_builder.UnsafeAppend(0);  // offsets start at 0

    RETURN_NOT_OK(VisitArraySpanInline<Type>(
        batch[0].array,
        [&](std::string_view s) {
          RETURN_NOT_OK(replacer.ReplaceString(s, &value_data_builder));
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
    ArrayData* output = out->array_data().get();
    RETURN_NOT_OK(value_data_builder.Finish(&output->buffers[2]));
    return offset_builder.Finish(&output->buffers[1]);
  }
};

struct PlainSubstringReplacer {
  const ReplaceSubstringOptions& options_;

  static Result<std::unique_ptr<PlainSubstringReplacer>> Make(
      const ReplaceSubstringOptions& options) {
    return std::make_unique<PlainSubstringReplacer>(options);
  }

  explicit PlainSubstringReplacer(const ReplaceSubstringOptions& options)
      : options_(options) {}

  Status ReplaceString(std::string_view s, TypedBufferBuilder<uint8_t>* builder) const {
    if (s.empty()) {
      // Special-case empty input as s.data() may not be a valid pointer
      return Status::OK();
    }
    const char* i = s.data();
    const char* end = s.data() + s.length();
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
    return builder->Append(reinterpret_cast<const uint8_t*>(i),
                           static_cast<int64_t>(end - i));
  }
};

#ifdef ARROW_WITH_RE2
template <typename Type>
struct RegexSubstringReplacer {
  const ReplaceSubstringOptions& options_;
  const RE2 regex_find_;
  const RE2 regex_replacement_;

  static Result<std::unique_ptr<RegexSubstringReplacer>> Make(
      const ReplaceSubstringOptions& options) {
    auto replacer = std::make_unique<RegexSubstringReplacer>(options);

    RETURN_NOT_OK(RegexStatus(replacer->regex_find_));
    RETURN_NOT_OK(RegexStatus(replacer->regex_replacement_));

    std::string replacement_error;
    if (!replacer->regex_replacement_.CheckRewriteString(replacer->options_.replacement,
                                                         &replacement_error)) {
      return Status::Invalid("Invalid replacement string: ",
                             std::move(replacement_error));
    }

    return std::move(replacer);
  }

  // Using RE2::FindAndConsume we can only find the pattern if it is a group, therefore
  // we have 2 regexes, one with () around it, one without.
  explicit RegexSubstringReplacer(const ReplaceSubstringOptions& options)
      : options_(options),
        regex_find_("(" + options_.pattern + ")", MakeRE2Options<Type>()),
        regex_replacement_(options_.pattern, MakeRE2Options<Type>()) {}

  Status ReplaceString(std::string_view s, TypedBufferBuilder<uint8_t>* builder) const {
    re2::StringPiece replacement(options_.replacement);

    // If s is empty, then it's essentially global
    if (options_.max_replacements == -1 || s.empty()) {
      std::string s_copy(s);
      RE2::GlobalReplace(&s_copy, regex_replacement_, replacement);
      return builder->Append(reinterpret_cast<const uint8_t*>(s_copy.data()),
                             s_copy.length());
    }

    // Since RE2 does not have the concept of max_replacements, we have to do some work
    // ourselves.
    // We might do this faster similar to RE2::GlobalReplace using Match and Rewrite
    const char* i = s.data();
    const char* end = s.data() + s.length();
    re2::StringPiece piece(s.data(), s.length());

    int64_t max_replacements = options_.max_replacements;
    while ((i < end) && (max_replacements != 0)) {
      std::string found;
      if (!RE2::FindAndConsume(&piece, regex_find_, &found)) {
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
        if (!RE2::Replace(&found, regex_replacement_, replacement)) {
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
    return builder->Append(reinterpret_cast<const uint8_t*>(i),
                           static_cast<int64_t>(end - i));
  }
};
#endif

template <typename Type>
using ReplaceSubstringPlain = ReplaceSubstring<Type, PlainSubstringReplacer>;

const FunctionDoc replace_substring_doc(
    "Replace matching non-overlapping substrings with replacement",
    ("For each string in `strings`, replace non-overlapping substrings that match\n"
     "the given literal `pattern` with the given `replacement`.\n"
     "If `max_replacements` is given and not equal to -1, it limits the\n"
     "maximum amount replacements per input, counted from the left.\n"
     "Null values emit null."),
    {"strings"}, "ReplaceSubstringOptions", /*options_required=*/true);

#ifdef ARROW_WITH_RE2
template <typename Type>
using ReplaceSubstringRegex = ReplaceSubstring<Type, RegexSubstringReplacer<Type>>;

const FunctionDoc replace_substring_regex_doc(
    "Replace matching non-overlapping substrings with replacement",
    ("For each string in `strings`, replace non-overlapping substrings that match\n"
     "the given regular expression `pattern` with the given `replacement`.\n"
     "If `max_replacements` is given and not equal to -1, it limits the\n"
     "maximum amount replacements per input, counted from the left.\n"
     "Null values emit null."),
    {"strings"}, "ReplaceSubstringOptions", /*options_required=*/true);
#endif

void AddAsciiStringReplaceSubstring(FunctionRegistry* registry) {
  {
    auto func = std::make_shared<ScalarFunction>("replace_substring", Arity::Unary(),
                                                 replace_substring_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto exec = GenerateVarBinaryToVarBinary<ReplaceSubstringPlain>(ty);
      ScalarKernel kernel{{ty}, ty, std::move(exec), ReplaceState::Init};
      kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
      DCHECK_OK(func->AddKernel(std::move(kernel)));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#ifdef ARROW_WITH_RE2
  {
    auto func = std::make_shared<ScalarFunction>(
        "replace_substring_regex", Arity::Unary(), replace_substring_regex_doc);
    for (const auto& ty : BaseBinaryTypes()) {
      auto exec = GenerateVarBinaryToVarBinary<ReplaceSubstringRegex>(ty);
      ScalarKernel kernel{{ty}, ty, std::move(exec), ReplaceState::Init};
      kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
      DCHECK_OK(func->AddKernel(std::move(kernel)));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
#endif
}

// ----------------------------------------------------------------------
// Extract with regex

#ifdef ARROW_WITH_RE2

using ExtractRegexState = OptionsWrapper<ExtractRegexOptions>;

// TODO cache this once per ExtractRegexOptions
struct ExtractRegexData {
  // Use unique_ptr<> because RE2 is non-movable (for ARROW_ASSIGN_OR_RAISE)
  std::unique_ptr<RE2> regex;
  std::vector<std::string> group_names;

  static Result<ExtractRegexData> Make(const ExtractRegexOptions& options,
                                       bool is_utf8 = true) {
    ExtractRegexData data(options.pattern, is_utf8);
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

  Result<TypeHolder> ResolveOutputType(const std::vector<TypeHolder>& types) const {
    const DataType* input_type = types[0].type;
    if (input_type == nullptr) {
      // No input type specified
      return nullptr;
    }
    // Input type is either [Large]Binary or [Large]String and is also the type
    // of each field in the output struct type.
    DCHECK(is_base_binary_like(input_type->id()));
    FieldVector fields;
    fields.reserve(group_names.size());
    std::shared_ptr<DataType> owned_type = input_type->GetSharedPtr();
    std::transform(group_names.begin(), group_names.end(), std::back_inserter(fields),
                   [&](const std::string& name) { return field(name, owned_type); });
    return struct_(std::move(fields));
  }

 private:
  explicit ExtractRegexData(const std::string& pattern, bool is_utf8 = true)
      : regex(new RE2(pattern, MakeRE2Options(is_utf8))) {}
};

Result<TypeHolder> ResolveExtractRegexOutput(KernelContext* ctx,
                                             const std::vector<TypeHolder>& types) {
  ExtractRegexOptions options = ExtractRegexState::Get(ctx);
  ARROW_ASSIGN_OR_RAISE(auto data, ExtractRegexData::Make(options));
  return data.ResolveOutputType(types);
}

struct ExtractRegexBase {
  const ExtractRegexData& data;
  const int group_count;
  std::vector<re2::StringPiece> found_values;
  std::vector<RE2::Arg> args;
  std::vector<const RE2::Arg*> args_pointers;
  const RE2::Arg** args_pointers_start;
  const RE2::Arg* null_arg = nullptr;

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

  bool Match(std::string_view s) {
    return RE2::PartialMatchN(ToStringPiece(s), *data.regex, args_pointers_start,
                              group_count);
  }
};

template <typename Type>
struct ExtractRegex : public ExtractRegexBase {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using ExtractRegexBase::ExtractRegexBase;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    ExtractRegexOptions options = ExtractRegexState::Get(ctx);
    ARROW_ASSIGN_OR_RAISE(auto data, ExtractRegexData::Make(options, Type::is_utf8));
    return ExtractRegex{data}.Extract(ctx, batch, out);
  }

  Status Extract(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    // TODO: why is this needed? Type resolution should already be
    // done and the output type set in the output variable
    ARROW_ASSIGN_OR_RAISE(TypeHolder out_type, data.ResolveOutputType(batch.GetTypes()));
    DCHECK_NE(out_type.type, nullptr);
    std::shared_ptr<DataType> type = out_type.GetSharedPtr();

    std::unique_ptr<ArrayBuilder> array_builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), type, &array_builder));
    StructBuilder* struct_builder = checked_cast<StructBuilder*>(array_builder.get());

    std::vector<BuilderType*> field_builders;
    field_builders.reserve(group_count);
    for (int i = 0; i < group_count; i++) {
      field_builders.push_back(
          checked_cast<BuilderType*>(struct_builder->field_builder(i)));
    }

    auto visit_null = [&]() { return struct_builder->AppendNull(); };
    auto visit_value = [&](std::string_view s) {
      if (Match(s)) {
        for (int i = 0; i < group_count; i++) {
          RETURN_NOT_OK(field_builders[i]->Append(ToStringView(found_values[i])));
        }
        return struct_builder->Append();
      } else {
        return struct_builder->AppendNull();
      }
    };
    RETURN_NOT_OK(VisitArraySpanInline<Type>(batch[0].array, visit_value, visit_null));

    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(struct_builder->Finish(&out_array));
    out->value = std::move(out_array->data());
    return Status::OK();
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
    {"strings"}, "ExtractRegexOptions", /*options_required=*/true);

void AddAsciiStringExtractRegex(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("extract_regex", Arity::Unary(),
                                               extract_regex_doc);
  OutputType out_ty(ResolveExtractRegexOutput);
  for (const auto& ty : BaseBinaryTypes()) {
    ScalarKernel kernel{{ty},
                        out_ty,
                        GenerateVarBinaryToVarBinary<ExtractRegex>(ty),
                        ExtractRegexState::Init};
    // Null values will be computed based on regex match or not
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    DCHECK_OK(func->AddKernel(kernel));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}
#endif  // ARROW_WITH_RE2

// ----------------------------------------------------------------------
// Replace slice

struct BinaryReplaceSliceTransform : ReplaceStringSliceTransformBase {
  using ReplaceStringSliceTransformBase::ReplaceStringSliceTransformBase;
  int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                    uint8_t* output) {
    const auto& opts = *options;
    int64_t before_slice = 0;
    int64_t after_slice = 0;
    uint8_t* output_start = output;

    if (opts.start >= 0) {
      // Count from left
      before_slice = std::min<int64_t>(input_string_ncodeunits, opts.start);
    } else {
      // Count from right
      before_slice = std::max<int64_t>(0, input_string_ncodeunits + opts.start);
    }
    // Mimic Pandas: if stop would be before start, treat as 0-length slice
    if (opts.stop >= 0) {
      // Count from left
      after_slice =
          std::min<int64_t>(input_string_ncodeunits, std::max(before_slice, opts.stop));
    } else {
      // Count from right
      after_slice = std::max<int64_t>(before_slice, input_string_ncodeunits + opts.stop);
    }
    output = std::copy(input, input + before_slice, output);
    output = std::copy(opts.replacement.begin(), opts.replacement.end(), output);
    output = std::copy(input + after_slice, input + input_string_ncodeunits, output);
    return output - output_start;
  }

  static int32_t FixedOutputSize(const ReplaceSliceOptions& opts, int32_t input_width) {
    int32_t before_slice = 0;
    int32_t after_slice = 0;
    const int32_t start = static_cast<int32_t>(opts.start);
    const int32_t stop = static_cast<int32_t>(opts.stop);
    if (opts.start >= 0) {
      // Count from left
      before_slice = std::min<int32_t>(input_width, start);
    } else {
      // Count from right
      before_slice = std::max<int32_t>(0, input_width + start);
    }
    if (opts.stop >= 0) {
      // Count from left
      after_slice = std::min<int32_t>(input_width, std::max<int32_t>(before_slice, stop));
    } else {
      // Count from right
      after_slice = std::max<int32_t>(before_slice, input_width + stop);
    }
    return static_cast<int32_t>(before_slice + opts.replacement.size() +
                                (input_width - after_slice));
  }
};

template <typename Type>
using BinaryReplaceSlice =
    StringTransformExecWithState<Type, BinaryReplaceSliceTransform>;

const FunctionDoc binary_replace_slice_doc(
    "Replace a slice of a binary string",
    ("For each string in `strings`, replace a slice of the string defined by `start`\n"
     "and `stop` indices with the given `replacement`. `start` is inclusive\n"
     "and `stop` is exclusive, and both are measured in bytes.\n"
     "Null values emit null."),
    {"strings"}, "ReplaceSliceOptions", /*options_required=*/true);

void AddAsciiStringReplaceSlice(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("binary_replace_slice", Arity::Unary(),
                                               binary_replace_slice_doc);
  for (const auto& ty : BaseBinaryTypes()) {
    DCHECK_OK(func->AddKernel({ty}, ty,
                              GenerateTypeAgnosticVarBinaryBase<BinaryReplaceSlice>(ty),
                              ReplaceStringSliceTransformBase::State::Init));
  }
  using TransformExec =
      FixedSizeBinaryTransformExecWithState<BinaryReplaceSliceTransform>;
  ScalarKernel fsb_kernel({InputType(Type::FIXED_SIZE_BINARY)},
                          OutputType(TransformExec::OutputType), TransformExec::Exec,
                          ReplaceStringSliceTransformBase::State::Init);
  fsb_kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(std::move(fsb_kernel)));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// Slice

namespace {
struct SliceBytesTransform : StringSliceTransformBase {
  int64_t MaxCodeunits(int64_t ninputs, int64_t input_bytes) override {
    const SliceOptions& opt = *this->options;
    if ((opt.start >= 0) != (opt.stop >= 0)) {
      // If start and stop don't have the same sign, we can't guess an upper bound
      // on the resulting slice lengths, so return a worst case estimate.
      return input_bytes;
    }
    int64_t max_slice_bytes = (opt.stop - opt.start + opt.step - 1) / opt.step;
    return std::min(input_bytes, ninputs * std::max<int64_t>(0, max_slice_bytes));
  }

  int64_t Transform(const uint8_t* input, int64_t input_string_bytes, uint8_t* output) {
    if (options->step >= 1) {
      return SliceForward(input, input_string_bytes, output);
    }
    return SliceBackward(input, input_string_bytes, output);
  }

  int64_t SliceForward(const uint8_t* input, int64_t input_string_bytes,
                       uint8_t* output) {
    // Slice in forward order (step > 0)
    const SliceOptions& opt = *this->options;
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_bytes;
    const uint8_t* begin_sliced;
    const uint8_t* end_sliced;

    if (!input_string_bytes) {
      return 0;
    }
    // First, compute begin_sliced and end_sliced
    if (opt.start >= 0) {
      // start counting from the left
      begin_sliced = std::min(begin + opt.start, end);
      if (opt.stop > opt.start) {
        // continue counting from begin_sliced
        const int64_t length = opt.stop - opt.start;
        end_sliced = std::min(begin_sliced + length, end);
      } else if (opt.stop < 0) {
        // from the end
        end_sliced = std::max(end + opt.stop, begin_sliced);
      } else {
        // zero length slice
        return 0;
      }
    } else {
      // start counting from the right
      begin_sliced = std::max(end + opt.start, begin);
      if (opt.stop > 0) {
        // continue counting from the left, we cannot start from begin_sliced because we
        // don't know how many bytes are between begin and begin_sliced
        end_sliced = std::min(begin + opt.stop, end);
        // and therefore we also need this
        if (end_sliced <= begin_sliced) {
          // zero length slice
          return 0;
        }
      } else if ((opt.stop < 0) && (opt.stop > opt.start)) {
        // stop is negative, but larger than start, so we count again from the right
        // in some cases we can optimize this, depending on the shortest path (from end
        // or begin_sliced), but begin_sliced and opt.start can be 'out of sync',
        // for instance when start=-100, when the string length is only 10.
        end_sliced = std::max(end + opt.stop, begin_sliced);
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
      *dest = *i;
      // skip step codeunits
      i += opt.step;
      dest++;
    }
    return dest - output;
  }

  int64_t SliceBackward(const uint8_t* input, int64_t input_string_bytes,
                        uint8_t* output) {
    // Slice in reverse order (step < 0)
    const SliceOptions& opt = *this->options;
    const uint8_t* begin = input;
    const uint8_t* end = input + input_string_bytes;
    const uint8_t* begin_sliced = begin;
    const uint8_t* end_sliced = end;

    if (!input_string_bytes) {
      return 0;
    }

    if (opt.start >= 0) {
      // +1 because begin_sliced acts as as the end of a reverse iterator
      begin_sliced = std::min(begin + opt.start + 1, end);
    } else {
      // -1 because start=-1 means the last byte, which is 0 advances
      begin_sliced = std::max(end + opt.start + 1, begin);
    }
    begin_sliced--;

    // similar to opt.start
    if (opt.stop >= 0) {
      end_sliced = std::min(begin + opt.stop + 1, end);
    } else {
      end_sliced = std::max(end + opt.stop + 1, begin);
    }
    end_sliced--;

    // Copy computed slice to output
    uint8_t* dest = output;
    const uint8_t* i = begin_sliced;
    while (i > end_sliced) {
      // write a single codepoint
      *dest = *i;
      // and skip the remainder
      i += opt.step;
      dest++;
    }

    return dest - output;
  }
};

template <typename Type>
using SliceBytes = StringTransformExec<Type, SliceBytesTransform>;

}  // namespace

const FunctionDoc binary_slice_doc(
    "Slice binary string",
    ("For each binary string in `strings`, emit the substring defined by\n"
     "(`start`, `stop`, `step`) as given by `SliceOptions` where `start` is\n"
     "inclusive and `stop` is exclusive. All three values are measured in\n"
     "bytes.\n"
     "If `step` is negative, the string will be advanced in reversed order.\n"
     "An error is raised if `step` is zero.\n"
     "Null inputs emit null."),
    {"strings"}, "SliceOptions", /*options_required=*/true);

void AddAsciiStringSlice(FunctionRegistry* registry) {
  auto func =
      std::make_shared<ScalarFunction>("binary_slice", Arity::Unary(), binary_slice_doc);
  for (const auto& ty : BinaryTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<SliceBytes>(ty);
    DCHECK_OK(
        func->AddKernel({ty}, ty, std::move(exec), SliceBytesTransform::State::Init));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// Split by pattern

using SplitPatternState = OptionsWrapper<SplitPatternOptions>;

struct SplitPatternFinder : public StringSplitFinderBase<SplitPatternOptions> {
  using Options = SplitPatternOptions;

  Status PreExec(const SplitPatternOptions& options) override {
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

template <typename Type, typename ListType>
using SplitPatternExec = StringSplitExec<Type, ListType, SplitPatternFinder>;

const FunctionDoc split_pattern_doc(
    "Split string according to separator",
    ("Split each string according to the exact `pattern` defined in\n"
     "SplitPatternOptions.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitPatternOptions."),
    {"strings"}, "SplitPatternOptions", /*options_required=*/true);

void AddAsciiStringSplitPattern(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("split_pattern", Arity::Unary(),
                                               split_pattern_doc);
  for (const auto& ty : BaseBinaryTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<SplitPatternExec, ListType>(ty);
    DCHECK_OK(
        func->AddKernel({ty}, {list(ty)}, std::move(exec), SplitPatternState::Init));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// Split by whitespace

struct SplitWhitespaceAsciiFinder : public StringSplitFinderBase<SplitOptions> {
  using Options = SplitOptions;

  static bool Find(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitOptions& options) {
    const uint8_t* i = begin;
    while (i < end) {
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

template <typename Type, typename ListType>
using SplitWhitespaceAsciiExec =
    StringSplitExec<Type, ListType, SplitWhitespaceAsciiFinder>;

const FunctionDoc ascii_split_whitespace_doc(
    "Split string according to any ASCII whitespace",
    ("Split each string according any non-zero length sequence of ASCII\n"
     "whitespace characters.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitOptions."),
    {"strings"}, "SplitOptions");

void AddAsciiStringSplitWhitespace(FunctionRegistry* registry) {
  static const SplitOptions default_options{};
  auto func =
      std::make_shared<ScalarFunction>("ascii_split_whitespace", Arity::Unary(),
                                       ascii_split_whitespace_doc, &default_options);

  for (const auto& ty : StringTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<SplitWhitespaceAsciiExec, ListType>(ty);
    DCHECK_OK(func->AddKernel({ty}, {list(ty)}, std::move(exec), StringSplitState::Init));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// Split by regex

#ifdef ARROW_WITH_RE2
template <typename Type>
struct SplitRegexFinder : public StringSplitFinderBase<SplitPatternOptions> {
  using Options = SplitPatternOptions;

  std::unique_ptr<RE2> regex_split;

  Status PreExec(const SplitPatternOptions& options) override {
    if (options.reverse) {
      return Status::NotImplemented("Cannot split in reverse with regex");
    }
    // RE2 does *not* give you the full match! Must wrap the regex in a capture group
    // There is FindAndConsume, but it would give only the end of the separator
    std::string pattern = "(";
    pattern.reserve(options.pattern.size() + 2);
    pattern += options.pattern;
    pattern += ')';
    regex_split = std::make_unique<RE2>(pattern, MakeRE2Options<Type>());
    return RegexStatus(*regex_split);
  }

  bool Find(const uint8_t* begin, const uint8_t* end, const uint8_t** separator_begin,
            const uint8_t** separator_end, const SplitPatternOptions& options) {
    re2::StringPiece piece(reinterpret_cast<const char*>(begin),
                           std::distance(begin, end));
    // "StringPiece is mutated to point to matched piece"
    re2::StringPiece result;
    if (!RE2::PartialMatch(piece, *regex_split, &result)) {
      return false;
    }
    *separator_begin = reinterpret_cast<const uint8_t*>(result.data());
    *separator_end = reinterpret_cast<const uint8_t*>(result.data() + result.size());
    return true;
  }

  bool FindReverse(const uint8_t* begin, const uint8_t* end,
                   const uint8_t** separator_begin, const uint8_t** separator_end,
                   const SplitPatternOptions& options) {
    // Unsupported (see PreExec)
    return false;
  }
};

template <typename Type, typename ListType>
using SplitRegexExec = StringSplitExec<Type, ListType, SplitRegexFinder<Type>>;

const FunctionDoc split_pattern_regex_doc(
    "Split string according to regex pattern",
    ("Split each string according to the regex `pattern` defined in\n"
     "SplitPatternOptions.  The output for each string input is a list\n"
     "of strings.\n"
     "\n"
     "The maximum number of splits and direction of splitting\n"
     "(forward, reverse) can optionally be defined in SplitPatternOptions."),
    {"strings"}, "SplitPatternOptions", /*options_required=*/true);

void AddAsciiStringSplitRegex(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("split_pattern_regex", Arity::Unary(),
                                               split_pattern_regex_doc);
  for (const auto& ty : BaseBinaryTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<SplitRegexExec, ListType>(ty);
    DCHECK_OK(
        func->AddKernel({ty}, {list(ty)}, std::move(exec), SplitPatternState::Init));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}
#endif  // ARROW_WITH_RE2

// ----------------------------------------------------------------------
// Binary join

template <typename BinaryType, typename ListType>
struct BinaryJoin {
  using ArrayType = typename TypeTraits<BinaryType>::ArrayType;
  using ListArrayType = typename TypeTraits<ListType>::ArrayType;
  using ListScalarType = typename TypeTraits<ListType>::ScalarType;
  using ListOffsetType = typename ListArrayType::offset_type;
  using BuilderType = typename TypeTraits<BinaryType>::BuilderType;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    if (batch[0].is_scalar()) {
      DCHECK(batch[1].is_array());
      return ExecScalarArray(ctx, *batch[0].scalar, batch[1].array, out);
    }
    DCHECK(batch[0].is_array());
    if (batch[1].is_scalar()) {
      return ExecArrayScalar(ctx, batch[0].array, *batch[1].scalar, out);
    }
    DCHECK(batch[1].is_array());
    return ExecArrayArray(ctx, batch[0].array, batch[1].array, out);
  }

  struct ListScalarOffsetLookup {
    const ArrayType& values;

    int64_t GetStart(int64_t i) { return 0; }
    int64_t GetStop(int64_t i) { return values.length(); }
    bool IsNull(int64_t i) { return false; }
  };

  struct ListArrayOffsetLookup {
    explicit ListArrayOffsetLookup(const ListArrayType& lists)
        : lists_(lists), offsets_(lists.raw_value_offsets()) {}

    int64_t GetStart(int64_t i) { return offsets_[i]; }
    int64_t GetStop(int64_t i) { return offsets_[i + 1]; }
    bool IsNull(int64_t i) { return lists_.IsNull(i); }

   private:
    const ListArrayType& lists_;
    const ListOffsetType* offsets_;
  };

  struct SeparatorScalarLookup {
    const std::string_view separator;

    bool IsNull(int64_t i) { return false; }
    std::string_view GetView(int64_t i) { return separator; }
  };

  struct SeparatorArrayLookup {
    const ArrayType& separators;

    bool IsNull(int64_t i) { return separators.IsNull(i); }
    std::string_view GetView(int64_t i) { return separators.GetView(i); }
  };

  // Scalar, array -> array
  static Status ExecScalarArray(KernelContext* ctx, const Scalar& left,
                                const ArraySpan& right, ExecResult* out) {
    const auto& list_scalar = checked_cast<const BaseListScalar&>(left);
    if (!list_scalar.is_valid) {
      ARROW_ASSIGN_OR_RAISE(
          auto nulls,
          MakeArrayOfNull(right.type->GetSharedPtr(), right.length, ctx->memory_pool()));
      out->value = std::move(nulls->data());
      return Status::OK();
    }
    const auto& strings = checked_cast<const ArrayType&>(*list_scalar.value);
    if (strings.null_count() != 0) {
      ARROW_ASSIGN_OR_RAISE(
          auto nulls,
          MakeArrayOfNull(right.type->GetSharedPtr(), right.length, ctx->memory_pool()));
      out->value = std::move(nulls->data());
      return Status::OK();
    }
    // TODO(wesm): rewrite to not use ArrayData
    const ArrayType separators(right.ToArrayData());

    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(separators.length()));

    // Presize data to avoid multiple reallocations when joining strings
    int64_t total_data_length = 0;
    const int64_t list_length = strings.length();
    if (list_length) {
      const int64_t string_length = strings.total_values_length();
      total_data_length +=
          string_length * (separators.length() - separators.null_count());
      for (int64_t i = 0; i < separators.length(); ++i) {
        if (separators.IsNull(i)) {
          continue;
        }
        total_data_length += (list_length - 1) * separators.value_length(i);
      }
    }
    RETURN_NOT_OK(builder.ReserveData(total_data_length));

    return JoinStrings(separators.length(), strings, ListScalarOffsetLookup{strings},
                       SeparatorArrayLookup{separators}, &builder, out);
  }

  // Array, scalar -> array
  static Status ExecArrayScalar(KernelContext* ctx, const ArraySpan& left,
                                const Scalar& right, ExecResult* out) {
    // TODO(wesm): rewrite to not use ArrayData
    const ListArrayType lists(left.ToArrayData());
    const auto& separator_scalar = checked_cast<const BaseBinaryScalar&>(right);

    if (!separator_scalar.is_valid) {
      ARROW_ASSIGN_OR_RAISE(
          auto nulls,
          MakeArrayOfNull(lists.value_type(), lists.length(), ctx->memory_pool()));
      out->value = std::move(nulls->data());
      return Status::OK();
    }

    std::string_view separator(*separator_scalar.value);
    const auto& strings = checked_cast<const ArrayType&>(*lists.values());
    const auto list_offsets = lists.raw_value_offsets();

    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(lists.length()));

    // Presize data to avoid multiple reallocations when joining strings
    int64_t total_data_length = strings.total_values_length();
    for (int64_t i = 0; i < lists.length(); ++i) {
      const auto start = list_offsets[i], end = list_offsets[i + 1];
      if (end > start && !ValuesContainNull(strings, start, end)) {
        total_data_length += (end - start - 1) * separator.length();
      }
    }
    RETURN_NOT_OK(builder.ReserveData(total_data_length));

    return JoinStrings(lists.length(), strings, ListArrayOffsetLookup{lists},
                       SeparatorScalarLookup{separator}, &builder, out);
  }

  // Array, array -> array
  static Status ExecArrayArray(KernelContext* ctx, const ArraySpan& left,
                               const ArraySpan& right, ExecResult* out) {
    // TODO(wesm): rewrite to not use ArrayData
    const ListArrayType lists(left.ToArrayData());
    const ArrayType separators(right.ToArrayData());

    const auto& strings = checked_cast<const ArrayType&>(*lists.values());
    const auto list_offsets = lists.raw_value_offsets();
    const auto string_offsets = strings.raw_value_offsets();

    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(lists.length()));

    // Presize data to avoid multiple reallocations when joining strings
    int64_t total_data_length = 0;
    for (int64_t i = 0; i < lists.length(); ++i) {
      if (separators.IsNull(i)) {
        continue;
      }
      const auto start = list_offsets[i], end = list_offsets[i + 1];
      if (end > start && !ValuesContainNull(strings, start, end)) {
        total_data_length += string_offsets[end] - string_offsets[start];
        total_data_length += (end - start - 1) * separators.value_length(i);
      }
    }
    RETURN_NOT_OK(builder.ReserveData(total_data_length));

    struct SeparatorLookup {
      const ArrayType& separators;

      bool IsNull(int64_t i) { return separators.IsNull(i); }
      std::string_view GetView(int64_t i) { return separators.GetView(i); }
    };
    return JoinStrings(lists.length(), strings, ListArrayOffsetLookup{lists},
                       SeparatorArrayLookup{separators}, &builder, out);
  }

  template <typename ListOffsetLookup, typename SeparatorLookup>
  static Status JoinStrings(int64_t length, const ArrayType& strings,
                            ListOffsetLookup&& list_offsets, SeparatorLookup&& separators,
                            BuilderType* builder, ExecResult* out) {
    for (int64_t i = 0; i < length; ++i) {
      if (list_offsets.IsNull(i) || separators.IsNull(i)) {
        builder->UnsafeAppendNull();
        continue;
      }
      const auto j_start = list_offsets.GetStart(i), j_end = list_offsets.GetStop(i);
      if (j_start == j_end) {
        builder->UnsafeAppendEmptyValue();
        continue;
      }
      if (ValuesContainNull(strings, j_start, j_end)) {
        builder->UnsafeAppendNull();
        continue;
      }
      builder->UnsafeAppend(strings.GetView(j_start));
      for (int64_t j = j_start + 1; j < j_end; ++j) {
        builder->UnsafeExtendCurrent(separators.GetView(i));
        builder->UnsafeExtendCurrent(strings.GetView(j));
      }
    }

    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder->Finish(&string_array));
    out->value = std::move(string_array->data());
    // Correct the output type based on the input
    out->array_data()->type = strings.type();
    return Status::OK();
  }

  static bool ValuesContainNull(const ArrayType& values, int64_t start, int64_t end) {
    if (values.null_count() == 0) {
      return false;
    }
    for (int64_t i = start; i < end; ++i) {
      if (values.IsNull(i)) {
        return true;
      }
    }
    return false;
  }
};

using BinaryJoinElementWiseState = OptionsWrapper<JoinOptions>;

template <typename Type>
struct BinaryJoinElementWise {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    JoinOptions options = BinaryJoinElementWiseState::Get(ctx);
    // Last argument is the separator (for consistency with binary_join)
    // Presize data to avoid reallocations
    int64_t final_size = 0;
    for (int64_t i = 0; i < batch.length; i++) {
      auto size = CalculateRowSize(options, batch, i);
      if (size > 0) final_size += size;
    }
    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    RETURN_NOT_OK(builder.ReserveData(final_size));

    std::vector<std::string_view> valid_cols(batch.num_values());
    for (int64_t row = 0; row < batch.length; row++) {
      int num_valid = 0;  // Not counting separator
      for (int col = 0; col < batch.num_values(); col++) {
        if (batch[col].is_scalar()) {
          const auto& scalar = *batch[col].scalar;
          if (scalar.is_valid) {
            valid_cols[col] = UnboxScalar<Type>::Unbox(scalar);
            if (col < batch.num_values() - 1) num_valid++;
          } else {
            valid_cols[col] = std::string_view();
          }
        } else {
          const ArraySpan& array = batch[col].array;
          if (!array.MayHaveNulls() ||
              bit_util::GetBit(array.buffers[0].data, array.offset + row)) {
            const offset_type* offsets = array.GetValues<offset_type>(1);
            const uint8_t* data = array.GetValues<uint8_t>(2, /*absolute_offset=*/0);
            const int64_t length = offsets[row + 1] - offsets[row];
            valid_cols[col] = std::string_view(
                reinterpret_cast<const char*>(data + offsets[row]), length);
            if (col < batch.num_values() - 1) num_valid++;
          } else {
            valid_cols[col] = std::string_view();
          }
        }
      }

      if (!valid_cols.back().data()) {
        // Separator is null
        builder.UnsafeAppendNull();
        continue;
      } else if (batch.num_values() == 1) {
        // Only given separator
        builder.UnsafeAppendEmptyValue();
        continue;
      } else if (num_valid < batch.num_values() - 1) {
        // We had some nulls
        if (options.null_handling == JoinOptions::EMIT_NULL) {
          builder.UnsafeAppendNull();
          continue;
        }
      }
      const auto separator = valid_cols.back();
      bool first = true;
      for (int col = 0; col < batch.num_values() - 1; col++) {
        std::string_view value = valid_cols[col];
        if (!value.data()) {
          switch (options.null_handling) {
            case JoinOptions::EMIT_NULL:
              DCHECK(false) << "unreachable";
              break;
            case JoinOptions::SKIP:
              continue;
            case JoinOptions::REPLACE:
              value = options.null_replacement;
              break;
          }
        }
        if (first) {
          builder.UnsafeAppend(value);
          first = false;
          continue;
        }
        builder.UnsafeExtendCurrent(separator);
        builder.UnsafeExtendCurrent(value);
      }
    }

    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder.Finish(&string_array));
    out->value = std::move(string_array->data());
    out->array_data()->type = batch[0].type()->GetSharedPtr();
    DCHECK_EQ(batch.length, out->array_data()->length);
    DCHECK_EQ(final_size,
              checked_cast<const ArrayType&>(*string_array).total_values_length());
    return Status::OK();
  }

  // Compute the length of the output for the given position, or -1 if it would be null.
  static int64_t CalculateRowSize(const JoinOptions& options, const ExecSpan& batch,
                                  const int64_t index) {
    const int num_args = batch.num_values();
    int64_t final_size = 0;
    int64_t num_non_null_args = 0;
    for (int i = 0; i < num_args; i++) {
      int64_t element_size = 0;
      bool valid = true;
      if (batch[i].is_scalar()) {
        const Scalar& scalar = *batch[i].scalar;
        valid = scalar.is_valid;
        element_size = UnboxScalar<Type>::Unbox(scalar).size();
      } else {
        const ArraySpan& array = batch[i].array;
        valid = !array.MayHaveNulls() ||
                bit_util::GetBit(array.buffers[0].data, array.offset + index);
        const offset_type* offsets = array.GetValues<offset_type>(1);
        element_size = offsets[index + 1] - offsets[index];
      }
      if (i == num_args - 1) {
        if (!valid) return -1;

        if (num_non_null_args > 1) {
          // Add separator size (only if there were values to join)
          final_size += (num_non_null_args - 1) * element_size;
        }
        break;
      }
      if (!valid) {
        switch (options.null_handling) {
          case JoinOptions::EMIT_NULL:
            return -1;
          case JoinOptions::SKIP:
            continue;
          case JoinOptions::REPLACE:
            element_size = options.null_replacement.size();
            break;
        }
      }
      num_non_null_args++;
      final_size += element_size;
    }
    return final_size;
  }
};

const FunctionDoc binary_join_doc(
    "Join a list of strings together with a separator",
    ("Concatenate the strings in `list`. The `separator` is inserted\n"
     "between each given string.\n"
     "Any null input and any null `list` element emits a null output."),
    {"strings", "separator"});

const FunctionDoc binary_join_element_wise_doc(
    "Join string arguments together, with the last argument as separator",
    ("Concatenate the `strings` except for the last one. The last argument\n"
     "in `strings` is inserted between each given string.\n"
     "Any null separator element emits a null output. Null elements either\n"
     "emit a null (the default), are skipped, or replaced with a given string."),
    {"*strings"}, "JoinOptions");

const JoinOptions* GetDefaultJoinOptions() {
  static const auto kDefaultJoinOptions = JoinOptions::Defaults();
  return &kDefaultJoinOptions;
}

template <typename ListType>
void AddBinaryJoinForListType(ScalarFunction* func) {
  for (const auto& ty : BaseBinaryTypes()) {
    auto exec =
        GenerateTypeAgnosticVarBinaryBase<BinaryJoin, ArrayKernelExec, ListType>(*ty);
    auto list_ty = std::make_shared<ListType>(ty);
    DCHECK_OK(func->AddKernel({InputType(list_ty), InputType(ty)}, ty, std::move(exec)));
  }
}

void AddAsciiStringJoin(FunctionRegistry* registry) {
  {
    auto func =
        std::make_shared<ScalarFunction>("binary_join", Arity::Binary(), binary_join_doc);
    AddBinaryJoinForListType<ListType>(func.get());
    AddBinaryJoinForListType<LargeListType>(func.get());
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<ScalarFunction>(
        "binary_join_element_wise", Arity::VarArgs(/*min_args=*/1),
        binary_join_element_wise_doc, GetDefaultJoinOptions());
    for (const auto& ty : BaseBinaryTypes()) {
      ScalarKernel kernel{KernelSignature::Make({InputType(ty)}, ty, /*is_varargs=*/true),
                          GenerateTypeAgnosticVarBinaryBase<BinaryJoinElementWise>(ty),
                          BinaryJoinElementWiseState::Init};
      kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
      kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
      DCHECK_OK(func->AddKernel(std::move(kernel)));
    }
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

// ----------------------------------------------------------------------
// Binary repeat

/// An ScalarFunction that promotes integer arguments to Int64.
struct ScalarCTypeToInt64Function : public ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    EnsureDictionaryDecoded(types);

    for (auto it = types->begin(); it < types->end(); ++it) {
      if (is_integer(it->id())) {
        *it = int64();
      }
    }

    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

template <typename Type1, typename Type2>
struct BinaryRepeatTransform : public StringBinaryTransformBase<Type1, Type2> {
  using ArrayType1 = typename TypeTraits<Type1>::ArrayType;
  using ArrayType2 = typename TypeTraits<Type2>::ArrayType;
  using offset_type = typename ArrayType1::offset_type;
  using repeat_type = typename Type2::c_type;

  Result<int64_t> MaxCodeunits(const int64_t input1_ncodeunits,
                               const ArraySpan& input2) override {
    int64_t total_num_repeats = 0;
    const repeat_type* repeats = input2.GetValues<repeat_type>(1);
    for (int64_t i = 0; i < input2.length; ++i) {
      ARROW_RETURN_NOT_OK(ValidateRepeatCount(repeats[i]));
      total_num_repeats += repeats[i];
    }
    return input1_ncodeunits * total_num_repeats;
  }

  Result<int64_t> MaxCodeunits(const ArraySpan& input1,
                               const int64_t num_repeats) override {
    ARROW_RETURN_NOT_OK(ValidateRepeatCount(num_repeats));
    return GetVarBinaryValuesLength<offset_type>(input1) * num_repeats;
  }

  Result<int64_t> MaxCodeunits(const ArraySpan& input1,
                               const ArraySpan& input2) override {
    int64_t total_codeunits = 0;
    const repeat_type* repeats = input2.GetValues<repeat_type>(1);
    const offset_type* offsets = input1.GetValues<offset_type>(1);
    for (int64_t i = 0; i < input2.length; ++i) {
      ARROW_RETURN_NOT_OK(ValidateRepeatCount(repeats[i]));
      total_codeunits += (offsets[i + 1] - offsets[i]) * repeats[i];
    }
    return total_codeunits;
  }

  static Result<int64_t> TransformSimpleLoop(const uint8_t* input,
                                             const int64_t input_string_ncodeunits,
                                             const int64_t num_repeats, uint8_t* output) {
    uint8_t* output_start = output;
    for (int64_t i = 0; i < num_repeats; ++i) {
      std::memcpy(output, input, input_string_ncodeunits);
      output += input_string_ncodeunits;
    }
    return output - output_start;
  }

  static Result<int64_t> TransformDoublingString(const uint8_t* input,
                                                 const int64_t input_string_ncodeunits,
                                                 const int64_t num_repeats,
                                                 uint8_t* output) {
    uint8_t* output_start = output;
    // Repeated doubling of string
    // NB: This implementation expects `num_repeats > 0`.
    std::memcpy(output, input, input_string_ncodeunits);
    output += input_string_ncodeunits;
    int64_t irep = 1;
    for (int64_t ilen = input_string_ncodeunits; irep <= (num_repeats / 2);
         irep *= 2, ilen *= 2) {
      std::memcpy(output, output_start, ilen);
      output += ilen;
    }

    // Epilogue remainder
    int64_t rem = (num_repeats - irep) * input_string_ncodeunits;
    std::memcpy(output, output_start, rem);
    output += rem;
    return output - output_start;
  }

  static Result<int64_t> Transform(const uint8_t* input,
                                   const int64_t input_string_ncodeunits,
                                   const int64_t num_repeats, uint8_t* output) {
    auto transform = (num_repeats < 4) ? TransformSimpleLoop : TransformDoublingString;
    return transform(input, input_string_ncodeunits, num_repeats, output);
  }

  static Status ValidateRepeatCount(const int64_t num_repeats) {
    if (num_repeats < 0) {
      return Status::Invalid("Repeat count must be a non-negative integer");
    }
    return Status::OK();
  }
};

template <typename Type1, typename Type2>
using BinaryRepeat =
    StringBinaryTransformExec<Type1, Type2, BinaryRepeatTransform<Type1, Type2>>;

const FunctionDoc binary_repeat_doc(
    "Repeat a binary string",
    ("For each binary string in `strings`, return a replicated version."),
    {"strings", "num_repeats"});

void AddAsciiStringRepeat(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarCTypeToInt64Function>(
      "binary_repeat", Arity::Binary(), binary_repeat_doc);
  for (const auto& ty : BaseBinaryTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<BinaryRepeat, Int64Type>(ty);
    ScalarKernel kernel{{ty, int64()}, ty, exec};
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------

}  // namespace

void RegisterScalarStringAscii(FunctionRegistry* registry) {
  AddAsciiStringPredicates(registry);
  AddAsciiStringCaseConversion(registry);
  AddAsciiStringLength(registry);
  AddAsciiStringReverse(registry);
  AddAsciiStringTrim(registry);
  AddAsciiStringPad(registry);
  AddAsciiStringMatchSubstring(registry);
  AddAsciiStringFindSubstring(registry);
  AddAsciiStringCountSubstring(registry);
  AddAsciiStringReplaceSubstring(registry);
#ifdef ARROW_WITH_RE2
  AddAsciiStringExtractRegex(registry);
#endif
  AddAsciiStringReplaceSlice(registry);
  AddAsciiStringSlice(registry);
  AddAsciiStringSplitPattern(registry);
  AddAsciiStringSplitWhitespace(registry);
#ifdef ARROW_WITH_RE2
  AddAsciiStringSplitRegex(registry);
#endif
  AddAsciiStringJoin(registry);
  AddAsciiStringRepeat(registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
