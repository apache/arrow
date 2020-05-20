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

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// Number / Boolean to String

template <typename I, typename O>
struct CastFunctor<O, I,
                   enable_if_t<is_string_like_type<O>::value &&
                               (is_number_type<I>::value || is_boolean_type<I>::value)>> {
  void operator()(KernelContext* ctx, const CastOptions& options, const ArrayData& input,
                  ArrayData* output) {
    ctx->SetStatus(Convert(ctx, options, input, output));
  }

  Status Convert(KernelContext* ctx, const CastOptions& options, const ArrayData& input,
                 ArrayData* output) {
    using value_type = typename TypeTraits<I>::CType;
    using BuilderType = typename TypeTraits<O>::BuilderType;
    using FormatterType = typename internal::StringFormatter<I>;

    FormatterType formatter(input.type);
    BuilderType builder(input.type, ctx->memory_pool());

    auto convert_value = [&](util::optional<value_type> v) {
      if (v.has_value()) {
        return formatter(*v, [&](util::string_view v) { return builder.Append(v); });
      } else {
        return builder.AppendNull();
      }
    };
    RETURN_NOT_OK(VisitArrayDataInline<I>(input, std::move(convert_value)));

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

template <typename I, typename O>
struct BinaryToStringSameWidthCastFunctor {
  void operator()(KernelContext* ctx, const CastOptions& options, const ArrayData& input,
                  ArrayData* output) {
    if (!options.allow_invalid_utf8) {
      util::InitializeUTF8();

      ArrayDataVisitor<I> visitor;
      Status st = visitor.Visit(input, this);
      if (!st.ok()) {
        ctx->SetStatus(st);
        return;
      }
    }
    ZeroCopyData(input, output);
  }

  Status VisitNull() { return Status::OK(); }

  Status VisitValue(util::string_view str) {
    if (ARROW_PREDICT_FALSE(!arrow::util::ValidateUTF8(str))) {
      return Status::Invalid("Invalid UTF8 payload");
    }
    return Status::OK();
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

}  // namespace compute
}  // namespace arrow
