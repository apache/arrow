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

#include <utility>

#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/ree_util.h"
#include "arrow/util/ree_util_internal.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

struct RunEndEncodingState : public KernelState {
  explicit RunEndEncodingState(std::shared_ptr<DataType> run_end_type)
      : run_end_type{std::move(run_end_type)} {}

  ~RunEndEncodingState() override = default;

  std::shared_ptr<DataType> run_end_type;
};

ARROW_NOINLINE Status RunEndEncodeNullArray(const std::shared_ptr<DataType>& run_end_type,
                                            KernelContext* ctx,
                                            const ArraySpan& input_array,
                                            ExecResult* output) {
  const int64_t input_length = input_array.length;
  ARROW_DCHECK(input_array.type->id() == Type::NA);

  if (input_length == 0) {
    ARROW_ASSIGN_OR_RAISE(
        auto output_array_data,
        ree_util::internal::MakeNullREEArray(run_end_type, 0, ctx->memory_pool()));
    output->value = std::move(output_array_data);
    return Status::OK();
  }

  // Abort if run-end type cannot hold the input length
  RETURN_NOT_OK(ree_util::internal::ValidateRunEndType(run_end_type, input_array.length));

  ARROW_ASSIGN_OR_RAISE(auto output_array_data,
                        ree_util::internal::MakeNullREEArray(run_end_type, input_length,
                                                             ctx->memory_pool()));

  output->value = std::move(output_array_data);
  return Status::OK();
}

struct RunEndEncodeExec {
  template <typename RunEndType, typename ValueType>
  static Status DoExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    ARROW_DCHECK(span.values[0].is_array());
    const auto& input_array = span.values[0].array;
    auto run_end_type = TypeTraits<RunEndType>::type_singleton();

    if constexpr (ValueType::type_id == Type::NA) {
      return RunEndEncodeNullArray(run_end_type, ctx, input_array, result);
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto encoded,
          ree_util::RunEndEncodeArray(input_array, run_end_type, ctx->memory_pool()));
      result->value = std::move(encoded);
      return Status::OK();
    }
  }

  template <typename ValueType>
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    auto state = checked_cast<const RunEndEncodingState*>(ctx->state());
    switch (state->run_end_type->id()) {
      case Type::INT16:
        return DoExec<Int16Type, ValueType>(ctx, span, result);
      case Type::INT32:
        return DoExec<Int32Type, ValueType>(ctx, span, result);
      case Type::INT64:
        return DoExec<Int64Type, ValueType>(ctx, span, result);
      default:
        break;
    }
    return Status::Invalid("Invalid run end type: ", *state->run_end_type);
  }

  /// \brief The OutputType::Resolver of the "run_end_decode" function.
  static Result<TypeHolder> ResolveOutputType(
      KernelContext* ctx, const std::vector<TypeHolder>& input_types) {
    auto state = checked_cast<const RunEndEncodingState*>(ctx->state());
    return TypeHolder(std::make_shared<RunEndEncodedType>(state->run_end_type,
                                                          input_types[0].GetSharedPtr()));
  }
};

Result<std::unique_ptr<KernelState>> RunEndEncodeInit(KernelContext*,
                                                      const KernelInitArgs& args) {
  auto* options = checked_cast<const RunEndEncodeOptions*>(args.options);
  auto run_end_type =
      options ? options->run_end_type : RunEndEncodeOptions::Defaults().run_end_type;
  return std::make_unique<RunEndEncodingState>(std::move(run_end_type));
}

template <typename RunEndType, typename ValueType, bool has_validity_buffer>
class RunEndDecodingLoop {
 public:
  using RunEndCType = typename RunEndType::c_type;

 private:
  using ReadWriteValue =
      arrow::ree_util::internal::ReadWriteValue<ValueType, has_validity_buffer>;
  using ValueRepr = typename ReadWriteValue::ValueRepr;

  const ArraySpan& input_array_;
  ReadWriteValue read_write_value_;
  int64_t values_offset_;

  RunEndDecodingLoop(const ArraySpan& input_array, const ArraySpan& input_array_values,
                     ArrayData* output_array_data)
      : input_array_(input_array),
        read_write_value_(input_array_values, output_array_data),
        values_offset_(input_array_values.offset) {}

 public:
  RunEndDecodingLoop(const ArraySpan& input_array, ArrayData* output_array_data)
      : RunEndDecodingLoop(input_array, arrow::ree_util::ValuesArray(input_array),
                           output_array_data) {}

  /// \brief For variable-length types, calculate the total length of the data
  /// buffer needed to store the expanded values.
  int64_t CalculateOutputDataBufferSize() const {
    auto& input_array_values = arrow::ree_util::ValuesArray(input_array_);
    DCHECK_EQ(input_array_values.type->id(), ValueType::type_id);
    if constexpr (is_base_binary_like(ValueType::type_id)) {
      using offset_type = typename ValueType::offset_type;
      int64_t data_buffer_size = 0;

      const arrow::ree_util::RunEndEncodedArraySpan<RunEndCType> ree_array_span(
          input_array_);
      const auto* offsets_buffer =
          input_array_values.template GetValues<offset_type>(1, 0);
      auto it = ree_array_span.begin();
      auto offset = offsets_buffer[input_array_values.offset + it.index_into_array()];
      while (it != ree_array_span.end()) {
        const int64_t i = input_array_values.offset + it.index_into_array();
        const int64_t value_length = offsets_buffer[i + 1] - offset;
        data_buffer_size += it.run_length() * value_length;
        offset = offsets_buffer[i + 1];
        ++it;
      }
      return data_buffer_size;
    }
    return 0;
  }

  /// \brief Expand all runs into the output array
  ///
  /// \return the number of non-null values written.
  ARROW_NOINLINE int64_t ExpandAllRuns() {
    read_write_value_.ZeroValidityPadding(input_array_.length);

    const arrow::ree_util::RunEndEncodedArraySpan<RunEndCType> ree_array_span(
        input_array_);
    int64_t write_offset = 0;
    int64_t output_valid_count = 0;
    for (auto it = ree_array_span.begin(); !it.is_end(ree_array_span); ++it) {
      const int64_t read_offset = values_offset_ + it.index_into_array();
      const int64_t run_length = it.run_length();
      ValueRepr value;
      const bool valid = read_write_value_.ReadValue(&value, read_offset);
      read_write_value_.WriteRun(write_offset, run_length, valid, value);
      write_offset += run_length;
      output_valid_count += valid ? run_length : 0;
    }
    ARROW_DCHECK(write_offset == ree_array_span.length());
    return output_valid_count;
  }
};

template <typename RunEndType, typename ValueType, bool has_validity_buffer>
class RunEndDecodeImpl {
 private:
  KernelContext* ctx_;
  const ArraySpan& input_array_;
  ExecResult* output_;

 public:
  using RunEndCType = typename RunEndType::c_type;

  RunEndDecodeImpl(KernelContext* ctx, const ArraySpan& input_array, ExecResult* out)
      : ctx_{ctx}, input_array_{input_array}, output_{out} {}

 public:
  Status Exec() {
    const auto* ree_type = checked_cast<const RunEndEncodedType*>(input_array_.type);
    const int64_t length = input_array_.length;
    int64_t data_buffer_size = 0;
    if constexpr (is_base_binary_like(ValueType::type_id)) {
      if (length > 0) {
        RunEndDecodingLoop<RunEndType, ValueType, has_validity_buffer> loop(input_array_,
                                                                            NULLPTR);
        data_buffer_size = loop.CalculateOutputDataBufferSize();
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto output_array_data,
                          arrow::ree_util::internal::PreallocateValuesArray(
                              ree_type->value_type(), has_validity_buffer, length,
                              ctx_->memory_pool(), data_buffer_size));

    int64_t output_null_count = 0;
    if (length > 0) {
      RunEndDecodingLoop<RunEndType, ValueType, has_validity_buffer> loop(
          input_array_, output_array_data.get());
      output_null_count = length - loop.ExpandAllRuns();
    }
    output_array_data->null_count = output_null_count;

    output_->value = std::move(output_array_data);
    return Status::OK();
  }
};

Status RunEndDecodeNullREEArray(KernelContext* ctx, const ArraySpan& input_array,
                                ExecResult* out) {
  auto ree_type = checked_cast<const RunEndEncodedType*>(input_array.type);
  ARROW_ASSIGN_OR_RAISE(auto output_array,
                        arrow::MakeArrayOfNull(ree_type->value_type(), input_array.length,
                                               ctx->memory_pool()));
  out->value = output_array->data();
  return Status::OK();
}

struct RunEndDecodeExec {
  template <typename RunEndType, typename ValueType>
  static Status DoExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    ARROW_DCHECK(span.values[0].is_array());
    auto& input_array = span.values[0].array;
    if constexpr (ValueType::type_id == Type::NA) {
      return RunEndDecodeNullREEArray(ctx, input_array, result);
    } else {
      const bool has_validity_buffer =
          arrow::ree_util::ValuesArray(input_array).GetNullCount() > 0;
      if (has_validity_buffer) {
        return RunEndDecodeImpl<RunEndType, ValueType, true>(ctx, input_array, result)
            .Exec();
      }
      return RunEndDecodeImpl<RunEndType, ValueType, false>(ctx, input_array, result)
          .Exec();
    }
  }

  template <typename ValueType>
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    const auto& ree_type = checked_cast<const RunEndEncodedType*>(span.values[0].type());
    switch (ree_type->run_end_type()->id()) {
      case Type::INT16:
        return DoExec<Int16Type, ValueType>(ctx, span, result);
      case Type::INT32:
        return DoExec<Int32Type, ValueType>(ctx, span, result);
      case Type::INT64:
        return DoExec<Int64Type, ValueType>(ctx, span, result);
      default:
        break;
    }
    return Status::Invalid("Invalid run end type: ", *ree_type->run_end_type());
  }

  /// \brief The OutputType::Resolver of the "run_end_decode" function.
  static Result<TypeHolder> ResolveOutputType(KernelContext*,
                                              const std::vector<TypeHolder>& in_types) {
    const auto* ree_type = checked_cast<const RunEndEncodedType*>(in_types[0].type);
    return TypeHolder(ree_type->value_type());
  }
};

static const FunctionDoc run_end_encode_doc(
    "Run-end encode array", ("Return a run-end encoded version of the input array."),
    {"array"}, "RunEndEncodeOptions");
static const FunctionDoc run_end_decode_doc(
    "Decode run-end encoded array",
    ("Return a decoded version of a run-end encoded input array."), {"array"});

}  // namespace

void RegisterVectorRunEndEncode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_end_encode", Arity::Unary(),
                                                   run_end_encode_doc);

  // NOTE: When the input to run_end_encode() is a ChunkedArray, the output is also a
  // ChunkedArray with the same number of chunks as the input. Each chunk in the output
  // has the same logical length as the corresponding chunk in the input. This simplicity
  // has a small downside: if a run of identical values crosses a chunk boundary, this run
  // cannot be encoded as a single run in the output. This is a conscious trade-off as
  // trying to solve this corner-case would complicate the implementation,
  // require reallocations, and could create surprising behavior for users of this API.

  // Runtime dispatcher for flexible REE value type matcher
  auto runtime_exec = [](KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    const auto& input_array = span.values[0].array;
    const Type::type type_id = input_array.type->id();

    // Dispatch to the appropriate exec based on input type
    switch (type_id) {
#define DISPATCH_REE_ENCODE_CASE(TypeClass, TYPE_ENUM) \
  case Type::TYPE_ENUM:                                \
    return RunEndEncodeExec::Exec<TypeClass##Type>(ctx, span, result);
      ARROW_REE_SUPPORTED_TYPES(DISPATCH_REE_ENCODE_CASE)
#undef DISPATCH_REE_ENCODE_CASE
      case Type::NA:
        return RunEndEncodeExec::Exec<NullType>(ctx, span, result);
      default:
        return Status::NotImplemented("run_end_encode does not support type ",
                                      input_array.type->ToString());
    }
  };

  // Use flexible matcher for REE value types (any non-nested type except Null)
  auto sig = KernelSignature::Make({InputType(match::REEValue())},
                                   OutputType(RunEndEncodeExec::ResolveOutputType));
  VectorKernel kernel(sig, runtime_exec, RunEndEncodeInit);
  // A REE has null_count=0, so no need to allocate a validity bitmap for them.
  kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
  DCHECK_OK(function->AddKernel(std::move(kernel)));

  // Also support Null type explicitly
  sig = KernelSignature::Make({InputType(Type::NA)},
                              OutputType(RunEndEncodeExec::ResolveOutputType));
  VectorKernel null_kernel(sig, runtime_exec, RunEndEncodeInit);
  null_kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
  DCHECK_OK(function->AddKernel(std::move(null_kernel)));

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

void RegisterVectorRunEndDecode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_end_decode", Arity::Unary(),
                                                   run_end_decode_doc);

  // Runtime dispatcher for flexible REE value type matcher
  auto runtime_exec = [](KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    const auto& input_array = span.values[0].array;
    const auto& ree_type = checked_cast<const RunEndEncodedType&>(*input_array.type);
    const Type::type value_type_id = ree_type.value_type()->id();

    // Dispatch to the appropriate exec based on value type
    switch (value_type_id) {
#define DISPATCH_REE_DECODE_CASE(TypeClass, TYPE_ENUM) \
  case Type::TYPE_ENUM:                                \
    return RunEndDecodeExec::Exec<TypeClass##Type>(ctx, span, result);
      ARROW_REE_SUPPORTED_TYPES(DISPATCH_REE_DECODE_CASE)
#undef DISPATCH_REE_DECODE_CASE
      case Type::NA:
        return RunEndDecodeExec::Exec<NullType>(ctx, span, result);
      default:
        return Status::NotImplemented("run_end_decode does not support value type ",
                                      ree_type.value_type()->ToString());
    }
  };

  for (const auto& run_end_type_id : {Type::INT16, Type::INT32, Type::INT64}) {
    // Use flexible matcher for REE value types (any non-nested type except Null)
    auto input_type_matcher =
        match::RunEndEncoded(match::SameTypeId(run_end_type_id), match::REEValue());
    auto sig = KernelSignature::Make({InputType(std::move(input_type_matcher))},
                                     OutputType(RunEndDecodeExec::ResolveOutputType));
    VectorKernel kernel(sig, runtime_exec);
    DCHECK_OK(function->AddKernel(std::move(kernel)));

    // Also register a kernel for null value type explicitly
    auto null_input_matcher = match::RunEndEncoded(match::SameTypeId(run_end_type_id),
                                                   match::SameTypeId(Type::NA));
    auto null_sig =
        KernelSignature::Make({InputType(std::move(null_input_matcher))},
                              OutputType(RunEndDecodeExec::ResolveOutputType));
    VectorKernel null_kernel(null_sig, runtime_exec);
    DCHECK_OK(function->AddKernel(std::move(null_kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
