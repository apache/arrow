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
#include "arrow/compute/kernels/ree_util_internal.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace compute {
namespace internal {

struct RunEndEncondingState : public KernelState {
  explicit RunEndEncondingState(std::shared_ptr<DataType> run_end_type)
      : run_end_type{std::move(run_end_type)} {}

  ~RunEndEncondingState() override = default;

  std::shared_ptr<DataType> run_end_type;
};

template <typename RunEndType, typename ValueType, bool has_validity_buffer>
class RunEndEncodingLoop {
 public:
  using RunEndCType = typename RunEndType::c_type;

 private:
  using ReadWriteValue = ree_util::ReadWriteValue<ValueType, has_validity_buffer>;
  using ValueRepr = typename ReadWriteValue::ValueRepr;

 private:
  const int64_t input_length_;
  const int64_t input_offset_;
  ReadWriteValue read_write_value_;
  // Needed only by WriteEncodedRuns()
  RunEndCType* output_run_ends_;

 public:
  explicit RunEndEncodingLoop(const ArraySpan& input_array,
                              ArrayData* output_values_array_data,
                              RunEndCType* output_run_ends)
      : input_length_(input_array.length),
        input_offset_(input_array.offset),
        read_write_value_(input_array, output_values_array_data),
        output_run_ends_(output_run_ends) {
    DCHECK_GT(input_array.length, 0);
  }

  /// \brief Give a pass over the input data and count the number of runs
  ///
  /// \return a tuple with the number of non-null run values, the total number of runs,
  /// and the data buffer size for string and binary types
  ARROW_NOINLINE std::tuple<int64_t, int64_t, int64_t> CountNumberOfRuns() const {
    int64_t read_offset = input_offset_;
    ValueRepr current_run;
    bool current_run_valid = read_write_value_.ReadValue(&current_run, read_offset);
    read_offset += 1;
    int64_t num_valid_runs = current_run_valid ? 1 : 0;
    int64_t num_output_runs = 1;
    int64_t data_buffer_size = 0;
    if constexpr (is_base_binary_like(ValueType::type_id)) {
      data_buffer_size = current_run_valid ? current_run.size() : 0;
    }
    for (; read_offset < input_offset_ + input_length_; read_offset += 1) {
      ValueRepr value;
      const bool valid = read_write_value_.ReadValue(&value, read_offset);

      const bool open_new_run =
          valid != current_run_valid || !read_write_value_.Compare(value, current_run);
      if (open_new_run) {
        // Open the new run
        current_run = value;
        current_run_valid = valid;
        // Count the new run
        num_output_runs += 1;
        num_valid_runs += valid ? 1 : 0;
        if constexpr (is_base_binary_like(ValueType::type_id)) {
          data_buffer_size += valid ? current_run.size() : 0;
        }
      }
    }
    return std::make_tuple(num_valid_runs, num_output_runs, data_buffer_size);
  }

  ARROW_NOINLINE int64_t WriteEncodedRuns() {
    DCHECK(output_run_ends_);
    int64_t read_offset = input_offset_;
    int64_t write_offset = 0;
    ValueRepr current_run;
    bool current_run_valid = read_write_value_.ReadValue(&current_run, read_offset);
    read_offset += 1;
    for (; read_offset < input_offset_ + input_length_; read_offset += 1) {
      ValueRepr value;
      const bool valid = read_write_value_.ReadValue(&value, read_offset);

      const bool open_new_run =
          valid != current_run_valid || !read_write_value_.Compare(value, current_run);
      if (open_new_run) {
        // Close the current run first by writing it out
        read_write_value_.WriteValue(write_offset, current_run_valid, current_run);
        const int64_t run_end = read_offset - input_offset_;
        output_run_ends_[write_offset] = static_cast<RunEndCType>(run_end);
        write_offset += 1;
        // Open the new run
        current_run_valid = valid;
        current_run = value;
      }
    }
    read_write_value_.WriteValue(write_offset, current_run_valid, current_run);
    DCHECK_EQ(input_length_, read_offset - input_offset_);
    output_run_ends_[write_offset] = static_cast<RunEndCType>(input_length_);
    return write_offset + 1;
  }
};

ARROW_NOINLINE Status ValidateRunEndType(const std::shared_ptr<DataType>& run_end_type,
                                         int64_t input_length) {
  int64_t run_end_max = std::numeric_limits<int64_t>::max();
  switch (run_end_type->id()) {
    case Type::INT16:
      run_end_max = std::numeric_limits<int16_t>::max();
      break;
    case Type::INT32:
      run_end_max = std::numeric_limits<int32_t>::max();
      break;
    default:
      DCHECK_EQ(run_end_type->id(), Type::INT64);
      break;
  }
  if (input_length < 0 || input_length > run_end_max) {
    return Status::Invalid(
        "Cannot run-end encode Arrays with more elements than the "
        "run end type can hold: ",
        run_end_max);
  }
  return Status::OK();
}

template <typename RunEndType, typename ValueType, bool has_validity_buffer>
class RunEndEncodeImpl {
 private:
  KernelContext* ctx_;
  const ArraySpan& input_array_;
  ExecResult* output_;

 public:
  using RunEndCType = typename RunEndType::c_type;

  RunEndEncodeImpl(KernelContext* ctx, const ArraySpan& input_array, ExecResult* out)
      : ctx_{ctx}, input_array_{input_array}, output_{out} {}

  Status Exec() {
    const int64_t input_length = input_array_.length;

    auto run_end_type = TypeTraits<RunEndType>::type_singleton();
    auto ree_type = std::make_shared<RunEndEncodedType>(
        run_end_type, input_array_.type->GetSharedPtr());
    if (input_length == 0) {
      ARROW_ASSIGN_OR_RAISE(
          auto output_array_data,
          ree_util::PreallocateREEArray(std::move(ree_type), has_validity_buffer,
                                        input_length, 0, 0, ctx_->memory_pool(), 0));
      output_->value = std::move(output_array_data);
      return Status::OK();
    }

    // First pass: count the number of runs
    int64_t num_valid_runs = 0;
    int64_t num_output_runs = 0;
    int64_t data_buffer_size = 0;  // for string and binary types
    RETURN_NOT_OK(ValidateRunEndType(run_end_type, input_length));

    RunEndEncodingLoop<RunEndType, ValueType, has_validity_buffer> counting_loop(
        input_array_,
        /*output_values_array_data=*/NULLPTR,
        /*output_run_ends=*/NULLPTR);
    std::tie(num_valid_runs, num_output_runs, data_buffer_size) =
        counting_loop.CountNumberOfRuns();

    ARROW_ASSIGN_OR_RAISE(
        auto output_array_data,
        ree_util::PreallocateREEArray(
            std::move(ree_type), has_validity_buffer, input_length, num_output_runs,
            num_output_runs - num_valid_runs, ctx_->memory_pool(), data_buffer_size));

    // Initialize the output pointers
    auto* output_run_ends =
        output_array_data->child_data[0]->template GetMutableValues<RunEndCType>(1, 0);
    auto* output_values_array_data = output_array_data->child_data[1].get();

    // Second pass: write the runs
    RunEndEncodingLoop<RunEndType, ValueType, has_validity_buffer> writing_loop(
        input_array_, output_values_array_data, output_run_ends);
    [[maybe_unused]] int64_t num_written_runs = writing_loop.WriteEncodedRuns();
    DCHECK_EQ(num_written_runs, num_output_runs);

    output_->value = std::move(output_array_data);
    return Status::OK();
  }
};

ARROW_NOINLINE Status RunEndEncodeNullArray(const std::shared_ptr<DataType>& run_end_type,
                                            KernelContext* ctx,
                                            const ArraySpan& input_array,
                                            ExecResult* output) {
  const int64_t input_length = input_array.length;
  DCHECK(input_array.type->id() == Type::NA);

  if (input_length == 0) {
    ARROW_ASSIGN_OR_RAISE(
        auto output_array_data,
        ree_util::MakeNullREEArray(run_end_type, 0, ctx->memory_pool()));
    output->value = std::move(output_array_data);
    return Status::OK();
  }

  // Abort if run-end type cannot hold the input length
  RETURN_NOT_OK(ValidateRunEndType(run_end_type, input_array.length));

  ARROW_ASSIGN_OR_RAISE(
      auto output_array_data,
      ree_util::MakeNullREEArray(run_end_type, input_length, ctx->memory_pool()));

  output->value = std::move(output_array_data);
  return Status::OK();
}

struct RunEndEncodeExec {
  template <typename RunEndType, typename ValueType>
  static Status DoExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    DCHECK(span.values[0].is_array());
    const auto& input_array = span.values[0].array;
    if constexpr (ValueType::type_id == Type::NA) {
      return RunEndEncodeNullArray(TypeTraits<RunEndType>::type_singleton(), ctx,
                                   input_array, result);
    } else {
      const bool has_validity_buffer = input_array.MayHaveNulls();
      if (has_validity_buffer) {
        return RunEndEncodeImpl<RunEndType, ValueType, true>(ctx, input_array, result)
            .Exec();
      }
      return RunEndEncodeImpl<RunEndType, ValueType, false>(ctx, input_array, result)
          .Exec();
    }
  }

  template <typename ValueType>
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    auto state = checked_cast<const RunEndEncondingState*>(ctx->state());
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
    auto state = checked_cast<const RunEndEncondingState*>(ctx->state());
    return TypeHolder(std::make_shared<RunEndEncodedType>(state->run_end_type,
                                                          input_types[0].GetSharedPtr()));
  }
};

Result<std::unique_ptr<KernelState>> RunEndEncodeInit(KernelContext*,
                                                      const KernelInitArgs& args) {
  auto* options = checked_cast<const RunEndEncodeOptions*>(args.options);
  auto run_end_type =
      options ? options->run_end_type : RunEndEncodeOptions::Defaults().run_end_type;
  return std::make_unique<RunEndEncondingState>(std::move(run_end_type));
}

template <typename RunEndType, typename ValueType, bool has_validity_buffer>
class RunEndDecodingLoop {
 public:
  using RunEndCType = typename RunEndType::c_type;

 private:
  using ReadWriteValue = ree_util::ReadWriteValue<ValueType, has_validity_buffer>;
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
    DCHECK(write_offset == ree_array_span.length());
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
                          ree_util::PreallocateValuesArray(
                              ree_type->value_type(), has_validity_buffer, length,
                              kUnknownNullCount, ctx_->memory_pool(), data_buffer_size));

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
    DCHECK(span.values[0].is_array());
    auto& input_array = span.values[0].array;
    if constexpr (ValueType::type_id == Type::NA) {
      return RunEndDecodeNullREEArray(ctx, input_array, result);
    } else {
      const bool has_validity_buffer =
          arrow::ree_util::ValuesArray(input_array).MayHaveNulls();
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

template <typename Functor>
static ArrayKernelExec GenerateREEKernelExec(Type::type type_id) {
  switch (type_id) {
    case Type::NA:
      return Functor::template Exec<NullType>;
    case Type::BOOL:
      return Functor::template Exec<BooleanType>;
    case Type::UINT8:
    case Type::INT8:
      return Functor::template Exec<UInt8Type>;
    case Type::UINT16:
    case Type::INT16:
      return Functor::template Exec<UInt16Type>;
    case Type::UINT32:
    case Type::INT32:
    case Type::FLOAT:
    case Type::DATE32:
    case Type::TIME32:
    case Type::INTERVAL_MONTHS:
      return Functor::template Exec<UInt32Type>;
    case Type::UINT64:
    case Type::INT64:
    case Type::DOUBLE:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
    case Type::INTERVAL_DAY_TIME:
      return Functor::template Exec<UInt64Type>;
    case Type::INTERVAL_MONTH_DAY_NANO:
      return Functor::template Exec<MonthDayNanoIntervalType>;
    case Type::DECIMAL128:
      return Functor::template Exec<Decimal128Type>;
    case Type::DECIMAL256:
      return Functor::template Exec<Decimal256Type>;
    case Type::FIXED_SIZE_BINARY:
      return Functor::template Exec<FixedSizeBinaryType>;
    case Type::STRING:
      return Functor::template Exec<StringType>;
    case Type::BINARY:
      return Functor::template Exec<BinaryType>;
    case Type::LARGE_STRING:
      return Functor::template Exec<LargeStringType>;
    case Type::LARGE_BINARY:
      return Functor::template Exec<LargeBinaryType>;
    default:
      DCHECK(false);
      return FailFunctor<ArrayKernelExec>::Exec;
  }
}

static const FunctionDoc run_end_encode_doc(
    "Run-end encode array", ("Return a run-end encoded version of the input array."),
    {"array"}, "RunEndEncodeOptions");
static const FunctionDoc run_end_decode_doc(
    "Decode run-end encoded array",
    ("Return a decoded version of a run-end encoded input array."), {"array"});

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
  auto add_kernel = [&function](Type::type type_id) {
    auto sig = KernelSignature::Make({InputType(match::SameTypeId(type_id))},
                                     OutputType(RunEndEncodeExec::ResolveOutputType));
    auto exec = GenerateREEKernelExec<RunEndEncodeExec>(type_id);
    VectorKernel kernel(sig, exec, RunEndEncodeInit);
    // A REE has null_count=0, so no need to allocate a validity bitmap for them.
    kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  };

  add_kernel(Type::NA);
  add_kernel(Type::BOOL);
  for (const auto& ty : NumericTypes()) {
    add_kernel(ty->id());
  }
  add_kernel(Type::DATE32);
  add_kernel(Type::DATE64);
  add_kernel(Type::TIME32);
  add_kernel(Type::TIME64);
  add_kernel(Type::TIMESTAMP);
  add_kernel(Type::DURATION);
  for (const auto& ty : IntervalTypes()) {
    add_kernel(ty->id());
  }
  add_kernel(Type::DECIMAL128);
  add_kernel(Type::DECIMAL256);
  add_kernel(Type::FIXED_SIZE_BINARY);
  add_kernel(Type::STRING);
  add_kernel(Type::BINARY);
  add_kernel(Type::LARGE_STRING);
  add_kernel(Type::LARGE_BINARY);

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

void RegisterVectorRunEndDecode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_end_decode", Arity::Unary(),
                                                   run_end_decode_doc);

  auto add_kernel = [&function](Type::type type_id) {
    for (const auto& run_end_type_id : {Type::INT16, Type::INT32, Type::INT64}) {
      auto exec = GenerateREEKernelExec<RunEndDecodeExec>(type_id);
      auto input_type_matcher = match::RunEndEncoded(match::SameTypeId(run_end_type_id),
                                                     match::SameTypeId(type_id));
      auto sig = KernelSignature::Make({InputType(std::move(input_type_matcher))},
                                       OutputType(RunEndDecodeExec::ResolveOutputType));
      VectorKernel kernel(sig, exec);
      DCHECK_OK(function->AddKernel(std::move(kernel)));
    }
  };

  add_kernel(Type::NA);
  add_kernel(Type::BOOL);
  for (const auto& ty : NumericTypes()) {
    add_kernel(ty->id());
  }
  add_kernel(Type::DATE32);
  add_kernel(Type::DATE64);
  add_kernel(Type::TIME32);
  add_kernel(Type::TIME64);
  add_kernel(Type::TIMESTAMP);
  add_kernel(Type::DURATION);
  for (const auto& ty : IntervalTypes()) {
    add_kernel(ty->id());
  }
  add_kernel(Type::DECIMAL128);
  add_kernel(Type::DECIMAL256);
  add_kernel(Type::FIXED_SIZE_BINARY);
  add_kernel(Type::STRING);
  add_kernel(Type::BINARY);
  add_kernel(Type::LARGE_STRING);
  add_kernel(Type::LARGE_BINARY);

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
