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
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename ArrowType, bool has_validity_buffer, typename Enable = void>
struct ReadWriteValueImpl {};

// Numeric and primitive C-compatible types
template <typename ArrowType, bool has_validity_buffer>
class ReadWriteValueImpl<ArrowType, has_validity_buffer,
                         enable_if_has_c_type<ArrowType>> {
 public:
  using ValueRepr = typename ArrowType::c_type;

 private:
  const uint8_t* input_validity_;
  const uint8_t* input_values_;

  // Needed only by the writing functions
  uint8_t* output_validity_;
  uint8_t* output_values_;

 public:
  explicit ReadWriteValueImpl(const ArraySpan& input_values_array,
                              ArrayData* output_values_array_data)
      : input_validity_(has_validity_buffer ? input_values_array.buffers[0].data
                                            : NULLPTR),
        input_values_(input_values_array.buffers[1].data),
        output_validity_(
            (has_validity_buffer && output_values_array_data)
                ? output_values_array_data->template GetMutableValues<uint8_t>(0)
                : NULLPTR),
        output_values_(
            output_values_array_data
                ? output_values_array_data->template GetMutableValues<uint8_t>(1)
                : NULLPTR) {}

  [[nodiscard]] bool ReadValue(ValueRepr* out, int64_t read_offset) const {
    bool valid = true;
    if constexpr (has_validity_buffer) {
      valid = bit_util::GetBit(input_validity_, read_offset);
    }
    if constexpr (std::is_same_v<ArrowType, BooleanType>) {
      *out = bit_util::GetBit(input_values_, read_offset);
    } else {
      *out = (reinterpret_cast<const ValueRepr*>(input_values_))[read_offset];
    }
    return valid;
  }

  /// \brief Ensure padding is zeroed in validity bitmap.
  void ZeroValidityPadding(int64_t length) const {
    DCHECK(output_values_);
    if constexpr (has_validity_buffer) {
      DCHECK(output_validity_);
      const int64_t validity_buffer_size = bit_util::BytesForBits(length);
      output_validity_[validity_buffer_size - 1] = 0;
    }
  }

  void WriteValue(int64_t write_offset, bool valid, ValueRepr value) const {
    if constexpr (has_validity_buffer) {
      bit_util::SetBitTo(output_validity_, write_offset, valid);
    }
    if (valid) {
      if constexpr (std::is_same_v<ArrowType, BooleanType>) {
        bit_util::SetBitTo(output_values_, write_offset, value);
      } else {
        (reinterpret_cast<ValueRepr*>(output_values_))[write_offset] = value;
      }
    }
  }

  void WriteRun(int64_t write_offset, int64_t run_length, bool valid,
                ValueRepr value) const {
    if constexpr (has_validity_buffer) {
      bit_util::SetBitsTo(output_validity_, write_offset, run_length, valid);
    }
    if (valid) {
      if constexpr (std::is_same_v<ArrowType, BooleanType>) {
        bit_util::SetBitsTo(reinterpret_cast<uint8_t*>(output_values_), write_offset,
                            run_length, value);
      } else {
        auto* output_values_c = reinterpret_cast<ValueRepr*>(output_values_);
        std::fill(output_values_c + write_offset,
                  output_values_c + write_offset + run_length, value);
      }
    }
  }
};

Result<std::shared_ptr<Buffer>> AllocateValuesBuffer(int64_t length, const DataType& type,
                                                     MemoryPool* pool) {
  DCHECK(is_fixed_width(type.id()));
  if (type.bit_width() == 1) {
    return AllocateBitmap(length, pool);
  } else {
    return AllocateBuffer(length * type.byte_width(), pool);
  }
}

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
  using ReadWriteValue = ReadWriteValueImpl<ValueType, has_validity_buffer>;
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
  /// \return a pair with the number of non-null run values and total number of runs
  ARROW_NOINLINE std::pair<int64_t, int64_t> CountNumberOfRuns() const {
    int64_t read_offset = input_offset_;
    ValueRepr current_run;
    bool current_run_valid = read_write_value_.ReadValue(&current_run, read_offset);
    read_offset += 1;
    int64_t num_valid_runs = current_run_valid ? 1 : 0;
    int64_t num_output_runs = 1;
    for (; read_offset < input_offset_ + input_length_; read_offset += 1) {
      ValueRepr value;
      const bool valid = read_write_value_.ReadValue(&value, read_offset);

      const bool open_new_run = valid != current_run_valid || value != current_run;
      if (open_new_run) {
        // Open the new run
        current_run = value;
        current_run_valid = valid;
        // Count the new run
        num_output_runs += 1;
        num_valid_runs += valid ? 1 : 0;
      }
    }
    return std::make_pair(num_valid_runs, num_output_runs);
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

      const bool open_new_run = valid != current_run_valid || value != current_run;
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

template <typename RunEndType>
Status ValidateRunEndType(int64_t input_length) {
  using RunEndCType = typename RunEndType::c_type;
  constexpr int64_t kRunEndMax = std::numeric_limits<RunEndCType>::max();
  if (input_length < 0 || input_length > kRunEndMax) {
    return Status::Invalid(
        "Cannot run-end encode Arrays with more elements than the "
        "run end type can hold: ",
        kRunEndMax);
  }
  return Status::OK();
}

/// \brief Preallocate the ArrayData for the run-end encoded version
/// of the input array
template <typename RunEndType, bool has_validity_buffer>
Result<std::shared_ptr<ArrayData>> PreallocateREEData(const ArraySpan& input_array,
                                                      int64_t physical_length,
                                                      int64_t physical_null_count,
                                                      MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_buffer,
      AllocateBuffer(physical_length * RunEndType().byte_width(), pool));
  std::shared_ptr<Buffer> validity_buffer = NULLPTR;
  if constexpr (has_validity_buffer) {
    ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateBitmap(physical_length, pool));
  }
  ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                        AllocateValuesBuffer(physical_length, *input_array.type, pool));

  auto ree_type = std::make_shared<RunEndEncodedType>(std::make_shared<RunEndType>(),
                                                      input_array.type->GetSharedPtr());
  auto run_ends_data =
      ArrayData::Make(ree_type->run_end_type(), physical_length,
                      {NULLPTR, std::move(run_ends_buffer)}, /*null_count=*/0);
  auto values_data = ArrayData::Make(
      ree_type->value_type(), physical_length,
      {std::move(validity_buffer), std::move(values_buffer)}, physical_null_count);

  return ArrayData::Make(std::move(ree_type), input_array.length, {NULLPTR},
                         {std::move(run_ends_data), std::move(values_data)},
                         /*null_count=*/0);
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

    if (input_length == 0) {
      ARROW_ASSIGN_OR_RAISE(auto output_array_data,
                            (PreallocateREEData<RunEndType, has_validity_buffer>(
                                input_array_, 0, 0, ctx_->memory_pool())));
      output_->value = std::move(output_array_data);
      return Status::OK();
    }

    // First pass: count the number of runs
    int64_t num_valid_runs = 0;
    int64_t num_output_runs = 0;
    RETURN_NOT_OK(ValidateRunEndType<RunEndType>(input_length));

    RunEndEncodingLoop<RunEndType, ValueType, has_validity_buffer> counting_loop(
        input_array_,
        /*output_values_array_data=*/NULLPTR,
        /*output_run_ends=*/NULLPTR);
    std::tie(num_valid_runs, num_output_runs) = counting_loop.CountNumberOfRuns();

    ARROW_ASSIGN_OR_RAISE(auto output_array_data,
                          (PreallocateREEData<RunEndType, has_validity_buffer>(
                              input_array_, num_output_runs,
                              num_output_runs - num_valid_runs, ctx_->memory_pool())));

    // Initialize the output pointers
    auto* output_run_ends =
        output_array_data->child_data[0]->template GetMutableValues<RunEndCType>(1);
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

template <typename RunEndType>
Result<std::shared_ptr<ArrayData>> PreallocateNullREEData(int64_t logical_length,
                                                          int64_t physical_length,
                                                          MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_buffer,
      AllocateBuffer(physical_length * RunEndType().byte_width(), pool));

  auto ree_type =
      std::make_shared<RunEndEncodedType>(std::make_shared<RunEndType>(), null());
  auto run_ends_data = ArrayData::Make(std::make_shared<RunEndType>(), physical_length,
                                       {NULLPTR, std::move(run_ends_buffer)},
                                       /*null_count=*/0);
  auto values_data = ArrayData::Make(null(), physical_length, {NULLPTR},
                                     /*null_count=*/physical_length);
  return ArrayData::Make(std::move(ree_type), logical_length, {NULLPTR},
                         {std::move(run_ends_data), std::move(values_data)},
                         /*null_count=*/0);
}

template <typename RunEndType>
Status RunEndEncodeNullArray(KernelContext* ctx, const ArraySpan& input_array,
                             ExecResult* output) {
  using RunEndCType = typename RunEndType::c_type;

  const int64_t input_length = input_array.length;
  DCHECK(input_array.type->id() == Type::NA);

  if (input_length == 0) {
    ARROW_ASSIGN_OR_RAISE(auto output_array_data,
                          PreallocateNullREEData<RunEndType>(0, 0, ctx->memory_pool()));
    output->value = std::move(output_array_data);
    return Status::OK();
  }

  // Abort if run-end type cannot hold the input length
  RETURN_NOT_OK(ValidateRunEndType<RunEndType>(input_array.length));

  ARROW_ASSIGN_OR_RAISE(auto output_array_data, PreallocateNullREEData<RunEndType>(
                                                    input_length, 1, ctx->memory_pool()));

  // Write the single run-end this REE has
  auto* output_run_ends =
      output_array_data->child_data[0]->template GetMutableValues<RunEndCType>(1);
  output_run_ends[0] = static_cast<RunEndCType>(input_length);

  output->value = std::move(output_array_data);
  return Status::OK();
}

template <typename ValueType>
struct RunEndEncodeExec {
  template <typename RunEndType>
  static Status DoExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    DCHECK(span.values[0].is_array());
    const auto& input_array = span.values[0].array;
    if constexpr (ValueType::type_id == Type::NA) {
      return RunEndEncodeNullArray<RunEndType>(ctx, input_array, result);
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

  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    auto state = checked_cast<const RunEndEncondingState*>(ctx->state());
    switch (state->run_end_type->id()) {
      case Type::INT16:
        return DoExec<Int16Type>(ctx, span, result);
      case Type::INT32:
        return DoExec<Int32Type>(ctx, span, result);
      case Type::INT64:
        return DoExec<Int64Type>(ctx, span, result);
      default:
        break;
    }
    return Status::Invalid("Invalid run end type: ", *state->run_end_type);
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
  using ReadWriteValue = ReadWriteValueImpl<ValueType, has_validity_buffer>;
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
      : RunEndDecodingLoop(input_array, ree_util::ValuesArray(input_array),
                           output_array_data) {}

  /// \brief Expand all runs into the output array
  ///
  /// \return the number of non-null values written.
  ARROW_NOINLINE int64_t ExpandAllRuns() {
    read_write_value_.ZeroValidityPadding(input_array_.length);

    const ree_util::RunEndEncodedArraySpan<RunEndCType> ree_array_span(input_array_);
    int64_t write_offset = 0;
    int64_t output_valid_count = 0;
    for (auto it = ree_array_span.begin(); it != ree_array_span.end(); ++it) {
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
    std::shared_ptr<Buffer> validity_buffer = NULLPTR;
    if constexpr (has_validity_buffer) {
      ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateBitmap(length, ctx_->memory_pool()));
    }
    ARROW_ASSIGN_OR_RAISE(
        auto values_buffer,
        AllocateValuesBuffer(length, *ree_type->value_type(), ctx_->memory_pool()));

    auto output_array_data =
        ArrayData::Make(ree_type->value_type(), length,
                        {std::move(validity_buffer), std::move(values_buffer)},
                        /*child_data=*/std::vector<std::shared_ptr<ArrayData>>{});

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

template <typename ValueType>
struct RunEndDecodeExec {
  template <typename RunEndType>
  static Status DoExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    DCHECK(span.values[0].is_array());
    auto& input_array = span.values[0].array;
    if constexpr (ValueType::type_id == Type::NA) {
      return RunEndDecodeNullREEArray(ctx, input_array, result);
    } else {
      const bool has_validity_buffer = ree_util::ValuesArray(input_array).MayHaveNulls();
      if (has_validity_buffer) {
        return RunEndDecodeImpl<RunEndType, ValueType, true>(ctx, input_array, result)
            .Exec();
      }
      return RunEndDecodeImpl<RunEndType, ValueType, false>(ctx, input_array, result)
          .Exec();
    }
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    const auto& ree_type = checked_cast<const RunEndEncodedType*>(span.values[0].type());
    switch (ree_type->run_end_type()->id()) {
      case Type::INT16:
        return DoExec<Int16Type>(ctx, span, result);
      case Type::INT32:
        return DoExec<Int32Type>(ctx, span, result);
      case Type::INT64:
        return DoExec<Int64Type>(ctx, span, result);
      default:
        break;
    }
    return Status::Invalid("Invalid run end type: ", *ree_type->run_end_type());
  }
};

static const FunctionDoc run_end_encode_doc(
    "Run-end encode array", ("Return a run-end encoded version of the input array."),
    {"array"}, "RunEndEncodeOptions");
static const FunctionDoc run_end_decode_doc(
    "Decode run-end encoded array",
    ("Return a decoded version of a run-end encoded input array."), {"array"});

static Result<TypeHolder> VectorRunEndEncodedResolver(
    KernelContext* ctx, const std::vector<TypeHolder>& input_types) {
  auto state = checked_cast<const RunEndEncondingState*>(ctx->state());
  return TypeHolder(std::make_shared<RunEndEncodedType>(state->run_end_type,
                                                        input_types[0].GetSharedPtr()));
}

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
  auto add_kernel = [&function](const std::shared_ptr<DataType>& ty) {
    auto sig =
        KernelSignature::Make({InputType(ty)}, OutputType(VectorRunEndEncodedResolver));
    auto exec = GenerateTypeAgnosticPrimitive<RunEndEncodeExec>(ty);
    VectorKernel kernel(sig, exec, RunEndEncodeInit);
    // A REE has null_count=0, so no need to allocate a validity bitmap for them.
    kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  };

  for (const auto& ty : NumericTypes()) {
    add_kernel(ty);
  }
  add_kernel(boolean());
  add_kernel(null());
  // TODO(GH-34195): Add support for more types

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

void RegisterVectorRunEndDecode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_end_decode", Arity::Unary(),
                                                   run_end_decode_doc);

  auto add_kernel = [&function](const std::shared_ptr<DataType>& ty) {
    for (const auto& run_end_type : {int16(), int32(), int64()}) {
      auto exec = GenerateTypeAgnosticPrimitive<RunEndDecodeExec>(ty);
      auto input_type = std::make_shared<RunEndEncodedType>(run_end_type, ty);
      auto sig = KernelSignature::Make({InputType(input_type)}, OutputType({ty}));
      VectorKernel kernel(sig, exec);
      DCHECK_OK(function->AddKernel(std::move(kernel)));
    }
  };

  for (const auto& ty : NumericTypes()) {
    add_kernel(ty);
  }
  add_kernel(boolean());
  add_kernel(null());
  // TODO(GH-34195): Add support for more types

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
