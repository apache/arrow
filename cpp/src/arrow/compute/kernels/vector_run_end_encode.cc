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

template <typename ArrowType, bool has_validity_buffer>
struct ReadValueImpl {
  using CType = typename ArrowType::c_type;

  [[nodiscard]] bool ReadValue(const uint8_t* input_validity, const void* input_values,
                               CType* out, int64_t read_offset) const {
    bool valid = true;
    if constexpr (has_validity_buffer) {
      valid = bit_util::GetBit(input_validity, read_offset);
    }
    if (valid) {
      *out = (reinterpret_cast<const CType*>(input_values))[read_offset];
    }
    return valid;
  }
};

template <>
bool ReadValueImpl<BooleanType, true>::ReadValue(const uint8_t* input_validity,
                                                 const void* input_values, CType* out,
                                                 int64_t read_offset) const {
  const bool valid = bit_util::GetBit(input_validity, read_offset);
  *out = valid &&
         bit_util::GetBit(reinterpret_cast<const uint8_t*>(input_values), read_offset);
  return valid;
}

template <>
bool ReadValueImpl<BooleanType, false>::ReadValue(const uint8_t* input_validity,
                                                  const void* input_values, CType* out,
                                                  int64_t read_offset) const {
  *out = bit_util::GetBit(reinterpret_cast<const uint8_t*>(input_values), read_offset);
  return true;
}

template <typename ArrowType, bool has_validity_buffer>
struct WriteValueImpl {
  using CType = typename ArrowType::c_type;

  void WriteValue(uint8_t* output_validity, void* output_values, int64_t write_offset,
                  bool valid, CType value) const {
    if constexpr (has_validity_buffer) {
      bit_util::SetBitsTo(output_validity, write_offset, 1, valid);
    }
    (reinterpret_cast<CType*>(output_values))[write_offset] = value;
  }

  void WriteRun(uint8_t* output_validity, void* output_values, int64_t write_offset,
                int64_t run_length, bool valid, CType value) const {
    if constexpr (has_validity_buffer) {
      bit_util::SetBitsTo(output_validity, write_offset, run_length, valid);
    }
    auto* output_values_c = reinterpret_cast<CType*>(output_values);
    std::fill(output_values_c + write_offset, output_values_c + write_offset + run_length,
              value);
  }
};

template <>
void WriteValueImpl<BooleanType, true>::WriteValue(uint8_t* output_validity,
                                                   void* output_values,
                                                   int64_t write_offset, bool valid,
                                                   CType value) const {
  bit_util::SetBitTo(output_validity, write_offset, valid);
  if (valid) {
    bit_util::SetBitTo(reinterpret_cast<uint8_t*>(output_values), write_offset, value);
  }
}

template <>
void WriteValueImpl<BooleanType, true>::WriteRun(uint8_t* output_validity,
                                                 void* output_values,
                                                 int64_t write_offset, int64_t run_length,
                                                 bool valid, CType value) const {
  bit_util::SetBitsTo(output_validity, write_offset, run_length, valid);
  if (valid) {
    bit_util::SetBitsTo(reinterpret_cast<uint8_t*>(output_values), write_offset,
                        run_length, value);
  }
}

template <>
void WriteValueImpl<BooleanType, false>::WriteValue(uint8_t*, void* output_values,
                                                    int64_t write_offset, bool,
                                                    CType value) const {
  bit_util::SetBitTo(reinterpret_cast<uint8_t*>(output_values), write_offset, value);
}

template <>
void WriteValueImpl<BooleanType, false>::WriteRun(uint8_t*, void* output_values,
                                                  int64_t write_offset,
                                                  int64_t run_length, bool,
                                                  CType value) const {
  bit_util::SetBitsTo(reinterpret_cast<uint8_t*>(output_values), write_offset, run_length,
                      value);
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
  using CType = typename ValueType::c_type;

 private:
  const int64_t input_length_;
  const int64_t input_offset_;

  const uint8_t* input_validity_;
  const void* input_values_;

  // Needed only by WriteEncodedRuns()
  uint8_t* output_validity_;
  void* output_values_;
  RunEndCType* output_run_ends_;

 public:
  RunEndEncodingLoop(int64_t input_length, int64_t input_offset,
                     const uint8_t* input_validity, const void* input_values,
                     uint8_t* output_validity = NULLPTR, void* output_values = NULLPTR,
                     RunEndCType* output_run_ends = NULLPTR)
      : input_length_(input_length),
        input_offset_(input_offset),
        input_validity_(input_validity),
        input_values_(input_values),
        output_validity_(output_validity),
        output_values_(output_values),
        output_run_ends_(output_run_ends) {
    DCHECK_GT(input_length, 0);
  }

 private:
  [[nodiscard]] inline bool ReadValue(CType* out, int64_t read_offset) const {
    return ReadValueImpl<ValueType, has_validity_buffer>{}.ReadValue(
        input_validity_, input_values_, out, read_offset);
  }

  void WriteValue(int64_t write_offset, bool valid, CType value) {
    WriteValueImpl<ValueType, has_validity_buffer>{}.WriteValue(
        output_validity_, output_values_, write_offset, valid, value);
  }

 public:
  /// \brief Give a pass over the input data and count the number of runs
  ///
  /// \return a pair with the number of non-null run values and total number of runs
  std::pair<int64_t, int64_t> CountNumberOfRuns() const {
    int64_t read_offset = input_offset_;
    CType current_run;
    bool current_run_valid = ReadValue(&current_run, read_offset);
    read_offset += 1;
    int64_t num_valid_runs = current_run_valid ? 1 : 0;
    int64_t num_output_runs = 1;
    for (; read_offset < input_offset_ + input_length_; read_offset += 1) {
      CType value;
      const bool valid = ReadValue(&value, read_offset);

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

  int64_t WriteEncodedRuns() {
    DCHECK(output_values_);
    DCHECK(output_run_ends_);
    int64_t read_offset = input_offset_;
    int64_t write_offset = 0;
    CType current_run;
    bool current_run_valid = ReadValue(&current_run, read_offset);
    read_offset += 1;
    for (; read_offset < input_offset_ + input_length_; read_offset += 1) {
      CType value;
      const bool valid = ReadValue(&value, read_offset);

      const bool open_new_run = valid != current_run_valid || value != current_run;
      if (open_new_run) {
        // Close the current run first by writing it out
        WriteValue(write_offset, current_run_valid, current_run);
        const int64_t run_end = read_offset - input_offset_;
        output_run_ends_[write_offset] = static_cast<RunEndCType>(run_end);
        write_offset += 1;
        // Open the new run
        current_run_valid = valid;
        current_run = value;
      }
    }
    WriteValue(write_offset, current_run_valid, current_run);
    DCHECK_EQ(input_length_, read_offset - input_offset_);
    output_run_ends_[write_offset] = static_cast<RunEndCType>(input_length_);
    return write_offset + 1;
  }
};

template <typename RunEndType>
Status ValidateRunEndType(int64_t input_length) {
  using RunEndCType = typename RunEndType::c_type;
  constexpr int64_t kRunEndMax = std::numeric_limits<RunEndCType>::max();
  if (input_length > kRunEndMax) {
    return Status::Invalid(
        "Cannot run-end encode Arrays with more elements than the "
        "run end type can hold: ",
        kRunEndMax);
  }
  return Status::OK();
}

template <typename RunEndType, typename ValueType, bool has_validity_buffer>
class RunEndEncodeImpl {
 private:
  KernelContext* ctx_;
  const ArraySpan input_array_;
  ExecResult* output_;

 public:
  using RunEndCType = typename RunEndType::c_type;
  using CType = typename ValueType::c_type;

  RunEndEncodeImpl(KernelContext* ctx, const ExecSpan& batch, ExecResult* out)
      : ctx_{ctx}, input_array_{batch.values[0].array}, output_{out} {}

  Status Exec() {
    const int64_t input_length = input_array_.length;
    const int64_t input_offset = input_array_.offset;
    const auto* input_validity = input_array_.buffers[0].data;
    const auto* input_values = input_array_.buffers[1].data;

    // First pass: count the number of runs
    int64_t num_valid_runs = 0;
    int64_t num_output_runs = 0;
    if (input_length > 0) {
      RETURN_NOT_OK(ValidateRunEndType<RunEndType>(input_length));

      RunEndEncodingLoop<RunEndType, ValueType, has_validity_buffer> counting_loop(
          input_array_.length, input_array_.offset, input_validity, input_values);
      std::tie(num_valid_runs, num_output_runs) = counting_loop.CountNumberOfRuns();
    }

    // Allocate the output array data
    std::shared_ptr<ArrayData> output_array_data;
    int64_t validity_buffer_size = 0;  // in bytes
    {
      ARROW_ASSIGN_OR_RAISE(auto run_ends_buffer,
                            AllocateBuffer(num_output_runs * RunEndType().bit_width(),
                                           ctx_->memory_pool()));
      std::shared_ptr<Buffer> validity_buffer = NULLPTR;
      if constexpr (has_validity_buffer) {
        validity_buffer_size = bit_util::BytesForBits(num_output_runs);
        ARROW_ASSIGN_OR_RAISE(validity_buffer,
                              AllocateBuffer(validity_buffer_size, ctx_->memory_pool()));
      }
      ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                            AllocateBuffer(bit_util::BytesForBits(
                                               num_output_runs * ValueType().bit_width()),
                                           ctx_->memory_pool()));

      auto ree_type = std::make_shared<RunEndEncodedType>(
          std::make_shared<RunEndType>(), input_array_.type->GetSharedPtr());
      auto run_ends_data =
          ArrayData::Make(ree_type->run_end_type(), num_output_runs,
                          {NULLPTR, std::move(run_ends_buffer)}, /*null_count=*/0);
      auto values_data =
          ArrayData::Make(ree_type->value_type(), num_output_runs,
                          {std::move(validity_buffer), std::move(values_buffer)},
                          /*null_count=*/num_output_runs - num_valid_runs);

      output_array_data =
          ArrayData::Make(std::move(ree_type), input_length, {NULLPTR},
                          {std::move(run_ends_data), std::move(values_data)},
                          /*null_count=*/0);
    }

    if (input_length > 0) {
      // Initialize the output pointers
      auto* output_run_ends =
          output_array_data->child_data[0]->template GetMutableValues<RunEndCType>(1);
      auto* output_validity =
          output_array_data->child_data[1]->template GetMutableValues<uint8_t>(0);
      auto* output_values =
          output_array_data->child_data[1]->template GetMutableValues<uint8_t>(1);

      if constexpr (has_validity_buffer) {
        // Clear last byte in validity buffer to ensure padding bits are zeroed
        output_validity[validity_buffer_size - 1] = 0;
      }

      // Second pass: write the runs
      RunEndEncodingLoop<RunEndType, ValueType, has_validity_buffer> writing_loop(
          input_length, input_offset, input_validity, input_values, output_validity,
          output_values, output_run_ends);
      [[maybe_unused]] int64_t num_written_runs = writing_loop.WriteEncodedRuns();
      DCHECK_EQ(num_written_runs, num_output_runs);
    }

    output_->value = std::move(output_array_data);
    return Status::OK();
  }
};

template <typename RunEndType>
Status RunEndEncodeNullArray(KernelContext* ctx, const ExecSpan& span,
                             ExecResult* output) {
  using RunEndCType = typename RunEndType::c_type;

  const auto& input_array = span.values[0].array;
  const int64_t input_length = input_array.length;
  auto input_array_type = input_array.type->GetSharedPtr();
  DCHECK(input_array_type->id() == Type::NA);

  int64_t num_output_runs = 0;
  if (input_length > 0) {
    // Abort if run-end type cannot hold the input length
    RETURN_NOT_OK(ValidateRunEndType<RunEndType>(input_array.length));
    num_output_runs = 1;
  }

  // Allocate the output array data
  std::shared_ptr<ArrayData> output_array_data;
  {
    ARROW_ASSIGN_OR_RAISE(
        auto run_ends_buffer,
        AllocateBuffer(num_output_runs * RunEndType().bit_width(), ctx->memory_pool()));

    auto ree_type = std::make_shared<RunEndEncodedType>(std::make_shared<RunEndType>(),
                                                        input_array_type);
    auto run_ends_data = ArrayData::Make(std::make_shared<RunEndType>(), num_output_runs,
                                         {NULLPTR, std::move(run_ends_buffer)},
                                         /*null_count=*/0);
    auto values_data = ArrayData::Make(input_array_type, num_output_runs, {NULLPTR},
                                       /*null_count=*/num_output_runs);

    output_array_data =
        ArrayData::Make(std::move(ree_type), input_length, {NULLPTR},
                        {std::move(run_ends_data), std::move(values_data)},
                        /*null_count=*/0);
  }

  if (input_length > 0) {
    auto* output_run_ends =
        output_array_data->child_data[0]->template GetMutableValues<RunEndCType>(1);

    // Write the single run-end this REE has
    output_run_ends[0] = static_cast<RunEndCType>(input_length);
  }

  output->value = std::move(output_array_data);
  return Status::OK();
}

template <typename ValueType>
struct RunEndEncodeExec {
  template <typename RunEndType>
  static Status DoExec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    if constexpr (ValueType::type_id == Type::NA) {
      return RunEndEncodeNullArray<RunEndType>(ctx, span, result);
    } else {
      const bool has_validity_buffer = span.values[0].array.MayHaveNulls();
      if (has_validity_buffer) {
        return RunEndEncodeImpl<RunEndType, ValueType, true>(ctx, span, result).Exec();
      }
      return RunEndEncodeImpl<RunEndType, ValueType, false>(ctx, span, result).Exec();
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
  auto options = checked_cast<const RunEndEncodeOptions*>(args.options);
  return std::make_unique<RunEndEncondingState>(options->run_end_type);
}

template <typename RunEndType, typename ValueType, bool has_validity_buffer>
class RunEndDecodingLoop {
 public:
  using RunEndCType = typename RunEndType::c_type;
  using CType = typename ValueType::c_type;

 private:
  const ArraySpan& input_array_;

  const uint8_t* input_validity_;
  const void* input_values_;
  int64_t values_offset_;

  uint8_t* output_validity_;
  void* output_values_;

 public:
  explicit RunEndDecodingLoop(const ArraySpan& input_array, ArrayData* output_array_data)
      : input_array_(input_array) {
    const ArraySpan& values = ree_util::ValuesArray(input_array);
    input_validity_ = values.buffers[0].data;
    input_values_ = values.buffers[1].data;
    values_offset_ = values.offset;

    output_validity_ = output_array_data->template GetMutableValues<uint8_t>(0);
    output_values_ = output_array_data->template GetMutableValues<CType>(1);
  }

 private:
  [[nodiscard]] inline bool ReadValue(CType* out, int64_t read_offset) const {
    return ReadValueImpl<ValueType, has_validity_buffer>{}.ReadValue(
        input_validity_, input_values_, out, read_offset);
  }

  void WriteRun(int64_t write_offset, int64_t run_length, bool valid, CType value) {
    WriteValueImpl<ValueType, has_validity_buffer>{}.WriteRun(
        output_validity_, output_values_, write_offset, run_length, valid, value);
  }

 public:
  /// \brief Expand all runs into the output array
  ///
  /// \return the number of non-null values written.
  int64_t ExpandAllRuns() {
    // Ensure padding is zeroed in validity bitmap
    if constexpr (has_validity_buffer) {
      const int64_t validity_buffer_size = bit_util::BytesForBits(input_array_.length);
      output_validity_[validity_buffer_size - 1] = 0;
    }

    const ree_util::RunEndEncodedArraySpan<RunEndCType> ree_array_span(input_array_);
    int64_t write_offset = 0;
    int64_t output_valid_count = 0;
    for (auto it = ree_array_span.begin(); it != ree_array_span.end(); ++it) {
      const int64_t read_offset = values_offset_ + it.index_into_array();
      const int64_t run_length = it.run_length();
      CType value;
      const bool valid = ReadValue(&value, read_offset);
      WriteRun(write_offset, run_length, valid, value);
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
  const ArraySpan input_array_;
  ExecResult* output_;

 public:
  using RunEndCType = typename RunEndType::c_type;
  using CType = typename ValueType::c_type;

  RunEndDecodeImpl(KernelContext* ctx, const ExecSpan& batch, ExecResult* out)
      : ctx_{ctx}, input_array_{batch.values[0].array}, output_{out} {}

 public:
  Status Exec() {
    const auto* ree_type = checked_cast<const RunEndEncodedType*>(input_array_.type);
    const int64_t length = input_array_.length;
    std::shared_ptr<Buffer> validity_buffer = NULLPTR;
    if constexpr (has_validity_buffer) {
      // in bytes
      int64_t validity_buffer_size = 0;
      validity_buffer_size = bit_util::BytesForBits(length);
      ARROW_ASSIGN_OR_RAISE(validity_buffer,
                            AllocateBuffer(validity_buffer_size, ctx_->memory_pool()));
    }
    ARROW_ASSIGN_OR_RAISE(
        auto values_buffer,
        AllocateBuffer(
            bit_util::BytesForBits(length * ree_type->value_type()->bit_width()),
            ctx_->memory_pool()));

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

Status RunEndDecodeNullREEArray(KernelContext* ctx, const ExecSpan& span,
                                ExecResult* out) {
  auto& input_array = span.values[0].array;
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
    if constexpr (ValueType::type_id == Type::NA) {
      return RunEndDecodeNullREEArray(ctx, span, result);
    } else {
      const bool has_validity_buffer =
          ree_util::ValuesArray(span.values[0].array).MayHaveNulls();
      if (has_validity_buffer) {
        return RunEndDecodeImpl<RunEndType, ValueType, true>(ctx, span, result).Exec();
      }
      return RunEndDecodeImpl<RunEndType, ValueType, false>(ctx, span, result).Exec();
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
    {"array"}, "RunEndEncodeOptions", true);
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
