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
#include "arrow/type_traits.h"
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
        output_validity_((has_validity_buffer && output_values_array_data)
                             ? output_values_array_data->buffers[0]->mutable_data()
                             : NULLPTR),
        output_values_(output_values_array_data
                           ? output_values_array_data->buffers[1]->mutable_data()
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

  bool Compare(ValueRepr lhs, ValueRepr rhs) const { return lhs == rhs; }
};

// FixedSizeBinary, Decimal128
template <typename ArrowType, bool has_validity_buffer>
class ReadWriteValueImpl<ArrowType, has_validity_buffer,
                         enable_if_fixed_size_binary<ArrowType>> {
 public:
  // Every value is represented as a pointer to byte_width_ bytes
  using ValueRepr = uint8_t const*;

 private:
  const uint8_t* input_validity_;
  const uint8_t* input_values_;

  // Needed only by the writing functions
  uint8_t* output_validity_;
  uint8_t* output_values_;

  const size_t byte_width_;

 public:
  ReadWriteValueImpl(const ArraySpan& input_values_array,
                     ArrayData* output_values_array_data)
      : input_validity_(has_validity_buffer ? input_values_array.buffers[0].data
                                            : NULLPTR),
        input_values_(input_values_array.buffers[1].data),
        output_validity_((has_validity_buffer && output_values_array_data)
                             ? output_values_array_data->buffers[0]->mutable_data()
                             : NULLPTR),
        output_values_(output_values_array_data
                           ? output_values_array_data->buffers[1]->mutable_data()
                           : NULLPTR),
        byte_width_(input_values_array.type->byte_width()) {}

  [[nodiscard]] bool ReadValue(ValueRepr* out, int64_t read_offset) const {
    bool valid = true;
    if constexpr (has_validity_buffer) {
      valid = bit_util::GetBit(input_validity_, read_offset);
    }
    *out = input_values_ + (read_offset * byte_width_);
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
      memcpy(output_values_ + (write_offset * byte_width_), value, byte_width_);
    }
  }

  void WriteRun(int64_t write_offset, int64_t run_length, bool valid,
                ValueRepr value) const {
    if constexpr (has_validity_buffer) {
      bit_util::SetBitsTo(output_validity_, write_offset, run_length, valid);
    }
    if (valid) {
      uint8_t* ptr = output_values_ + (write_offset * byte_width_);
      for (int64_t i = 0; i < run_length; ++i) {
        memcpy(ptr, value, byte_width_);
        ptr += byte_width_;
      }
    }
  }

  bool Compare(ValueRepr lhs, ValueRepr rhs) const {
    return memcmp(lhs, rhs, byte_width_) == 0;
  }
};

// Binary, String...
template <typename ArrowType, bool has_validity_buffer>
class ReadWriteValueImpl<ArrowType, has_validity_buffer,
                         enable_if_base_binary<ArrowType>> {
 public:
  using ValueRepr = std::string_view;
  using offset_type = typename ArrowType::offset_type;

 private:
  const uint8_t* input_validity_;
  const offset_type* input_offsets_;
  const uint8_t* input_values_;

  // Needed only by the writing functions
  uint8_t* output_validity_;
  offset_type* output_offsets_;
  uint8_t* output_values_;

 public:
  ReadWriteValueImpl(const ArraySpan& input_values_array,
                     ArrayData* output_values_array_data)
      : input_validity_(has_validity_buffer ? input_values_array.buffers[0].data
                                            : NULLPTR),
        input_offsets_(input_values_array.template GetValues<offset_type>(1, 0)),
        input_values_(input_values_array.buffers[2].data),
        output_validity_((has_validity_buffer && output_values_array_data)
                             ? output_values_array_data->buffers[0]->mutable_data()
                             : NULLPTR),
        output_offsets_(
            output_values_array_data
                ? output_values_array_data->template GetMutableValues<offset_type>(1, 0)
                : NULLPTR),
        output_values_(output_values_array_data
                           ? output_values_array_data->buffers[2]->mutable_data()
                           : NULLPTR) {}

  [[nodiscard]] bool ReadValue(ValueRepr* out, int64_t read_offset) const {
    bool valid = true;
    if constexpr (has_validity_buffer) {
      valid = bit_util::GetBit(input_validity_, read_offset);
    }
    if (valid) {
      const offset_type offset0 = input_offsets_[read_offset];
      const offset_type offset1 = input_offsets_[read_offset + 1];
      *out = std::string_view(reinterpret_cast<const char*>(input_values_ + offset0),
                              offset1 - offset0);
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
    const offset_type offset0 = output_offsets_[write_offset];
    const offset_type offset1 =
        offset0 + (valid ? static_cast<offset_type>(value.size()) : 0);
    output_offsets_[write_offset + 1] = offset1;
    if (valid) {
      memcpy(output_values_ + offset0, value.data(), value.size());
    }
  }

  void WriteRun(int64_t write_offset, int64_t run_length, bool valid,
                ValueRepr value) const {
    if constexpr (has_validity_buffer) {
      bit_util::SetBitsTo(output_validity_, write_offset, run_length, valid);
    }
    if (valid) {
      int64_t i = write_offset;
      offset_type offset = output_offsets_[i];
      while (i < write_offset + run_length) {
        memcpy(output_values_ + offset, value.data(), value.size());
        offset += static_cast<offset_type>(value.size());
        i += 1;
        output_offsets_[i] = offset;
      }
    } else {
      offset_type offset = output_offsets_[write_offset];
      offset_type* begin = output_offsets_ + write_offset + 1;
      std::fill(begin, begin + run_length, offset);
    }
  }

  bool Compare(ValueRepr lhs, ValueRepr rhs) const { return lhs == rhs; }
};

Result<std::shared_ptr<Buffer>> AllocateValuesBuffer(int64_t length, const DataType& type,
                                                     MemoryPool* pool,
                                                     int64_t data_buffer_size) {
  if (type.bit_width() == 1) {
    return AllocateBitmap(length, pool);
  } else if (is_fixed_width(type.id())) {
    return AllocateBuffer(length * type.byte_width(), pool);
  } else {
    DCHECK(is_base_binary_like(type.id()));
    return AllocateBuffer(data_buffer_size, pool);
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

Result<std::shared_ptr<ArrayData>> PreallocateRunEndsArray(
    const std::shared_ptr<DataType>& run_end_type, int64_t physical_length,
    MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_buffer,
      AllocateBuffer(physical_length * run_end_type->byte_width(), pool));
  return ArrayData::Make(run_end_type, physical_length,
                         {NULLPTR, std::move(run_ends_buffer)}, /*null_count=*/0);
}

ARROW_NOINLINE Result<std::shared_ptr<ArrayData>> PreallocateValuesArray(
    const std::shared_ptr<DataType>& value_type, bool has_validity_buffer, int64_t length,
    int64_t null_count, MemoryPool* pool, int64_t data_buffer_size) {
  std::vector<std::shared_ptr<Buffer>> values_data_buffers;
  std::shared_ptr<Buffer> validity_buffer = NULLPTR;
  if (has_validity_buffer) {
    ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateBitmap(length, pool));
  }
  ARROW_ASSIGN_OR_RAISE(auto values_buffer, AllocateValuesBuffer(length, *value_type,
                                                                 pool, data_buffer_size));
  if (is_base_binary_like(value_type->id())) {
    const int offset_byte_width = offset_bit_width(value_type->id()) / 8;
    ARROW_ASSIGN_OR_RAISE(auto offsets_buffer,
                          AllocateBuffer((length + 1) * offset_byte_width, pool));
    // Ensure the first offset is zero
    memset(offsets_buffer->mutable_data(), 0, offset_byte_width);
    offsets_buffer->ZeroPadding();
    values_data_buffers = {std::move(validity_buffer), std::move(offsets_buffer),
                           std::move(values_buffer)};
  } else {
    values_data_buffers = {std::move(validity_buffer), std::move(values_buffer)};
  }
  return ArrayData::Make(value_type, length, std::move(values_data_buffers), null_count);
}

/// \brief Preallocate the ArrayData for the run-end encoded version
/// of the flat input array
///
/// \param data_buffer_size the size of the data buffer for string and binary types
ARROW_NOINLINE Result<std::shared_ptr<ArrayData>> PreallocateREEArray(
    std::shared_ptr<RunEndEncodedType> ree_type, bool has_validity_buffer,
    int64_t logical_length, int64_t physical_length, int64_t physical_null_count,
    MemoryPool* pool, int64_t data_buffer_size) {
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_data,
      PreallocateRunEndsArray(ree_type->run_end_type(), physical_length, pool));
  ARROW_ASSIGN_OR_RAISE(
      auto values_data,
      PreallocateValuesArray(ree_type->value_type(), has_validity_buffer, physical_length,
                             physical_null_count, pool, data_buffer_size));

  return ArrayData::Make(std::move(ree_type), logical_length, {NULLPTR},
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

    auto ree_type = std::make_shared<RunEndEncodedType>(
        TypeTraits<RunEndType>::type_singleton(), input_array_.type->GetSharedPtr());
    if (input_length == 0) {
      ARROW_ASSIGN_OR_RAISE(
          auto output_array_data,
          PreallocateREEArray(std::move(ree_type), has_validity_buffer, input_length, 0,
                              0, ctx_->memory_pool(), 0));
      output_->value = std::move(output_array_data);
      return Status::OK();
    }

    // First pass: count the number of runs
    int64_t num_valid_runs = 0;
    int64_t num_output_runs = 0;
    int64_t data_buffer_size = 0;  // for string and binary types
    RETURN_NOT_OK(ValidateRunEndType<RunEndType>(input_length));

    RunEndEncodingLoop<RunEndType, ValueType, has_validity_buffer> counting_loop(
        input_array_,
        /*output_values_array_data=*/NULLPTR,
        /*output_run_ends=*/NULLPTR);
    std::tie(num_valid_runs, num_output_runs, data_buffer_size) =
        counting_loop.CountNumberOfRuns();

    ARROW_ASSIGN_OR_RAISE(
        auto output_array_data,
        PreallocateREEArray(std::move(ree_type), has_validity_buffer, input_length,
                            num_output_runs, num_output_runs - num_valid_runs,
                            ctx_->memory_pool(), data_buffer_size));

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

template <typename RunEndType>
Result<std::shared_ptr<ArrayData>> PreallocateNullREEArray(int64_t logical_length,
                                                           int64_t physical_length,
                                                           MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto run_ends_buffer,
      AllocateBuffer(TypeTraits<RunEndType>::bytes_required(physical_length), pool));

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
                          PreallocateNullREEArray<RunEndType>(0, 0, ctx->memory_pool()));
    output->value = std::move(output_array_data);
    return Status::OK();
  }

  // Abort if run-end type cannot hold the input length
  RETURN_NOT_OK(ValidateRunEndType<RunEndType>(input_array.length));

  ARROW_ASSIGN_OR_RAISE(auto output_array_data, PreallocateNullREEArray<RunEndType>(
                                                    input_length, 1, ctx->memory_pool()));

  // Write the single run-end this REE has
  auto* output_run_ends =
      output_array_data->child_data[0]->template GetMutableValues<RunEndCType>(1, 0);
  output_run_ends[0] = static_cast<RunEndCType>(input_length);

  output->value = std::move(output_array_data);
  return Status::OK();
}

struct RunEndEncodeExec {
  template <typename RunEndType, typename ValueType>
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

  /// \brief For variable-length types, calculate the total length of the data
  /// buffer needed to store the expanded values.
  int64_t CalculateOutputDataBufferSize() const {
    auto& input_array_values = ree_util::ValuesArray(input_array_);
    DCHECK_EQ(input_array_values.type->id(), ValueType::type_id);
    if constexpr (is_base_binary_like(ValueType::type_id)) {
      using offset_type = typename ValueType::offset_type;
      int64_t data_buffer_size = 0;

      const ree_util::RunEndEncodedArraySpan<RunEndCType> ree_array_span(input_array_);
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

    const ree_util::RunEndEncodedArraySpan<RunEndCType> ree_array_span(input_array_);
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

    ARROW_ASSIGN_OR_RAISE(
        auto output_array_data,
        PreallocateValuesArray(ree_type->value_type(), has_validity_buffer, length,
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
      const bool has_validity_buffer = ree_util::ValuesArray(input_array).MayHaveNulls();
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
