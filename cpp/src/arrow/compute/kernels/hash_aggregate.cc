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

#include <cmath>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/hash_aggregate_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/compute/row/row_encoder_internal.h"
#include "arrow/record_batch.h"
#include "arrow/stl_allocator.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/ree_util.h"
#include "arrow/util/span.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {

using ::arrow::internal::checked_cast;
using ::arrow::internal::FirstTimeBitmapWriter;
using ::arrow::util::span;

namespace {

// ----------------------------------------------------------------------
// Count implementation

// Nullary-count implementation -- COUNT(*).
struct GroupedCountAllImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    counts_ = BufferBuilder(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    return counts_.Append(added_groups * sizeof(int64_t), 0);
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedCountAllImpl*>(&raw_other);

    auto* counts = counts_.mutable_data_as<int64_t>();
    const auto* other_counts = other->counts_.data_as<int64_t>();

    auto* g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
    }
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    auto* counts = counts_.mutable_data_as<int64_t>();
    auto* g_begin = batch[0].array.GetValues<uint32_t>(1);
    for (auto g_itr = g_begin, end = g_itr + batch.length; g_itr != end; g_itr++) {
      counts[*g_itr] += 1;
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto counts, counts_.Finish());
    return std::make_shared<Int64Array>(num_groups_, std::move(counts));
  }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

  int64_t num_groups_ = 0;
  BufferBuilder counts_;
};

struct GroupedCountImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    options_ = checked_cast<const CountOptions&>(*args.options);
    counts_ = BufferBuilder(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    return counts_.Append(added_groups * sizeof(int64_t), 0);
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedCountImpl*>(&raw_other);

    auto* counts = counts_.mutable_data_as<int64_t>();
    const auto* other_counts = other->counts_.data_as<int64_t>();

    auto* g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
    }
    return Status::OK();
  }

  template <bool count_valid>
  struct RunEndEncodedCountImpl {
    /// Count the number of valid or invalid values in a run-end-encoded array.
    ///
    /// \param[in] input the run-end-encoded array
    /// \param[out] counts the counts being accumulated
    /// \param[in] g the group ids of the values in the array
    template <typename RunEndCType>
    void DoCount(const ArraySpan& input, int64_t* counts, const uint32_t* g) {
      ree_util::RunEndEncodedArraySpan<RunEndCType> ree_span(input);
      const auto* physical_validity = ree_util::ValuesArray(input).GetValues<uint8_t>(0);
      auto end = ree_span.end();
      for (auto it = ree_span.begin(); it != end; ++it) {
        const bool is_valid = bit_util::GetBit(physical_validity, it.index_into_array());
        if (is_valid == count_valid) {
          for (int64_t i = 0; i < it.run_length(); ++i, ++g) {
            counts[*g] += 1;
          }
        } else {
          g += it.run_length();
        }
      }
    }

    void operator()(const ArraySpan& input, int64_t* counts, const uint32_t* g) {
      auto ree_type = checked_cast<const RunEndEncodedType*>(input.type);
      switch (ree_type->run_end_type()->id()) {
        case Type::INT16:
          DoCount<int16_t>(input, counts, g);
          break;
        case Type::INT32:
          DoCount<int32_t>(input, counts, g);
          break;
        default:
          DoCount<int64_t>(input, counts, g);
          break;
      }
    }
  };

  Status Consume(const ExecSpan& batch) override {
    auto* counts = counts_.mutable_data_as<int64_t>();
    auto* g_begin = batch[1].array.GetValues<uint32_t>(1);

    if (options_.mode == CountOptions::ALL) {
      for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
        counts[*g_begin] += 1;
      }
    } else if (batch[0].is_array()) {
      const ArraySpan& input = batch[0].array;
      if (options_.mode == CountOptions::ONLY_VALID) {  // ONLY_VALID
        if (input.type->id() != arrow::Type::NA) {
          const uint8_t* bitmap = input.buffers[0].data;
          if (bitmap) {
            arrow::internal::VisitSetBitRunsVoid(
                bitmap, input.offset, input.length, [&](int64_t offset, int64_t length) {
                  auto g = g_begin + offset;
                  for (int64_t i = 0; i < length; ++i, ++g) {
                    counts[*g] += 1;
                  }
                });
          } else {
            // Array without validity bitmaps require special handling of nulls.
            const bool all_valid = !input.MayHaveLogicalNulls();
            if (all_valid) {
              for (int64_t i = 0; i < input.length; ++i, ++g_begin) {
                counts[*g_begin] += 1;
              }
            } else {
              switch (input.type->id()) {
                case Type::RUN_END_ENCODED:
                  RunEndEncodedCountImpl<true>{}(input, counts, g_begin);
                  break;
                default:  // Generic and forward-compatible version.
                  for (int64_t i = 0; i < input.length; ++i, ++g_begin) {
                    counts[*g_begin] += input.IsValid(i);
                  }
                  break;
              }
            }
          }
        }
      } else {  // ONLY_NULL
        if (input.type->id() == arrow::Type::NA) {
          for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
            counts[*g_begin] += 1;
          }
        } else if (input.MayHaveLogicalNulls()) {
          if (input.HasValidityBitmap()) {
            auto end = input.offset + input.length;
            for (int64_t i = input.offset; i < end; ++i, ++g_begin) {
              counts[*g_begin] += !bit_util::GetBit(input.buffers[0].data, i);
            }
          } else {
            // Arrays without validity bitmaps require special handling of nulls.
            switch (input.type->id()) {
              case Type::RUN_END_ENCODED:
                RunEndEncodedCountImpl<false>{}(input, counts, g_begin);
                break;
              default:  // Generic and forward-compatible version.
                for (int64_t i = 0; i < input.length; ++i, ++g_begin) {
                  counts[*g_begin] += input.IsNull(i);
                }
                break;
            }
          }
        }
      }
    } else {
      const Scalar& input = *batch[0].scalar;
      if (options_.mode == CountOptions::ONLY_VALID) {
        for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
          counts[*g_begin] += input.is_valid;
        }
      } else {  // ONLY_NULL
        for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
          counts[*g_begin] += !input.is_valid;
        }
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto counts, counts_.Finish());
    return std::make_shared<Int64Array>(num_groups_, std::move(counts));
  }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

  int64_t num_groups_ = 0;
  CountOptions options_;
  BufferBuilder counts_;
};

// ----------------------------------------------------------------------
// MinMax implementation

template <typename CType>
struct AntiExtrema {
  static constexpr CType anti_min() { return std::numeric_limits<CType>::max(); }
  static constexpr CType anti_max() { return std::numeric_limits<CType>::min(); }
};

template <>
struct AntiExtrema<bool> {
  static constexpr bool anti_min() { return true; }
  static constexpr bool anti_max() { return false; }
};

template <>
struct AntiExtrema<float> {
  static constexpr float anti_min() { return std::numeric_limits<float>::infinity(); }
  static constexpr float anti_max() { return -std::numeric_limits<float>::infinity(); }
};

template <>
struct AntiExtrema<double> {
  static constexpr double anti_min() { return std::numeric_limits<double>::infinity(); }
  static constexpr double anti_max() { return -std::numeric_limits<double>::infinity(); }
};

template <>
struct AntiExtrema<Decimal32> {
  static constexpr Decimal32 anti_min() { return BasicDecimal32::GetMaxSentinel(); }
  static constexpr Decimal32 anti_max() { return BasicDecimal32::GetMinSentinel(); }
};

template <>
struct AntiExtrema<Decimal64> {
  static constexpr Decimal64 anti_min() { return BasicDecimal64::GetMaxSentinel(); }
  static constexpr Decimal64 anti_max() { return BasicDecimal64::GetMinSentinel(); }
};

template <>
struct AntiExtrema<Decimal128> {
  static constexpr Decimal128 anti_min() { return BasicDecimal128::GetMaxSentinel(); }
  static constexpr Decimal128 anti_max() { return BasicDecimal128::GetMinSentinel(); }
};

template <>
struct AntiExtrema<Decimal256> {
  static constexpr Decimal256 anti_min() { return BasicDecimal256::GetMaxSentinel(); }
  static constexpr Decimal256 anti_max() { return BasicDecimal256::GetMinSentinel(); }
};

template <typename Type, typename Enable = void>
struct GroupedMinMaxImpl final : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;
  using GetSet = GroupedValueTraits<Type>;
  using ArrType =
      typename std::conditional<is_boolean_type<Type>::value, uint8_t, CType>::type;

  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    options_ = *checked_cast<const ScalarAggregateOptions*>(args.options);
    // type_ initialized by MinMaxInit
    mins_ = TypedBufferBuilder<CType>(ctx->memory_pool());
    maxes_ = TypedBufferBuilder<CType>(ctx->memory_pool());
    has_values_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    has_nulls_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(mins_.Append(added_groups, AntiExtrema<CType>::anti_min()));
    RETURN_NOT_OK(maxes_.Append(added_groups, AntiExtrema<CType>::anti_max()));
    RETURN_NOT_OK(has_values_.Append(added_groups, false));
    RETURN_NOT_OK(has_nulls_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    auto raw_mins = mins_.mutable_data();
    auto raw_maxes = maxes_.mutable_data();

    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, CType val) {
          GetSet::Set(raw_mins, g, std::min(GetSet::Get(raw_mins, g), val));
          GetSet::Set(raw_maxes, g, std::max(GetSet::Get(raw_maxes, g), val));
          bit_util::SetBit(has_values_.mutable_data(), g);
        },
        [&](uint32_t g) { bit_util::SetBit(has_nulls_.mutable_data(), g); });
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedMinMaxImpl*>(&raw_other);

    auto raw_mins = mins_.mutable_data();
    auto raw_maxes = maxes_.mutable_data();

    auto other_raw_mins = other->mins_.mutable_data();
    auto other_raw_maxes = other->maxes_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      GetSet::Set(
          raw_mins, *g,
          std::min(GetSet::Get(raw_mins, *g), GetSet::Get(other_raw_mins, other_g)));
      GetSet::Set(
          raw_maxes, *g,
          std::max(GetSet::Get(raw_maxes, *g), GetSet::Get(other_raw_maxes, other_g)));

      if (bit_util::GetBit(other->has_values_.data(), other_g)) {
        bit_util::SetBit(has_values_.mutable_data(), *g);
      }
      if (bit_util::GetBit(other->has_nulls_.data(), other_g)) {
        bit_util::SetBit(has_nulls_.mutable_data(), *g);
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    // aggregation for group is valid if there was at least one value in that group
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_values_.Finish());

    if (!options_.skip_nulls) {
      // ... and there were no nulls in that group
      ARROW_ASSIGN_OR_RAISE(auto has_nulls, has_nulls_.Finish());
      arrow::internal::BitmapAndNot(null_bitmap->data(), 0, has_nulls->data(), 0,
                                    num_groups_, 0, null_bitmap->mutable_data());
    }

    auto mins = ArrayData::Make(type_, num_groups_, {null_bitmap, nullptr});
    auto maxes = ArrayData::Make(type_, num_groups_, {std::move(null_bitmap), nullptr});
    ARROW_ASSIGN_OR_RAISE(mins->buffers[1], mins_.Finish());
    ARROW_ASSIGN_OR_RAISE(maxes->buffers[1], maxes_.Finish());

    return ArrayData::Make(out_type(), num_groups_, {nullptr},
                           {std::move(mins), std::move(maxes)});
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", type_), field("max", type_)});
  }

  int64_t num_groups_;
  TypedBufferBuilder<CType> mins_, maxes_;
  TypedBufferBuilder<bool> has_values_, has_nulls_;
  std::shared_ptr<DataType> type_;
  ScalarAggregateOptions options_;
};

// For binary-like types
// In principle, FixedSizeBinary could use base implementation
template <typename Type>
struct GroupedMinMaxImpl<Type,
                         enable_if_t<is_base_binary_type<Type>::value ||
                                     std::is_same<Type, FixedSizeBinaryType>::value>>
    final : public GroupedAggregator {
  using Allocator = arrow::stl::allocator<char>;
  using StringType = std::basic_string<char, std::char_traits<char>, Allocator>;

  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    ctx_ = ctx;
    allocator_ = Allocator(ctx->memory_pool());
    options_ = *checked_cast<const ScalarAggregateOptions*>(args.options);
    // type_ initialized by MinMaxInit
    has_values_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    has_nulls_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    DCHECK_GE(added_groups, 0);
    num_groups_ = new_num_groups;
    mins_.resize(new_num_groups);
    maxes_.resize(new_num_groups);
    RETURN_NOT_OK(has_values_.Append(added_groups, false));
    RETURN_NOT_OK(has_nulls_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, std::string_view val) {
          if (!mins_[g] || val < *mins_[g]) {
            mins_[g].emplace(val.data(), val.size(), allocator_);
          }
          if (!maxes_[g] || val > *maxes_[g]) {
            maxes_[g].emplace(val.data(), val.size(), allocator_);
          }
          bit_util::SetBit(has_values_.mutable_data(), g);
          return Status::OK();
        },
        [&](uint32_t g) {
          bit_util::SetBit(has_nulls_.mutable_data(), g);
          return Status::OK();
        });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedMinMaxImpl*>(&raw_other);
    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      if (!mins_[*g] ||
          (mins_[*g] && other->mins_[other_g] && *mins_[*g] > *other->mins_[other_g])) {
        mins_[*g] = std::move(other->mins_[other_g]);
      }
      if (!maxes_[*g] || (maxes_[*g] && other->maxes_[other_g] &&
                          *maxes_[*g] < *other->maxes_[other_g])) {
        maxes_[*g] = std::move(other->maxes_[other_g]);
      }

      if (bit_util::GetBit(other->has_values_.data(), other_g)) {
        bit_util::SetBit(has_values_.mutable_data(), *g);
      }
      if (bit_util::GetBit(other->has_nulls_.data(), other_g)) {
        bit_util::SetBit(has_nulls_.mutable_data(), *g);
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    // aggregation for group is valid if there was at least one value in that group
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_values_.Finish());

    if (!options_.skip_nulls) {
      // ... and there were no nulls in that group
      ARROW_ASSIGN_OR_RAISE(auto has_nulls, has_nulls_.Finish());
      arrow::internal::BitmapAndNot(null_bitmap->data(), 0, has_nulls->data(), 0,
                                    num_groups_, 0, null_bitmap->mutable_data());
    }

    auto mins = ArrayData::Make(type_, num_groups_, {null_bitmap, nullptr});
    auto maxes = ArrayData::Make(type_, num_groups_, {std::move(null_bitmap), nullptr});
    RETURN_NOT_OK(MakeOffsetsValues(mins.get(), mins_));
    RETURN_NOT_OK(MakeOffsetsValues(maxes.get(), maxes_));
    return ArrayData::Make(out_type(), num_groups_, {nullptr},
                           {std::move(mins), std::move(maxes)});
  }

  template <typename T = Type>
  enable_if_base_binary<T, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<std::optional<StringType>>& values) {
    using offset_type = typename T::offset_type;
    ARROW_ASSIGN_OR_RAISE(
        auto raw_offsets,
        AllocateBuffer((1 + values.size()) * sizeof(offset_type), ctx_->memory_pool()));
    auto* offsets = raw_offsets->mutable_data_as<offset_type>();
    offsets[0] = 0;
    offsets++;
    const uint8_t* null_bitmap = array->buffers[0]->data();
    offset_type total_length = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        if (value->size() >
                static_cast<size_t>(std::numeric_limits<offset_type>::max()) ||
            arrow::internal::AddWithOverflow(
                total_length, static_cast<offset_type>(value->size()), &total_length)) {
          return Status::Invalid("Result is too large to fit in ", *array->type,
                                 " cast to large_ variant of type");
        }
      }
      offsets[i] = total_length;
    }
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), value->size());
        offset += value->size();
      }
    }
    array->buffers[1] = std::move(raw_offsets);
    array->buffers.push_back(std::move(data));
    return Status::OK();
  }

  template <typename T = Type>
  enable_if_same<T, FixedSizeBinaryType, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<std::optional<StringType>>& values) {
    const uint8_t* null_bitmap = array->buffers[0]->data();
    const int32_t slot_width =
        checked_cast<const FixedSizeBinaryType&>(*array->type).byte_width();
    int64_t total_length = values.size() * slot_width;
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), slot_width);
      } else {
        std::memset(data->mutable_data() + offset, 0x00, slot_width);
      }
      offset += slot_width;
    }
    array->buffers[1] = std::move(data);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", type_), field("max", type_)});
  }

  ExecContext* ctx_;
  Allocator allocator_;
  int64_t num_groups_;
  std::vector<std::optional<StringType>> mins_, maxes_;
  TypedBufferBuilder<bool> has_values_, has_nulls_;
  std::shared_ptr<DataType> type_;
  ScalarAggregateOptions options_;
};

struct GroupedNullMinMaxImpl final : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs&) override { return Status::OK(); }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override { return Status::OK(); }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    return ArrayData::Make(
        out_type(), num_groups_, {nullptr},
        {
            ArrayData::Make(null(), num_groups_, {nullptr}, num_groups_),
            ArrayData::Make(null(), num_groups_, {nullptr}, num_groups_),
        });
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", null()), field("max", null())});
  }

  int64_t num_groups_;
};

template <typename T>
Result<std::unique_ptr<KernelState>> MinMaxInit(KernelContext* ctx,
                                                const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<GroupedMinMaxImpl<T>>(ctx, args));
  static_cast<GroupedMinMaxImpl<T>*>(impl.get())->type_ = args.inputs[0].GetSharedPtr();
  return impl;
}

template <MinOrMax min_or_max>
HashAggregateKernel MakeMinOrMaxKernel(HashAggregateFunction* min_max_func) {
  HashAggregateKernel kernel;
  kernel.init = [min_max_func](
                    KernelContext* ctx,
                    const KernelInitArgs& args) -> Result<std::unique_ptr<KernelState>> {
    std::vector<TypeHolder> inputs = args.inputs;
    ARROW_ASSIGN_OR_RAISE(auto kernel, min_max_func->DispatchExact(args.inputs));
    KernelInitArgs new_args{kernel, inputs, args.options};
    return kernel->init(ctx, new_args);
  };
  kernel.signature =
      KernelSignature::Make({InputType::Any(), Type::UINT32}, OutputType(FirstType));
  kernel.resize = HashAggregateResize;
  kernel.consume = HashAggregateConsume;
  kernel.merge = HashAggregateMerge;
  kernel.finalize = [](KernelContext* ctx, Datum* out) {
    ARROW_ASSIGN_OR_RAISE(Datum temp,
                          checked_cast<GroupedAggregator*>(ctx->state())->Finalize());
    *out = temp.array_as<StructArray>()->field(static_cast<uint8_t>(min_or_max));
    return Status::OK();
  };
  return kernel;
}

struct GroupedMinMaxFactory {
  template <typename T>
  enable_if_physical_integer<T, Status> Visit(const T&) {
    using PhysicalType = typename T::PhysicalType;
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<PhysicalType>);
    return Status::OK();
  }

  // MSVC2015 apparently doesn't compile this properly if we use
  // enable_if_floating_point
  Status Visit(const FloatType&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<FloatType>);
    return Status::OK();
  }

  Status Visit(const DoubleType&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<DoubleType>);
    return Status::OK();
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<T>);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<FixedSizeBinaryType>);
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    kernel = MakeKernel(std::move(argument_type), MinMaxInit<BooleanType>);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<GroupedNullMinMaxImpl>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Computing min/max of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Computing min/max of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedMinMaxFactory factory;
    factory.argument_type = type->id();
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// FirstLast implementation

template <typename Type, typename Enable = void>
struct GroupedFirstLastImpl final : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;
  using GetSet = GroupedValueTraits<Type>;
  using ArrType =
      typename std::conditional<is_boolean_type<Type>::value, uint8_t, CType>::type;

  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    options_ = *checked_cast<const ScalarAggregateOptions*>(args.options);

    // First and last non-null values
    firsts_ = TypedBufferBuilder<CType>(ctx->memory_pool());
    lasts_ = TypedBufferBuilder<CType>(ctx->memory_pool());

    // Whether the first/last element is null
    first_is_nulls_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    last_is_nulls_ = TypedBufferBuilder<bool>(ctx->memory_pool());

    has_values_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    has_any_values_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    // Reusing AntiExtrema as uninitialized value here because it doesn't
    // matter what the value is. We never output the uninitialized
    // first/last value.
    RETURN_NOT_OK(firsts_.Append(added_groups, AntiExtrema<CType>::anti_min()));
    RETURN_NOT_OK(lasts_.Append(added_groups, AntiExtrema<CType>::anti_max()));
    RETURN_NOT_OK(has_values_.Append(added_groups, false));
    RETURN_NOT_OK(first_is_nulls_.Append(added_groups, false));
    RETURN_NOT_OK(last_is_nulls_.Append(added_groups, false));
    RETURN_NOT_OK(has_any_values_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    auto raw_firsts = firsts_.mutable_data();
    auto raw_lasts = lasts_.mutable_data();
    auto raw_has_values = has_values_.mutable_data();
    auto raw_has_any_values = has_any_values_.mutable_data();
    auto raw_first_is_nulls = first_is_nulls_.mutable_data();
    auto raw_last_is_nulls = last_is_nulls_.mutable_data();

    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, CType val) {
          if (!bit_util::GetBit(raw_has_values, g)) {
            GetSet::Set(raw_firsts, g, val);
            bit_util::SetBit(raw_has_values, g);
            bit_util::SetBit(raw_has_any_values, g);
          }
          // No not need to set first_is_nulls because
          // Once first_is_nulls is set to true it never
          // changes
          bit_util::SetBitTo(raw_last_is_nulls, g, false);
          GetSet::Set(raw_lasts, g, val);
          DCHECK(bit_util::GetBit(raw_has_values, g));
        },
        [&](uint32_t g) {
          // We update first_is_null to true if this is called
          // before we see any non-null values
          if (!bit_util::GetBit(raw_has_values, g)) {
            bit_util::SetBit(raw_first_is_nulls, g);
            bit_util::SetBit(raw_has_any_values, g);
          }
          bit_util::SetBit(raw_last_is_nulls, g);
        });
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    // The merge is asymmetric. "first" from this state gets pick over "first" from other
    // state. "last" from other state gets pick over from this state. This is so that when
    // using with segmented aggregation, we still get the correct "first" and "last"
    // value for the entire segment.
    auto other = checked_cast<GroupedFirstLastImpl*>(&raw_other);

    auto raw_firsts = firsts_.mutable_data();
    auto raw_lasts = lasts_.mutable_data();
    auto raw_has_values = has_values_.mutable_data();
    auto raw_has_any_values = has_any_values_.mutable_data();
    auto raw_first_is_nulls = first_is_nulls_.mutable_data();
    auto raw_last_is_nulls = last_is_nulls_.mutable_data();

    auto other_raw_firsts = other->firsts_.mutable_data();
    auto other_raw_lasts = other->lasts_.mutable_data();
    auto other_raw_has_values = other->has_values_.mutable_data();
    auto other_raw_has_any_values = other->has_values_.mutable_data();
    auto other_raw_last_is_nulls = other->last_is_nulls_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);

    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      if (!bit_util::GetBit(raw_has_values, *g)) {
        if (bit_util::GetBit(other_raw_has_values, other_g)) {
          GetSet::Set(raw_firsts, *g, GetSet::Get(other_raw_firsts, other_g));
        }
      }
      if (bit_util::GetBit(other_raw_has_values, other_g)) {
        GetSet::Set(raw_lasts, *g, GetSet::Get(other_raw_lasts, other_g));
      }
      // If the current state doesn't have any nulls (null or non-null), then
      // We take the "first_is_null" from rhs
      if (!bit_util::GetBit(raw_has_any_values, *g)) {
        bit_util::SetBitTo(raw_first_is_nulls, *g,
                           bit_util::GetBit(other->first_is_nulls_.data(), other_g));
      }
      if (bit_util::GetBit(other_raw_last_is_nulls, other_g)) {
        bit_util::SetBit(raw_last_is_nulls, *g);
      }

      if (bit_util::GetBit(other_raw_has_values, other_g)) {
        bit_util::SetBit(raw_has_values, *g);
      }

      if (bit_util::GetBit(other_raw_has_any_values, other_g)) {
        bit_util::SetBit(raw_has_any_values, *g);
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    // We initialize the null bitmap with first_is_nulls and last_is_nulls
    // then update it depending on has_values
    ARROW_ASSIGN_OR_RAISE(auto first_null_bitmap, first_is_nulls_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto last_null_bitmap, last_is_nulls_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto has_values, has_values_.Finish());

    auto raw_first_null_bitmap = first_null_bitmap->mutable_data();
    auto raw_last_null_bitmap = last_null_bitmap->mutable_data();
    auto raw_has_values = has_values->data();

    if (options_.skip_nulls) {
      for (int i = 0; i < num_groups_; i++) {
        const bool has_value = bit_util::GetBit(has_values->data(), i);
        bit_util::SetBitTo(raw_first_null_bitmap, i, has_value);
        bit_util::SetBitTo(raw_last_null_bitmap, i, has_value);
      }
    } else {
      for (int i = 0; i < num_groups_; i++) {
        // If first is null, we set the mask to false to output null
        if (bit_util::GetBit(raw_first_null_bitmap, i)) {
          bit_util::SetBitTo(raw_first_null_bitmap, i, false);
        } else {
          bit_util::SetBitTo(raw_first_null_bitmap, i,
                             bit_util::GetBit(raw_has_values, i));
        }
      }
      for (int i = 0; i < num_groups_; i++) {
        // If last is null, we set the mask to false to output null
        if (bit_util::GetBit(raw_last_null_bitmap, i)) {
          bit_util::SetBitTo(raw_last_null_bitmap, i, false);
        } else {
          bit_util::SetBitTo(raw_last_null_bitmap, i,
                             bit_util::GetBit(raw_has_values, i));
        }
      }
    }

    auto firsts =
        ArrayData::Make(type_, num_groups_, {std::move(first_null_bitmap), nullptr});
    auto lasts =
        ArrayData::Make(type_, num_groups_, {std::move(last_null_bitmap), nullptr});
    ARROW_ASSIGN_OR_RAISE(firsts->buffers[1], firsts_.Finish());
    ARROW_ASSIGN_OR_RAISE(lasts->buffers[1], lasts_.Finish());

    return ArrayData::Make(out_type(), num_groups_, {nullptr},
                           {std::move(firsts), std::move(lasts)});
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("first", type_), field("last", type_)});
  }

  int64_t num_groups_;
  TypedBufferBuilder<CType> firsts_, lasts_;
  // has_values is true if there is non-null values
  // has_any_values is true if there is either null or non-null values
  TypedBufferBuilder<bool> has_values_, has_any_values_, first_is_nulls_, last_is_nulls_;
  std::shared_ptr<DataType> type_;
  ScalarAggregateOptions options_;
};

template <typename Type>
struct GroupedFirstLastImpl<Type,
                            enable_if_t<is_base_binary_type<Type>::value ||
                                        std::is_same<Type, FixedSizeBinaryType>::value>>
    final : public GroupedAggregator {
  using Allocator = arrow::stl::allocator<char>;
  using StringType = std::basic_string<char, std::char_traits<char>, Allocator>;

  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    ctx_ = ctx;
    allocator_ = Allocator(ctx->memory_pool());
    options_ = *checked_cast<const ScalarAggregateOptions*>(args.options);
    // type_ initialized by FirstLastInit
    // Whether the first/last element is null
    first_is_nulls_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    last_is_nulls_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    has_values_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    has_any_values_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    DCHECK_GE(added_groups, 0);
    num_groups_ = new_num_groups;
    firsts_.resize(new_num_groups);
    lasts_.resize(new_num_groups);
    RETURN_NOT_OK(has_values_.Append(added_groups, false));
    RETURN_NOT_OK(has_any_values_.Append(added_groups, false));
    RETURN_NOT_OK(first_is_nulls_.Append(added_groups, false));
    RETURN_NOT_OK(last_is_nulls_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    auto raw_has_values = has_values_.mutable_data();
    auto raw_has_any_values = has_any_values_.mutable_data();
    auto raw_first_is_nulls = first_is_nulls_.mutable_data();
    auto raw_last_is_nulls = last_is_nulls_.mutable_data();

    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, std::string_view val) {
          if (!firsts_[g]) {
            firsts_[g].emplace(val.data(), val.size(), allocator_);
            bit_util::SetBit(raw_has_values, g);
            bit_util::SetBit(raw_has_any_values, g);
          }
          bit_util::SetBitTo(raw_last_is_nulls, g, false);
          lasts_[g].emplace(val.data(), val.size(), allocator_);
          return Status::OK();
        },
        [&](uint32_t g) {
          if (!bit_util::GetBit(raw_has_values, g)) {
            bit_util::SetBit(raw_first_is_nulls, g);
            bit_util::SetBit(raw_has_any_values, g);
          }
          bit_util::SetBit(raw_last_is_nulls, g);
          return Status::OK();
        });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedFirstLastImpl*>(&raw_other);
    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      if (!firsts_[*g]) {
        firsts_[*g] = std::move(other->firsts_[other_g]);
      }
      lasts_[*g] = std::move(other->lasts_[other_g]);

      if (!bit_util::GetBit(has_any_values_.data(), *g)) {
        bit_util::SetBitTo(first_is_nulls_.mutable_data(), *g,
                           bit_util::GetBit(other->first_is_nulls_.data(), other_g));
      }
      if (bit_util::GetBit(other->last_is_nulls_.data(), other_g)) {
        bit_util::SetBit(last_is_nulls_.mutable_data(), *g);
      }
      if (bit_util::GetBit(other->has_values_.data(), other_g)) {
        bit_util::SetBit(has_values_.mutable_data(), *g);
      }
      if (bit_util::GetBit(other->has_any_values_.data(), other_g)) {
        bit_util::SetBit(has_any_values_.mutable_data(), *g);
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto first_null_bitmap, first_is_nulls_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto last_null_bitmap, last_is_nulls_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto has_values, has_values_.Finish());

    if (!options_.skip_nulls) {
      for (int i = 0; i < num_groups_; i++) {
        const bool first_is_null = bit_util::GetBit(first_null_bitmap->data(), i);
        const bool has_value = bit_util::GetBit(has_values->data(), i);
        if (first_is_null) {
          bit_util::SetBitTo(first_null_bitmap->mutable_data(), i, false);
        } else {
          bit_util::SetBitTo(first_null_bitmap->mutable_data(), i, has_value);
        }
      }

      for (int i = 0; i < num_groups_; i++) {
        const bool last_is_null = bit_util::GetBit(last_null_bitmap->data(), i);
        const bool has_value = bit_util::GetBit(has_values->data(), i);
        if (last_is_null) {
          bit_util::SetBitTo(last_null_bitmap->mutable_data(), i, false);
        } else {
          bit_util::SetBitTo(last_null_bitmap->mutable_data(), i, has_value);
        }
      }
    } else {
      for (int i = 0; i < num_groups_; i++) {
        const bool has_value = bit_util::GetBit(has_values->data(), i);
        bit_util::SetBitTo(first_null_bitmap->mutable_data(), i, has_value);
        bit_util::SetBitTo(last_null_bitmap->mutable_data(), i, has_value);
      }
    }

    auto firsts =
        ArrayData::Make(type_, num_groups_, {std::move(first_null_bitmap), nullptr});
    auto lasts =
        ArrayData::Make(type_, num_groups_, {std::move(last_null_bitmap), nullptr});
    RETURN_NOT_OK(MakeOffsetsValues(firsts.get(), firsts_));
    RETURN_NOT_OK(MakeOffsetsValues(lasts.get(), lasts_));
    return ArrayData::Make(out_type(), num_groups_, {nullptr},
                           {std::move(firsts), std::move(lasts)});
  }

  template <typename T = Type>
  enable_if_base_binary<T, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<std::optional<StringType>>& values) {
    using offset_type = typename T::offset_type;
    ARROW_ASSIGN_OR_RAISE(
        auto raw_offsets,
        AllocateBuffer((1 + values.size()) * sizeof(offset_type), ctx_->memory_pool()));
    auto* offsets = raw_offsets->mutable_data_as<offset_type>();
    offsets[0] = 0;
    offsets++;
    const uint8_t* null_bitmap = array->buffers[0]->data();
    offset_type total_length = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        if (value->size() >
                static_cast<size_t>(std::numeric_limits<offset_type>::max()) ||
            arrow::internal::AddWithOverflow(
                total_length, static_cast<offset_type>(value->size()), &total_length)) {
          return Status::Invalid("Result is too large to fit in ", *array->type,
                                 " cast to large_ variant of type");
        }
      }
      offsets[i] = total_length;
    }
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), value->size());
        offset += value->size();
      }
    }
    array->buffers[1] = std::move(raw_offsets);
    array->buffers.push_back(std::move(data));
    return Status::OK();
  }

  template <typename T = Type>
  enable_if_same<T, FixedSizeBinaryType, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<std::optional<StringType>>& values) {
    const uint8_t* null_bitmap = array->buffers[0]->data();
    const int32_t slot_width =
        checked_cast<const FixedSizeBinaryType&>(*array->type).byte_width();
    int64_t total_length = values.size() * slot_width;
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), slot_width);
      } else {
        std::memset(data->mutable_data() + offset, 0x00, slot_width);
      }
      offset += slot_width;
    }
    array->buffers[1] = std::move(data);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("first", type_), field("last", type_)});
  }

  ExecContext* ctx_;
  Allocator allocator_;
  int64_t num_groups_;
  std::vector<std::optional<StringType>> firsts_, lasts_;
  TypedBufferBuilder<bool> has_values_, has_any_values_, first_is_nulls_, last_is_nulls_;
  std::shared_ptr<DataType> type_;
  ScalarAggregateOptions options_;
};

template <typename T>
Result<std::unique_ptr<KernelState>> FirstLastInit(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<GroupedFirstLastImpl<T>>(ctx, args));
  static_cast<GroupedFirstLastImpl<T>*>(impl.get())->type_ =
      args.inputs[0].GetSharedPtr();
  return impl;
}

template <FirstOrLast first_or_last>
HashAggregateKernel MakeFirstOrLastKernel(HashAggregateFunction* first_last_func) {
  HashAggregateKernel kernel;
  kernel.init = [first_last_func](
                    KernelContext* ctx,
                    const KernelInitArgs& args) -> Result<std::unique_ptr<KernelState>> {
    std::vector<TypeHolder> inputs = args.inputs;
    ARROW_ASSIGN_OR_RAISE(auto kernel, first_last_func->DispatchExact(args.inputs));
    KernelInitArgs new_args{kernel, inputs, args.options};
    return kernel->init(ctx, new_args);
  };

  kernel.signature =
      KernelSignature::Make({InputType::Any(), Type::UINT32}, OutputType(FirstType));
  kernel.resize = HashAggregateResize;
  kernel.consume = HashAggregateConsume;
  kernel.merge = HashAggregateMerge;
  kernel.finalize = [](KernelContext* ctx, Datum* out) {
    ARROW_ASSIGN_OR_RAISE(Datum temp,
                          checked_cast<GroupedAggregator*>(ctx->state())->Finalize());
    *out = temp.array_as<StructArray>()->field(static_cast<uint8_t>(first_or_last));
    return Status::OK();
  };
  kernel.ordered = true;
  return kernel;
}

struct GroupedFirstLastFactory {
  template <typename T>
  enable_if_physical_integer<T, Status> Visit(const T&) {
    using PhysicalType = typename T::PhysicalType;
    kernel = MakeKernel(std::move(argument_type), FirstLastInit<PhysicalType>,
                        /*ordered*/ true);
    return Status::OK();
  }

  Status Visit(const FloatType&) {
    kernel =
        MakeKernel(std::move(argument_type), FirstLastInit<FloatType>, /*ordered*/ true);
    return Status::OK();
  }

  Status Visit(const DoubleType&) {
    kernel =
        MakeKernel(std::move(argument_type), FirstLastInit<DoubleType>, /*ordered*/ true);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), FirstLastInit<T>);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType&) {
    kernel = MakeKernel(std::move(argument_type), FirstLastInit<FixedSizeBinaryType>);
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    kernel = MakeKernel(std::move(argument_type), FirstLastInit<BooleanType>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Computing first/last of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Computing first/last of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedFirstLastFactory factory;
    factory.argument_type = type->id();
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return factory.kernel;
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// Any/All implementation

template <typename Impl>
struct GroupedBooleanAggregator : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    options_ = checked_cast<const ScalarAggregateOptions&>(*args.options);
    pool_ = ctx->memory_pool();
    reduced_ = TypedBufferBuilder<bool>(pool_);
    no_nulls_ = TypedBufferBuilder<bool>(pool_);
    counts_ = TypedBufferBuilder<int64_t>(pool_);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(reduced_.Append(added_groups, Impl::NullValue()));
    RETURN_NOT_OK(no_nulls_.Append(added_groups, true));
    return counts_.Append(added_groups, 0);
  }

  Status Consume(const ExecSpan& batch) override {
    uint8_t* reduced = reduced_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();
    int64_t* counts = counts_.mutable_data();
    auto g = batch[1].array.GetValues<uint32_t>(1);

    if (batch[0].is_array()) {
      const ArraySpan& input = batch[0].array;
      const uint8_t* bitmap = input.buffers[1].data;
      if (input.MayHaveNulls()) {
        arrow::internal::VisitBitBlocksVoid(
            input.buffers[0].data, input.offset, input.length,
            [&](int64_t position) {
              counts[*g]++;
              Impl::UpdateGroupWith(reduced, *g, bit_util::GetBit(bitmap, position));
              g++;
            },
            [&] { bit_util::SetBitTo(no_nulls, *g++, false); });
      } else {
        arrow::internal::VisitBitBlocksVoid(
            bitmap, input.offset, input.length,
            [&](int64_t) {
              Impl::UpdateGroupWith(reduced, *g, true);
              counts[*g++]++;
            },
            [&]() {
              Impl::UpdateGroupWith(reduced, *g, false);
              counts[*g++]++;
            });
      }
    } else {
      const Scalar& input = *batch[0].scalar;
      if (input.is_valid) {
        const bool value = UnboxScalar<BooleanType>::Unbox(input);
        for (int64_t i = 0; i < batch.length; i++) {
          Impl::UpdateGroupWith(reduced, *g, value);
          counts[*g++]++;
        }
      } else {
        for (int64_t i = 0; i < batch.length; i++) {
          bit_util::SetBitTo(no_nulls, *g++, false);
        }
      }
    }
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedBooleanAggregator<Impl>*>(&raw_other);

    uint8_t* reduced = reduced_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();
    int64_t* counts = counts_.mutable_data();

    const uint8_t* other_reduced = other->reduced_.mutable_data();
    const uint8_t* other_no_nulls = other->no_nulls_.mutable_data();
    const int64_t* other_counts = other->counts_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
      Impl::UpdateGroupWith(reduced, *g, bit_util::GetBit(other_reduced, other_g));
      bit_util::SetBitTo(
          no_nulls, *g,
          bit_util::GetBit(no_nulls, *g) && bit_util::GetBit(other_no_nulls, other_g));
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    std::shared_ptr<Buffer> null_bitmap;
    const int64_t* counts = counts_.data();
    int64_t null_count = 0;

    for (int64_t i = 0; i < num_groups_; ++i) {
      if (counts[i] >= options_.min_count) continue;

      if (null_bitmap == nullptr) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(num_groups_, pool_));
        bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, num_groups_, true);
      }

      null_count += 1;
      bit_util::SetBitTo(null_bitmap->mutable_data(), i, false);
    }

    ARROW_ASSIGN_OR_RAISE(auto reduced, reduced_.Finish());
    if (!options_.skip_nulls) {
      null_count = kUnknownNullCount;
      ARROW_ASSIGN_OR_RAISE(auto no_nulls, no_nulls_.Finish());
      Impl::AdjustForMinCount(no_nulls->mutable_data(), reduced->data(), num_groups_);
      if (null_bitmap) {
        arrow::internal::BitmapAnd(null_bitmap->data(), /*left_offset=*/0,
                                   no_nulls->data(), /*right_offset=*/0, num_groups_,
                                   /*out_offset=*/0, null_bitmap->mutable_data());
      } else {
        null_bitmap = std::move(no_nulls);
      }
    }

    return ArrayData::Make(out_type(), num_groups_,
                           {std::move(null_bitmap), std::move(reduced)}, null_count);
  }

  std::shared_ptr<DataType> out_type() const override { return boolean(); }

  int64_t num_groups_ = 0;
  ScalarAggregateOptions options_;
  TypedBufferBuilder<bool> reduced_, no_nulls_;
  TypedBufferBuilder<int64_t> counts_;
  MemoryPool* pool_;
};

struct GroupedAnyImpl : public GroupedBooleanAggregator<GroupedAnyImpl> {
  // The default value for a group.
  static bool NullValue() { return false; }

  // Update the value for a group given an observation.
  static void UpdateGroupWith(uint8_t* seen, uint32_t g, bool value) {
    if (!bit_util::GetBit(seen, g) && value) {
      bit_util::SetBit(seen, g);
    }
  }

  // Combine the array of observed nulls with the array of group values.
  static void AdjustForMinCount(uint8_t* no_nulls, const uint8_t* seen,
                                int64_t num_groups) {
    arrow::internal::BitmapOr(no_nulls, /*left_offset=*/0, seen, /*right_offset=*/0,
                              num_groups, /*out_offset=*/0, no_nulls);
  }
};

struct GroupedAllImpl : public GroupedBooleanAggregator<GroupedAllImpl> {
  static bool NullValue() { return true; }

  static void UpdateGroupWith(uint8_t* seen, uint32_t g, bool value) {
    if (!value) {
      bit_util::ClearBit(seen, g);
    }
  }

  static void AdjustForMinCount(uint8_t* no_nulls, const uint8_t* seen,
                                int64_t num_groups) {
    arrow::internal::BitmapOrNot(no_nulls, /*left_offset=*/0, seen, /*right_offset=*/0,
                                 num_groups, /*out_offset=*/0, no_nulls);
  }
};

// ----------------------------------------------------------------------
// CountDistinct/Distinct implementation

struct GroupedCountDistinctImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    ctx_ = ctx;
    pool_ = ctx->memory_pool();
    options_ = checked_cast<const CountOptions&>(*args.options);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    ARROW_ASSIGN_OR_RAISE(std::ignore, grouper_->Consume(batch));
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedCountDistinctImpl*>(&raw_other);

    // Get (value, group_id) pairs, then translate the group IDs and consume them
    // ourselves
    ARROW_ASSIGN_OR_RAISE(ExecBatch uniques, other->grouper_->GetUniques());
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> remapped_g,
                          AllocateBuffer(uniques.length * sizeof(uint32_t), pool_));

    const auto* g_mapping = group_id_mapping.buffers[1]->data_as<uint32_t>();
    const auto* other_g = uniques[1].array()->buffers[1]->data_as<uint32_t>();
    auto* g = remapped_g->mutable_data_as<uint32_t>();

    for (int64_t i = 0; i < uniques.length; i++) {
      g[i] = g_mapping[other_g[i]];
    }

    ExecSpan uniques_span(uniques);
    uniques_span.values[1].array.SetBuffer(1, remapped_g);
    return Consume(uniques_span);
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values,
                          AllocateBuffer(num_groups_ * sizeof(int64_t), pool_));
    auto* counts = values->mutable_data_as<int64_t>();
    std::fill(counts, counts + num_groups_, 0);

    ARROW_ASSIGN_OR_RAISE(auto uniques, grouper_->GetUniques());
    auto* g = uniques[1].array()->GetValues<uint32_t>(1);
    const auto& items = *uniques[0].array();
    const auto* valid = items.GetValues<uint8_t>(0, 0);
    if (options_.mode == CountOptions::ALL ||
        (options_.mode == CountOptions::ONLY_VALID && !valid)) {
      for (int64_t i = 0; i < uniques.length; i++) {
        counts[g[i]]++;
      }
    } else if (options_.mode == CountOptions::ONLY_VALID) {
      for (int64_t i = 0; i < uniques.length; i++) {
        counts[g[i]] += bit_util::GetBit(valid, items.offset + i);
      }
    } else if (valid) {  // ONLY_NULL
      for (int64_t i = 0; i < uniques.length; i++) {
        counts[g[i]] += !bit_util::GetBit(valid, items.offset + i);
      }
    }

    return ArrayData::Make(int64(), num_groups_, {nullptr, std::move(values)},
                           /*null_count=*/0);
  }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

  ExecContext* ctx_;
  MemoryPool* pool_;
  int64_t num_groups_;
  CountOptions options_;
  std::unique_ptr<Grouper> grouper_;
  std::shared_ptr<DataType> out_type_;
};

struct GroupedDistinctImpl : public GroupedCountDistinctImpl {
  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto uniques, grouper_->GetUniques());
    ARROW_ASSIGN_OR_RAISE(
        auto groupings, Grouper::MakeGroupings(*uniques[1].array_as<UInt32Array>(),
                                               static_cast<uint32_t>(num_groups_), ctx_));
    ARROW_ASSIGN_OR_RAISE(
        auto list, Grouper::ApplyGroupings(*groupings, *uniques[0].make_array(), ctx_));
    const auto& values = list->values();
    DCHECK_EQ(values->offset(), 0);
    auto* offsets = list->value_offsets()->mutable_data_as<int32_t>();
    if (options_.mode == CountOptions::ALL ||
        (options_.mode == CountOptions::ONLY_VALID && values->null_count() == 0)) {
      return list;
    } else if (options_.mode == CountOptions::ONLY_VALID) {
      int32_t prev_offset = offsets[0];
      for (int64_t i = 0; i < list->length(); i++) {
        const int32_t slot_length = offsets[i + 1] - prev_offset;
        const int64_t null_count =
            slot_length - arrow::internal::CountSetBits(values->null_bitmap()->data(),
                                                        prev_offset, slot_length);
        DCHECK_LE(null_count, 1);
        const int32_t offset = null_count > 0 ? slot_length - 1 : slot_length;
        prev_offset = offsets[i + 1];
        offsets[i + 1] = offsets[i] + offset;
      }
      auto filter =
          std::make_shared<BooleanArray>(values->length(), values->null_bitmap());
      ARROW_ASSIGN_OR_RAISE(
          auto new_values,
          Filter(std::move(values), filter, FilterOptions(FilterOptions::DROP), ctx_));
      return std::make_shared<ListArray>(list->type(), list->length(),
                                         list->value_offsets(), new_values.make_array());
    }
    // ONLY_NULL
    if (values->null_count() == 0) {
      std::fill(offsets + 1, offsets + list->length() + 1, offsets[0]);
    } else {
      int32_t prev_offset = offsets[0];
      for (int64_t i = 0; i < list->length(); i++) {
        const int32_t slot_length = offsets[i + 1] - prev_offset;
        const int64_t null_count =
            slot_length - arrow::internal::CountSetBits(values->null_bitmap()->data(),
                                                        prev_offset, slot_length);
        const int32_t offset = null_count > 0 ? 1 : 0;
        prev_offset = offsets[i + 1];
        offsets[i + 1] = offsets[i] + offset;
      }
    }
    ARROW_ASSIGN_OR_RAISE(
        auto new_values,
        MakeArrayOfNull(out_type_,
                        list->length() > 0 ? offsets[list->length()] - offsets[0] : 0,
                        pool_));
    return std::make_shared<ListArray>(list->type(), list->length(),
                                       list->value_offsets(), std::move(new_values));
  }

  std::shared_ptr<DataType> out_type() const override { return list(out_type_); }
};

template <typename Impl>
Result<std::unique_ptr<KernelState>> GroupedDistinctInit(KernelContext* ctx,
                                                         const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<Impl>(ctx, args));
  auto instance = static_cast<Impl*>(impl.get());
  instance->out_type_ = args.inputs[0].GetSharedPtr();
  ARROW_ASSIGN_OR_RAISE(instance->grouper_,
                        Grouper::Make(args.inputs, ctx->exec_context()));
  return impl;
}

// ----------------------------------------------------------------------
// One implementation

template <typename Type, typename Enable = void>
struct GroupedOneImpl final : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;
  using GetSet = GroupedValueTraits<Type>;

  Status Init(ExecContext* ctx, const KernelInitArgs&) override {
    // out_type_ initialized by GroupedOneInit
    ones_ = TypedBufferBuilder<CType>(ctx->memory_pool());
    has_one_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(ones_.Append(added_groups, static_cast<CType>(0)));
    RETURN_NOT_OK(has_one_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    auto raw_ones_ = ones_.mutable_data();

    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, CType val) -> Status {
          if (!bit_util::GetBit(has_one_.data(), g)) {
            GetSet::Set(raw_ones_, g, val);
            bit_util::SetBit(has_one_.mutable_data(), g);
          }
          return Status::OK();
        },
        [&](uint32_t g) -> Status { return Status::OK(); });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedOneImpl*>(&raw_other);

    auto raw_ones = ones_.mutable_data();
    auto other_raw_ones = other->ones_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      if (!bit_util::GetBit(has_one_.data(), *g)) {
        if (bit_util::GetBit(other->has_one_.data(), other_g)) {
          GetSet::Set(raw_ones, *g, GetSet::Get(other_raw_ones, other_g));
          bit_util::SetBit(has_one_.mutable_data(), *g);
        }
      }
    }

    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_one_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto data, ones_.Finish());
    return ArrayData::Make(out_type_, num_groups_,
                           {std::move(null_bitmap), std::move(data)});
  }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  int64_t num_groups_;
  TypedBufferBuilder<CType> ones_;
  TypedBufferBuilder<bool> has_one_;
  std::shared_ptr<DataType> out_type_;
};

struct GroupedNullOneImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs&) override { return Status::OK(); }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override { return Status::OK(); }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    return ArrayData::Make(null(), num_groups_, {nullptr}, num_groups_);
  }

  std::shared_ptr<DataType> out_type() const override { return null(); }

  int64_t num_groups_;
};

template <typename Type>
struct GroupedOneImpl<Type, enable_if_t<is_base_binary_type<Type>::value ||
                                        std::is_same<Type, FixedSizeBinaryType>::value>>
    final : public GroupedAggregator {
  using Allocator = arrow::stl::allocator<char>;
  using StringType = std::basic_string<char, std::char_traits<char>, Allocator>;

  Status Init(ExecContext* ctx, const KernelInitArgs&) override {
    ctx_ = ctx;
    allocator_ = Allocator(ctx->memory_pool());
    // out_type_ initialized by GroupedOneInit
    has_one_ = TypedBufferBuilder<bool>(ctx->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    DCHECK_GE(added_groups, 0);
    num_groups_ = new_num_groups;
    ones_.resize(new_num_groups);
    RETURN_NOT_OK(has_one_.Append(added_groups, false));
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, std::string_view val) -> Status {
          if (!bit_util::GetBit(has_one_.data(), g)) {
            ones_[g].emplace(val.data(), val.size(), allocator_);
            bit_util::SetBit(has_one_.mutable_data(), g);
          }
          return Status::OK();
        },
        [&](uint32_t g) -> Status { return Status::OK(); });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedOneImpl*>(&raw_other);
    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < group_id_mapping.length;
         ++other_g, ++g) {
      if (!bit_util::GetBit(has_one_.data(), *g)) {
        if (bit_util::GetBit(other->has_one_.data(), other_g)) {
          ones_[*g] = std::move(other->ones_[other_g]);
          bit_util::SetBit(has_one_.mutable_data(), *g);
        }
      }
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap, has_one_.Finish());
    auto ones =
        ArrayData::Make(out_type(), num_groups_, {std::move(null_bitmap), nullptr});
    RETURN_NOT_OK(MakeOffsetsValues(ones.get(), ones_));
    return ones;
  }

  template <typename T = Type>
  enable_if_base_binary<T, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<std::optional<StringType>>& values) {
    using offset_type = typename T::offset_type;
    ARROW_ASSIGN_OR_RAISE(
        auto raw_offsets,
        AllocateBuffer((1 + values.size()) * sizeof(offset_type), ctx_->memory_pool()));
    auto* offsets = raw_offsets->mutable_data_as<offset_type>();
    offsets[0] = 0;
    offsets++;
    const uint8_t* null_bitmap = array->buffers[0]->data();
    offset_type total_length = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        if (value->size() >
                static_cast<size_t>(std::numeric_limits<offset_type>::max()) ||
            arrow::internal::AddWithOverflow(
                total_length, static_cast<offset_type>(value->size()), &total_length)) {
          return Status::Invalid("Result is too large to fit in ", *array->type,
                                 " cast to large_ variant of type");
        }
      }
      offsets[i] = total_length;
    }
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), value->size());
        offset += value->size();
      }
    }
    array->buffers[1] = std::move(raw_offsets);
    array->buffers.push_back(std::move(data));
    return Status::OK();
  }

  template <typename T = Type>
  enable_if_same<T, FixedSizeBinaryType, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<std::optional<StringType>>& values) {
    const uint8_t* null_bitmap = array->buffers[0]->data();
    const int32_t slot_width =
        checked_cast<const FixedSizeBinaryType&>(*array->type).byte_width();
    int64_t total_length = values.size() * slot_width;
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), slot_width);
      } else {
        std::memset(data->mutable_data() + offset, 0x00, slot_width);
      }
      offset += slot_width;
    }
    array->buffers[1] = std::move(data);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  ExecContext* ctx_;
  Allocator allocator_;
  int64_t num_groups_;
  std::vector<std::optional<StringType>> ones_;
  TypedBufferBuilder<bool> has_one_;
  std::shared_ptr<DataType> out_type_;
};

template <typename T>
Result<std::unique_ptr<KernelState>> GroupedOneInit(KernelContext* ctx,
                                                    const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<GroupedOneImpl<T>>(ctx, args));
  auto instance = static_cast<GroupedOneImpl<T>*>(impl.get());
  instance->out_type_ = args.inputs[0].GetSharedPtr();
  return impl;
}

struct GroupedOneFactory {
  template <typename T>
  enable_if_physical_integer<T, Status> Visit(const T&) {
    using PhysicalType = typename T::PhysicalType;
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<PhysicalType>);
    return Status::OK();
  }

  template <typename T>
  enable_if_floating_point<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<T>);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<FixedSizeBinaryType>);
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    kernel = MakeKernel(std::move(argument_type), GroupedOneInit<BooleanType>);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    kernel = MakeKernel(std::move(argument_type), HashAggregateInit<GroupedNullOneImpl>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Outputting one of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Outputting one of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedOneFactory factory;
    factory.argument_type = type->id();
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// List implementation

template <typename Type, typename Enable = void>
struct GroupedListImpl final : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;
  using GetSet = GroupedValueTraits<Type>;

  Status Init(ExecContext* ctx, const KernelInitArgs&) override {
    ctx_ = ctx;
    has_nulls_ = false;
    // out_type_ initialized by GroupedListInit
    values_ = TypedBufferBuilder<CType>(ctx_->memory_pool());
    groups_ = TypedBufferBuilder<uint32_t>(ctx_->memory_pool());
    values_bitmap_ = TypedBufferBuilder<bool>(ctx_->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    const ArraySpan& values_array_data = batch[0].array;
    const ArraySpan& groups_array_data = batch[1].array;

    int64_t num_values = values_array_data.length;
    const auto* groups = groups_array_data.GetValues<uint32_t>(1, 0);
    DCHECK_EQ(groups_array_data.offset, 0);
    RETURN_NOT_OK(groups_.Append(groups, num_values));

    int64_t offset = values_array_data.offset;
    const uint8_t* values = values_array_data.buffers[1].data;
    RETURN_NOT_OK(GetSet::AppendBuffers(&values_, values, offset, num_values));

    if (batch[0].null_count() > 0) {
      if (!has_nulls_) {
        has_nulls_ = true;
        RETURN_NOT_OK(values_bitmap_.Append(num_args_, true));
      }
      const uint8_t* values_bitmap = values_array_data.buffers[0].data;
      RETURN_NOT_OK(GroupedValueTraits<BooleanType>::AppendBuffers(
          &values_bitmap_, values_bitmap, offset, num_values));
    } else if (has_nulls_) {
      RETURN_NOT_OK(values_bitmap_.Append(num_values, true));
    }
    num_args_ += num_values;
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedListImpl*>(&raw_other);
    const auto* other_raw_groups = other->groups_.data();
    const auto* g = group_id_mapping.GetValues<uint32_t>(1);

    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < other->num_args_;
         ++other_g) {
      RETURN_NOT_OK(groups_.Append(g[other_raw_groups[other_g]]));
    }

    const auto* values = reinterpret_cast<const uint8_t*>(other->values_.data());
    RETURN_NOT_OK(GetSet::AppendBuffers(&values_, values, 0, other->num_args_));

    if (other->has_nulls_) {
      if (!has_nulls_) {
        has_nulls_ = true;
        RETURN_NOT_OK(values_bitmap_.Append(num_args_, true));
      }
      const uint8_t* values_bitmap = other->values_bitmap_.data();
      RETURN_NOT_OK(GroupedValueTraits<BooleanType>::AppendBuffers(
          &values_bitmap_, values_bitmap, 0, other->num_args_));
    } else if (has_nulls_) {
      RETURN_NOT_OK(values_bitmap_.Append(other->num_args_, true));
    }
    num_args_ += other->num_args_;
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto values_buffer, values_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto groups_buffer, groups_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap_buffer, values_bitmap_.Finish());

    auto groups = UInt32Array(num_args_, groups_buffer);
    ARROW_ASSIGN_OR_RAISE(
        auto groupings,
        Grouper::MakeGroupings(groups, static_cast<uint32_t>(num_groups_), ctx_));

    auto values_array_data = ArrayData::Make(
        out_type_, num_args_,
        {has_nulls_ ? std::move(null_bitmap_buffer) : nullptr, std::move(values_buffer)});
    auto values = MakeArray(values_array_data);
    return Grouper::ApplyGroupings(*groupings, *values);
  }

  std::shared_ptr<DataType> out_type() const override { return list(out_type_); }

  ExecContext* ctx_;
  int64_t num_groups_, num_args_ = 0;
  bool has_nulls_ = false;
  TypedBufferBuilder<CType> values_;
  TypedBufferBuilder<uint32_t> groups_;
  TypedBufferBuilder<bool> values_bitmap_;
  std::shared_ptr<DataType> out_type_;
};

template <typename Type>
struct GroupedListImpl<Type, enable_if_t<is_base_binary_type<Type>::value ||
                                         std::is_same<Type, FixedSizeBinaryType>::value>>
    final : public GroupedAggregator {
  using Allocator = arrow::stl::allocator<char>;
  using StringType = std::basic_string<char, std::char_traits<char>, Allocator>;
  using GetSet = GroupedValueTraits<Type>;

  Status Init(ExecContext* ctx, const KernelInitArgs&) override {
    ctx_ = ctx;
    allocator_ = Allocator(ctx_->memory_pool());
    // out_type_ initialized by GroupedListInit
    groups_ = TypedBufferBuilder<uint32_t>(ctx_->memory_pool());
    values_bitmap_ = TypedBufferBuilder<bool>(ctx_->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    const ArraySpan& values_array_data = batch[0].array;
    int64_t num_values = values_array_data.length;
    int64_t offset = values_array_data.offset;

    const ArraySpan& groups_array_data = batch[1].array;
    const uint32_t* groups = groups_array_data.GetValues<uint32_t>(1, 0);
    DCHECK_EQ(groups_array_data.offset, 0);
    RETURN_NOT_OK(groups_.Append(groups, num_values));

    if (batch[0].null_count() == 0) {
      RETURN_NOT_OK(values_bitmap_.Append(num_values, true));
    } else {
      const uint8_t* values_bitmap = values_array_data.buffers[0].data;
      RETURN_NOT_OK(GroupedValueTraits<BooleanType>::AppendBuffers(
          &values_bitmap_, values_bitmap, offset, num_values));
    }
    num_args_ += num_values;
    return VisitGroupedValues<Type>(
        batch,
        [&](uint32_t group, std::string_view val) -> Status {
          values_.emplace_back(StringType(val.data(), val.size(), allocator_));
          return Status::OK();
        },
        [&](uint32_t group) -> Status {
          values_.emplace_back("");
          return Status::OK();
        });
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedListImpl*>(&raw_other);
    const auto* other_raw_groups = other->groups_.data();
    const auto* g = group_id_mapping.GetValues<uint32_t>(1);

    for (uint32_t other_g = 0; static_cast<int64_t>(other_g) < other->num_args_;
         ++other_g) {
      RETURN_NOT_OK(groups_.Append(g[other_raw_groups[other_g]]));
    }

    values_.insert(values_.end(), other->values_.begin(), other->values_.end());

    const uint8_t* values_bitmap = other->values_bitmap_.data();
    RETURN_NOT_OK(GroupedValueTraits<BooleanType>::AppendBuffers(
        &values_bitmap_, values_bitmap, 0, other->num_args_));
    num_args_ += other->num_args_;
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto groups_buffer, groups_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto null_bitmap_buffer, values_bitmap_.Finish());

    auto groups = UInt32Array(num_args_, groups_buffer);
    ARROW_ASSIGN_OR_RAISE(
        auto groupings,
        Grouper::MakeGroupings(groups, static_cast<uint32_t>(num_groups_), ctx_));

    auto values_array_data =
        ArrayData::Make(out_type_, num_args_, {std::move(null_bitmap_buffer), nullptr});
    RETURN_NOT_OK(MakeOffsetsValues(values_array_data.get(), values_));
    auto values = MakeArray(values_array_data);
    return Grouper::ApplyGroupings(*groupings, *values);
  }

  template <typename T = Type>
  enable_if_base_binary<T, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<std::optional<StringType>>& values) {
    using offset_type = typename T::offset_type;
    ARROW_ASSIGN_OR_RAISE(
        auto raw_offsets,
        AllocateBuffer((1 + values.size()) * sizeof(offset_type), ctx_->memory_pool()));
    auto* offsets = raw_offsets->mutable_data_as<offset_type>();
    offsets[0] = 0;
    offsets++;
    const uint8_t* null_bitmap = array->buffers[0]->data();
    offset_type total_length = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        if (value->size() >
                static_cast<size_t>(std::numeric_limits<offset_type>::max()) ||
            arrow::internal::AddWithOverflow(
                total_length, static_cast<offset_type>(value->size()), &total_length)) {
          return Status::Invalid("Result is too large to fit in ", *array->type,
                                 " cast to large_ variant of type");
        }
      }
      offsets[i] = total_length;
    }
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), value->size());
        offset += value->size();
      }
    }
    array->buffers[1] = std::move(raw_offsets);
    array->buffers.push_back(std::move(data));
    return Status::OK();
  }

  template <typename T = Type>
  enable_if_same<T, FixedSizeBinaryType, Status> MakeOffsetsValues(
      ArrayData* array, const std::vector<std::optional<StringType>>& values) {
    const uint8_t* null_bitmap = array->buffers[0]->data();
    const int32_t slot_width =
        checked_cast<const FixedSizeBinaryType&>(*array->type).byte_width();
    int64_t total_length = values.size() * slot_width;
    ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(total_length, ctx_->memory_pool()));
    int64_t offset = 0;
    for (size_t i = 0; i < values.size(); i++) {
      if (bit_util::GetBit(null_bitmap, i)) {
        const std::optional<StringType>& value = values[i];
        DCHECK(value.has_value());
        std::memcpy(data->mutable_data() + offset, value->data(), slot_width);
      } else {
        std::memset(data->mutable_data() + offset, 0x00, slot_width);
      }
      offset += slot_width;
    }
    array->buffers[1] = std::move(data);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return list(out_type_); }

  ExecContext* ctx_;
  Allocator allocator_;
  int64_t num_groups_, num_args_ = 0;
  std::vector<std::optional<StringType>> values_;
  TypedBufferBuilder<uint32_t> groups_;
  TypedBufferBuilder<bool> values_bitmap_;
  std::shared_ptr<DataType> out_type_;
};

struct GroupedNullListImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs&) override {
    ctx_ = ctx;
    counts_ = TypedBufferBuilder<int64_t>(ctx_->memory_pool());
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    return counts_.Append(added_groups, 0);
  }

  Status Consume(const ExecSpan& batch) override {
    int64_t* counts = counts_.mutable_data();
    const auto* g_begin = batch[1].array.GetValues<uint32_t>(1);
    for (int64_t i = 0; i < batch.length; ++i, ++g_begin) {
      counts[*g_begin] += 1;
    }
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedNullListImpl*>(&raw_other);

    int64_t* counts = counts_.mutable_data();
    const int64_t* other_counts = other->counts_.data();

    const auto* g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
    }

    return Status::OK();
  }

  Result<Datum> Finalize() override {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(ctx_->memory_pool(), list(null()), &builder));
    auto list_builder = checked_cast<ListBuilder*>(builder.get());
    auto value_builder = checked_cast<NullBuilder*>(list_builder->value_builder());
    const int64_t* counts = counts_.data();

    for (int64_t group = 0; group < num_groups_; ++group) {
      RETURN_NOT_OK(list_builder->Append(true));
      RETURN_NOT_OK(value_builder->AppendNulls(counts[group]));
    }
    return list_builder->Finish();
  }

  std::shared_ptr<DataType> out_type() const override { return list(null()); }

  ExecContext* ctx_;
  int64_t num_groups_ = 0;
  TypedBufferBuilder<int64_t> counts_;
};

template <typename T>
Result<std::unique_ptr<KernelState>> GroupedListInit(KernelContext* ctx,
                                                     const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(auto impl, HashAggregateInit<GroupedListImpl<T>>(ctx, args));
  auto instance = static_cast<GroupedListImpl<T>*>(impl.get());
  instance->out_type_ = args.inputs[0].GetSharedPtr();
  return impl;
}

struct GroupedListFactory {
  template <typename T>
  enable_if_physical_integer<T, Status> Visit(const T&) {
    using PhysicalType = typename T::PhysicalType;
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<PhysicalType>);
    return Status::OK();
  }

  template <typename T>
  enable_if_floating_point<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<T>);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<T>);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<FixedSizeBinaryType>);
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    kernel = MakeKernel(std::move(argument_type), GroupedListInit<BooleanType>);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    kernel = MakeKernel(std::move(argument_type), HashAggregateInit<GroupedNullListImpl>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Outputting list of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Outputting list of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedListFactory factory;
    factory.argument_type = type->id();
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// Docstrings

const FunctionDoc hash_count_doc{
    "Count the number of null / non-null values in each group",
    ("By default, only non-null values are counted.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array", "group_id_array"},
    "CountOptions"};

const FunctionDoc hash_count_all_doc{"Count the number of rows in each group",
                                     ("Not caring about the values of any column."),
                                     {"group_id_array"}};

const FunctionDoc hash_first_last_doc{
    "Compute the first and last of values in each group",
    ("Null values are ignored by default.\n"
     "If skip_nulls = false, then this will return the first and last values\n"
     "regardless if it is null"),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_first_doc{
    "Compute the first value in each group",
    ("Null values are ignored by default.\n"
     "If skip_nulls = false, then this will return the first and last values\n"
     "regardless if it is null"),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_last_doc{
    "Compute the first value in each group",
    ("Null values are ignored by default.\n"
     "If skip_nulls = false, then this will return the first and last values\n"
     "regardless if it is null"),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_min_max_doc{
    "Compute the minimum and maximum of values in each group",
    ("Null values are ignored by default.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_min_or_max_doc{
    "Compute the minimum or maximum of values in each group",
    ("Null values are ignored by default.\n"
     "This can be changed through ScalarAggregateOptions."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_any_doc{"Whether any element in each group evaluates to true",
                               ("Null values are ignored."),
                               {"array", "group_id_array"},
                               "ScalarAggregateOptions"};

const FunctionDoc hash_all_doc{"Whether all elements in each group evaluate to true",
                               ("Null values are ignored."),
                               {"array", "group_id_array"},
                               "ScalarAggregateOptions"};

const FunctionDoc hash_count_distinct_doc{
    "Count the distinct values in each group",
    ("Whether nulls/values are counted is controlled by CountOptions.\n"
     "NaNs and signed zeroes are not normalized."),
    {"array", "group_id_array"},
    "CountOptions"};

const FunctionDoc hash_distinct_doc{
    "Keep the distinct values in each group",
    ("Whether nulls/values are kept is controlled by CountOptions.\n"
     "NaNs and signed zeroes are not normalized."),
    {"array", "group_id_array"},
    "CountOptions"};

const FunctionDoc hash_one_doc{"Get one value from each group",
                               ("Null values are also returned."),
                               {"array", "group_id_array"}};

const FunctionDoc hash_list_doc{"List all values in each group",
                                ("Null values are also returned."),
                                {"array", "group_id_array"}};

}  // namespace

void RegisterHashAggregateBasic(FunctionRegistry* registry) {
  static const auto default_count_options = CountOptions::Defaults();
  static const auto default_scalar_aggregate_options = ScalarAggregateOptions::Defaults();
  static const auto default_tdigest_options = TDigestOptions::Defaults();
  static const auto default_variance_options = VarianceOptions::Defaults();
  static const auto default_skew_options = SkewOptions::Defaults();

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_count", Arity::Binary(), hash_count_doc, &default_count_options);

    DCHECK_OK(func->AddKernel(
        MakeKernel(InputType::Any(), HashAggregateInit<GroupedCountImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>("hash_count_all", Arity::Unary(),
                                                        hash_count_all_doc, NULLPTR);

    DCHECK_OK(func->AddKernel(MakeUnaryKernel(HashAggregateInit<GroupedCountAllImpl>)));
    auto status = registry->AddFunction(std::move(func));
    DCHECK_OK(status);
  }

  HashAggregateFunction* first_last_func = nullptr;
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_first_last", Arity::Binary(), hash_first_last_doc,
        &default_scalar_aggregate_options);

    DCHECK_OK(
        AddHashAggKernels(NumericTypes(), GroupedFirstLastFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(TemporalTypes(), GroupedFirstLastFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(BaseBinaryTypes(), GroupedFirstLastFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({boolean(), fixed_size_binary(1)},
                                GroupedFirstLastFactory::Make, func.get()));

    first_last_func = func.get();
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_first", Arity::Binary(), hash_first_doc, &default_scalar_aggregate_options);
    DCHECK_OK(
        func->AddKernel(MakeFirstOrLastKernel<FirstOrLast::First>(first_last_func)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_last", Arity::Binary(), hash_last_doc, &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeFirstOrLastKernel<FirstOrLast::Last>(first_last_func)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  HashAggregateFunction* min_max_func = nullptr;
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_min_max", Arity::Binary(), hash_min_max_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(AddHashAggKernels(NumericTypes(), GroupedMinMaxFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(TemporalTypes(), GroupedMinMaxFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(BaseBinaryTypes(), GroupedMinMaxFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({null(), boolean(), decimal128(1, 1), decimal256(1, 1),
                                 month_interval(), fixed_size_binary(1)},
                                GroupedMinMaxFactory::Make, func.get()));
    min_max_func = func.get();
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_min", Arity::Binary(), hash_min_or_max_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeMinOrMaxKernel<MinOrMax::Min>(min_max_func)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_max", Arity::Binary(), hash_min_or_max_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeMinOrMaxKernel<MinOrMax::Max>(min_max_func)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_any", Arity::Binary(), hash_any_doc, &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeKernel(boolean(), HashAggregateInit<GroupedAnyImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_all", Arity::Binary(), hash_all_doc, &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeKernel(boolean(), HashAggregateInit<GroupedAllImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_count_distinct", Arity::Binary(), hash_count_distinct_doc,
        &default_count_options);
    DCHECK_OK(func->AddKernel(
        MakeKernel(InputType::Any(), GroupedDistinctInit<GroupedCountDistinctImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_distinct", Arity::Binary(), hash_distinct_doc, &default_count_options);
    DCHECK_OK(func->AddKernel(
        MakeKernel(InputType::Any(), GroupedDistinctInit<GroupedDistinctImpl>)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>("hash_one", Arity::Binary(),
                                                        hash_one_doc);
    DCHECK_OK(AddHashAggKernels(NumericTypes(), GroupedOneFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(TemporalTypes(), GroupedOneFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(BaseBinaryTypes(), GroupedOneFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null(), boolean(), decimal128(1, 1), decimal256(1, 1),
                                 month_interval(), fixed_size_binary(1)},
                                GroupedOneFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  {
    auto func = std::make_shared<HashAggregateFunction>("hash_list", Arity::Binary(),
                                                        hash_list_doc);
    DCHECK_OK(AddHashAggKernels(NumericTypes(), GroupedListFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(TemporalTypes(), GroupedListFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(BaseBinaryTypes(), GroupedListFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null(), boolean(), decimal128(1, 1), decimal256(1, 1),
                                 month_interval(), fixed_size_binary(1)},
                                GroupedListFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

}  // namespace arrow::compute::internal
