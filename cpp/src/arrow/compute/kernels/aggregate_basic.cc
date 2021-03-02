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

#include <unordered_map>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_basic_internal.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace compute {

namespace {

void AggregateConsume(KernelContext* ctx, const ExecBatch& batch) {
  checked_cast<ScalarAggregator*>(ctx->state())->Consume(ctx, batch);
}

void AggregateMerge(KernelContext* ctx, KernelState&& src, KernelState* dst) {
  checked_cast<ScalarAggregator*>(dst)->MergeFrom(ctx, std::move(src));
}

void AggregateFinalize(KernelContext* ctx, Datum* out) {
  checked_cast<ScalarAggregator*>(ctx->state())->Finalize(ctx, out);
}

}  // namespace

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFunction* func, SimdLevel::type simd_level,
                  bool nomerge) {
  ScalarAggregateKernel kernel(std::move(sig), init, AggregateConsume, AggregateMerge,
                               AggregateFinalize, nomerge);
  // Set the simd level
  kernel.simd_level = simd_level;
  DCHECK_OK(func->AddKernel(kernel));
}

namespace aggregate {

// ----------------------------------------------------------------------
// Count implementation

struct CountImpl : public ScalarAggregator {
  explicit CountImpl(CountOptions options) : options(std::move(options)) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    const ArrayData& input = *batch[0].array();
    const int64_t nulls = input.GetNullCount();
    this->nulls += nulls;
    this->non_nulls += input.length - nulls;
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other_state = checked_cast<const CountImpl&>(src);
    this->non_nulls += other_state.non_nulls;
    this->nulls += other_state.nulls;
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    const auto& state = checked_cast<const CountImpl&>(*ctx->state());
    switch (state.options.count_mode) {
      case CountOptions::COUNT_NON_NULL:
        *out = Datum(state.non_nulls);
        break;
      case CountOptions::COUNT_NULL:
        *out = Datum(state.nulls);
        break;
      default:
        ctx->SetStatus(Status::Invalid("Unknown CountOptions encountered"));
        break;
    }
  }

  CountOptions options;
  int64_t non_nulls = 0;
  int64_t nulls = 0;
};

struct GroupedAggregator {
  virtual ~GroupedAggregator() = default;

  virtual void Consume(KernelContext*, const Datum& aggregand,
                       const uint32_t* group_ids) = 0;

  virtual void Finalize(KernelContext* ctx, Datum* out) = 0;

  virtual void Resize(KernelContext* ctx, int64_t new_num_groups) = 0;

  virtual int64_t num_groups() const = 0;

  void MaybeResize(KernelContext* ctx, int64_t length, const uint32_t* group_ids) {
    if (length == 0) return;

    // maybe a batch of group_ids should include the min/max group id
    int64_t max_group = *std::max_element(group_ids, group_ids + length);
    auto old_size = num_groups();

    if (max_group >= old_size) {
      auto new_size = BufferBuilder::GrowByFactor(old_size, max_group + 1);
      Resize(ctx, new_size);
    }
  }

  virtual std::shared_ptr<DataType> out_type() const = 0;
};

struct GroupedCountImpl : public GroupedAggregator {
  static std::unique_ptr<GroupedCountImpl> Make(KernelContext* ctx,
                                                const std::shared_ptr<DataType>&,
                                                const FunctionOptions* options) {
    auto out = ::arrow::internal::make_unique<GroupedCountImpl>();
    out->options_ = checked_cast<const CountOptions&>(*options);
    ctx->SetStatus(ctx->Allocate(0).Value(&out->counts_));
    return out;
  }

  void Resize(KernelContext* ctx, int64_t new_num_groups) override {
    auto old_size = num_groups();
    KERNEL_RETURN_IF_ERROR(ctx, counts_->TypedResize<int64_t>(new_num_groups));
    auto new_size = num_groups();

    auto raw_counts = reinterpret_cast<int64_t*>(counts_->mutable_data());
    for (auto i = old_size; i < new_size; ++i) {
      raw_counts[i] = 0;
    }
  }

  void Consume(KernelContext* ctx, const Datum& aggregand,
               const uint32_t* group_ids) override {
    MaybeResize(ctx, aggregand.length(), group_ids);
    if (ctx->HasError()) return;

    auto raw_counts = reinterpret_cast<int64_t*>(counts_->mutable_data());

    const auto& input = aggregand.array();

    if (options_.count_mode == CountOptions::COUNT_NULL) {
      for (int64_t i = 0, input_i = input->offset; i < input->length; ++i, ++input_i) {
        auto g = group_ids[i];
        raw_counts[g] += !BitUtil::GetBit(input->buffers[0]->data(), input_i);
      }
      return;
    }

    arrow::internal::VisitSetBitRunsVoid(
        input->buffers[0], input->offset, input->length,
        [&](int64_t begin, int64_t length) {
          for (int64_t input_i = begin, i = begin - input->offset;
               input_i < begin + length; ++input_i, ++i) {
            auto g = group_ids[i];
            raw_counts[g] += 1;
          }
        });
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    auto length = num_groups();
    *out = std::make_shared<Int64Array>(length, std::move(counts_));
  }

  int64_t num_groups() const override { return counts_->size() / sizeof(int64_t); }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

  CountOptions options_;
  std::shared_ptr<ResizableBuffer> counts_;
};

struct GroupedSumImpl : public GroupedAggregator {
  // NB: whether we are accumulating into double, int64_t, or uint64_t
  // we always have 64 bits per group in the sums buffer.
  static constexpr size_t kSumSize = sizeof(int64_t);

  using ConsumeImpl = std::function<void(const std::shared_ptr<ArrayData>&,
                                         const uint32_t*, Buffer*, Buffer*)>;

  struct GetConsumeImpl {
    template <typename T,
              typename AccumulatorType = typename FindAccumulatorType<T>::Type>
    Status Visit(const T&) {
      consume_impl = [](const std::shared_ptr<ArrayData>& input,
                        const uint32_t* group_ids, Buffer* sums, Buffer* counts) {
        auto raw_input = reinterpret_cast<const typename TypeTraits<T>::CType*>(
            input->buffers[1]->data());
        auto raw_sums = reinterpret_cast<typename TypeTraits<AccumulatorType>::CType*>(
            sums->mutable_data());
        auto raw_counts = reinterpret_cast<int64_t*>(counts->mutable_data());

        arrow::internal::VisitSetBitRunsVoid(
            input->buffers[0], input->offset, input->length,
            [&](int64_t begin, int64_t length) {
              for (int64_t input_i = begin, i = begin - input->offset;
                   input_i < begin + length; ++input_i, ++i) {
                auto g = group_ids[i];
                raw_sums[g] += raw_input[input_i];
                raw_counts[g] += 1;
              }
            });
      };
      out_type = TypeTraits<AccumulatorType>::type_singleton();
      return Status::OK();
    }

    Status Visit(const BooleanType&) {
      consume_impl = [](const std::shared_ptr<ArrayData>& input,
                        const uint32_t* group_ids, Buffer* sums, Buffer* counts) {
        auto raw_input = input->buffers[1]->data();
        auto raw_sums = reinterpret_cast<uint64_t*>(sums->mutable_data());
        auto raw_counts = reinterpret_cast<int64_t*>(counts->mutable_data());

        arrow::internal::VisitSetBitRunsVoid(
            input->buffers[0], input->offset, input->length,
            [&](int64_t begin, int64_t length) {
              for (int64_t input_i = begin, i = begin - input->offset;
                   input_i < begin + length; ++input_i) {
                auto g = group_ids[i];
                raw_sums[g] += BitUtil::GetBit(raw_input, input_i);
                raw_counts[g] += 1;
              }
            });
      };
      out_type = boolean();
      return Status::OK();
    }

    Status Visit(const HalfFloatType& type) {
      return Status::NotImplemented("Summing data of type ", type);
    }

    Status Visit(const DataType& type) {
      return Status::NotImplemented("Summing data of type ", type);
    }

    ConsumeImpl consume_impl;
    std::shared_ptr<DataType> out_type;
  };

  static std::unique_ptr<GroupedSumImpl> Make(KernelContext* ctx,
                                              const std::shared_ptr<DataType>& input_type,
                                              const FunctionOptions* options) {
    auto out = ::arrow::internal::make_unique<GroupedSumImpl>();

    ctx->SetStatus(ctx->Allocate(0).Value(&out->sums_));
    if (ctx->HasError()) return nullptr;

    ctx->SetStatus(ctx->Allocate(0).Value(&out->counts_));
    if (ctx->HasError()) return nullptr;

    GetConsumeImpl get_consume_impl;
    ctx->SetStatus(VisitTypeInline(*input_type, &get_consume_impl));

    out->consume_impl_ = std::move(get_consume_impl.consume_impl);
    out->out_type_ = std::move(get_consume_impl.out_type);
    return out;
  }

  void Resize(KernelContext* ctx, int64_t new_num_groups) override {
    auto old_size = num_groups() * kSumSize;
    KERNEL_RETURN_IF_ERROR(ctx, sums_->Resize(new_num_groups * kSumSize));
    KERNEL_RETURN_IF_ERROR(ctx, counts_->Resize(new_num_groups * sizeof(int64_t)));
    auto new_size = num_groups() * kSumSize;
    std::memset(sums_->mutable_data() + old_size, 0, new_size - old_size);
    std::memset(counts_->mutable_data() + old_size, 0, new_size - old_size);
  }

  void Consume(KernelContext* ctx, const Datum& aggregand,
               const uint32_t* group_ids) override {
    MaybeResize(ctx, aggregand.length(), group_ids);
    if (ctx->HasError()) return;
    consume_impl_(aggregand.array(), group_ids, sums_.get(), counts_.get());
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    std::shared_ptr<Buffer> null_bitmap;
    int64_t null_count = 0;

    for (int64_t i = 0; i < num_groups(); ++i) {
      if (reinterpret_cast<const int64_t*>(counts_->data())[i] > 0) continue;

      if (null_bitmap == nullptr) {
        KERNEL_ASSIGN_OR_RAISE(null_bitmap, ctx, ctx->AllocateBitmap(num_groups()));
        BitUtil::SetBitsTo(null_bitmap->mutable_data(), 0, num_groups(), true);
      }

      null_count += 1;
      BitUtil::SetBitTo(null_bitmap->mutable_data(), i, false);
    }

    *out = ArrayData::Make(std::move(out_type_), num_groups(),
                           {std::move(null_bitmap), std::move(sums_)}, null_count);
  }

  int64_t num_groups() const override { return counts_->size() / sizeof(int64_t); }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  std::shared_ptr<ResizableBuffer> sums_, counts_;
  std::shared_ptr<DataType> out_type_;
  ConsumeImpl consume_impl_;
};

struct GroupedMinMaxImpl : public GroupedAggregator {
  using ConsumeImpl = std::function<void(const std::shared_ptr<ArrayData>&,
                                         const uint32_t*, BufferVector*)>;

  using ResizeImpl = std::function<Status(Buffer*, int64_t)>;

  struct GetImpl {
    template <typename T, typename CType = typename TypeTraits<T>::CType>
    enable_if_number<T, Status> Visit(const T&) {
      consume_impl = [](const std::shared_ptr<ArrayData>& input,
                        const uint32_t* group_ids, BufferVector* buffers) {
        auto raw_inputs = reinterpret_cast<const CType*>(input->buffers[1]->data());

        auto raw_mins = reinterpret_cast<CType*>(buffers->at(0)->mutable_data());
        auto raw_maxes = reinterpret_cast<CType*>(buffers->at(1)->mutable_data());

        auto raw_has_nulls = buffers->at(2)->mutable_data();
        auto raw_has_values = buffers->at(3)->mutable_data();

        for (int64_t i = 0, input_i = input->offset; i < input->length; ++i, ++input_i) {
          auto g = group_ids[i];
          bool is_valid = BitUtil::GetBit(input->buffers[0]->data(), input_i);
          if (is_valid) {
            raw_maxes[g] = std::max(raw_maxes[g], raw_inputs[input_i]);
            raw_mins[g] = std::min(raw_mins[g], raw_inputs[input_i]);
            BitUtil::SetBit(raw_has_values, g);
          } else {
            BitUtil::SetBit(raw_has_nulls, g);
          }
        }
      };

      for (auto pair :
           {std::make_pair(&resize_min_impl, std::numeric_limits<CType>::max()),
            std::make_pair(&resize_max_impl, std::numeric_limits<CType>::min())}) {
        *pair.first = [pair](Buffer* vals, int64_t new_num_groups) {
          int64_t old_num_groups = vals->size() / sizeof(CType);

          int64_t new_size = new_num_groups * sizeof(CType);
          RETURN_NOT_OK(checked_cast<ResizableBuffer*>(vals)->Resize(new_size));

          auto raw_vals = reinterpret_cast<CType*>(vals->mutable_data());
          for (int64_t i = old_num_groups; i != new_num_groups; ++i) {
            raw_vals[i] = pair.second;
          }
          return Status::OK();
        };
      }

      return Status::OK();
    }

    Status Visit(const BooleanType& type) {
      return Status::NotImplemented("Grouped MinMax data of type ", type);
    }

    Status Visit(const HalfFloatType& type) {
      return Status::NotImplemented("Grouped MinMax data of type ", type);
    }

    Status Visit(const DataType& type) {
      return Status::NotImplemented("Grouped MinMax data of type ", type);
    }

    ConsumeImpl consume_impl;
    ResizeImpl resize_min_impl, resize_max_impl;
  };

  static std::unique_ptr<GroupedMinMaxImpl> Make(
      KernelContext* ctx, const std::shared_ptr<DataType>& input_type,
      const FunctionOptions* options) {
    auto out = ::arrow::internal::make_unique<GroupedMinMaxImpl>();
    out->options_ = *checked_cast<const MinMaxOptions*>(options);
    out->type_ = input_type;

    out->buffers_.resize(4);
    for (auto& buf : out->buffers_) {
      ctx->SetStatus(ctx->Allocate(0).Value(&buf));
      if (ctx->HasError()) return nullptr;
    }

    GetImpl get_impl;
    ctx->SetStatus(VisitTypeInline(*input_type, &get_impl));

    out->consume_impl_ = std::move(get_impl.consume_impl);
    out->resize_min_impl_ = std::move(get_impl.resize_min_impl);
    out->resize_max_impl_ = std::move(get_impl.resize_max_impl);
    return out;
  }

  void Resize(KernelContext* ctx, int64_t new_num_groups) override {
    auto old_num_groups = num_groups_;
    num_groups_ = new_num_groups;

    KERNEL_RETURN_IF_ERROR(ctx, resize_min_impl_(buffers_[0].get(), new_num_groups));
    KERNEL_RETURN_IF_ERROR(ctx, resize_max_impl_(buffers_[1].get(), new_num_groups));

    for (auto buffer : {buffers_[2].get(), buffers_[3].get()}) {
      KERNEL_RETURN_IF_ERROR(ctx, checked_cast<ResizableBuffer*>(buffer)->Resize(
                                      BitUtil::BytesForBits(new_num_groups)));
      BitUtil::SetBitsTo(buffer->mutable_data(), old_num_groups, new_num_groups, false);
    }
  }

  void Consume(KernelContext* ctx, const Datum& aggregand,
               const uint32_t* group_ids) override {
    MaybeResize(ctx, aggregand.length(), group_ids);
    if (ctx->HasError()) return;
    consume_impl_(aggregand.array(), group_ids, &buffers_);
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    // aggregation for group is valid if there was at least one value in that group
    std::shared_ptr<Buffer> null_bitmap = std::move(buffers_[3]);

    if (options_.null_handling == MinMaxOptions::EMIT_NULL) {
      // ... and there were no nulls in that group
      for (int64_t i = 0; i < num_groups(); ++i) {
        if (BitUtil::GetBit(buffers_[2]->data(), i)) {
          BitUtil::ClearBit(null_bitmap->mutable_data(), i);
        }
      }
    }

    auto mins =
        ArrayData::Make(type_, num_groups(), {null_bitmap, std::move(buffers_[0])});

    auto maxes = ArrayData::Make(type_, num_groups(),
                                 {std::move(null_bitmap), std::move(buffers_[1])});

    *out = ArrayData::Make(out_type(), num_groups(), {nullptr},
                           {std::move(mins), std::move(maxes)});
  }

  int64_t num_groups() const override { return num_groups_; }

  std::shared_ptr<DataType> out_type() const override {
    return struct_({field("min", type_), field("max", type_)});
  }

  int64_t num_groups_;
  BufferVector buffers_;
  std::shared_ptr<DataType> type_;
  ConsumeImpl consume_impl_;
  ResizeImpl resize_min_impl_, resize_max_impl_;
  MinMaxOptions options_;
};

std::unique_ptr<KernelState> CountInit(KernelContext*, const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<CountImpl>(
      static_cast<const CountOptions&>(*args.options));
}

// ----------------------------------------------------------------------
// Sum implementation

template <typename ArrowType>
struct SumImplDefault : public SumImpl<ArrowType, SimdLevel::NONE> {};

template <typename ArrowType>
struct MeanImplDefault : public MeanImpl<ArrowType, SimdLevel::NONE> {};

std::unique_ptr<KernelState> SumInit(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<SumImplDefault> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

std::unique_ptr<KernelState> MeanInit(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<MeanImplDefault> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

// ----------------------------------------------------------------------
// MinMax implementation

std::unique_ptr<KernelState> MinMaxInit(KernelContext* ctx, const KernelInitArgs& args) {
  MinMaxInitState<SimdLevel::NONE> visitor(
      ctx, *args.inputs[0].type, args.kernel->signature->out_type().type(),
      static_cast<const MinMaxOptions&>(*args.options));
  return visitor.Create();
}

// ----------------------------------------------------------------------
// Any implementation

struct BooleanAnyImpl : public ScalarAggregator {
  void Consume(KernelContext*, const ExecBatch& batch) override {
    // short-circuit if seen a True already
    if (this->any == true) {
      return;
    }

    const auto& data = *batch[0].array();
    arrow::internal::OptionalBinaryBitBlockCounter counter(
        data.buffers[0], data.offset, data.buffers[1], data.offset, data.length);
    int64_t position = 0;
    while (position < data.length) {
      const auto block = counter.NextAndBlock();
      if (block.popcount > 0) {
        this->any = true;
        break;
      }
      position += block.length;
    }
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const BooleanAnyImpl&>(src);
    this->any |= other.any;
  }

  void Finalize(KernelContext*, Datum* out) override {
    out->value = std::make_shared<BooleanScalar>(this->any);
  }
  bool any = false;
};

std::unique_ptr<KernelState> AnyInit(KernelContext*, const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<BooleanAnyImpl>();
}

// ----------------------------------------------------------------------
// All implementation

struct BooleanAllImpl : public ScalarAggregator {
  void Consume(KernelContext*, const ExecBatch& batch) override {
    // short-circuit if seen a false already
    if (this->all == false) {
      return;
    }

    const auto& data = *batch[0].array();
    arrow::internal::OptionalBinaryBitBlockCounter counter(
        data.buffers[1], data.offset, data.buffers[0], data.offset, data.length);
    int64_t position = 0;
    while (position < data.length) {
      const auto block = counter.NextOrNotBlock();
      if (!block.AllSet()) {
        this->all = false;
        break;
      }
      position += block.length;
    }
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const BooleanAllImpl&>(src);
    this->all &= other.all;
  }

  void Finalize(KernelContext*, Datum* out) override {
    out->value = std::make_shared<BooleanScalar>(this->all);
  }
  bool all = true;
};

std::unique_ptr<KernelState> AllInit(KernelContext*, const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<BooleanAllImpl>();
}

struct GroupByImpl : public ScalarAggregator {
  using AddLengthImpl = std::function<void(const std::shared_ptr<ArrayData>&, int32_t*)>;

  struct GetAddLengthImpl {
    static constexpr int32_t null_extra_byte = 1;

    static void AddFixedLength(int32_t fixed_length, int64_t num_repeats,
                               int32_t* lengths) {
      for (int64_t i = 0; i < num_repeats; ++i) {
        lengths[i] += fixed_length + null_extra_byte;
      }
    }

    static void AddVarLength(const std::shared_ptr<ArrayData>& data, int32_t* lengths) {
      using offset_type = typename StringType::offset_type;
      constexpr int32_t length_extra_bytes = sizeof(offset_type);
      auto offset = data->offset;
      const auto offsets = data->GetValues<offset_type>(1);
      if (data->MayHaveNulls()) {
        const uint8_t* nulls = data->buffers[0]->data();

        for (int64_t i = 0; i < data->length; ++i) {
          bool is_null = !BitUtil::GetBit(nulls, offset + i);
          if (is_null) {
            lengths[i] += null_extra_byte + length_extra_bytes;
          } else {
            lengths[i] += null_extra_byte + length_extra_bytes + offsets[offset + i + 1] -
                          offsets[offset + i];
          }
        }
      } else {
        for (int64_t i = 0; i < data->length; ++i) {
          lengths[i] += null_extra_byte + length_extra_bytes + offsets[offset + i + 1] -
                        offsets[offset + i];
        }
      }
    }

    template <typename T>
    Status Visit(const T& input_type) {
      int32_t num_bytes = (bit_width(input_type.id()) + 7) / 8;
      add_length_impl = [num_bytes](const std::shared_ptr<ArrayData>& data,
                                    int32_t* lengths) {
        AddFixedLength(num_bytes, data->length, lengths);
      };
      return Status::OK();
    }

    Status Visit(const StringType&) {
      add_length_impl = [](const std::shared_ptr<ArrayData>& data, int32_t* lengths) {
        AddVarLength(data, lengths);
      };
      return Status::OK();
    }

    Status Visit(const BinaryType&) {
      add_length_impl = [](const std::shared_ptr<ArrayData>& data, int32_t* lengths) {
        AddVarLength(data, lengths);
      };
      return Status::OK();
    }

    Status Visit(const FixedSizeBinaryType& type) {
      int32_t num_bytes = type.byte_width();
      add_length_impl = [num_bytes](const std::shared_ptr<ArrayData>& data,
                                    int32_t* lengths) {
        AddFixedLength(num_bytes, data->length, lengths);
      };
      return Status::OK();
    }

    AddLengthImpl add_length_impl;
  };

  using EncodeNextImpl =
      std::function<void(const std::shared_ptr<ArrayData>&, uint8_t**)>;

  struct GetEncodeNextImpl {
    template <int NumBits>
    static void EncodeSmallFixed(const std::shared_ptr<ArrayData>& data,
                                 uint8_t** encoded_bytes) {
      auto raw_input = data->buffers[1]->data();
      auto offset = data->offset;
      if (data->MayHaveNulls()) {
        const uint8_t* nulls = data->buffers[0]->data();
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          bool is_null = !BitUtil::GetBit(nulls, offset + i);
          encoded_ptr[0] = is_null ? 1 : 0;
          encoded_ptr += 1;
          uint64_t null_multiplier = is_null ? 0 : 1;
          if (NumBits == 1) {
            encoded_ptr[0] = static_cast<uint8_t>(
                null_multiplier * (BitUtil::GetBit(raw_input, offset + i) ? 1 : 0));
            encoded_ptr += 1;
          }
          if (NumBits == 8) {
            encoded_ptr[0] =
                static_cast<uint8_t>(null_multiplier * reinterpret_cast<const uint8_t*>(
                                                           raw_input)[offset + i]);
            encoded_ptr += 1;
          }
          if (NumBits == 16) {
            reinterpret_cast<uint16_t*>(encoded_ptr)[0] =
                static_cast<uint16_t>(null_multiplier * reinterpret_cast<const uint16_t*>(
                                                            raw_input)[offset + i]);
            encoded_ptr += 2;
          }
          if (NumBits == 32) {
            reinterpret_cast<uint32_t*>(encoded_ptr)[0] =
                static_cast<uint32_t>(null_multiplier * reinterpret_cast<const uint32_t*>(
                                                            raw_input)[offset + i]);
            encoded_ptr += 4;
          }
          if (NumBits == 64) {
            reinterpret_cast<uint64_t*>(encoded_ptr)[0] =
                static_cast<uint64_t>(null_multiplier * reinterpret_cast<const uint64_t*>(
                                                            raw_input)[offset + i]);
            encoded_ptr += 8;
          }
        }
      } else {
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          encoded_ptr[0] = 0;
          encoded_ptr += 1;
          if (NumBits == 1) {
            encoded_ptr[0] = (BitUtil::GetBit(raw_input, offset + i) ? 1 : 0);
            encoded_ptr += 1;
          }
          if (NumBits == 8) {
            encoded_ptr[0] = reinterpret_cast<const uint8_t*>(raw_input)[offset + i];
            encoded_ptr += 1;
          }
          if (NumBits == 16) {
            reinterpret_cast<uint16_t*>(encoded_ptr)[0] =
                reinterpret_cast<const uint16_t*>(raw_input)[offset + i];
            encoded_ptr += 2;
          }
          if (NumBits == 32) {
            reinterpret_cast<uint32_t*>(encoded_ptr)[0] =
                reinterpret_cast<const uint32_t*>(raw_input)[offset + i];
            encoded_ptr += 4;
          }
          if (NumBits == 64) {
            reinterpret_cast<uint64_t*>(encoded_ptr)[0] =
                reinterpret_cast<const uint64_t*>(raw_input)[offset + i];
            encoded_ptr += 8;
          }
        }
      }
    }

    static void EncodeBigFixed(int num_bytes, const std::shared_ptr<ArrayData>& data,
                               uint8_t** encoded_bytes) {
      auto raw_input = data->buffers[1]->data();
      auto offset = data->offset;
      if (data->MayHaveNulls()) {
        const uint8_t* nulls = data->buffers[0]->data();
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          bool is_null = !BitUtil::GetBit(nulls, offset + i);
          encoded_ptr[0] = is_null ? 1 : 0;
          encoded_ptr += 1;
          if (is_null) {
            memset(encoded_ptr, 0, num_bytes);
          } else {
            memcpy(encoded_ptr, raw_input + num_bytes * (offset + i), num_bytes);
          }
          encoded_ptr += num_bytes;
        }
      } else {
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          encoded_ptr[0] = 0;
          encoded_ptr += 1;
          memcpy(encoded_ptr, raw_input + num_bytes * (offset + i), num_bytes);
          encoded_ptr += num_bytes;
        }
      }
    }

    static void EncodeVarLength(const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
      using offset_type = typename StringType::offset_type;
      auto offset = data->offset;
      const auto offsets = data->GetValues<offset_type>(1);
      auto raw_input = data->buffers[2]->data();
      if (data->MayHaveNulls()) {
        const uint8_t* nulls = data->buffers[0]->data();
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          bool is_null = !BitUtil::GetBit(nulls, offset + i);
          if (is_null) {
            encoded_ptr[0] = 1;
            encoded_ptr++;
            reinterpret_cast<offset_type*>(encoded_ptr)[0] = 0;
            encoded_ptr += sizeof(offset_type);
          } else {
            encoded_ptr[0] = 0;
            encoded_ptr++;
            size_t num_bytes = offsets[offset + i + 1] - offsets[offset + i];
            reinterpret_cast<offset_type*>(encoded_ptr)[0] = num_bytes;
            encoded_ptr += sizeof(offset_type);
            memcpy(encoded_ptr, raw_input + offsets[offset + i], num_bytes);
            encoded_ptr += num_bytes;
          }
        }
      } else {
        for (int64_t i = 0; i < data->length; ++i) {
          auto& encoded_ptr = encoded_bytes[i];
          encoded_ptr[0] = 0;
          encoded_ptr++;
          size_t num_bytes = offsets[offset + i + 1] - offsets[offset + i];
          reinterpret_cast<offset_type*>(encoded_ptr)[0] = num_bytes;
          encoded_ptr += sizeof(offset_type);
          memcpy(encoded_ptr, raw_input + offsets[offset + i], num_bytes);
          encoded_ptr += num_bytes;
        }
      }
    }

    template <typename T>
    Status Visit(const T& input_type) {
      int32_t num_bits = bit_width(input_type.id());
      switch (num_bits) {
        case 1:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<1>(data, encoded_bytes);
          };
          break;
        case 8:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<8>(data, encoded_bytes);
          };
          break;
        case 16:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<16>(data, encoded_bytes);
          };
          break;
        case 32:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<32>(data, encoded_bytes);
          };
          break;
        case 64:
          encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                                uint8_t** encoded_bytes) {
            EncodeSmallFixed<64>(data, encoded_bytes);
          };
          break;
      }
      return Status::OK();
    }

    Status Visit(const StringType&) {
      encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                            uint8_t** encoded_bytes) {
        EncodeVarLength(data, encoded_bytes);
      };
      return Status::OK();
    }

    Status Visit(const BinaryType&) {
      encode_next_impl = [](const std::shared_ptr<ArrayData>& data,
                            uint8_t** encoded_bytes) {
        EncodeVarLength(data, encoded_bytes);
      };
      return Status::OK();
    }

    Status Visit(const FixedSizeBinaryType& type) {
      int32_t num_bytes = type.byte_width();
      encode_next_impl = [num_bytes](const std::shared_ptr<ArrayData>& data,
                                     uint8_t** encoded_bytes) {
        EncodeBigFixed(num_bytes, data, encoded_bytes);
      };
      return Status::OK();
    }

    EncodeNextImpl encode_next_impl;
  };

  using DecodeNextImpl = std::function<void(KernelContext*, int32_t, uint8_t**,
                                            std::shared_ptr<ArrayData>*)>;

  struct GetDecodeNextImpl {
    static Status DecodeNulls(KernelContext* ctx, int32_t length, uint8_t** encoded_bytes,
                              std::shared_ptr<ResizableBuffer>* null_buf,
                              int32_t* null_count) {
      // Do we have nulls?
      *null_count = 0;
      for (int32_t i = 0; i < length; ++i) {
        *null_count += encoded_bytes[i][0];
      }
      if (*null_count > 0) {
        ARROW_ASSIGN_OR_RAISE(*null_buf, ctx->AllocateBitmap(length));
        uint8_t* nulls = (*null_buf)->mutable_data();
        memset(nulls, 0, (*null_buf)->size());
        for (int32_t i = 0; i < length; ++i) {
          if (!encoded_bytes[i][0]) {
            BitUtil::SetBit(nulls, i);
          }
          encoded_bytes[i] += 1;
        }
      } else {
        for (int32_t i = 0; i < length; ++i) {
          encoded_bytes[i] += 1;
        }
      }
      return Status ::OK();
    }

    template <int NumBits>
    static void DecodeSmallFixed(KernelContext* ctx, const Type::type& output_type,
                                 int32_t length, uint8_t** encoded_bytes,
                                 std::shared_ptr<ArrayData>* out) {
      std::shared_ptr<ResizableBuffer> null_buf;
      int32_t null_count;
      KERNEL_RETURN_IF_ERROR(
          ctx, DecodeNulls(ctx, length, encoded_bytes, &null_buf, &null_count));

      KERNEL_ASSIGN_OR_RAISE(
          auto key_buf, ctx,
          ctx->Allocate(NumBits == 1 ? (length + 7) / 8 : (NumBits / 8) * length));

      uint8_t* raw_output = key_buf->mutable_data();
      for (int32_t i = 0; i < length; ++i) {
        auto& encoded_ptr = encoded_bytes[i];
        if (NumBits == 1) {
          BitUtil::SetBitTo(raw_output, i, encoded_ptr[0] != 0);
          encoded_ptr += 1;
        }
        if (NumBits == 8) {
          raw_output[i] = encoded_ptr[0];
          encoded_ptr += 1;
        }
        if (NumBits == 16) {
          reinterpret_cast<uint16_t*>(raw_output)[i] =
              reinterpret_cast<const uint16_t*>(encoded_bytes[i])[0];
          encoded_ptr += 2;
        }
        if (NumBits == 32) {
          reinterpret_cast<uint32_t*>(raw_output)[i] =
              reinterpret_cast<const uint32_t*>(encoded_bytes[i])[0];
          encoded_ptr += 4;
        }
        if (NumBits == 64) {
          reinterpret_cast<uint64_t*>(raw_output)[i] =
              reinterpret_cast<const uint64_t*>(encoded_bytes[i])[0];
          encoded_ptr += 8;
        }
      }

      DCHECK(is_integer(output_type) || output_type == Type::BOOL);
      *out = ArrayData::Make(int64(), length, {null_buf, key_buf}, null_count);
    }

    static void DecodeBigFixed(KernelContext* ctx, int num_bytes, int32_t length,
                               uint8_t** encoded_bytes, std::shared_ptr<ArrayData>* out) {
      std::shared_ptr<ResizableBuffer> null_buf;
      int32_t null_count;
      KERNEL_RETURN_IF_ERROR(
          ctx, DecodeNulls(ctx, length, encoded_bytes, &null_buf, &null_count));

      KERNEL_ASSIGN_OR_RAISE(auto key_buf, ctx, ctx->Allocate(num_bytes * length));
      auto raw_output = key_buf->mutable_data();
      for (int32_t i = 0; i < length; ++i) {
        memcpy(raw_output + i * num_bytes, encoded_bytes[i], num_bytes);
        encoded_bytes[i] += num_bytes;
      }

      *out = ArrayData::Make(fixed_size_binary(num_bytes), length, {null_buf, key_buf},
                             null_count);
    }

    static void DecodeVarLength(KernelContext* ctx, bool is_string, int32_t length,
                                uint8_t** encoded_bytes,
                                std::shared_ptr<ArrayData>* out) {
      std::shared_ptr<ResizableBuffer> null_buf;
      int32_t null_count;
      KERNEL_RETURN_IF_ERROR(
          ctx, DecodeNulls(ctx, length, encoded_bytes, &null_buf, &null_count));

      using offset_type = typename StringType::offset_type;

      int32_t length_sum = 0;
      for (int32_t i = 0; i < length; ++i) {
        length_sum += reinterpret_cast<offset_type*>(encoded_bytes)[0];
      }

      KERNEL_ASSIGN_OR_RAISE(auto offset_buf, ctx,
                             ctx->Allocate(sizeof(offset_type) * (1 + length)));
      KERNEL_ASSIGN_OR_RAISE(auto key_buf, ctx, ctx->Allocate(length_sum));

      auto raw_offsets = offset_buf->mutable_data();
      auto raw_keys = key_buf->mutable_data();
      int32_t current_offset = 0;
      for (int32_t i = 0; i < length; ++i) {
        offset_type key_length = reinterpret_cast<offset_type*>(encoded_bytes[i])[0];
        reinterpret_cast<offset_type*>(raw_offsets)[i] = current_offset;
        encoded_bytes[i] += sizeof(offset_type);
        memcpy(raw_keys + current_offset, encoded_bytes[i], key_length);
        encoded_bytes[i] += key_length;
        current_offset += key_length;
      }
      reinterpret_cast<offset_type*>(raw_offsets)[length] = current_offset;

      if (is_string) {
        *out = ArrayData::Make(utf8(), length, {null_buf, offset_buf, key_buf},
                               null_count, 0);
      } else {
        *out = ArrayData::Make(binary(), length, {null_buf, offset_buf, key_buf},
                               null_count, 0);
      }
    }

    template <typename T>
    Status Visit(const T& input_type) {
      int32_t num_bits = bit_width(input_type.id());
      auto type_id = input_type.id();
      switch (num_bits) {
        case 1:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<1>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
        case 8:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<8>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
        case 16:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<16>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
        case 32:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<32>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
        case 64:
          decode_next_impl = [type_id](KernelContext* ctx, int32_t length,
                                       uint8_t** encoded_bytes,
                                       std::shared_ptr<ArrayData>* out) {
            DecodeSmallFixed<64>(ctx, type_id, length, encoded_bytes, out);
          };
          break;
      }
      return Status::OK();
    }

    Status Visit(const StringType&) {
      decode_next_impl = [](KernelContext* ctx, int32_t length, uint8_t** encoded_bytes,
                            std::shared_ptr<ArrayData>* out) {
        DecodeVarLength(ctx, true, length, encoded_bytes, out);
      };
      return Status::OK();
    }

    Status Visit(const BinaryType&) {
      decode_next_impl = [](KernelContext* ctx, int32_t length, uint8_t** encoded_bytes,
                            std::shared_ptr<ArrayData>* out) {
        DecodeVarLength(ctx, false, length, encoded_bytes, out);
      };
      return Status::OK();
    }

    Status Visit(const FixedSizeBinaryType& type) {
      int32_t num_bytes = type.byte_width();
      decode_next_impl = [num_bytes](KernelContext* ctx, int32_t length,
                                     uint8_t** encoded_bytes,
                                     std::shared_ptr<ArrayData>* out) {
        DecodeBigFixed(ctx, num_bytes, length, encoded_bytes, out);
      };
      return Status::OK();
    }

    DecodeNextImpl decode_next_impl;
  };

  void Consume(KernelContext* ctx, const ExecBatch& batch) override {
    ArrayDataVector aggregands, keys;

    size_t i;
    for (i = 0; i < aggregators.size(); ++i) {
      aggregands.push_back(batch[i].array());
    }
    while (i < static_cast<size_t>(batch.num_values())) {
      keys.push_back(batch[i++].array());
    }

    offsets_batch_.clear();
    offsets_batch_.resize(batch.length + 1);
    offsets_batch_[0] = 0;
    memset(offsets_batch_.data(), 0, sizeof(offsets_batch_[0]) * offsets_batch_.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      add_length_impl[i].add_length_impl(keys[i], offsets_batch_.data());
    }
    int32_t total_length = 0;
    for (int64_t i = 0; i < batch.length; ++i) {
      auto total_length_before = total_length;
      total_length += offsets_batch_[i];
      offsets_batch_[i] = total_length_before;
    }
    offsets_batch_[batch.length] = total_length;

    key_bytes_batch_.clear();
    key_bytes_batch_.resize(total_length);
    key_buf_ptrs_.clear();
    key_buf_ptrs_.resize(batch.length);
    for (int64_t i = 0; i < batch.length; ++i) {
      key_buf_ptrs_[i] = key_bytes_batch_.data() + offsets_batch_[i];
    }
    for (size_t i = 0; i < keys.size(); ++i) {
      encode_next_impl[i].encode_next_impl(keys[i], key_buf_ptrs_.data());
    }

    group_ids_batch_.clear();
    group_ids_batch_.resize(batch.length);
    for (int64_t i = 0; i < batch.length; ++i) {
      int32_t key_length = offsets_batch_[i + 1] - offsets_batch_[i];
      std::string key(
          reinterpret_cast<const char*>(key_bytes_batch_.data() + offsets_batch_[i]),
          key_length);
      auto iter = map_.find(key);
      if (iter == map_.end()) {
        group_ids_batch_[i] = n_groups++;
        auto next_key_offset = static_cast<int32_t>(key_bytes_.size());
        key_bytes_.resize(next_key_offset + key_length);
        offsets_.push_back(next_key_offset + key_length);
        memcpy(key_bytes_.data() + next_key_offset, key.c_str(), key_length);
        map_.insert(std::make_pair(key, group_ids_batch_[i]));
      } else {
        group_ids_batch_[i] = iter->second;
      }
    }

    for (size_t i = 0; i < aggregators.size(); ++i) {
      aggregators[i]->Consume(ctx, aggregands[i], group_ids_batch_.data());
      if (ctx->HasError()) return;
    }
  }

  void MergeFrom(KernelContext* ctx, KernelState&& src) override {
    // TODO(ARROW-11840) merge two hash tables
    ctx->SetStatus(Status::NotImplemented("merging grouped aggregations"));
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    size_t n_keys = decode_next_impl.size();
    ArrayDataVector out_columns(aggregators.size() + n_keys);
    for (size_t i = 0; i < aggregators.size(); ++i) {
      Datum aggregand;
      aggregators[i]->Finalize(ctx, &aggregand);
      if (ctx->HasError()) return;
      out_columns[i] = aggregand.array();
    }

    key_buf_ptrs_.clear();
    key_buf_ptrs_.resize(n_groups);
    for (int64_t i = 0; i < n_groups; ++i) {
      key_buf_ptrs_[i] = key_bytes_.data() + offsets_[i];
    }

    int64_t length = n_groups;
    for (size_t i = 0; i < n_keys; ++i) {
      std::shared_ptr<ArrayData> key_array;
      decode_next_impl[i].decode_next_impl(ctx, static_cast<int32_t>(length),
                                           key_buf_ptrs_.data(), &key_array);
      out_columns[aggregators.size() + i] = std::move(key_array);
    }

    *out = ArrayData::Make(std::move(out_type), length, {/*null_bitmap=*/nullptr},
                           std::move(out_columns));
  }
  std::vector<int32_t> offsets_batch_;
  std::vector<uint8_t> key_bytes_batch_;
  std::vector<uint8_t*> key_buf_ptrs_;
  std::vector<uint32_t> group_ids_batch_;

  std::unordered_map<std::string, uint32_t> map_;
  std::vector<int32_t> offsets_;
  std::vector<uint8_t> key_bytes_;
  uint32_t n_groups;

  std::shared_ptr<DataType> out_type;
  GroupByOptions options;
  std::vector<std::unique_ptr<GroupedAggregator>> aggregators;

  std::vector<GetAddLengthImpl> add_length_impl;
  std::vector<GetEncodeNextImpl> encode_next_impl;
  std::vector<GetDecodeNextImpl> decode_next_impl;
};

template <typename Aggregator>
std::unique_ptr<Aggregator> MakeAggregator(KernelContext* ctx,
                                           const std::string& function_name,
                                           const std::shared_ptr<DataType>& input_type,
                                           const FunctionOptions* options) {
  if (options == nullptr) {
    if (auto function = ctx->exec_context()
                            ->func_registry()
                            ->GetFunction(function_name)
                            .ValueOr(nullptr)) {
      options = function->default_options();
    }
  }

  return Aggregator::Make(ctx, input_type, options);
}

std::unique_ptr<KernelState> GroupByInit(KernelContext* ctx, const KernelInitArgs& args) {
  auto impl = ::arrow::internal::make_unique<GroupByImpl>();
  impl->options = *checked_cast<const GroupByOptions*>(args.options);
  const auto& aggregates = impl->options.aggregates;

  impl->n_groups = 0;
  impl->offsets_.push_back(0);

  if (aggregates.size() > args.inputs.size()) {
    ctx->SetStatus(Status::Invalid("more aggegates than inputs!"));
    return nullptr;
  }

  FieldVector out_fields(args.inputs.size());

  impl->aggregators.resize(aggregates.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    const std::string& function = aggregates[i].function;
    const FunctionOptions* options = aggregates[i].options;
    const auto& input_type = args.inputs[i].type;

    if (function == "count") {
      impl->aggregators[i] =
          MakeAggregator<GroupedCountImpl>(ctx, function, input_type, options);
    } else if (function == "sum") {
      impl->aggregators[i] =
          MakeAggregator<GroupedSumImpl>(ctx, function, input_type, options);
    } else if (function == "min_max") {
      impl->aggregators[i] =
          MakeAggregator<GroupedMinMaxImpl>(ctx, function, input_type, options);
    } else {
      ctx->SetStatus(Status::NotImplemented("Grouped aggregate ", function));
    }
    if (ctx->HasError()) return nullptr;

    out_fields[i] = field("", impl->aggregators[i]->out_type());
  }

  size_t n_keys = args.inputs.size() - aggregates.size();
  for (size_t i = 0; i < n_keys; ++i) {
    const auto& key_type = args.inputs[aggregates.size() + i].type;
    switch (key_type->id()) {
      // Supported types of keys
      case Type::BOOL:
      case Type::UINT8:
      case Type::INT8:
      case Type::UINT16:
      case Type::INT16:
      case Type::UINT32:
      case Type::INT32:
      case Type::UINT64:
      case Type::INT64:
      case Type::STRING:
      case Type::BINARY:
      case Type::FIXED_SIZE_BINARY:
        break;
      default:
        ctx->SetStatus(Status::NotImplemented("Key of type", key_type->ToString()));
        return nullptr;
    }
    out_fields[aggregates.size() + i] = field("", key_type);
  }

  impl->add_length_impl.resize(n_keys);
  impl->encode_next_impl.resize(n_keys);
  impl->decode_next_impl.resize(n_keys);
  for (size_t i = 0; i < n_keys; ++i) {
    const auto& key_type = args.inputs[aggregates.size() + i].type;
    ctx->SetStatus(VisitTypeInline(*key_type.get(), &impl->add_length_impl[i]));
    ctx->SetStatus(VisitTypeInline(*key_type.get(), &impl->encode_next_impl[i]));
    ctx->SetStatus(VisitTypeInline(*key_type.get(), &impl->decode_next_impl[i]));
  }

  impl->out_type = struct_(std::move(out_fields));

  return impl;
}

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func,
                        SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    // array[InT] -> scalar[OutT]
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, simd_level);
  }
}

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func, SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    // array[T] -> scalar[struct<min: T, max: T>]
    auto out_ty = struct_({field("min", ty), field("max", ty)});
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, simd_level);
  }
}

}  // namespace aggregate

namespace internal {
namespace {

const FunctionDoc count_doc{"Count the number of null / non-null values",
                            ("By default, non-null values are counted.\n"
                             "This can be changed through CountOptions."),
                            {"array"},
                            "CountOptions"};

const FunctionDoc sum_doc{
    "Sum values of a numeric array", ("Null values are ignored."), {"array"}};

const FunctionDoc mean_doc{"Compute the mean of a numeric array",
                           ("Null values are ignored. The result is always computed\n"
                            "as a double, regardless of the input types"),
                           {"array"}};

const FunctionDoc min_max_doc{"Compute the minimum and maximum values of a numeric array",
                              ("Null values are ignored by default.\n"
                               "This can be changed through MinMaxOptions."),
                              {"array"},
                              "MinMaxOptions"};

const FunctionDoc any_doc{"Test whether any element in a boolean array evaluates to true",
                          ("Null values are ignored."),
                          {"array"}};

const FunctionDoc all_doc{"Test whether all elements in a boolean array evaluate to true",
                          ("Null values are ignored."),
                          {"array"}};

const FunctionDoc group_by_doc{
    ("Compute aggregations on input arrays, grouped by key columns"),
    ("Leading arguments are passed to the corresponding aggregation function\n"
     "named in GroupByOptions, remaining inputs are used as keys for grouping."),
    {"*args"},
    "GroupByOptions"};

}  // namespace

void RegisterScalarAggregateBasic(FunctionRegistry* registry) {
  static auto default_count_options = CountOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "count", Arity::Unary(), &count_doc, &default_count_options);

  // Takes any array input, outputs int64 scalar
  InputType any_array(ValueDescr::ARRAY);
  AddAggKernel(KernelSignature::Make({any_array}, ValueDescr::Scalar(int64())),
               aggregate::CountInit, func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary(), &sum_doc);
  aggregate::AddBasicAggKernels(aggregate::SumInit, {boolean()}, int64(), func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, SignedIntTypes(), int64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, UnsignedIntTypes(), uint64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, FloatingPointTypes(), float64(),
                                func.get());
  // Add the SIMD variants for sum
#if defined(ARROW_HAVE_RUNTIME_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX512)
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddSumAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddSumAvx512AggKernels(func.get());
  }
#endif
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary(), &mean_doc);
  aggregate::AddBasicAggKernels(aggregate::MeanInit, {boolean()}, float64(), func.get());
  aggregate::AddBasicAggKernels(aggregate::MeanInit, NumericTypes(), float64(),
                                func.get());
  // Add the SIMD variants for mean
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddMeanAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddMeanAvx512AggKernels(func.get());
  }
#endif
  DCHECK_OK(registry->AddFunction(std::move(func)));

  static auto default_minmax_options = MinMaxOptions::Defaults();
  func = std::make_shared<ScalarAggregateFunction>("min_max", Arity::Unary(),
                                                   &min_max_doc, &default_minmax_options);
  aggregate::AddMinMaxKernels(aggregate::MinMaxInit, {boolean()}, func.get());
  aggregate::AddMinMaxKernels(aggregate::MinMaxInit, NumericTypes(), func.get());
  // Add the SIMD variants for min max
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddMinMaxAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddMinMaxAvx512AggKernels(func.get());
  }
#endif

  DCHECK_OK(registry->AddFunction(std::move(func)));

  // any
  func = std::make_shared<ScalarAggregateFunction>("any", Arity::Unary(), &any_doc);
  aggregate::AddBasicAggKernels(aggregate::AnyInit, {boolean()}, boolean(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // all
  func = std::make_shared<ScalarAggregateFunction>("all", Arity::Unary(), &all_doc);
  aggregate::AddBasicAggKernels(aggregate::AllInit, {boolean()}, boolean(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // group_by
  func = std::make_shared<ScalarAggregateFunction>("group_by", Arity::VarArgs(),
                                                   &group_by_doc);
  {
    auto sig = KernelSignature::Make(
        {ValueDescr::ARRAY},
        OutputType([](KernelContext* ctx, const std::vector<ValueDescr>&) {
          return Result<ValueDescr>{
              checked_cast<aggregate::GroupByImpl*>(ctx->state())->out_type};
        }),
        /*is_varargs=*/true);
    AddAggKernel(std::move(sig), aggregate::GroupByInit, func.get(), SimdLevel::NONE,
                 true);
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
