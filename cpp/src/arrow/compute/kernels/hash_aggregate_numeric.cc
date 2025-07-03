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

#include "arrow/array/concatenate.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/aggregate_var_std_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/hash_aggregate_internal.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int128_internal.h"
#include "arrow/util/span.h"
#include "arrow/util/tdigest_internal.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {
namespace {

// ----------------------------------------------------------------------
// Sum/Mean/Product implementation

template <typename Type, typename Impl,
          typename AccumulateType = typename FindAccumulatorType<Type>::Type>
struct GroupedReducingAggregator : public GroupedAggregator {
  using AccType = AccumulateType;
  using CType = typename TypeTraits<AccType>::CType;
  using InputCType = typename TypeTraits<Type>::CType;

  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    pool_ = ctx->memory_pool();
    options_ = checked_cast<const ScalarAggregateOptions&>(*args.options);
    reduced_ = TypedBufferBuilder<CType>(pool_);
    counts_ = TypedBufferBuilder<int64_t>(pool_);
    no_nulls_ = TypedBufferBuilder<bool>(pool_);
    ARROW_ASSIGN_OR_RAISE(out_type_, GetOutType(args.inputs[0].GetSharedPtr()));
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(reduced_.Append(added_groups, Impl::NullValue(*out_type_)));
    RETURN_NOT_OK(counts_.Append(added_groups, 0));
    RETURN_NOT_OK(no_nulls_.Append(added_groups, true));
    return Status::OK();
  }

  Status Consume(const ExecSpan& batch) override {
    CType* reduced = reduced_.mutable_data();
    int64_t* counts = counts_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();

    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, InputCType value) {
          reduced[g] = Impl::Reduce(*out_type_, reduced[g], static_cast<CType>(value));
          counts[g]++;
        },
        [&](uint32_t g) { bit_util::SetBitTo(no_nulls, g, false); });
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other =
        checked_cast<GroupedReducingAggregator<Type, Impl, AccType>*>(&raw_other);

    CType* reduced = reduced_.mutable_data();
    int64_t* counts = counts_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();

    const CType* other_reduced = other->reduced_.data();
    const int64_t* other_counts = other->counts_.data();
    const uint8_t* other_no_nulls = other->no_nulls_.data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      counts[*g] += other_counts[other_g];
      reduced[*g] = Impl::Reduce(*out_type_, reduced[*g], other_reduced[other_g]);
      bit_util::SetBitTo(
          no_nulls, *g,
          bit_util::GetBit(no_nulls, *g) && bit_util::GetBit(other_no_nulls, other_g));
    }
    return Status::OK();
  }

  // Generate the values/nulls buffers
  static Result<std::shared_ptr<Buffer>> Finish(MemoryPool* pool,
                                                const ScalarAggregateOptions& options,
                                                const int64_t* counts,
                                                TypedBufferBuilder<CType>* reduced,
                                                int64_t num_groups, int64_t* null_count,
                                                std::shared_ptr<Buffer>* null_bitmap) {
    for (int64_t i = 0; i < num_groups; ++i) {
      if (counts[i] >= options.min_count) continue;

      if ((*null_bitmap) == nullptr) {
        ARROW_ASSIGN_OR_RAISE(*null_bitmap, AllocateBitmap(num_groups, pool));
        bit_util::SetBitsTo((*null_bitmap)->mutable_data(), 0, num_groups, true);
      }

      (*null_count)++;
      bit_util::SetBitTo((*null_bitmap)->mutable_data(), i, false);
    }
    return reduced->Finish();
  }

  Result<Datum> Finalize() override {
    std::shared_ptr<Buffer> null_bitmap = nullptr;
    const int64_t* counts = counts_.data();
    int64_t null_count = 0;

    ARROW_ASSIGN_OR_RAISE(auto values,
                          Impl::Finish(pool_, options_, counts, &reduced_, num_groups_,
                                       &null_count, &null_bitmap));

    if (!options_.skip_nulls) {
      null_count = kUnknownNullCount;
      if (null_bitmap) {
        arrow::internal::BitmapAnd(null_bitmap->data(), /*left_offset=*/0,
                                   no_nulls_.data(), /*right_offset=*/0, num_groups_,
                                   /*out_offset=*/0, null_bitmap->mutable_data());
      } else {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, no_nulls_.Finish());
      }
    }

    return ArrayData::Make(out_type(), num_groups_,
                           {std::move(null_bitmap), std::move(values)}, null_count);
  }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  template <typename T = Type>
  enable_if_t<!is_decimal_type<T>::value, Result<std::shared_ptr<DataType>>> GetOutType(
      const std::shared_ptr<DataType>& in_type) {
    return TypeTraits<AccType>::type_singleton();
  }

  template <typename T = Type>
  enable_if_decimal<T, Result<std::shared_ptr<DataType>>> GetOutType(
      const std::shared_ptr<DataType>& in_type) {
    if (PromoteDecimal()) {
      return WidenDecimalToMaxPrecision(in_type);
    } else {
      return in_type;
    }
  }

  // If this returns true, then the aggregator will promote a decimal to the maximum
  // precision for that type. For instance, a decimal128(3, 2) will be promoted to a
  // decimal128(38, 2)
  //
  // TODO: Ideally this should be configurable via the function options with an enum
  // PrecisionPolicy { PROMOTE_TO_MAX, DEMOTE_TO_DOUBLE, NO_PROMOTION }
  virtual bool PromoteDecimal() const { return true; }

  int64_t num_groups_ = 0;
  ScalarAggregateOptions options_;
  TypedBufferBuilder<CType> reduced_;
  TypedBufferBuilder<int64_t> counts_;
  TypedBufferBuilder<bool> no_nulls_;
  std::shared_ptr<DataType> out_type_;
  MemoryPool* pool_;
};

struct GroupedNullImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    pool_ = ctx->memory_pool();
    options_ = checked_cast<const ScalarAggregateOptions&>(*args.options);
    return Status::OK();
  }

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
    if (options_.skip_nulls && options_.min_count == 0) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> data,
                            AllocateBuffer(num_groups_ * sizeof(int64_t), pool_));
      output_empty(data);
      return ArrayData::Make(out_type(), num_groups_, {nullptr, std::move(data)});
    } else {
      return MakeArrayOfNull(out_type(), num_groups_, pool_);
    }
  }

  virtual void output_empty(const std::shared_ptr<Buffer>& data) = 0;

  int64_t num_groups_;
  ScalarAggregateOptions options_;
  MemoryPool* pool_;
};

template <template <typename> class Impl, const char* kFriendlyName, class NullImpl>
struct GroupedReducingFactory {
  template <typename T, typename AccType = typename FindAccumulatorType<T>::Type>
  Status Visit(const T&) {
    kernel = MakeKernel(std::move(argument_type), HashAggregateInit<Impl<T>>);
    return Status::OK();
  }

  Status Visit(const Decimal128Type&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<Impl<Decimal128Type>>);
    return Status::OK();
  }

  Status Visit(const Decimal256Type&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<Impl<Decimal256Type>>);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    kernel = MakeKernel(std::move(argument_type), HashAggregateInit<NullImpl>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Computing ", kFriendlyName, " of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Computing ", kFriendlyName, " of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedReducingFactory<Impl, kFriendlyName, NullImpl> factory;
    factory.argument_type = type->id();
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

// ----------------------------------------------------------------------
// Sum implementation

template <typename Type>
struct GroupedSumImpl : public GroupedReducingAggregator<Type, GroupedSumImpl<Type>> {
  using Base = GroupedReducingAggregator<Type, GroupedSumImpl<Type>>;
  using CType = typename Base::CType;
  using InputCType = typename Base::InputCType;

  // Default value for a group
  static CType NullValue(const DataType&) { return CType(0); }

  template <typename T = Type>
  static enable_if_number<T, CType> Reduce(const DataType&, const CType u,
                                           const InputCType v) {
    return static_cast<CType>(to_unsigned(u) + to_unsigned(static_cast<CType>(v)));
  }

  static CType Reduce(const DataType&, const CType u, const CType v) {
    return static_cast<CType>(to_unsigned(u) + to_unsigned(v));
  }

  using Base::Finish;
};

struct GroupedSumNullImpl final : public GroupedNullImpl {
  std::shared_ptr<DataType> out_type() const override { return int64(); }

  void output_empty(const std::shared_ptr<Buffer>& data) override {
    std::fill_n(data->mutable_data_as<int64_t>(), num_groups_, 0);
  }
};

static constexpr const char kSumName[] = "sum";
using GroupedSumFactory =
    GroupedReducingFactory<GroupedSumImpl, kSumName, GroupedSumNullImpl>;

// ----------------------------------------------------------------------
// Product implementation

template <typename Type>
struct GroupedProductImpl final
    : public GroupedReducingAggregator<Type, GroupedProductImpl<Type>> {
  using Base = GroupedReducingAggregator<Type, GroupedProductImpl<Type>>;
  using AccType = typename Base::AccType;
  using CType = typename Base::CType;
  using InputCType = typename Base::InputCType;

  static CType NullValue(const DataType& out_type) {
    return MultiplyTraits<AccType>::one(out_type);
  }

  template <typename T = Type>
  static enable_if_number<T, CType> Reduce(const DataType& out_type, const CType u,
                                           const InputCType v) {
    return MultiplyTraits<AccType>::Multiply(out_type, u, static_cast<CType>(v));
  }

  static CType Reduce(const DataType& out_type, const CType u, const CType v) {
    return MultiplyTraits<AccType>::Multiply(out_type, u, v);
  }

  bool PromoteDecimal() const override { return false; }

  using Base::Finish;
};

struct GroupedProductNullImpl final : public GroupedNullImpl {
  std::shared_ptr<DataType> out_type() const override { return int64(); }

  void output_empty(const std::shared_ptr<Buffer>& data) override {
    std::fill_n(data->mutable_data_as<int64_t>(), num_groups_, 1);
  }
};

static constexpr const char kProductName[] = "product";
using GroupedProductFactory =
    GroupedReducingFactory<GroupedProductImpl, kProductName, GroupedProductNullImpl>;

// ----------------------------------------------------------------------
// Mean implementation

template <typename T>
struct GroupedMeanAccType {
  using Type = typename std::conditional<is_number_type<T>::value, DoubleType,
                                         typename FindAccumulatorType<T>::Type>::type;
};

template <typename Type>
struct GroupedMeanImpl
    : public GroupedReducingAggregator<Type, GroupedMeanImpl<Type>,
                                       typename GroupedMeanAccType<Type>::Type> {
  using Base = GroupedReducingAggregator<Type, GroupedMeanImpl<Type>,
                                         typename GroupedMeanAccType<Type>::Type>;
  using CType = typename Base::CType;
  using InputCType = typename Base::InputCType;
  using MeanType =
      typename std::conditional<is_decimal_type<Type>::value, CType, double>::type;

  static CType NullValue(const DataType&) { return CType(0); }

  template <typename T = Type>
  static enable_if_number<T, CType> Reduce(const DataType&, const CType u,
                                           const InputCType v) {
    return static_cast<CType>(u) + static_cast<CType>(v);
  }

  static CType Reduce(const DataType&, const CType u, const CType v) {
    return static_cast<CType>(to_unsigned(u) + to_unsigned(v));
  }

  template <typename T = Type>
  static enable_if_decimal<T, Result<MeanType>> DoMean(CType reduced, int64_t count) {
    static_assert(std::is_same<MeanType, CType>::value, "");
    CType quotient, remainder;
    ARROW_ASSIGN_OR_RAISE(std::tie(quotient, remainder), reduced.Divide(count));
    // Round the decimal result based on the remainder
    remainder.Abs();
    if (remainder * 2 >= count) {
      if (reduced >= 0) {
        quotient += 1;
      } else {
        quotient -= 1;
      }
    }
    return quotient;
  }

  template <typename T = Type>
  static enable_if_t<!is_decimal_type<T>::value, Result<MeanType>> DoMean(CType reduced,
                                                                          int64_t count) {
    return static_cast<MeanType>(reduced) / count;
  }

  static Result<std::shared_ptr<Buffer>> Finish(MemoryPool* pool,
                                                const ScalarAggregateOptions& options,
                                                const int64_t* counts,
                                                TypedBufferBuilder<CType>* reduced_,
                                                int64_t num_groups, int64_t* null_count,
                                                std::shared_ptr<Buffer>* null_bitmap) {
    const CType* reduced = reduced_->data();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values,
                          AllocateBuffer(num_groups * sizeof(MeanType), pool));
    auto* means = values->mutable_data_as<MeanType>();
    for (int64_t i = 0; i < num_groups; ++i) {
      if (counts[i] >= options.min_count) {
        ARROW_ASSIGN_OR_RAISE(means[i], DoMean(reduced[i], counts[i]));
        continue;
      }
      means[i] = MeanType(0);

      if ((*null_bitmap) == nullptr) {
        ARROW_ASSIGN_OR_RAISE(*null_bitmap, AllocateBitmap(num_groups, pool));
        bit_util::SetBitsTo((*null_bitmap)->mutable_data(), 0, num_groups, true);
      }

      (*null_count)++;
      bit_util::SetBitTo((*null_bitmap)->mutable_data(), i, false);
    }
    return values;
  }

  bool PromoteDecimal() const override { return false; }

  std::shared_ptr<DataType> out_type() const override {
    if (is_decimal_type<Type>::value) return this->out_type_;
    return float64();
  }
};

struct GroupedMeanNullImpl final : public GroupedNullImpl {
  std::shared_ptr<DataType> out_type() const override { return float64(); }

  void output_empty(const std::shared_ptr<Buffer>& data) override {
    std::fill_n(data->mutable_data_as<double>(), num_groups_, 0);
  }
};

static constexpr const char kMeanName[] = "mean";
using GroupedMeanFactory =
    GroupedReducingFactory<GroupedMeanImpl, kMeanName, GroupedMeanNullImpl>;

// Variance/Stdev implementation

using arrow::internal::int128_t;

template <typename Type>
struct GroupedStatisticImpl : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;
  using SumType = typename internal::GetSumType<Type>::SumType;

  // This method is defined solely to make GroupedStatisticImpl instantiable
  // in ConsumeImpl below. It will be redefined in subclasses.
  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    return Status::NotImplemented("");
  }

  // Init helper for hash_variance and hash_stddev
  Status InitInternal(ExecContext* ctx, const KernelInitArgs& args,
                      StatisticType stat_type, const VarianceOptions& options) {
    return InitInternal(ctx, args, stat_type, options.ddof, options.skip_nulls,
                        /*biased=*/false, options.min_count);
  }

  // Init helper for hash_skew and hash_kurtosis
  Status InitInternal(ExecContext* ctx, const KernelInitArgs& args,
                      StatisticType stat_type, const SkewOptions& options) {
    return InitInternal(ctx, args, stat_type, /*ddof=*/0, options.skip_nulls,
                        options.biased, options.min_count);
  }

  Status InitInternal(ExecContext* ctx, const KernelInitArgs& args,
                      StatisticType stat_type, int ddof, bool skip_nulls, bool biased,
                      uint32_t min_count) {
    if constexpr (is_decimal_type<Type>::value) {
      int32_t decimal_scale =
          checked_cast<const DecimalType&>(*args.inputs[0].type).scale();
      return InitInternal(ctx, stat_type, decimal_scale, ddof, skip_nulls, biased,
                          min_count);
    } else {
      return InitInternal(ctx, stat_type, /*decimal_scale=*/0, ddof, skip_nulls, biased,
                          min_count);
    }
  }

  Status InitInternal(ExecContext* ctx, StatisticType stat_type, int32_t decimal_scale,
                      int ddof, bool skip_nulls, bool biased, uint32_t min_count) {
    stat_type_ = stat_type;
    moments_level_ = moments_level_for_statistic(stat_type_);
    decimal_scale_ = decimal_scale;
    skip_nulls_ = skip_nulls;
    biased_ = biased;
    min_count_ = min_count;
    ddof_ = ddof;
    ctx_ = ctx;
    pool_ = ctx->memory_pool();
    counts_ = TypedBufferBuilder<int64_t>(pool_);
    means_ = TypedBufferBuilder<double>(pool_);
    m2s_ = TypedBufferBuilder<double>(pool_);
    m3s_ = TypedBufferBuilder<double>(pool_);
    m4s_ = TypedBufferBuilder<double>(pool_);
    no_nulls_ = TypedBufferBuilder<bool>(pool_);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    auto added_groups = new_num_groups - num_groups_;
    num_groups_ = new_num_groups;
    RETURN_NOT_OK(counts_.Append(added_groups, 0));
    RETURN_NOT_OK(means_.Append(added_groups, 0));
    RETURN_NOT_OK(m2s_.Append(added_groups, 0));
    if (moments_level_ >= 3) {
      RETURN_NOT_OK(m3s_.Append(added_groups, 0));
      if (moments_level_ >= 4) {
        RETURN_NOT_OK(m4s_.Append(added_groups, 0));
      }
    }
    RETURN_NOT_OK(no_nulls_.Append(added_groups, true));
    return Status::OK();
  }

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal32& value) const { return value.ToDouble(decimal_scale_); }
  double ToDouble(const Decimal64& value) const { return value.ToDouble(decimal_scale_); }
  double ToDouble(const Decimal128& value) const {
    return value.ToDouble(decimal_scale_);
  }
  double ToDouble(const Decimal256& value) const {
    return value.ToDouble(decimal_scale_);
  }

  Status Consume(const ExecSpan& batch) override {
    constexpr bool kCanUseIntArithmetic = std::is_integral_v<CType> && sizeof(CType) <= 4;

    if constexpr (kCanUseIntArithmetic) {
      if (moments_level_ == 2) {
        return ConsumeIntegral(batch);
      }
    }
    return ConsumeGeneric(batch);
  }

  // float/double/int64/decimal: calculate `m2` (sum((X-mean)^2)) with
  // two pass algorithm (see aggregate_var_std.cc)
  Status ConsumeGeneric(const ExecSpan& batch) {
    GroupedStatisticImpl<Type> state;
    RETURN_NOT_OK(state.InitInternal(ctx_, stat_type_, decimal_scale_, ddof_, skip_nulls_,
                                     biased_, min_count_));
    RETURN_NOT_OK(state.Resize(num_groups_));
    int64_t* counts = state.counts_.mutable_data();
    double* means = state.means_.mutable_data();
    uint8_t* no_nulls = state.no_nulls_.mutable_data();

    // XXX this uses naive summation; we should switch to pairwise summation
    // (as the scalar aggregate kernel does) or Kahan summation.
    std::vector<SumType> sums(num_groups_);
    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, typename TypeTraits<Type>::CType value) {
          sums[g] += static_cast<SumType>(value);
          counts[g]++;
        },
        [&](uint32_t g) { bit_util::ClearBit(no_nulls, g); });

    for (int64_t i = 0; i < num_groups_; i++) {
      means[i] = ToDouble(sums[i]) / counts[i];
    }

    double* m2s = state.m2s_mutable_data();
    double* m3s = state.m3s_mutable_data();
    double* m4s = state.m4s_mutable_data();
    // Having distinct VisitGroupedValuesNonNull calls based on moments_level_
    // would increase code generation for relatively little benefit.
    VisitGroupedValuesNonNull<Type>(
        batch, [&](uint32_t g, typename TypeTraits<Type>::CType value) {
          const double d = ToDouble(value) - means[g];
          const double d2 = d * d;
          switch (moments_level_) {
            case 4:
              m4s[g] += d2 * d2;
              [[fallthrough]];
            case 3:
              m3s[g] += d2 * d;
              [[fallthrough]];
            default:
              m2s[g] += d2;
              break;
          }
        });

    return MergeSameGroups(std::move(state));
  }

  // int32/16/8: textbook one pass algorithm to compute `m2` with integer arithmetic
  // (see aggregate_var_std.cc)
  Status ConsumeIntegral(const ExecSpan& batch) {
    // max number of elements that sum will not overflow int64 (2Gi int32 elements)
    // for uint32:    0 <= sum < 2^63 (int64 >= 0)
    // for int32: -2^62 <= sum < 2^62
    constexpr int64_t max_length = 1ULL << (63 - sizeof(CType) * 8);

    const auto* g = batch[1].array.GetValues<uint32_t>(1);
    if (batch[0].is_scalar() && !batch[0].scalar->is_valid) {
      uint8_t* no_nulls = no_nulls_.mutable_data();
      for (int64_t i = 0; i < batch.length; i++) {
        bit_util::ClearBit(no_nulls, g[i]);
      }
      return Status::OK();
    }

    std::vector<IntegerVarStd> var_std(num_groups_);

    for (int64_t start_index = 0; start_index < batch.length; start_index += max_length) {
      // process in chunks that overflow will never happen

      // reset state
      var_std.clear();
      var_std.resize(num_groups_);
      GroupedStatisticImpl<Type> state;
      RETURN_NOT_OK(state.InitInternal(ctx_, stat_type_, decimal_scale_, ddof_,
                                       skip_nulls_, biased_, min_count_));
      RETURN_NOT_OK(state.Resize(num_groups_));
      int64_t* other_counts = state.counts_.mutable_data();
      double* other_means = state.means_.mutable_data();
      double* other_m2s = state.m2s_mutable_data();
      uint8_t* other_no_nulls = state.no_nulls_.mutable_data();

      if (batch[0].is_array()) {
        const ArraySpan& array = batch[0].array;
        const CType* values = array.GetValues<CType>(1);
        auto visit_values = [&](int64_t pos, int64_t len) {
          for (int64_t i = 0; i < len; ++i) {
            const int64_t index = start_index + pos + i;
            const auto value = values[index];
            var_std[g[index]].ConsumeOne(value);
          }
        };

        if (array.MayHaveNulls()) {
          arrow::internal::BitRunReader reader(
              array.buffers[0].data, array.offset + start_index,
              std::min(max_length, batch.length - start_index));
          int64_t position = 0;
          while (true) {
            auto run = reader.NextRun();
            if (run.length == 0) break;
            if (run.set) {
              visit_values(position, run.length);
            } else {
              for (int64_t i = 0; i < run.length; ++i) {
                bit_util::ClearBit(other_no_nulls, g[start_index + position + i]);
              }
            }
            position += run.length;
          }
        } else {
          visit_values(0, array.length);
        }
      } else {
        const auto value = UnboxScalar<Type>::Unbox(*batch[0].scalar);
        for (int64_t i = 0; i < std::min(max_length, batch.length - start_index); ++i) {
          const int64_t index = start_index + i;
          var_std[g[index]].ConsumeOne(value);
        }
      }

      for (int64_t i = 0; i < num_groups_; i++) {
        if (var_std[i].count == 0) continue;

        other_counts[i] = var_std[i].count;
        other_means[i] = var_std[i].mean();
        other_m2s[i] = var_std[i].m2();
      }
      RETURN_NOT_OK(MergeSameGroups(std::move(state)));
    }
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    DCHECK_EQ(group_id_mapping.length,
              checked_cast<GroupedStatisticImpl*>(&raw_other)->num_groups_);
    const uint32_t* g = group_id_mapping.GetValues<uint32_t>(1);
    return MergeInternal(std::move(raw_other),
                         [g](int64_t other_g) { return g[other_g]; });
  }

  Status MergeSameGroups(GroupedAggregator&& raw_other) {
    return MergeInternal(std::move(raw_other), [](int64_t other_g) { return other_g; });
  }

  template <typename GroupIdMapper>
  Status MergeInternal(GroupedAggregator&& raw_other, GroupIdMapper&& group_id_mapper) {
    // Combine moments from two chunks
    auto other = checked_cast<GroupedStatisticImpl*>(&raw_other);
    DCHECK_EQ(moments_level_, other->moments_level_);

    int64_t* counts = counts_.mutable_data();
    double* means = means_.mutable_data();
    double* m2s = m2s_mutable_data();
    // Moments above the current level will just be ignored.
    double* m3s = m3s_mutable_data();
    double* m4s = m4s_mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();

    const int64_t* other_counts = other->counts_.data();
    const double* other_means = other->means_.data();
    const double* other_m2s = other->m2s_data();
    const double* other_m3s = other->m3s_data();
    const double* other_m4s = other->m4s_data();
    const uint8_t* other_no_nulls = other->no_nulls_.data();

    const int64_t num_other_groups = other->num_groups_;

    for (int64_t other_g = 0; other_g < num_other_groups; ++other_g) {
      const auto g = group_id_mapper(other_g);
      if (!bit_util::GetBit(other_no_nulls, other_g)) {
        bit_util::ClearBit(no_nulls, g);
      }
      if (other_counts[other_g] == 0) continue;
      auto moments = Moments::Merge(
          moments_level_, Moments(counts[g], means[g], m2s[g], m3s[g], m4s[g]),
          Moments(other_counts[other_g], other_means[other_g], other_m2s[other_g],
                  other_m3s[other_g], other_m4s[other_g]));
      counts[g] = moments.count;
      means[g] = moments.mean;
      // Fill moments in reverse order, in case m3s or m4s is the same as m2s.
      m4s[g] = moments.m4;
      m3s[g] = moments.m3;
      m2s[g] = moments.m2;
    }
    return Status::OK();
  }

  Result<Datum> Finalize() override {
    std::shared_ptr<Buffer> null_bitmap;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values,
                          AllocateBuffer(num_groups_ * sizeof(double), pool_));
    int64_t null_count = 0;

    auto* results = values->mutable_data_as<double>();
    const int64_t* counts = counts_.data();
    const double* means = means_.data();
    const double* m2s = m2s_data();
    const double* m3s = m3s_data();
    const double* m4s = m4s_data();
    for (int64_t i = 0; i < num_groups_; ++i) {
      if (counts[i] > ddof_ && counts[i] >= min_count_ &&
          (stat_type_ != StatisticType::Skew || biased_ || counts[i] > 2) &&
          (stat_type_ != StatisticType::Kurtosis || biased_ || counts[i] > 3)) {
        const auto moments = Moments(counts[i], means[i], m2s[i], m3s[i], m4s[i]);
        switch (stat_type_) {
          case StatisticType::Var:
            results[i] = moments.Variance(ddof_);
            break;
          case StatisticType::Std:
            results[i] = moments.Stddev(ddof_);
            break;
          case StatisticType::Skew:
            results[i] = moments.Skew(biased_);
            break;
          case StatisticType::Kurtosis:
            results[i] = moments.Kurtosis(biased_);
            break;
          default:
            return Status::NotImplemented("Statistic type ",
                                          static_cast<int>(stat_type_));
        }
        continue;
      }

      results[i] = 0;
      if (null_bitmap == nullptr) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(num_groups_, pool_));
        bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, num_groups_, true);
      }

      null_count += 1;
      bit_util::SetBitTo(null_bitmap->mutable_data(), i, false);
    }
    if (!skip_nulls_) {
      if (null_bitmap) {
        arrow::internal::BitmapAnd(null_bitmap->data(), 0, no_nulls_.data(), 0,
                                   num_groups_, 0, null_bitmap->mutable_data());
      } else {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, no_nulls_.Finish());
      }
      null_count = kUnknownNullCount;
    }

    return ArrayData::Make(float64(), num_groups_,
                           {std::move(null_bitmap), std::move(values)}, null_count);
  }

  std::shared_ptr<DataType> out_type() const override { return float64(); }

  const double* m2s_data() const { return m2s_.data(); }
  // If moments_level_ < 3, the values read from m3s_data() will be ignored,
  // but we still need to point to a valid buffer of the appropriate size.
  // The trick is to reuse m2s_, which simplifies the code.
  const double* m3s_data() const {
    return (moments_level_ >= 3) ? m3s_.data() : m2s_.data();
  }
  const double* m4s_data() const {
    return (moments_level_ >= 4) ? m4s_.data() : m2s_.data();
  }

  double* m2s_mutable_data() { return m2s_.mutable_data(); }
  double* m3s_mutable_data() {
    return (moments_level_ >= 3) ? m3s_.mutable_data() : m2s_.mutable_data();
  }
  double* m4s_mutable_data() {
    return (moments_level_ >= 4) ? m4s_.mutable_data() : m2s_.mutable_data();
  }

  StatisticType stat_type_;
  int moments_level_;
  int32_t decimal_scale_;
  bool skip_nulls_;
  bool biased_;
  uint32_t min_count_;
  int ddof_;
  int64_t num_groups_ = 0;
  // m2 = count * s2 = sum((X-mean)^2)
  TypedBufferBuilder<int64_t> counts_;
  TypedBufferBuilder<double> means_, m2s_, m3s_, m4s_;
  TypedBufferBuilder<bool> no_nulls_;
  ExecContext* ctx_;
  MemoryPool* pool_;
};

template <typename Type, typename OptionsType, StatisticType kStatType>
struct ConcreteGroupedStatisticImpl : public GroupedStatisticImpl<Type> {
  using GroupedStatisticImpl<Type>::InitInternal;

  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    const auto& options = checked_cast<const OptionsType&>(*args.options);
    return InitInternal(ctx, args, kStatType, options);
  }
};

template <typename Type>
using GroupedVarianceImpl =
    ConcreteGroupedStatisticImpl<Type, VarianceOptions, StatisticType::Var>;
template <typename Type>
using GroupedStddevImpl =
    ConcreteGroupedStatisticImpl<Type, VarianceOptions, StatisticType::Std>;
template <typename Type>
using GroupedSkewImpl =
    ConcreteGroupedStatisticImpl<Type, SkewOptions, StatisticType::Skew>;
template <typename Type>
using GroupedKurtosisImpl =
    ConcreteGroupedStatisticImpl<Type, SkewOptions, StatisticType::Kurtosis>;

template <template <typename Type> typename GroupedImpl>
Result<HashAggregateKernel> MakeGroupedStatisticKernel(
    const std::shared_ptr<DataType>& type) {
  auto make_kernel = [&](auto&& type) -> Result<HashAggregateKernel> {
    using T = std::decay_t<decltype(type)>;
    // Supporting all number types except float16
    if constexpr (is_integer_type<T>::value ||
                  (is_floating_type<T>::value && !is_half_float_type<T>::value) ||
                  is_decimal_type<T>::value) {
      return MakeKernel(InputType(T::type_id), HashAggregateInit<GroupedImpl<T>>);
    }
    return Status::NotImplemented("Computing higher-order statistic of data of type ",
                                  type);
  };

  return VisitType(*type, make_kernel);
}

Status AddHashAggregateStatisticKernels(HashAggregateFunction* func,
                                        HashAggregateKernelFactory make_kernel) {
  RETURN_NOT_OK(AddHashAggKernels(SignedIntTypes(), make_kernel, func));
  RETURN_NOT_OK(AddHashAggKernels(UnsignedIntTypes(), make_kernel, func));
  RETURN_NOT_OK(AddHashAggKernels(FloatingPointTypes(), make_kernel, func));
  RETURN_NOT_OK(AddHashAggKernels(
      {decimal32(1, 1), decimal64(1, 1), decimal128(1, 1), decimal256(1, 1)}, make_kernel,
      func));
  return Status::OK();
}

// ----------------------------------------------------------------------
// TDigest implementation

using arrow::internal::TDigest;

template <typename Type>
struct GroupedTDigestImpl : public GroupedAggregator {
  using CType = typename TypeTraits<Type>::CType;

  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    options_ = *checked_cast<const TDigestOptions*>(args.options);
    if (is_decimal_type<Type>::value) {
      decimal_scale_ = checked_cast<const DecimalType&>(*args.inputs[0].type).scale();
    } else {
      decimal_scale_ = 0;
    }
    ctx_ = ctx;
    pool_ = ctx->memory_pool();
    counts_ = TypedBufferBuilder<int64_t>(pool_);
    no_nulls_ = TypedBufferBuilder<bool>(pool_);
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    const int64_t added_groups = new_num_groups - tdigests_.size();
    tdigests_.reserve(new_num_groups);
    for (int64_t i = 0; i < added_groups; i++) {
      tdigests_.emplace_back(options_.delta, options_.buffer_size);
    }
    RETURN_NOT_OK(counts_.Append(new_num_groups, 0));
    RETURN_NOT_OK(no_nulls_.Append(new_num_groups, true));
    return Status::OK();
  }

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal32& value) const { return value.ToDouble(decimal_scale_); }
  double ToDouble(const Decimal64& value) const { return value.ToDouble(decimal_scale_); }
  double ToDouble(const Decimal128& value) const {
    return value.ToDouble(decimal_scale_);
  }
  double ToDouble(const Decimal256& value) const {
    return value.ToDouble(decimal_scale_);
  }

  Status Consume(const ExecSpan& batch) override {
    int64_t* counts = counts_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();
    VisitGroupedValues<Type>(
        batch,
        [&](uint32_t g, CType value) {
          tdigests_[g].NanAdd(ToDouble(value));
          counts[g]++;
        },
        [&](uint32_t g) { bit_util::SetBitTo(no_nulls, g, false); });
    return Status::OK();
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedTDigestImpl*>(&raw_other);

    int64_t* counts = counts_.mutable_data();
    uint8_t* no_nulls = no_nulls_.mutable_data();

    const int64_t* other_counts = other->counts_.data();
    const uint8_t* other_no_nulls = no_nulls_.mutable_data();

    auto g = group_id_mapping.GetValues<uint32_t>(1);
    for (int64_t other_g = 0; other_g < group_id_mapping.length; ++other_g, ++g) {
      tdigests_[*g].Merge(other->tdigests_[other_g]);
      counts[*g] += other_counts[other_g];
      bit_util::SetBitTo(
          no_nulls, *g,
          bit_util::GetBit(no_nulls, *g) && bit_util::GetBit(other_no_nulls, other_g));
    }

    return Status::OK();
  }

  Result<Datum> Finalize() override {
    const int64_t slot_length = options_.q.size();
    const int64_t num_values = tdigests_.size() * slot_length;
    const int64_t* counts = counts_.data();
    std::shared_ptr<Buffer> null_bitmap;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> values,
                          AllocateBuffer(num_values * sizeof(double), pool_));
    int64_t null_count = 0;

    auto* results = values->mutable_data_as<double>();
    for (int64_t i = 0; static_cast<size_t>(i) < tdigests_.size(); ++i) {
      if (!tdigests_[i].is_empty() && counts[i] >= options_.min_count &&
          (options_.skip_nulls || bit_util::GetBit(no_nulls_.data(), i))) {
        for (int64_t j = 0; j < slot_length; j++) {
          results[i * slot_length + j] = tdigests_[i].Quantile(options_.q[j]);
        }
        continue;
      }

      if (!null_bitmap) {
        ARROW_ASSIGN_OR_RAISE(null_bitmap, AllocateBitmap(num_values, pool_));
        bit_util::SetBitsTo(null_bitmap->mutable_data(), 0, num_values, true);
      }
      null_count += slot_length;
      bit_util::SetBitsTo(null_bitmap->mutable_data(), i * slot_length, slot_length,
                          false);
      std::fill(&results[i * slot_length], &results[(i + 1) * slot_length], 0.0);
    }

    auto child = ArrayData::Make(float64(), num_values,
                                 {std::move(null_bitmap), std::move(values)}, null_count);
    return ArrayData::Make(out_type(), tdigests_.size(), {nullptr}, {std::move(child)},
                           /*null_count=*/0);
  }

  std::shared_ptr<DataType> out_type() const override {
    return fixed_size_list(float64(), static_cast<int32_t>(options_.q.size()));
  }

  TDigestOptions options_;
  int32_t decimal_scale_;
  std::vector<TDigest> tdigests_;
  TypedBufferBuilder<int64_t> counts_;
  TypedBufferBuilder<bool> no_nulls_;
  ExecContext* ctx_;
  MemoryPool* pool_;
};

struct GroupedTDigestFactory {
  template <typename T>
  enable_if_number<T, Status> Visit(const T&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<GroupedTDigestImpl<T>>);
    return Status::OK();
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    kernel =
        MakeKernel(std::move(argument_type), HashAggregateInit<GroupedTDigestImpl<T>>);
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    return Status::NotImplemented("Computing t-digest of data of type ", type);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Computing t-digest of data of type ", type);
  }

  static Result<HashAggregateKernel> Make(const std::shared_ptr<DataType>& type) {
    GroupedTDigestFactory factory;
    factory.argument_type = type->id();
    RETURN_NOT_OK(VisitTypeInline(*type, &factory));
    return std::move(factory.kernel);
  }

  HashAggregateKernel kernel;
  InputType argument_type;
};

HashAggregateKernel MakeApproximateMedianKernel(HashAggregateFunction* tdigest_func) {
  HashAggregateKernel kernel;
  kernel.init = [tdigest_func](
                    KernelContext* ctx,
                    const KernelInitArgs& args) -> Result<std::unique_ptr<KernelState>> {
    ARROW_ASSIGN_OR_RAISE(auto kernel, tdigest_func->DispatchExact(args.inputs));
    const auto& scalar_options =
        checked_cast<const ScalarAggregateOptions&>(*args.options);
    TDigestOptions options;
    // Default q = 0.5
    options.min_count = scalar_options.min_count;
    options.skip_nulls = scalar_options.skip_nulls;
    KernelInitArgs new_args{kernel, args.inputs, &options};
    return kernel->init(ctx, new_args);
  };
  kernel.signature = KernelSignature::Make({InputType::Any(), Type::UINT32}, float64());
  kernel.resize = HashAggregateResize;
  kernel.consume = HashAggregateConsume;
  kernel.merge = HashAggregateMerge;
  kernel.finalize = [](KernelContext* ctx, Datum* out) {
    ARROW_ASSIGN_OR_RAISE(Datum temp,
                          checked_cast<GroupedAggregator*>(ctx->state())->Finalize());
    *out = temp.array_as<FixedSizeListArray>()->values();
    return Status::OK();
  };
  return kernel;
}

// ----------------------------------------------------------------------
// Docstrings

const FunctionDoc hash_sum_doc{"Sum values in each group",
                               ("Null values are ignored."),
                               {"array", "group_id_array"},
                               "ScalarAggregateOptions"};

const FunctionDoc hash_product_doc{
    "Compute the product of values in each group",
    ("Null values are ignored.\n"
     "On integer overflow, the result will wrap around as if the calculation\n"
     "was done with unsigned integers."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_mean_doc{
    "Compute the mean of values in each group",
    ("Null values are ignored.\n"
     "For integers and floats, NaN is emitted if min_count = 0 and\n"
     "there are no values in a group. For decimals, null is emitted instead."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

const FunctionDoc hash_stddev_doc{
    "Compute the standard deviation of values in each group",
    ("The number of degrees of freedom can be controlled using VarianceOptions.\n"
     "By default (`ddof` = 0), the population standard deviation is calculated.\n"
     "Nulls are ignored.  If there are not enough non-null values in a group\n"
     "to satisfy `ddof`, null is emitted."),
    {"array", "group_id_array"}};

const FunctionDoc hash_variance_doc{
    "Compute the variance of values in each group",
    ("The number of degrees of freedom can be controlled using VarianceOptions.\n"
     "By default (`ddof` = 0), the population variance is calculated.\n"
     "Nulls are ignored.  If there are not enough non-null values in a group\n"
     "to satisfy `ddof`, null is emitted."),
    {"array", "group_id_array"}};

const FunctionDoc hash_skew_doc{
    "Compute the skewness of values in each group",
    ("Nulls are ignored by default.  If there are not enough non-null values\n"
     "in a group to satisfy `min_count`, null is emitted.\n"
     "The behavior of nulls and the `min_count` parameter can be changed\n"
     "in SkewOptions."),
    {"array", "group_id_array"}};

const FunctionDoc hash_kurtosis_doc{
    "Compute the kurtosis of values in each group",
    ("Nulls are ignored by default.  If there are not enough non-null values\n"
     "in a group to satisfy `min_count`, null is emitted.\n"
     "The behavior of nulls and the `min_count` parameter can be changed\n"
     "in SkewOptions."),
    {"array", "group_id_array"}};

const FunctionDoc hash_tdigest_doc{
    "Compute approximate quantiles of values in each group",
    ("The T-Digest algorithm is used for a fast approximation.\n"
     "By default, the 0.5 quantile (i.e. median) is emitted.\n"
     "Nulls and NaNs are ignored.\n"
     "Nulls are returned if there are no valid data points."),
    {"array", "group_id_array"},
    "TDigestOptions"};

const FunctionDoc hash_approximate_median_doc{
    "Compute approximate medians of values in each group",
    ("The T-Digest algorithm is used for a fast approximation.\n"
     "Nulls and NaNs are ignored.\n"
     "Nulls are returned if there are no valid data points."),
    {"array", "group_id_array"},
    "ScalarAggregateOptions"};

}  // namespace

void RegisterHashAggregateNumeric(FunctionRegistry* registry) {
  static const auto default_scalar_aggregate_options = ScalarAggregateOptions::Defaults();
  static const auto default_tdigest_options = TDigestOptions::Defaults();
  static const auto default_variance_options = VarianceOptions::Defaults();
  static const auto default_skew_options = SkewOptions::Defaults();

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_sum", Arity::Binary(), hash_sum_doc, &default_scalar_aggregate_options);
    DCHECK_OK(AddHashAggKernels({boolean()}, GroupedSumFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(SignedIntTypes(), GroupedSumFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(UnsignedIntTypes(), GroupedSumFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(FloatingPointTypes(), GroupedSumFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedSumFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null()}, GroupedSumFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_product", Arity::Binary(), hash_product_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(AddHashAggKernels({boolean()}, GroupedProductFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(SignedIntTypes(), GroupedProductFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(UnsignedIntTypes(), GroupedProductFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(FloatingPointTypes(), GroupedProductFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedProductFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null()}, GroupedProductFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_mean", Arity::Binary(), hash_mean_doc, &default_scalar_aggregate_options);
    DCHECK_OK(AddHashAggKernels({boolean()}, GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels(SignedIntTypes(), GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(UnsignedIntTypes(), GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(FloatingPointTypes(), GroupedMeanFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(AddHashAggKernels({null()}, GroupedMeanFactory::Make, func.get()));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_stddev", Arity::Binary(), hash_stddev_doc, &default_variance_options);
    DCHECK_OK(AddHashAggregateStatisticKernels(
        func.get(), MakeGroupedStatisticKernel<GroupedStddevImpl>));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_variance", Arity::Binary(), hash_variance_doc, &default_variance_options);
    DCHECK_OK(AddHashAggregateStatisticKernels(
        func.get(), MakeGroupedStatisticKernel<GroupedVarianceImpl>));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_skew", Arity::Binary(), hash_skew_doc, &default_skew_options);
    DCHECK_OK(AddHashAggregateStatisticKernels(
        func.get(), MakeGroupedStatisticKernel<GroupedSkewImpl>));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_kurtosis", Arity::Binary(), hash_kurtosis_doc, &default_skew_options);
    DCHECK_OK(AddHashAggregateStatisticKernels(
        func.get(), MakeGroupedStatisticKernel<GroupedKurtosisImpl>));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }

  HashAggregateFunction* tdigest_func = nullptr;
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_tdigest", Arity::Binary(), hash_tdigest_doc, &default_tdigest_options);
    DCHECK_OK(
        AddHashAggKernels(SignedIntTypes(), GroupedTDigestFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(UnsignedIntTypes(), GroupedTDigestFactory::Make, func.get()));
    DCHECK_OK(
        AddHashAggKernels(FloatingPointTypes(), GroupedTDigestFactory::Make, func.get()));
    // Type parameters are ignored
    DCHECK_OK(AddHashAggKernels({decimal128(1, 1), decimal256(1, 1)},
                                GroupedTDigestFactory::Make, func.get()));
    tdigest_func = func.get();
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_approximate_median", Arity::Binary(), hash_approximate_median_doc,
        &default_scalar_aggregate_options);
    DCHECK_OK(func->AddKernel(MakeApproximateMedianKernel(tdigest_func)));
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

}  // namespace arrow::compute::internal
