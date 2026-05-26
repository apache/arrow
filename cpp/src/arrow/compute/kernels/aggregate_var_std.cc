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

#include <array>
#include <cmath>
#include <type_traits>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/aggregate_var_std_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int128_internal.h"

namespace arrow::compute::internal {

using ::arrow::internal::checked_cast;

namespace {

using arrow::internal::int128_t;
using arrow::internal::VisitSetBitRunsVoid;

template <typename ArrowType>
struct MomentsState {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using CType = typename TypeTraits<ArrowType>::CType;
  using SumType = typename internal::GetSumType<ArrowType>::SumType;
  using ThisType = MomentsState<ArrowType>;

  MomentsState(int level, int32_t decimal_scale, bool skip_nulls)
      : level(level), decimal_scale(decimal_scale), skip_nulls(skip_nulls) {}

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal32& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal64& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal128& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal256& value) const { return value.ToDouble(decimal_scale); }

  int64_t count() const { return moments.count; }

  void Consume(const ArraySpan& array) {
    constexpr bool kCanUseIntArithmetic = std::is_integral_v<CType> && sizeof(CType) <= 4;

    this->all_valid = array.GetNullCount() == 0;
    int64_t valid_count = array.length - array.GetNullCount();
    if (valid_count == 0 || (!this->all_valid && !this->skip_nulls)) {
      return;
    }

    if constexpr (kCanUseIntArithmetic) {
      if (level == 2) {
        // int32/16/8: textbook one pass algorithm for M2 with integer arithmetic

        // max number of elements that sum will not overflow int64 (2Gi int32 elements)
        // for uint32:    0 <= sum < 2^63 (int64 >= 0)
        // for int32: -2^62 <= sum < 2^62
        constexpr int64_t kMaxChunkLength = 1ULL << (63 - sizeof(CType) * 8);
        int64_t start_index = 0;

        ArraySpan slice = array;
        while (valid_count > 0) {
          // process in chunks that overflow will never happen
          slice.SetSlice(start_index + array.offset,
                         std::min(kMaxChunkLength, array.length - start_index));
          const int64_t count = slice.length - slice.GetNullCount();
          start_index += slice.length;
          valid_count -= count;

          if (count > 0) {
            IntegerVarStd var_std;
            const CType* values = slice.GetValues<CType>(1);
            VisitSetBitRunsVoid(slice.buffers[0].data, slice.offset, slice.length,
                                [&](int64_t pos, int64_t len) {
                                  for (int64_t i = 0; i < len; ++i) {
                                    const auto value = values[pos + i];
                                    var_std.ConsumeOne(value);
                                  }
                                });

            // merge variance
            auto slice_moments = Moments(var_std.count, var_std.mean(), var_std.m2());
            this->moments.MergeFrom(level, slice_moments);
          }
        }
        return;
      }
    }

    // float/double/int64/decimal: calculate each moment in a separate pass.
    // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Two-pass_algorithm
    SumType sum = internal::SumArray<CType, SumType, SimdLevel::NONE>(array);

    const double mean = ToDouble(sum) / valid_count;
    const double m2 = internal::SumArray<CType, double, SimdLevel::NONE>(
        array, [this, mean](CType value) {
          const double v = ToDouble(value);
          return (v - mean) * (v - mean);
        });
    double m3 = 0, m4 = 0;
    if (level >= 3) {
      m3 = internal::SumArray<CType, double, SimdLevel::NONE>(
          array, [this, mean](CType value) {
            const double v = ToDouble(value);
            return (v - mean) * (v - mean) * (v - mean);
          });
      if (level >= 4) {
        m4 = internal::SumArray<CType, double, SimdLevel::NONE>(
            array, [this, mean](CType value) {
              const double v = ToDouble(value);
              return (v - mean) * (v - mean) * (v - mean) * (v - mean);
            });
      }
    }
    this->moments.MergeFrom(level, Moments(valid_count, mean, m2, m3, m4));
  }

  void Consume(const Scalar& scalar, const int64_t count) {
    if (scalar.is_valid) {
      double value = ToDouble(UnboxScalar<ArrowType>::Unbox(scalar));
      this->moments = Moments::FromScalar(level, value, count);
    } else {
      this->moments = Moments();
      this->all_valid = false;
    }
  }

  // Combine `m2` from two chunks (m2 = n*s2)
  // https://www.emathzone.com/tutorials/basic-statistics/combined-variance.html
  void MergeFrom(const ThisType& state) {
    this->all_valid = this->all_valid && state.all_valid;
    this->moments.MergeFrom(level, state.moments);
  }

  const int level;
  const int32_t decimal_scale;
  const bool skip_nulls;
  Moments moments;
  bool all_valid = true;
};

template <typename ArrowType>
struct StatisticImpl : public ScalarAggregator {
  using ThisType = StatisticImpl<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  StatisticImpl(StatisticType stat_type, int32_t decimal_scale,
                const std::shared_ptr<DataType>& out_type, const VarianceOptions& options)
      : out_type(out_type),
        stat_type(stat_type),
        skip_nulls(options.skip_nulls),
        min_count(options.min_count),
        ddof(options.ddof),
        state(moments_level_for_statistic(stat_type), decimal_scale, skip_nulls) {}

  StatisticImpl(StatisticType stat_type, int32_t decimal_scale,
                const std::shared_ptr<DataType>& out_type, const SkewOptions& options)
      : out_type(out_type),
        stat_type(stat_type),
        skip_nulls(options.skip_nulls),
        biased(options.biased),
        min_count(options.min_count),
        ddof(0),
        state(moments_level_for_statistic(stat_type), decimal_scale, skip_nulls) {}

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (batch[0].is_array()) {
      this->state.Consume(batch[0].array);
    } else {
      this->state.Consume(*batch[0].scalar, batch.length);
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->state.MergeFrom(other.state);
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    if (state.count() <= ddof || state.count() < min_count ||
        (!state.all_valid && !skip_nulls) ||
        (stat_type == StatisticType::Skew && !biased && state.count() < 3) ||
        (stat_type == StatisticType::Kurtosis && !biased && state.count() < 4)) {
      out->value = std::make_shared<DoubleScalar>();
    } else {
      switch (stat_type) {
        case StatisticType::Std:
          out->value = std::make_shared<DoubleScalar>(state.moments.Stddev(ddof));
          break;
        case StatisticType::Var:
          out->value = std::make_shared<DoubleScalar>(state.moments.Variance(ddof));
          break;
        case StatisticType::Skew:
          out->value = std::make_shared<DoubleScalar>(state.moments.Skew(biased));
          break;
        case StatisticType::Kurtosis:
          out->value = std::make_shared<DoubleScalar>(state.moments.Kurtosis(biased));
          break;
        default:
          return Status::NotImplemented("Unsupported statistic type ",
                                        static_cast<int>(stat_type));
      }
    }
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type;
  StatisticType stat_type;
  bool skip_nulls;
  bool biased;
  uint32_t min_count;
  int ddof = 0;
  MomentsState<ArrowType> state;
};

template <typename OptionsType>
Result<std::unique_ptr<KernelState>> StatisticInit(
    StatisticType stat_type, const DataType& in_type,
    const std::shared_ptr<DataType>& out_type, const OptionsType& options) {
  auto make_kernel_state = [&](auto&& type, int32_t decimal_scale = 0) {
    using Type = std::decay_t<decltype(type)>;
    return std::unique_ptr<KernelState>(
        new StatisticImpl<Type>(stat_type, decimal_scale, out_type, options));
  };

  auto visit = [&](auto&& type) -> Result<std::unique_ptr<KernelState>> {
    using Type = std::decay_t<decltype(type)>;
    // Decimals
    if constexpr (is_decimal_type<Type>::value) {
      return make_kernel_state(type, type.scale());
    }
    // Numbers (except half-float)
    if constexpr (is_number_type<Type>::value && !is_half_float_type<Type>::value) {
      return make_kernel_state(type);
    }
    return Status::NotImplemented("No variance/stddev implemented for ",
                                  in_type.ToString());
  };
  return VisitType(in_type, visit);
}

template <typename OptionsType, StatisticType kStatType>
Result<std::unique_ptr<KernelState>> StatisticInit(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
  const DataType& in_type = *args.inputs[0].type;
  const std::shared_ptr<DataType>& out_type = args.kernel->signature->out_type().type();
  const OptionsType& options = checked_cast<const OptionsType&>(*args.options);

  return StatisticInit(kStatType, in_type, out_type, options);
}

void AddStatisticKernels(KernelInit init,
                         const std::vector<std::shared_ptr<DataType>>& types,
                         ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    auto sig = KernelSignature::Make({InputType(ty->id())}, float64());
    AddAggKernel(std::move(sig), init, func);
  }
}

void AddStatisticKernels(KernelInit init, ScalarAggregateFunction* func) {
  AddStatisticKernels(init, NumericTypes(), func);
  AddStatisticKernels(
      init, {decimal32(1, 1), decimal64(1, 1), decimal128(1, 1), decimal256(1, 1)}, func);
}

const FunctionDoc stddev_doc{
    "Calculate the standard deviation of a numeric array",
    ("The number of degrees of freedom can be controlled using VarianceOptions.\n"
     "By default (`ddof` = 0), the population standard deviation is calculated.\n"
     "Nulls are ignored.  If there are not enough non-null values in the array\n"
     "to satisfy `ddof`, null is returned."),
    {"array"},
    "VarianceOptions"};

const FunctionDoc variance_doc{
    "Calculate the variance of a numeric array",
    ("The number of degrees of freedom can be controlled using VarianceOptions.\n"
     "By default (`ddof` = 0), the population variance is calculated.\n"
     "Nulls are ignored.  If there are not enough non-null values in the array\n"
     "to satisfy `ddof`, null is returned."),
    {"array"},
    "VarianceOptions"};

const FunctionDoc skew_doc{
    "Calculate the skewness of a numeric array",
    ("Nulls are ignored by default.  If there are not enough non-null values\n"
     "in the array to satisfy `min_count`, null is returned.\n"
     "The behavior of nulls and the `min_count` parameter can be changed\n"
     "in SkewOptions."),
    {"array"},
    "SkewOptions"};

const FunctionDoc kurtosis_doc{
    "Calculate the kurtosis of a numeric array",
    ("Nulls are ignored by default.  If there are not enough non-null values\n"
     "in the array to satisfy `min_count`, null is returned.\n"
     "The behavior of nulls and the `min_count` parameter can be changed\n"
     "in SkewOptions."),
    {"array"},
    "SkewOptions"};

std::shared_ptr<ScalarAggregateFunction> AddStddevAggKernels() {
  static const auto default_std_options = VarianceOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("stddev", Arity::Unary(),
                                                        stddev_doc, &default_std_options);
  AddStatisticKernels(StatisticInit<VarianceOptions, StatisticType::Std>, func.get());
  return func;
}

std::shared_ptr<ScalarAggregateFunction> AddVarianceAggKernels() {
  static const auto default_var_options = VarianceOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "variance", Arity::Unary(), variance_doc, &default_var_options);
  AddStatisticKernels(StatisticInit<VarianceOptions, StatisticType::Var>, func.get());
  return func;
}

std::shared_ptr<ScalarAggregateFunction> AddSkewAggKernels() {
  static const auto default_options = SkewOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("skew", Arity::Unary(), skew_doc,
                                                        &default_options);
  AddStatisticKernels(StatisticInit<SkewOptions, StatisticType::Skew>, func.get());
  return func;
}

std::shared_ptr<ScalarAggregateFunction> AddKurtosisAggKernels() {
  static const auto default_options = SkewOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("kurtosis", Arity::Unary(),
                                                        kurtosis_doc, &default_options);
  AddStatisticKernels(StatisticInit<SkewOptions, StatisticType::Kurtosis>, func.get());
  return func;
}

}  // namespace

void RegisterScalarAggregateVariance(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(AddVarianceAggKernels()));
  DCHECK_OK(registry->AddFunction(AddStddevAggKernels()));
  DCHECK_OK(registry->AddFunction(AddSkewAggKernels()));
  DCHECK_OK(registry->AddFunction(AddKurtosisAggKernels()));
}

}  // namespace arrow::compute::internal
