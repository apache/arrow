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

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/aggregate_var_std_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/int128_internal.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

using arrow::internal::int128_t;
using arrow::internal::VisitSetBitRunsVoid;

template <typename ArrowType>
struct VarStdState {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using CType = typename TypeTraits<ArrowType>::CType;
  using ThisType = VarStdState<ArrowType>;

  explicit VarStdState(int32_t decimal_scale, VarianceOptions options)
      : decimal_scale(decimal_scale), options(options) {}

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal128& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal256& value) const { return value.ToDouble(decimal_scale); }

  // float/double/int64/decimal: calculate `m2` (sum((X-mean)^2)) with `two pass
  // algorithm`
  // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Two-pass_algorithm
  template <typename T = ArrowType>
  enable_if_t<is_floating_type<T>::value || (sizeof(CType) > 4)> Consume(
      const ArraySpan& array) {
    this->all_valid = array.GetNullCount() == 0;
    int64_t count = array.length - array.GetNullCount();
    if (count == 0 || (!this->all_valid && !options.skip_nulls)) {
      return;
    }

    using SumType = typename internal::GetSumType<T>::SumType;
    SumType sum = internal::SumArray<CType, SumType, SimdLevel::NONE>(array);

    const double mean = ToDouble(sum) / count;
    const double m2 = internal::SumArray<CType, double, SimdLevel::NONE>(
        array, [this, mean](CType value) {
          const double v = ToDouble(value);
          return (v - mean) * (v - mean);
        });

    ThisType state(decimal_scale, options);
    state.count = count;
    state.mean = mean;
    state.m2 = m2;
    this->MergeFrom(state);
  }

  // int32/16/8: textbook one pass algorithm with integer arithmetic
  template <typename T = ArrowType>
  enable_if_t<is_integer_type<T>::value && (sizeof(CType) <= 4)> Consume(
      const ArraySpan& array) {
    // max number of elements that sum will not overflow int64 (2Gi int32 elements)
    // for uint32:    0 <= sum < 2^63 (int64 >= 0)
    // for int32: -2^62 <= sum < 2^62
    constexpr int64_t max_length = 1ULL << (63 - sizeof(CType) * 8);

    this->all_valid = array.GetNullCount() == 0;
    if (!this->all_valid && !options.skip_nulls) return;
    int64_t start_index = 0;
    int64_t valid_count = array.length - array.GetNullCount();

    ArraySpan slice = array;
    while (valid_count > 0) {
      // process in chunks that overflow will never happen
      slice.SetSlice(start_index + array.offset,
                     std::min(max_length, array.length - start_index));
      const int64_t count = slice.length - slice.GetNullCount();
      start_index += slice.length;
      valid_count -= count;

      if (count > 0) {
        IntegerVarStd<ArrowType> var_std;
        const CType* values = slice.GetValues<CType>(1);
        VisitSetBitRunsVoid(slice.buffers[0].data, slice.offset, slice.length,
                            [&](int64_t pos, int64_t len) {
                              for (int64_t i = 0; i < len; ++i) {
                                const auto value = values[pos + i];
                                var_std.ConsumeOne(value);
                              }
                            });

        // merge variance
        ThisType state(decimal_scale, options);
        state.count = var_std.count;
        state.mean = var_std.mean();
        state.m2 = var_std.m2();
        this->MergeFrom(state);
      }
    }
  }

  // Scalar: textbook algorithm
  void Consume(const Scalar& scalar, const int64_t count) {
    this->m2 = 0;
    if (scalar.is_valid) {
      this->count = count;
      this->mean = ToDouble(UnboxScalar<ArrowType>::Unbox(scalar));
    } else {
      this->count = 0;
      this->mean = 0;
      this->all_valid = false;
    }
  }

  // Combine `m2` from two chunks (m2 = n*s2)
  // https://www.emathzone.com/tutorials/basic-statistics/combined-variance.html
  void MergeFrom(const ThisType& state) {
    this->all_valid = this->all_valid && state.all_valid;
    if (state.count == 0) {
      return;
    }
    if (this->count == 0) {
      this->count = state.count;
      this->mean = state.mean;
      this->m2 = state.m2;
      return;
    }
    MergeVarStd(this->count, this->mean, state.count, state.mean, state.m2, &this->count,
                &this->mean, &this->m2);
  }

  const int32_t decimal_scale;
  const VarianceOptions options;
  int64_t count = 0;
  double mean = 0;
  double m2 = 0;  // m2 = count*s2 = sum((X-mean)^2)
  bool all_valid = true;
};

template <typename ArrowType>
struct VarStdImpl : public ScalarAggregator {
  using ThisType = VarStdImpl<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  explicit VarStdImpl(int32_t decimal_scale, const std::shared_ptr<DataType>& out_type,
                      const VarianceOptions& options, VarOrStd return_type)
      : out_type(out_type), state(decimal_scale, options), return_type(return_type) {}

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
    if (state.count <= state.options.ddof || state.count < state.options.min_count ||
        (!state.all_valid && !state.options.skip_nulls)) {
      out->value = std::make_shared<DoubleScalar>();
    } else {
      double var = state.m2 / (state.count - state.options.ddof);
      out->value =
          std::make_shared<DoubleScalar>(return_type == VarOrStd::Var ? var : sqrt(var));
    }
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type;
  VarStdState<ArrowType> state;
  VarOrStd return_type;
};

struct VarStdInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  const std::shared_ptr<DataType>& out_type;
  const VarianceOptions& options;
  VarOrStd return_type;

  VarStdInitState(KernelContext* ctx, const DataType& in_type,
                  const std::shared_ptr<DataType>& out_type,
                  const VarianceOptions& options, VarOrStd return_type)
      : ctx(ctx),
        in_type(in_type),
        out_type(out_type),
        options(options),
        return_type(return_type) {}

  Status Visit(const DataType&) {
    return Status::NotImplemented("No variance/stddev implemented");
  }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No variance/stddev implemented");
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    state.reset(
        new VarStdImpl<Type>(/*decimal_scale=*/0, out_type, options, return_type));
    return Status::OK();
  }

  template <typename Type>
  enable_if_decimal<Type, Status> Visit(const Type&) {
    state.reset(new VarStdImpl<Type>(checked_cast<const DecimalType&>(in_type).scale(),
                                     out_type, options, return_type));
    return Status::OK();
  }

  Result<std::unique_ptr<KernelState>> Create() {
    RETURN_NOT_OK(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

Result<std::unique_ptr<KernelState>> StddevInit(KernelContext* ctx,
                                                const KernelInitArgs& args) {
  VarStdInitState visitor(
      ctx, *args.inputs[0].type, args.kernel->signature->out_type().type(),
      static_cast<const VarianceOptions&>(*args.options), VarOrStd::Std);
  return visitor.Create();
}

Result<std::unique_ptr<KernelState>> VarianceInit(KernelContext* ctx,
                                                  const KernelInitArgs& args) {
  VarStdInitState visitor(
      ctx, *args.inputs[0].type, args.kernel->signature->out_type().type(),
      static_cast<const VarianceOptions&>(*args.options), VarOrStd::Var);
  return visitor.Create();
}

void AddVarStdKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    auto sig = KernelSignature::Make({InputType(ty->id())}, float64());
    AddAggKernel(std::move(sig), init, func);
  }
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

std::shared_ptr<ScalarAggregateFunction> AddStddevAggKernels() {
  static auto default_std_options = VarianceOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("stddev", Arity::Unary(),
                                                        stddev_doc, &default_std_options);
  AddVarStdKernels(StddevInit, NumericTypes(), func.get());
  AddVarStdKernels(StddevInit, {decimal128(1, 1), decimal256(1, 1)}, func.get());
  return func;
}

std::shared_ptr<ScalarAggregateFunction> AddVarianceAggKernels() {
  static auto default_var_options = VarianceOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "variance", Arity::Unary(), variance_doc, &default_var_options);
  AddVarStdKernels(VarianceInit, NumericTypes(), func.get());
  AddVarStdKernels(VarianceInit, {decimal128(1, 1), decimal256(1, 1)}, func.get());
  return func;
}

}  // namespace

void RegisterScalarAggregateVariance(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(AddVarianceAggKernels()));
  DCHECK_OK(registry->AddFunction(AddStddevAggKernels()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
