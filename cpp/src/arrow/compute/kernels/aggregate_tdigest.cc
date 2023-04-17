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

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/tdigest.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

using arrow::internal::TDigest;
using arrow::internal::VisitSetBitRunsVoid;

template <typename ArrowType>
struct TDigestImpl : public ScalarAggregator {
  using ThisType = TDigestImpl<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using CType = typename TypeTraits<ArrowType>::CType;

  TDigestImpl(const TDigestOptions& options, const DataType& in_type)
      : options{options},
        tdigest{options.delta, options.buffer_size},
        count{0},
        decimal_scale{0},
        all_valid{true} {
    if (is_decimal_type<ArrowType>::value) {
      decimal_scale = checked_cast<const DecimalType&>(in_type).scale();
    }
  }

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal128& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal256& value) const { return value.ToDouble(decimal_scale); }

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (!this->all_valid) return Status::OK();
    if (!options.skip_nulls && batch[0].null_count() > 0) {
      this->all_valid = false;
      return Status::OK();
    }
    if (batch[0].is_array()) {
      const ArraySpan& data = batch[0].array;
      const CType* values = data.GetValues<CType>(1);

      if (data.length > data.GetNullCount()) {
        this->count += data.length - data.GetNullCount();
        VisitSetBitRunsVoid(data.buffers[0].data, data.offset, data.length,
                            [&](int64_t pos, int64_t len) {
                              for (int64_t i = 0; i < len; ++i) {
                                this->tdigest.NanAdd(ToDouble(values[pos + i]));
                              }
                            });
      }
    } else {
      const CType value = UnboxScalar<ArrowType>::Unbox(*batch[0].scalar);
      if (batch[0].scalar->is_valid) {
        this->count += 1;
        for (int64_t i = 0; i < batch.length; i++) {
          this->tdigest.NanAdd(ToDouble(value));
        }
      }
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    if (!this->all_valid || !other.all_valid) {
      this->all_valid = false;
      return Status::OK();
    }
    this->tdigest.Merge(other.tdigest);
    this->count += other.count;
    return Status::OK();
  }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    const int64_t out_length = options.q.size();
    auto out_data = ArrayData::Make(float64(), out_length, 0);
    out_data->buffers.resize(2, nullptr);
    ARROW_ASSIGN_OR_RAISE(out_data->buffers[1],
                          ctx->Allocate(out_length * sizeof(double)));
    double* out_buffer = out_data->template GetMutableValues<double>(1);

    if (this->tdigest.is_empty() || !this->all_valid || this->count < options.min_count) {
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[0], ctx->AllocateBitmap(out_length));
      std::memset(out_data->buffers[0]->mutable_data(), 0x00,
                  out_data->buffers[0]->size());
      std::fill(out_buffer, out_buffer + out_length, 0.0);
      out_data->null_count = out_length;
    } else {
      for (int64_t i = 0; i < out_length; ++i) {
        out_buffer[i] = this->tdigest.Quantile(this->options.q[i]);
      }
    }
    *out = Datum(std::move(out_data));
    return Status::OK();
  }

  const TDigestOptions options;
  TDigest tdigest;
  int64_t count;
  int32_t decimal_scale;
  bool all_valid;
};

struct TDigestInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  const TDigestOptions& options;

  TDigestInitState(KernelContext* ctx, const DataType& in_type,
                   const TDigestOptions& options)
      : ctx(ctx), in_type(in_type), options(options) {}

  Status Visit(const DataType&) {
    return Status::NotImplemented("No tdigest implemented");
  }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No tdigest implemented");
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    state.reset(new TDigestImpl<Type>(options, in_type));
    return Status::OK();
  }

  template <typename Type>
  enable_if_decimal<Type, Status> Visit(const Type&) {
    state.reset(new TDigestImpl<Type>(options, in_type));
    return Status::OK();
  }

  Result<std::unique_ptr<KernelState>> Create() {
    RETURN_NOT_OK(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

Result<std::unique_ptr<KernelState>> TDigestInit(KernelContext* ctx,
                                                 const KernelInitArgs& args) {
  TDigestInitState visitor(ctx, *args.inputs[0].type,
                           static_cast<const TDigestOptions&>(*args.options));
  return visitor.Create();
}

void AddTDigestKernels(KernelInit init,
                       const std::vector<std::shared_ptr<DataType>>& types,
                       ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    auto sig = KernelSignature::Make({InputType(ty->id())}, float64());
    AddAggKernel(std::move(sig), init, func);
  }
}

const FunctionDoc tdigest_doc{
    "Approximate quantiles of a numeric array with T-Digest algorithm",
    ("By default, 0.5 quantile (median) is returned.\n"
     "Nulls and NaNs are ignored.\n"
     "An array of nulls is returned if there is no valid data point."),
    {"array"},
    "TDigestOptions"};

const FunctionDoc approximate_median_doc{
    "Approximate median of a numeric array with T-Digest algorithm",
    ("Nulls and NaNs are ignored.\n"
     "A null scalar is returned if there is no valid data point."),
    {"array"},
    "ScalarAggregateOptions"};

std::shared_ptr<ScalarAggregateFunction> AddTDigestAggKernels() {
  static auto default_tdigest_options = TDigestOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "tdigest", Arity::Unary(), tdigest_doc, &default_tdigest_options);
  AddTDigestKernels(TDigestInit, NumericTypes(), func.get());
  AddTDigestKernels(TDigestInit, {decimal128(1, 1), decimal256(1, 1)}, func.get());
  return func;
}

std::shared_ptr<ScalarAggregateFunction> AddApproximateMedianAggKernels(
    const ScalarAggregateFunction* tdigest_func) {
  static ScalarAggregateOptions default_scalar_aggregate_options;

  auto median = std::make_shared<ScalarAggregateFunction>(
      "approximate_median", Arity::Unary(), approximate_median_doc,
      &default_scalar_aggregate_options);

  auto sig = KernelSignature::Make({InputType::Any()}, float64());

  auto init = [tdigest_func](
                  KernelContext* ctx,
                  const KernelInitArgs& args) -> Result<std::unique_ptr<KernelState>> {
    std::vector<TypeHolder> types = args.inputs;
    ARROW_ASSIGN_OR_RAISE(auto kernel, tdigest_func->DispatchBest(&types));
    const auto& scalar_options =
        checked_cast<const ScalarAggregateOptions&>(*args.options);
    TDigestOptions options;
    // Default q = 0.5
    options.min_count = scalar_options.min_count;
    options.skip_nulls = scalar_options.skip_nulls;
    KernelInitArgs new_args{kernel, types, &options};
    return kernel->init(ctx, new_args);
  };

  auto finalize = [](KernelContext* ctx, Datum* out) -> Status {
    Datum temp;
    RETURN_NOT_OK(checked_cast<ScalarAggregator*>(ctx->state())->Finalize(ctx, &temp));
    const auto arr = temp.make_array();
    DCHECK_EQ(arr->length(), 1);
    return arr->GetScalar(0).Value(out);
  };

  AddAggKernel(std::move(sig), std::move(init), std::move(finalize), median.get());
  return median;
}

}  // namespace

void RegisterScalarAggregateTDigest(FunctionRegistry* registry) {
  auto tdigest = AddTDigestAggKernels();
  DCHECK_OK(registry->AddFunction(tdigest));

  auto approx_median = AddApproximateMedianAggKernels(tdigest.get());
  DCHECK_OK(registry->AddFunction(approx_median));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
