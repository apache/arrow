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
#include "arrow/compute/registry_internal.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/tdigest_internal.h"
namespace arrow {
namespace compute {
namespace internal {

namespace {

using arrow::internal::TDigest;
using arrow::internal::TDigestScalerK0;
using arrow::internal::TDigestScalerK1;
using arrow::internal::VisitSetBitRunsVoid;

struct TDigestBaseImpl : public ScalarAggregator {
  explicit TDigestBaseImpl(std::unique_ptr<TDigest::Scaler> scaler, uint32_t buffer_size)
      : tdigest{std::move(scaler), buffer_size}, count{0}, all_valid{true} {
    auto output_size = tdigest.delta();
    out_type = struct_({field("mean", fixed_size_list(float64(), output_size), false),
                        field("weight", fixed_size_list(float64(), output_size), false),
                        field("count", uint64(), false)});
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const TDigestBaseImpl&>(src);
    if (!this->all_valid || !other.all_valid) {
      this->all_valid = false;
      return Status::OK();
    }
    this->tdigest.Merge(other.tdigest);
    this->count += other.count;
    return Status::OK();
  }

  static Result<std::unique_ptr<TDigest::Scaler>> MakeScaler(
      TDigestOptions::Scaler scaler, uint32_t delta) {
    switch (scaler) {
      case TDigestOptions::K0:
        return std::make_unique<TDigestScalerK0>(delta);
      case TDigestOptions::K1:
        return std::make_unique<TDigestScalerK1>(delta);
    }
    return Status::NotImplemented("Invalid TDigest scaler");
  }

  TDigest tdigest;
  uint64_t count;
  bool all_valid;
  std::shared_ptr<DataType> out_type;
};

struct TDigestQuantileFinalizer : public TDigestBaseImpl {
  template <typename... Args>
  explicit TDigestQuantileFinalizer(std::vector<double> q, uint32_t min_count,
                                    Args&&... args)
      : TDigestBaseImpl(std::forward<Args>(args)...),
        q(std::move(q)),
        min_count(min_count) {}

  Status Finalize(KernelContext* ctx, Datum* out) override {
    const int64_t out_length = q.size();
    auto out_data = ArrayData::Make(float64(), out_length, 0);
    out_data->buffers.resize(2, nullptr);
    ARROW_ASSIGN_OR_RAISE(out_data->buffers[1],
                          ctx->Allocate(out_length * sizeof(double)));
    double* out_buffer = out_data->template GetMutableValues<double>(1);

    if (this->tdigest.is_empty() || !this->all_valid || this->count < min_count) {
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[0], ctx->AllocateBitmap(out_length));
      std::memset(out_data->buffers[0]->mutable_data(), 0x00,
                  out_data->buffers[0]->size());
      std::fill(out_buffer, out_buffer + out_length, 0.0);
      out_data->null_count = out_length;
    } else {
      for (int64_t i = 0; i < out_length; ++i) {
        out_buffer[i] = this->tdigest.Quantile(this->q[i]);
      }
    }
    *out = Datum(std::move(out_data));
    return Status::OK();
  }

  std::vector<double> q;
  uint32_t min_count;
};

struct TDigestCentroidFinalizer : public TDigestBaseImpl {
  template <typename... Args>
  explicit TDigestCentroidFinalizer(Args&&... args)
      : TDigestBaseImpl(std::forward<Args>(args)...) {}

  Status Finalize(KernelContext* ctx, Datum* out) override {
    if (!this->all_valid) {
      *out = MakeNullScalar(out_type);
    } else {
      // Float64Array
      const int64_t out_length = tdigest.delta();
      auto mean_data = ArrayData::Make(float64(), out_length, 0);
      mean_data->buffers.resize(2, nullptr);
      ARROW_ASSIGN_OR_RAISE(mean_data->buffers[1],
                            ctx->Allocate(out_length * sizeof(double)));
      double* mean_buffer = mean_data->template GetMutableValues<double>(1);

      auto weight_data = ArrayData::Make(float64(), out_length, 0);
      weight_data->buffers.resize(2, nullptr);
      ARROW_ASSIGN_OR_RAISE(weight_data->buffers[1],
                            ctx->Allocate(out_length * sizeof(double)));
      double* weight_buffer = weight_data->template GetMutableValues<double>(1);

      ARROW_ASSIGN_OR_RAISE(auto bitmap, ctx->AllocateBitmap(out_length));
      auto bitmap_data = bitmap->mutable_data();
      bit_util::SetBitsTo(bitmap_data, 0, out_length, true);
      mean_data->buffers[0] = bitmap;
      weight_data->buffers[0] = bitmap;

      for (int64_t i = 0; i < out_length; ++i) {
        auto maybe_c = this->tdigest.GetCentroid(i);
        if (maybe_c) {
          std::tie(mean_buffer[i], weight_buffer[i]) = *std::move(maybe_c);
        } else {
          bit_util::SetBitsTo(bitmap_data, i, out_length, false);
          auto null_count = out_length - i;
          std::fill(mean_buffer + i, mean_buffer + out_length, 0.0);
          std::fill(weight_buffer + i, weight_buffer + out_length, 0.0);
          mean_data->SetNullCount(null_count);
          weight_data->SetNullCount(null_count);
          break;
        }
      }

      auto mean = std::make_shared<FixedSizeListScalar>(MakeArray(mean_data));
      auto weight = std::make_shared<FixedSizeListScalar>(MakeArray(weight_data));
      auto count = std::make_shared<UInt64Scalar>(this->count);
      *out = std::make_shared<StructScalar>(
          std::vector<std::shared_ptr<Scalar>>{mean, weight, count}, out_type);
    }

    return Status::OK();
  }
};

template <typename ArrowType, typename TDigestFinalizer_T>
struct TDigestInputConsumerImpl : public TDigestFinalizer_T {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using CType = typename TypeTraits<ArrowType>::CType;

  template <typename... Args>
  explicit TDigestInputConsumerImpl(bool skip_nulls, const DataType& in_type,
                                    Args&&... args)
      : TDigestFinalizer_T(std::forward<Args>(args)...),
        skip_nulls{skip_nulls},
        decimal_scale{0} {
    if (is_decimal_type<ArrowType>::value) {
      decimal_scale = checked_cast<const DecimalType&>(in_type).scale();
    }
  }

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal32& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal64& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal128& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal256& value) const { return value.ToDouble(decimal_scale); }

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (!this->all_valid) return Status::OK();
    if (!skip_nulls && batch[0].null_count() > 0) {
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

  bool skip_nulls;
  int32_t decimal_scale;
};

template <typename TDigestFinalizer_T>
struct TDigestCentroidConsumerImpl : public TDigestFinalizer_T {
  template <typename... Args>
  explicit TDigestCentroidConsumerImpl(Args&&... args)
      : TDigestFinalizer_T(std::forward<Args>(args)...) {}

  Status Consume(const Scalar* scalar) {
    const auto* input_struct_scalar = checked_cast<const StructScalar*>(scalar);
    auto mean_array =
        checked_cast<const FixedSizeListScalar*>(input_struct_scalar->value[0].get())
            ->value;
    auto weight_array =
        checked_cast<const FixedSizeListScalar*>(input_struct_scalar->value[1].get())
            ->value;
    auto count = checked_cast<const UInt64Scalar*>(input_struct_scalar->value[2].get());
    auto mean_double_array = checked_cast<const DoubleArray*>(mean_array.get());
    auto weight_double_array = checked_cast<const DoubleArray*>(weight_array.get());
    DCHECK_EQ(mean_double_array->length(), this->tdigest.delta());
    DCHECK_EQ(weight_double_array->length(), this->tdigest.delta());
    for (int64_t i = 0; i < this->tdigest.delta(); i++) {
      if (mean_double_array->IsNull(i)) {
        break;
      }
      DCHECK(weight_double_array->IsValid(i));

      this->tdigest.NanAdd(mean_double_array->Value(i), weight_double_array->Value(i));
    }
    this->count += count->value;
    return Status::OK();
  }
  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (!this->all_valid) return Status::OK();
    if (batch[0].null_count() > 0) {
      this->all_valid = false;
      return Status::OK();
    }
    if (batch[0].is_array()) {
      std::shared_ptr<Array> array = MakeArray(batch[0].array.ToArrayData());
      for (int i = 0; i < array->length(); ++i) {
        ARROW_ASSIGN_OR_RAISE(auto scalar, array->GetScalar(0));
        ARROW_RETURN_NOT_OK(Consume(scalar.get()));
      }
    } else {
      const Scalar* scalar = batch[0].scalar;
      ARROW_RETURN_NOT_OK(Consume(scalar));
    }
    return Status::OK();
  }
};

template <typename ArrowType>
struct TDigestImpl
    : public TDigestInputConsumerImpl<ArrowType, TDigestQuantileFinalizer> {
  // using TDigestBaseImpl::all_valid;
  // using TDigestBaseImpl::count;
  // using TDigestBaseImpl::out_type;
  // using TDigestBaseImpl::tdigest;

  explicit TDigestImpl(const TDigestOptions& options, const DataType& in_type,
                       std::unique_ptr<TDigest::Scaler> scaler)
      : TDigestInputConsumerImpl<ArrowType, TDigestQuantileFinalizer>(
            // TDigestInputConsumerImpl
            options.skip_nulls, in_type,
            // TDigestQuantileFinalizer
            options.q, options.min_count,
            // TDigestBaseImpl
            std::move(scaler), options.buffer_size) {}
};

template <typename ArrowType>
struct TDigestMapImpl
    : public TDigestInputConsumerImpl<ArrowType, TDigestCentroidFinalizer> {
  explicit TDigestMapImpl(const TDigestMapOptions& options, const DataType& in_type,
                          std::unique_ptr<TDigest::Scaler> scaler)
      : TDigestInputConsumerImpl<ArrowType, TDigestCentroidFinalizer>(

            // TDigestInputConsumerImpl
            options.skip_nulls, in_type,
            // TDigestCentroidFinalizer
            // TDigestBaseImpl
            std::move(scaler), options.buffer_size) {}
};

struct TDigestReduceImpl : public TDigestCentroidConsumerImpl<TDigestCentroidFinalizer> {
  explicit TDigestReduceImpl(const TDigestReduceOptions& options,
                             std::unique_ptr<TDigest::Scaler> scaler, uint32_t size)
      : TDigestCentroidConsumerImpl<TDigestCentroidFinalizer>(
            // TDigestCentroidConsumerImpl
            // TDigestCentroidFinalizer
            // TDigestBaseImpl
            std::move(scaler), size) {}
};

struct TDigestQuantileImpl
    : public TDigestCentroidConsumerImpl<TDigestQuantileFinalizer> {
  explicit TDigestQuantileImpl(const TDigestQuantileOptions& options,
                               std::unique_ptr<TDigest::Scaler> scaler, uint32_t size)
      : TDigestCentroidConsumerImpl<TDigestQuantileFinalizer>(

            // TDigestCentroidConsumerImpl
            // TDigestQuantileFinalizer
            options.q, options.min_count,
            // TDigestBaseImpl
            std::move(scaler), size) {}
};

template <template <typename> typename TDigestImpl_T, typename TDigestOptions_T>
struct TDigestInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  const TDigestOptions_T& options;

  TDigestInitState(KernelContext* ctx, const DataType& in_type,
                   const TDigestOptions_T& options)
      : ctx(ctx), in_type(in_type), options(options) {}

  Status Visit(const DataType&) {
    return Status::NotImplemented("No tdigest implemented");
  }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No tdigest implemented");
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type&) {
    ARROW_ASSIGN_OR_RAISE(auto scaler,
                          TDigestBaseImpl::MakeScaler(options.scaler, options.delta));
    state.reset(new TDigestImpl_T<Type>(options, in_type, std::move(scaler)));
    return Status::OK();
  }

  template <typename Type>
  enable_if_decimal<Type, Status> Visit(const Type&) {
    ARROW_ASSIGN_OR_RAISE(auto scaler,
                          TDigestBaseImpl::MakeScaler(options.scaler, options.delta));
    state.reset(new TDigestImpl_T<Type>(options, in_type, std::move(scaler)));
    return Status::OK();
  }

  Result<std::unique_ptr<KernelState>> Create() {
    RETURN_NOT_OK(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

struct TDigestCentroidTypeMatcher : public TypeMatcher {
  ~TDigestCentroidTypeMatcher() override = default;

  static Result<uint32_t> getDelta(const DataType& type) {
    if (Type::STRUCT == type.id()) {
      const auto& input_struct_type = checked_cast<const StructType&>(type);
      if (3 == input_struct_type.num_fields()) {
        if (Type::FIXED_SIZE_LIST == input_struct_type.field(0)->type()->id() &&
            input_struct_type.field(0)->type()->Equals(
                input_struct_type.field(1)->type()) &&
            Type::UINT64 == input_struct_type.field(2)->type()->id()) {
          auto fsl = checked_cast<const FixedSizeListType*>(
              input_struct_type.field(0)->type().get());
          return fsl->list_size();
        }
      }
    }
    return Status::Invalid("Type ", type.ToString(), " does not match ",
                           ToStringStatic());
  }

  bool Matches(const DataType& type) const override { return getDelta(type).ok(); }

  static std::string ToStringStatic() {
    return "struct{mean:fixed_size_list<item: double>[N], weight:fixed_size_list<item: "
           "double>[N], count:int64}";
  }
  std::string ToString() const override { return ToStringStatic(); }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const TDigestCentroidTypeMatcher*>(&other);
    return casted != nullptr;
  }

  static std::shared_ptr<TDigestCentroidTypeMatcher> getMatcher() {
    static auto matcher = std::make_shared<TDigestCentroidTypeMatcher>();
    return matcher;
  }
};

Result<std::unique_ptr<KernelState>> TDigestInit(KernelContext* ctx,
                                                 const KernelInitArgs& args) {
  TDigestInitState<TDigestImpl, TDigestOptions> visitor(
      ctx, *args.inputs[0].type, static_cast<const TDigestOptions&>(*args.options));
  return visitor.Create();
}

Result<std::unique_ptr<KernelState>> TDigestMapInit(KernelContext* ctx,
                                                    const KernelInitArgs& args) {
  TDigestInitState<TDigestMapImpl, TDigestMapOptions> visitor(
      ctx, *args.inputs[0].type, static_cast<const TDigestMapOptions&>(*args.options));
  return visitor.Create();
}

Result<std::unique_ptr<KernelState>> TDigestReduceInit(KernelContext* ctx,
                                                       const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(uint32_t delta,
                        TDigestCentroidTypeMatcher::getDelta(*args.inputs[0].type));
  auto options = static_cast<const TDigestReduceOptions&>(*args.options);
  ARROW_ASSIGN_OR_RAISE(auto scaler, TDigestBaseImpl::MakeScaler(options.scaler, delta));
  return std::make_unique<TDigestReduceImpl>(options, std::move(scaler), delta);
}

Result<std::unique_ptr<KernelState>> TDigestQuantileInit(KernelContext* ctx,
                                                         const KernelInitArgs& args) {
  ARROW_ASSIGN_OR_RAISE(uint32_t delta,
                        TDigestCentroidTypeMatcher::getDelta(*args.inputs[0].type));
  auto options = static_cast<const TDigestQuantileOptions&>(*args.options);
  ARROW_ASSIGN_OR_RAISE(auto scaler, TDigestBaseImpl::MakeScaler(options.scaler, delta));
  return std::make_unique<TDigestQuantileImpl>(options, std::move(scaler), delta);
}

void AddTDigestKernels(KernelInit init,
                       const std::vector<std::shared_ptr<DataType>>& types,
                       ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    auto sig = KernelSignature::Make({InputType(ty->id())}, float64());
    AddAggKernel(std::move(sig), init, func);
  }
}

Result<TypeHolder> TDigestMapReduceType(KernelContext* ctx,
                                        const std::vector<TypeHolder>& types) {
  auto base = checked_cast<TDigestBaseImpl*>(ctx->state());
  return base->out_type;
}

void AddTDigestMapKernels(KernelInit init,
                          const std::vector<std::shared_ptr<DataType>>& types,
                          ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    auto sig = KernelSignature::Make({InputType(ty->id())}, TDigestMapReduceType);
    AddAggKernel(std::move(sig), init, func);
  }
}

void AddTDigestReduceKernels(KernelInit init, ScalarAggregateFunction* func) {
  auto sig = KernelSignature::Make({InputType(TDigestCentroidTypeMatcher::getMatcher())},
                                   TDigestMapReduceType);
  AddAggKernel(std::move(sig), init, func);
}

void AddTDigestQuantileKernels(KernelInit init, ScalarAggregateFunction* func) {
  auto sig = KernelSignature::Make({InputType(TDigestCentroidTypeMatcher::getMatcher())},
                                   float64());
  AddAggKernel(std::move(sig), init, func);
}

const FunctionDoc tdigest_doc{
    "Approximate quantiles of a numeric array with T-Digest algorithm",
    ("By default, 0.5 quantile (median) is returned.\n"
     "Nulls and NaNs are ignored.\n"
     "An array of nulls is returned if there is no valid data point."),
    {"array"},
    "TDigestOptions"};

const FunctionDoc tdigest_map_doc{
    "Calculate centroids with T-Digest algorithm",
    ("By default, 0.5 quantile (median) is returned.\n"
     "Nulls and NaNs are ignored.\n"
     "An array of nulls is returned if there is no valid data point."),
    {"array"},
    "TDigestMapOptions"};

const FunctionDoc tdigest_reduce_doc{
    "Merge multiple centroid sets to single set with T-Digest algorithm",
    ("By default, 0.5 quantile (median) is returned.\n"
     "Nulls and NaNs are ignored.\n"
     "An array of nulls is returned if there is no valid data point."),
    {"array"},
    "TDigestMapOptions"};

const FunctionDoc tdigest_quantile_doc{
    "Calculate centroids with T-Digest algorithm",
    ("By default, 0.5 quantile (median) is returned.\n"
     "Nulls and NaNs are ignored.\n"
     "An array of nulls is returned if there is no valid data point."),
    {"array"},
    "TDigestMapOptions"};

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

std::shared_ptr<ScalarAggregateFunction> AddTDigestMapAggKernels() {
  static auto default_tdigest_options = TDigestMapOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "tdigest_map", Arity::Unary(), tdigest_map_doc, &default_tdigest_options);
  AddTDigestMapKernels(TDigestMapInit, NumericTypes(), func.get());
  AddTDigestMapKernels(TDigestMapInit, {decimal128(1, 1), decimal256(1, 1)}, func.get());
  return func;
}

std::shared_ptr<ScalarAggregateFunction> AddTDigestReduceAggKernels() {
  static auto default_tdigest_options = TDigestReduceOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "tdigest_reduce", Arity::Unary(), tdigest_reduce_doc, &default_tdigest_options);
  AddTDigestReduceKernels(TDigestReduceInit, func.get());
  return func;
}

std::shared_ptr<ScalarAggregateFunction> AddTDigestQuantileAggKernels() {
  static auto default_tdigest_options = TDigestMapOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "tdigest_quantile", Arity::Unary(), tdigest_quantile_doc, &default_tdigest_options);
  AddTDigestQuantileKernels(TDigestQuantileInit, func.get());
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
  auto tdigest_map = AddTDigestMapAggKernels();
  DCHECK_OK(registry->AddFunction(tdigest_map));
  auto tdigest_merge = AddTDigestReduceAggKernels();
  DCHECK_OK(registry->AddFunction(tdigest_merge));
  auto tdigest_quantile = AddTDigestQuantileAggKernels();
  DCHECK_OK(registry->AddFunction(tdigest_quantile));

  auto approx_median = AddApproximateMedianAggKernels(tdigest.get());
  DCHECK_OK(registry->AddFunction(approx_median));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
