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

#include "arrow/compute/kernels/aggregate_basic_internal.h"

namespace arrow {
namespace compute {
namespace aggregate {

namespace {

// Based on https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
template <typename ArrowType>
struct StdevState {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using c_type = typename ArrowType::c_type;
  using ThisType = StdevState<ArrowType>;

  // Calculate stdev of one chunk with two pass algorithm
  // Always use `double` to calculate stdev for any array type
  void Consume(const ArrayType& array) {
    int64_t count = array.length() - array.null_count();
    if (count == 0) {
      return;
    }

    double sum = 0;
    VisitArrayDataInline<ArrowType>(
        *array.data(), [&sum](c_type value) { sum += static_cast<double>(value); },
        []() {});

    double mean = sum / count, m2 = 0;
    VisitArrayDataInline<ArrowType>(
        *array.data(),
        [mean, &m2](c_type value) {
          double v = static_cast<double>(value);
          m2 += (v - mean) * (v - mean);
        },
        []() {});

    this->count = count;
    this->sum = sum;
    this->m2 = m2;
  }

  // Combine stdev from two chunks
  void MergeFrom(const ThisType& state) {
    if (state.count == 0) {
      return;
    }
    if (this->count == 0) {
      this->count = state.count;
      this->sum = state.sum;
      this->m2 = state.m2;
      return;
    }
    double delta = this->sum / this->count - state.sum / state.count;
    this->m2 += state.m2 +
                delta * delta * this->count * state.count / (this->count + state.count);
    this->count += state.count;
    this->sum += state.sum;
  }

  int64_t count = 0;
  double sum = 0;
  double m2 = 0;  // sum((X-mean)^2)
};

template <typename ArrowType>
struct StdevImpl : public ScalarAggregator {
  using ThisType = StdevImpl<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  explicit StdevImpl(const std::shared_ptr<DataType>& out_type,
                     const StdevOptions& options)
      : out_type(out_type), options(options) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    ArrayType array(batch[0].array());
    this->state.Consume(array);
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->state.MergeFrom(other.state);
  }

  void Finalize(KernelContext*, Datum* out) override {
    if (this->state.count <= options.ddof) {
      out->value = std::make_shared<DoubleScalar>();
    } else {
      double stdev = sqrt(this->state.m2 / (this->state.count - options.ddof));
      out->value = std::make_shared<DoubleScalar>(stdev);
    }
  }

  std::shared_ptr<DataType> out_type;
  StdevState<ArrowType> state;
  StdevOptions options;
};

struct StdevInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type;
  const std::shared_ptr<DataType>& out_type;
  const StdevOptions& options;

  StdevInitState(KernelContext* ctx, const DataType& in_type,
                 const std::shared_ptr<DataType>& out_type, const StdevOptions& options)
      : ctx(ctx), in_type(in_type), out_type(out_type), options(options) {}

  Status Visit(const DataType&) { return Status::NotImplemented("No stdev implemented"); }

  Status Visit(const HalfFloatType&) {
    return Status::NotImplemented("No stdev implemented");
  }

  template <typename Type>
  enable_if_t<is_number_type<Type>::value, Status> Visit(const Type&) {
    state.reset(new StdevImpl<Type>(out_type, options));
    return Status::OK();
  }

  std::unique_ptr<KernelState> Create() {
    ctx->SetStatus(VisitTypeInline(in_type, this));
    return std::move(state);
  }
};

std::unique_ptr<KernelState> StdevInit(KernelContext* ctx, const KernelInitArgs& args) {
  StdevInitState visitor(ctx, *args.inputs[0].type,
                         args.kernel->signature->out_type().type(),
                         static_cast<const StdevOptions&>(*args.options));
  return visitor.Create();
}

void AddStdevKernels(KernelInit init, const std::vector<std::shared_ptr<DataType>>& types,
                     ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    auto sig = KernelSignature::Make({InputType::Array(ty)}, float64());
    AddAggKernel(std::move(sig), init, func);
  }
}

}  // namespace

std::shared_ptr<ScalarAggregateFunction> AddStdevAggKernels() {
  static auto default_stdev_options = StdevOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("stdev", Arity::Unary(),
                                                        &default_stdev_options);
  AddStdevKernels(StdevInit, internal::NumericTypes(), func.get());
  return func;
}

}  // namespace aggregate
}  // namespace compute
}  // namespace arrow
