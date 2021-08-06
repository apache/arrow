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
#include "arrow/compute/kernels/common.h"
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
  using CType = typename ArrowType::c_type;

  explicit TDigestImpl(const TDigestOptions& options)
      : q{options.q}, tdigest{options.delta, options.buffer_size} {}

  Status Consume(KernelContext*, const ExecBatch& batch) override {
    if (batch[0].is_array()) {
      const ArrayData& data = *batch[0].array();
      const CType* values = data.GetValues<CType>(1);

      if (data.length > data.GetNullCount()) {
        VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                            [&](int64_t pos, int64_t len) {
                              for (int64_t i = 0; i < len; ++i) {
                                this->tdigest.NanAdd(values[pos + i]);
                              }
                            });
      }
    } else {
      const CType value = UnboxScalar<ArrowType>::Unbox(*batch[0].scalar());
      if (batch[0].scalar()->is_valid) {
        this->tdigest.NanAdd(value);
      }
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    auto& other = checked_cast<ThisType&>(src);
    std::vector<TDigest> other_tdigest;
    other_tdigest.push_back(std::move(other.tdigest));
    this->tdigest.Merge(&other_tdigest);
    return Status::OK();
  }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    const int64_t out_length = this->tdigest.is_empty() ? 0 : this->q.size();
    auto out_data = ArrayData::Make(float64(), out_length, 0);
    out_data->buffers.resize(2, nullptr);

    if (out_length > 0) {
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[1],
                            ctx->Allocate(out_length * sizeof(double)));
      double* out_buffer = out_data->template GetMutableValues<double>(1);
      for (int64_t i = 0; i < out_length; ++i) {
        out_buffer[i] = this->tdigest.Quantile(this->q[i]);
      }
    }

    *out = Datum(std::move(out_data));
    return Status::OK();
  }

  const std::vector<double>& q;
  TDigest tdigest;
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
  enable_if_t<is_number_type<Type>::value, Status> Visit(const Type&) {
    state.reset(new TDigestImpl<Type>(options));
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
    auto sig = KernelSignature::Make({InputType(ty)}, float64());
    AddAggKernel(std::move(sig), init, func);
  }
}

const FunctionDoc tdigest_doc{
    "Approximate quantiles of a numeric array with T-Digest algorithm",
    ("By default, 0.5 quantile (median) is returned.\n"
     "Nulls and NaNs are ignored.\n"
     "An empty array is returned if there is no valid data point."),
    {"array"},
    "TDigestOptions"};

std::shared_ptr<ScalarAggregateFunction> AddTDigestAggKernels() {
  static auto default_tdigest_options = TDigestOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "tdigest", Arity::Unary(), &tdigest_doc, &default_tdigest_options);
  AddTDigestKernels(TDigestInit, NumericTypes(), func.get());
  return func;
}

}  // namespace

void RegisterScalarAggregateTDigest(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(AddTDigestAggKernels()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
