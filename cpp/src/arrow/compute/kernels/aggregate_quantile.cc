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
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_run_reader.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

using arrow::internal::VisitSetBitRunsVoid;

// We need to preserve the options
using QuantileState = internal::OptionsWrapper<QuantileOptions>;

template <typename OutType, typename InType>
struct QuantileExecutor {
  using CType = typename InType::c_type;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // validate arguments
    if (ctx->state() == nullptr) {
      ctx->SetStatus(Status::Invalid("Quantile requires QuantileOptions"));
      return;
    }

    const QuantileOptions& options = QuantileState::Get(ctx);
    if (options.q.empty()) {
      ctx->SetStatus(Status::Invalid("Requires quantile argument"));
      return;
    }
    for (double q : options.q) {
      if (q < 0 || q > 1) {
        ctx->SetStatus(Status::Invalid("Quantile must be between 0 and 1"));
        return;
      }
    }

    // copy all chunks to a buffer, ignore nulls and nans
    std::vector<CType> in_buffer;

    const Datum& datum = batch[0];
    const int64_t in_length = datum.length() - datum.null_count();
    if (in_length > 0) {
      in_buffer.resize(in_length);

      int64_t index = 0;
      for (const auto& array : datum.chunks()) {
        index += CopyArray(in_buffer.data() + index, *array);
      }
      DCHECK_EQ(index, in_length);

      // drop nan
      if (is_floating_type<InType>::value) {
        const auto& nan_begins = std::partition(in_buffer.begin(), in_buffer.end(),
                                                [](CType v) { return v == v; });
        in_buffer.resize(nan_begins - in_buffer.cbegin());
      }
    }

    // prepare out array
    int64_t out_length = options.q.size();
    if (in_buffer.empty()) {
      out_length = 0;  // input is empty or only contains null and nan, return empty array
    }
    auto out_data = ArrayData::Make(float64(), out_length, 0);
    out_data->buffers.resize(2, nullptr);

    // calculate quantiles
    if (out_length > 0) {
      KERNEL_ASSIGN_OR_RAISE(out_data->buffers[1], ctx,
                             ctx->Allocate(out_length * sizeof(double)));
      double* out_buffer = out_data->template GetMutableValues<double>(1);

      for (int64_t i = 0; i < out_length; ++i) {
        out_buffer[i] = GetQuantile(in_buffer, options.q[i], options.interpolation);
      }
    }

    *out = Datum(std::move(out_data));
  }

  static int64_t CopyArray(CType* buffer, const Array& array) {
    const int64_t n = array.length() - array.null_count();
    if (n > 0) {
      int64_t index = 0;
      const ArrayData& data = *array.data();
      const CType* values = data.GetValues<CType>(1);
      VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                          [&](int64_t pos, int64_t len) {
                            memcpy(buffer + index, values + pos, len * sizeof(CType));
                            index += len;
                          });
      DCHECK_EQ(index, n);
    }
    return n;
  }

  static double GetQuantile(std::vector<CType>& in, double q,
                            enum QuantileOptions::Interpolation interpolation) {
    DCHECK_GE(in.size(), 1);
    const double index = (in.size() - 1) * q;
    const uint64_t index_lower = static_cast<uint64_t>(index);
    const double fraction = index - index_lower;

    // convert nearest interpolation method to lower or higher
    if (interpolation == QuantileOptions::NEAREST) {
      if (fraction < 0.5) {
        interpolation = QuantileOptions::LOWER;
      } else if (fraction > 0.5) {
        interpolation = QuantileOptions::HIGHER;
      } else {
        // round 0.5 to nearest even number, similar to numpy.around
        interpolation =
            (index_lower & 1) ? QuantileOptions::HIGHER : QuantileOptions::LOWER;
      }
    }

    // only lower data point is required
    if (fraction == 0 || interpolation == QuantileOptions::LOWER) {
      // calculate lower data point
      std::nth_element(in.begin(), in.begin() + index_lower, in.end());
      return static_cast<double>(in[index_lower]);
    }

    // calculate higher data point
    const uint64_t index_higher = index_lower + 1;
    DCHECK_LT(index_higher, in.size());
    std::nth_element(in.begin(), in.begin() + index_higher, in.end());
    const double higher_value = static_cast<double>(in[index_higher]);

    // only higher data point is required
    if (interpolation == QuantileOptions::HIGHER) {
      return higher_value;
    }

    // do interpolation

    // calculate lower data point
    // input is already partitioned around higher data point
    // lower data point must be the maximal value left of higher index
    const double lower_value =
        static_cast<double>(*std::max_element(in.begin(), in.begin() + index_higher));

    // check infinity
    if (is_floating_type<InType>::value) {
      if (lower_value == -INFINITY) {
        return -INFINITY;
      } else if (higher_value == INFINITY) {
        return INFINITY;
      }
    }

    switch (interpolation) {
      case QuantileOptions::LINEAR:
        // more stable than naive linear interpolation
        return fraction * higher_value + (1 - fraction) * lower_value;
      case QuantileOptions::MIDPOINT:
        return lower_value / 2 + higher_value / 2;
      default:
        // should never reach here
        DCHECK(false);
        return NAN;
    }
  }
};

void AddQuantileKernels(VectorFunction* func) {
  VectorKernel base;
  base.init = QuantileState::Init;
  base.can_execute_chunkwise = false;
  base.output_chunked = false;

  for (const auto& ty : NumericTypes()) {
    base.signature =
        KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Array(float64()));
    base.exec = GenerateNumeric<QuantileExecutor, DoubleType>(*ty);
    DCHECK_OK(func->AddKernel(base));
  }
}

const FunctionDoc quantile_doc{
    "Return an array of quantiles of a numeric array or chunked array",
    ("By default, 0.5 quantile (median) is returned.\n"
     "If quantile lies between two data points, an interpolated value is "
     "returned based on selected interpolation method.\n"
     "Nulls and NaNs are ignored. Return empty array if no valid data points."),
    {"array"},
    "QuantileOptions"};

}  // namespace

void RegisterScalarAggregateQuantile(FunctionRegistry* registry) {
  auto func = std::make_shared<VectorFunction>("quantile", Arity::Unary(), &quantile_doc);
  AddQuantileKernels(func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
