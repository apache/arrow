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

using arrow::internal::checked_pointer_cast;
using arrow::internal::VisitSetBitRunsVoid;

using QuantileState = internal::OptionsWrapper<QuantileOptions>;

// output is at some exact input data point, not interpolated
bool IsExactDataPoint(const QuantileOptions& options) {
  // some interpolation methods return exact input data point
  if (options.interpolation == QuantileOptions::LOWER ||
      options.interpolation == QuantileOptions::HIGHER ||
      options.interpolation == QuantileOptions::NEAREST) {
    return true;
  }
  // return exact value if quantiles only contain 0 or 1 (follow numpy behaviour)
  for (auto q : options.q) {
    if (q != 0 && q != 1) {
      return false;
    }
  }
  return true;
}

Result<ValueDescr> ResolveOutputFromOptions(KernelContext* ctx,
                                            const std::vector<ValueDescr>& args) {
  const QuantileOptions& options = QuantileState::Get(ctx);
  if (IsExactDataPoint(options)) {
    return ValueDescr::Array(args[0].type);
  } else {
    return ValueDescr::Array(float64());
  }
}

template <typename Dummy, typename InType>
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
    // out type depends on interpolation method
    const bool is_exact = IsExactDataPoint(options);
    std::shared_ptr<DataType> out_type;
    if (is_exact) {
      out_type = TypeTraits<InType>::type_singleton();
    } else {
      out_type = float64();
    }
    auto out_data = ArrayData::Make(out_type, out_length, 0);
    out_data->buffers.resize(2, nullptr);

    // calculate quantiles
    if (out_length > 0) {
      const auto out_bit_width = checked_pointer_cast<NumberType>(out_type)->bit_width();
      KERNEL_ASSIGN_OR_RAISE(out_data->buffers[1], ctx,
                             ctx->Allocate(out_length * out_bit_width / 8));

      if (is_exact) {
        CType* out_buffer = out_data->template GetMutableValues<CType>(1);
        for (int64_t i = 0; i < out_length; ++i) {
          out_buffer[i] =
              GetQuantileExact(in_buffer, options.q[i], options.interpolation);
        }
      } else {
        double* out_buffer = out_data->template GetMutableValues<double>(1);
        for (int64_t i = 0; i < out_length; ++i) {
          out_buffer[i] =
              GetQuantileInterp(in_buffer, options.q[i], options.interpolation);
        }
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

  // partition input array and return sorted index-th element
  static CType PartitionArray(std::vector<CType>& in, uint64_t index) {
    DCHECK_LT(index, in.size());
    std::nth_element(in.begin(), in.begin() + index, in.end());
    return in[index];
  }

  // return quantile located exactly at some input data point
  static CType GetQuantileExact(std::vector<CType>& in, double q,
                                enum QuantileOptions::Interpolation interpolation) {
    if (q == 0) {
      return *std::min_element(in.cbegin(), in.cend());
    } else if (q == 1) {
      return *std::max_element(in.cbegin(), in.cend());
    }

    const double index = (in.size() - 1) * q;
    const uint64_t index_lower = static_cast<uint64_t>(index);
    const double fraction = index - index_lower;

    // convert NEAREST interpolation method to LOWER or HIGHER
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

    if (interpolation == QuantileOptions::LOWER || fraction == 0) {
      return PartitionArray(in, index_lower);
    } else if (interpolation == QuantileOptions::HIGHER) {
      return PartitionArray(in, index_lower + 1);
    } else {
      DCHECK(false);
      return 0;
    }
  }

  // return quantile interpolated from adjacent input data points
  static double GetQuantileInterp(std::vector<CType>& in, double q,
                                  enum QuantileOptions::Interpolation interpolation) {
    const double index = (in.size() - 1) * q;
    const uint64_t index_lower = static_cast<uint64_t>(index);
    const double fraction = index - index_lower;

    const double lower_value = static_cast<double>(PartitionArray(in, index_lower));
    if (fraction == 0 || lower_value == -INFINITY) {
      return lower_value;
    }

    // input is already partitioned around lower value
    // higher value must be the minimal of values after lower value index
    const double higher_value =
        static_cast<double>(*std::min_element(in.cbegin() + index_lower + 1, in.cend()));
    if (higher_value == INFINITY) {
      return INFINITY;
    }

    if (interpolation == QuantileOptions::LINEAR) {
      // more stable than naive linear interpolation
      return fraction * higher_value + (1 - fraction) * lower_value;
    } else if (interpolation == QuantileOptions::MIDPOINT) {
      return lower_value / 2 + higher_value / 2;
    } else {
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
    base.signature = KernelSignature::Make({InputType::Array(ty)},
                                           OutputType(ResolveOutputFromOptions));
    // output type is determined at runtime, set template argument to nulltype
    base.exec = GenerateNumeric<QuantileExecutor, NullType>(*ty);
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
