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
#include "arrow/stl_allocator.h"
#include "arrow/util/bit_run_reader.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

using arrow::internal::checked_pointer_cast;
using arrow::internal::VisitSetBitRunsVoid;

using QuantileState = internal::OptionsWrapper<QuantileOptions>;

// output is at some input data point, not interpolated
bool IsDataPoint(const QuantileOptions& options) {
  // some interpolation methods return exact data point
  return options.interpolation == QuantileOptions::LOWER ||
         options.interpolation == QuantileOptions::HIGHER ||
         options.interpolation == QuantileOptions::NEAREST;
}

template <typename Dummy, typename InType>
struct QuantileExecutor {
  using CType = typename InType::c_type;
  using Allocator = arrow::stl::allocator<CType>;

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
    std::vector<CType, Allocator> in_buffer(Allocator(ctx->memory_pool()));

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
        const auto& it = std::remove_if(in_buffer.begin(), in_buffer.end(),
                                        [](CType v) { return v != v; });
        in_buffer.resize(it - in_buffer.begin());
      }
    }

    // prepare out array
    int64_t out_length = options.q.size();
    if (in_buffer.empty()) {
      out_length = 0;  // input is empty or only contains null and nan, return empty array
    }
    // out type depends on options
    const bool is_datapoint = IsDataPoint(options);
    std::shared_ptr<DataType> out_type;
    if (is_datapoint) {
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

      // find quantiles in descending order
      std::vector<int64_t> q_indices(out_length);
      std::iota(q_indices.begin(), q_indices.end(), 0);
      std::sort(q_indices.begin(), q_indices.end(),
                [&options](int64_t left_index, int64_t right_index) {
                  return options.q[right_index] < options.q[left_index];
                });

      // input array is partitioned around data point at `last_index` (pivot)
      // for next quatile which is smaller, we only consider inputs left of the pivot
      uint64_t last_index = in_buffer.size();
      if (is_datapoint) {
        CType* out_buffer = out_data->template GetMutableValues<CType>(1);
        for (int64_t i = 0; i < out_length; ++i) {
          const int64_t q_index = q_indices[i];
          out_buffer[q_index] = GetQuantileAtDataPoint(
              in_buffer, &last_index, options.q[q_index], options.interpolation);
        }
      } else {
        double* out_buffer = out_data->template GetMutableValues<double>(1);
        for (int64_t i = 0; i < out_length; ++i) {
          const int64_t q_index = q_indices[i];
          out_buffer[q_index] = GetQuantileByInterp(
              in_buffer, &last_index, options.q[q_index], options.interpolation);
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

  // return quantile located exactly at some input data point
  static CType GetQuantileAtDataPoint(std::vector<CType, Allocator>& in,
                                      uint64_t* last_index, double q,
                                      enum QuantileOptions::Interpolation interpolation) {
    const double index = (in.size() - 1) * q;
    uint64_t datapoint_index = static_cast<uint64_t>(index);
    const double fraction = index - datapoint_index;

    if (interpolation == QuantileOptions::LINEAR ||
        interpolation == QuantileOptions::MIDPOINT) {
      DCHECK_EQ(fraction, 0);
    }

    // convert NEAREST interpolation method to LOWER or HIGHER
    if (interpolation == QuantileOptions::NEAREST) {
      if (fraction < 0.5) {
        interpolation = QuantileOptions::LOWER;
      } else if (fraction > 0.5) {
        interpolation = QuantileOptions::HIGHER;
      } else {
        // round 0.5 to nearest even number, similar to numpy.around
        interpolation =
            (datapoint_index & 1) ? QuantileOptions::HIGHER : QuantileOptions::LOWER;
      }
    }

    if (interpolation == QuantileOptions::HIGHER && fraction != 0) {
      ++datapoint_index;
    }

    if (datapoint_index != *last_index) {
      DCHECK_LT(datapoint_index, *last_index);
      std::nth_element(in.begin(), in.begin() + datapoint_index,
                       in.begin() + *last_index);
      *last_index = datapoint_index;
    }

    return in[datapoint_index];
  }

  // return quantile interpolated from adjacent input data points
  static double GetQuantileByInterp(std::vector<CType, Allocator>& in,
                                    uint64_t* last_index, double q,
                                    enum QuantileOptions::Interpolation interpolation) {
    const double index = (in.size() - 1) * q;
    const uint64_t lower_index = static_cast<uint64_t>(index);
    const double fraction = index - lower_index;

    if (lower_index != *last_index) {
      DCHECK_LT(lower_index, *last_index);
      std::nth_element(in.begin(), in.begin() + lower_index, in.begin() + *last_index);
    }

    const double lower_value = static_cast<double>(in[lower_index]);
    if (fraction == 0) {
      *last_index = lower_index;
      return lower_value;
    }

    const uint64_t higher_index = lower_index + 1;
    DCHECK_LT(higher_index, in.size());
    if (lower_index != *last_index && higher_index != *last_index) {
      DCHECK_LT(higher_index, *last_index);
      // higher value must be the minimal value after lower_index
      auto min = std::min_element(in.begin() + higher_index, in.begin() + *last_index);
      std::iter_swap(in.begin() + higher_index, min);
    }
    *last_index = lower_index;

    const double higher_value = static_cast<double>(in[higher_index]);

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

Result<ValueDescr> ResolveOutput(KernelContext* ctx,
                                 const std::vector<ValueDescr>& args) {
  const QuantileOptions& options = QuantileState::Get(ctx);
  if (IsDataPoint(options)) {
    return ValueDescr::Array(args[0].type);
  } else {
    return ValueDescr::Array(float64());
  }
}

void AddQuantileKernels(VectorFunction* func) {
  VectorKernel base;
  base.init = QuantileState::Init;
  base.can_execute_chunkwise = false;
  base.output_chunked = false;

  for (const auto& ty : NumericTypes()) {
    base.signature =
        KernelSignature::Make({InputType::Array(ty)}, OutputType(ResolveOutput));
    // output type is determined at runtime, set template argument to nulltype
    base.exec = GenerateNumeric<QuantileExecutor, NullType>(*ty);
    DCHECK_OK(func->AddKernel(base));
  }
}

const FunctionDoc quantile_doc{
    "Compute an array of quantiles of a numeric array or chunked array",
    ("By default, 0.5 quantile (median) is returned.\n"
     "If quantile lies between two data points, an interpolated value is\n"
     "returned based on selected interpolation method.\n"
     "Nulls and NaNs are ignored.\n"
     "An empty array is returned if there is no valid data point."),
    {"array"},
    "QuantileOptions"};

}  // namespace

void RegisterScalarAggregateQuantile(FunctionRegistry* registry) {
  static QuantileOptions default_options;
  auto func = std::make_shared<VectorFunction>("quantile", Arity::Unary(), &quantile_doc,
                                               &default_options);
  AddQuantileKernels(func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
