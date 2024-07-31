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
#include <numeric>
#include <vector>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/stl_allocator.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

using QuantileState = internal::OptionsWrapper<QuantileOptions>;

// output is at some input data point, not interpolated
bool IsDataPoint(const QuantileOptions& options) {
  // some interpolation methods return exact data point
  return options.interpolation == QuantileOptions::LOWER ||
         options.interpolation == QuantileOptions::HIGHER ||
         options.interpolation == QuantileOptions::NEAREST;
}

// quantile to exact datapoint index (IsDataPoint == true)
uint64_t QuantileToDataPoint(size_t length, double q,
                             enum QuantileOptions::Interpolation interpolation) {
  const double index = (length - 1) * q;
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

  return datapoint_index;
}

template <typename T>
double DataPointToDouble(T value, const DataType&) {
  return static_cast<double>(value);
}
double DataPointToDouble(const Decimal128& value, const DataType& ty) {
  return value.ToDouble(checked_cast<const DecimalType&>(ty).scale());
}
double DataPointToDouble(const Decimal256& value, const DataType& ty) {
  return value.ToDouble(checked_cast<const DecimalType&>(ty).scale());
}

// copy and nth_element approach, large memory footprint
template <typename InType>
struct SortQuantiler {
  using CType = typename TypeTraits<InType>::CType;
  using Allocator = arrow::stl::allocator<CType>;

  Status ComputeQuantile(KernelContext* ctx, const QuantileOptions& options,
                         const std::shared_ptr<DataType>& type,
                         std::vector<CType, Allocator>& in_buffer, ExecResult* out) {
    // prepare out array
    // out type depends on options
    const bool is_datapoint = IsDataPoint(options);
    const std::shared_ptr<DataType> out_type = is_datapoint ? type : float64();
    int64_t out_length = options.q.size();
    if (in_buffer.empty()) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> result,
                            MakeArrayOfNull(out_type, out_length, ctx->memory_pool()));
      out->value = result->data();
      return Status::OK();
    }
    auto out_data = ArrayData::Make(out_type, out_length, 0);
    out_data->buffers.resize(2, nullptr);

    // calculate quantiles
    if (out_length > 0) {
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[1],
                            ctx->Allocate(out_length * out_type->byte_width()));

      // find quantiles in descending order
      std::vector<int64_t> q_indices(out_length);
      std::iota(q_indices.begin(), q_indices.end(), 0);
      std::sort(q_indices.begin(), q_indices.end(),
                [&options](int64_t left_index, int64_t right_index) {
                  return options.q[right_index] < options.q[left_index];
                });

      // input array is partitioned around data point at `last_index` (pivot)
      // for next quantile which is smaller, we only consider inputs left of the pivot
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
              in_buffer, &last_index, options.q[q_index], options.interpolation, *type);
        }
      }
    }

    out->value = std::move(out_data);
    return Status::OK();
  }

  template <typename Container>
  void FillBuffer(const QuantileOptions& options, const Container& container,
                  int64_t length, int64_t null_count,
                  std::vector<CType, Allocator>* in_buffer) {
    int64_t in_length = 0;
    if ((!options.skip_nulls && null_count > 0) ||
        (length - null_count < options.min_count)) {
      in_length = 0;
    } else {
      in_length = length - null_count;
    }

    if (in_length > 0) {
      in_buffer->resize(in_length);
      CopyNonNullValues(container, in_buffer->data());

      // drop nan
      if (is_floating_type<InType>::value) {
        const auto& it = std::remove_if(in_buffer->begin(), in_buffer->end(),
                                        [](CType v) { return v != v; });
        in_buffer->resize(it - in_buffer->begin());
      }
    }
  }

  Status Exec(KernelContext* ctx, const ArraySpan& values, ExecResult* out) {
    const QuantileOptions& options = QuantileState::Get(ctx);

    // copy all chunks to a buffer, ignore nulls and nans
    std::vector<CType, Allocator> in_buffer(Allocator(ctx->memory_pool()));
    FillBuffer(options, values, values.length, values.GetNullCount(), &in_buffer);
    return ComputeQuantile(ctx, options, values.type->GetSharedPtr(), in_buffer, out);
  }

  Status Exec(KernelContext* ctx, const ChunkedArray& values, Datum* out) {
    const QuantileOptions& options = QuantileState::Get(ctx);

    // copy all chunks to a buffer, ignore nulls and nans
    std::vector<CType, Allocator> in_buffer(Allocator(ctx->memory_pool()));
    FillBuffer(options, values, values.length(), values.null_count(), &in_buffer);
    ExecResult result;
    RETURN_NOT_OK(ComputeQuantile(ctx, options, values.type(), in_buffer, &result));
    *out = result.array_data();
    return Status::OK();
  }

  // return quantile located exactly at some input data point
  CType GetQuantileAtDataPoint(std::vector<CType, Allocator>& in, uint64_t* last_index,
                               double q,
                               enum QuantileOptions::Interpolation interpolation) {
    const uint64_t datapoint_index = QuantileToDataPoint(in.size(), q, interpolation);

    if (datapoint_index != *last_index) {
      DCHECK_LT(datapoint_index, *last_index);
      std::nth_element(in.begin(), in.begin() + datapoint_index,
                       in.begin() + *last_index);
      *last_index = datapoint_index;
    }

    return in[datapoint_index];
  }

  // return quantile interpolated from adjacent input data points
  double GetQuantileByInterp(std::vector<CType, Allocator>& in, uint64_t* last_index,
                             double q, enum QuantileOptions::Interpolation interpolation,
                             const DataType& in_type) {
    const double index = (in.size() - 1) * q;
    const uint64_t lower_index = static_cast<uint64_t>(index);
    const double fraction = index - lower_index;

    if (lower_index != *last_index) {
      DCHECK_LT(lower_index, *last_index);
      std::nth_element(in.begin(), in.begin() + lower_index, in.begin() + *last_index);
    }

    const double lower_value = DataPointToDouble(in[lower_index], in_type);
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

    const double higher_value = DataPointToDouble(in[higher_index], in_type);

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

// histogram approach with constant memory, only for integers within limited value range
template <typename InType>
struct CountQuantiler {
  using CType = typename InType::c_type;

  CType min;
  std::vector<uint64_t> counts;  // counts[i]: # of values equals i + min

  // indices to adjacent non-empty bins covering current quantile
  struct AdjacentBins {
    int left_index;
    int right_index;
    uint64_t total_count;  // accumulated counts till left_index (inclusive)
  };

  CountQuantiler(CType min, CType max) {
    uint32_t value_range = static_cast<uint32_t>(max - min) + 1;
    DCHECK_LT(value_range, 1 << 30);
    this->min = min;
    this->counts.resize(value_range, 0);
  }

  Status ComputeQuantile(KernelContext* ctx, const QuantileOptions& options,
                         int64_t in_length, ExecResult* out) {
    // prepare out array
    // out type depends on options
    const bool is_datapoint = IsDataPoint(options);
    const std::shared_ptr<DataType> out_type =
        is_datapoint ? TypeTraits<InType>::type_singleton() : float64();
    int64_t out_length = options.q.size();
    if (in_length == 0) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> result,
                            MakeArrayOfNull(out_type, out_length, ctx->memory_pool()));
      out->value = std::move(result->data());
      return Status::OK();
    }
    auto out_data = ArrayData::Make(out_type, out_length, 0);
    out_data->buffers.resize(2, nullptr);

    // calculate quantiles
    if (out_length > 0) {
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[1],
                            ctx->Allocate(out_length * out_type->byte_width()));

      // find quantiles in ascending order
      std::vector<int64_t> q_indices(out_length);
      std::iota(q_indices.begin(), q_indices.end(), 0);
      std::sort(q_indices.begin(), q_indices.end(),
                [&options](int64_t left_index, int64_t right_index) {
                  return options.q[left_index] < options.q[right_index];
                });

      AdjacentBins bins{0, 0, this->counts[0]};
      if (is_datapoint) {
        CType* out_buffer = out_data->template GetMutableValues<CType>(1);
        for (int64_t i = 0; i < out_length; ++i) {
          const int64_t q_index = q_indices[i];
          out_buffer[q_index] = GetQuantileAtDataPoint(
              in_length, &bins, options.q[q_index], options.interpolation);
        }
      } else {
        double* out_buffer = out_data->template GetMutableValues<double>(1);
        for (int64_t i = 0; i < out_length; ++i) {
          const int64_t q_index = q_indices[i];
          out_buffer[q_index] = GetQuantileByInterp(in_length, &bins, options.q[q_index],
                                                    options.interpolation);
        }
      }
    }
    out->value = std::move(out_data);
    return Status::OK();
  }

  Status Exec(KernelContext* ctx, const ArraySpan& values, ExecResult* out) {
    const QuantileOptions& options = QuantileState::Get(ctx);

    // count values in all chunks, ignore nulls
    int64_t in_length = 0;
    if ((options.skip_nulls || (!options.skip_nulls && values.GetNullCount() == 0)) &&
        (values.length - values.GetNullCount() >= options.min_count)) {
      in_length = CountValues<CType>(values, this->min, this->counts.data());
    }

    return ComputeQuantile(ctx, options, in_length, out);
  }

  Status Exec(KernelContext* ctx, const ChunkedArray& values, Datum* out) {
    const QuantileOptions& options = QuantileState::Get(ctx);

    // count values in all chunks, ignore nulls
    int64_t in_length = 0;
    if ((options.skip_nulls || (!options.skip_nulls && values.null_count() == 0)) &&
        (values.length() - values.null_count() >= options.min_count)) {
      in_length = CountValues<CType>(values, this->min, this->counts.data());
    }

    ExecResult result;
    RETURN_NOT_OK(ComputeQuantile(ctx, options, in_length, &result));
    *out = result.array_data();
    return Status::OK();
  }

  // return quantile located exactly at some input data point
  CType GetQuantileAtDataPoint(int64_t in_length, AdjacentBins* bins, double q,
                               enum QuantileOptions::Interpolation interpolation) {
    const uint64_t datapoint_index = QuantileToDataPoint(in_length, q, interpolation);
    while (datapoint_index >= bins->total_count &&
           static_cast<size_t>(bins->left_index) < this->counts.size() - 1) {
      ++bins->left_index;
      bins->total_count += this->counts[bins->left_index];
    }
    DCHECK_LT(datapoint_index, bins->total_count);
    return static_cast<CType>(bins->left_index + this->min);
  }

  // return quantile interpolated from adjacent input data points
  double GetQuantileByInterp(int64_t in_length, AdjacentBins* bins, double q,
                             enum QuantileOptions::Interpolation interpolation) {
    const double index = (in_length - 1) * q;
    const uint64_t index_floor = static_cast<uint64_t>(index);
    const double fraction = index - index_floor;

    while (index_floor >= bins->total_count &&
           static_cast<size_t>(bins->left_index) < this->counts.size() - 1) {
      ++bins->left_index;
      bins->total_count += this->counts[bins->left_index];
    }
    DCHECK_LT(index_floor, bins->total_count);
    const double lower_value = static_cast<double>(bins->left_index + this->min);

    // quantile lies in this bin, no interpolation needed
    if (index <= bins->total_count - 1) {
      return lower_value;
    }

    // quantile lies across two bins, locate next bin if not already done
    DCHECK_EQ(index_floor, bins->total_count - 1);
    if (bins->right_index <= bins->left_index) {
      bins->right_index = bins->left_index + 1;
      while (static_cast<size_t>(bins->right_index) < this->counts.size() - 1 &&
             this->counts[bins->right_index] == 0) {
        ++bins->right_index;
      }
    }
    DCHECK_LT(static_cast<size_t>(bins->right_index), this->counts.size());
    DCHECK_GT(this->counts[bins->right_index], 0);
    const double higher_value = static_cast<double>(bins->right_index + this->min);

    if (interpolation == QuantileOptions::LINEAR) {
      return fraction * higher_value + (1 - fraction) * lower_value;
    } else if (interpolation == QuantileOptions::MIDPOINT) {
      return lower_value / 2 + higher_value / 2;
    } else {
      DCHECK(false);
      return NAN;
    }
  }
};

// histogram or 'copy & nth_element' approach per value range and size, only for integers
template <typename InType>
struct CountOrSortQuantiler {
  using CType = typename InType::c_type;

  // cross point to benefit from histogram approach
  // parameters estimated from ad-hoc benchmarks manually
  static constexpr int kMinArraySize = 65536;
  static constexpr int kMaxValueRange = 65536;

  Status Exec(KernelContext* ctx, const ArraySpan& values, ExecResult* out) {
    if (values.length - values.GetNullCount() >= kMinArraySize) {
      CType min, max;
      std::tie(min, max) = GetMinMax<CType>(values);
      if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <= kMaxValueRange) {
        return CountQuantiler<InType>(min, max).Exec(ctx, values, out);
      }
    }
    return SortQuantiler<InType>().Exec(ctx, values, out);
  }

  Status Exec(KernelContext* ctx, const ChunkedArray& values, Datum* out) {
    if (values.length() - values.null_count() >= kMinArraySize) {
      CType min, max;
      std::tie(min, max) = GetMinMax<CType>(values);
      if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <= kMaxValueRange) {
        return CountQuantiler<InType>(min, max).Exec(ctx, values, out);
      }
    }
    return SortQuantiler<InType>().Exec(ctx, values, out);
  }
};

template <typename InType, typename Enable = void>
struct ExactQuantiler;

template <>
struct ExactQuantiler<UInt8Type> {
  CountQuantiler<UInt8Type> impl;
  ExactQuantiler() : impl(0, 255) {}
};

template <>
struct ExactQuantiler<Int8Type> {
  CountQuantiler<Int8Type> impl;
  ExactQuantiler() : impl(-128, 127) {}
};

template <typename InType>
struct ExactQuantiler<InType, enable_if_t<(is_integer_type<InType>::value &&
                                           (sizeof(typename InType::c_type) > 1))>> {
  CountOrSortQuantiler<InType> impl;
};

template <typename InType>
struct ExactQuantiler<InType, enable_if_t<is_floating_type<InType>::value>> {
  SortQuantiler<InType> impl;
};

template <typename InType>
struct ExactQuantiler<InType, enable_if_t<is_decimal_type<InType>::value>> {
  SortQuantiler<InType> impl;
};

Status CheckQuantileOptions(KernelContext* ctx) {
  if (ctx->state() == nullptr) {
    return Status::Invalid("Quantile requires QuantileOptions");
  }

  const QuantileOptions& options = QuantileState::Get(ctx);
  if (options.q.empty()) {
    return Status::Invalid("Requires quantile argument");
  }
  for (double q : options.q) {
    if (q < 0 || q > 1) {
      return Status::Invalid("Quantile must be between 0 and 1");
    }
  }
  return Status::OK();
}

template <typename OutputTypeUnused, typename InType>
struct QuantileExecutor {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    RETURN_NOT_OK(CheckQuantileOptions(ctx));
    return ExactQuantiler<InType>().impl.Exec(ctx, batch[0].array, out);
  }
};

template <typename OutputTypeUnused, typename InType>
struct QuantileExecutorChunked {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    RETURN_NOT_OK(CheckQuantileOptions(ctx));
    return ExactQuantiler<InType>().impl.Exec(ctx, *batch[0].chunked_array(), out);
  }
};

Result<TypeHolder> ResolveOutput(KernelContext* ctx,
                                 const std::vector<TypeHolder>& types) {
  const QuantileOptions& options = QuantileState::Get(ctx);
  if (IsDataPoint(options)) {
    return types[0];
  } else {
    return float64();
  }
}

void AddQuantileKernels(VectorFunction* func) {
  VectorKernel base;
  base.init = QuantileState::Init;
  base.can_execute_chunkwise = false;
  base.output_chunked = false;

  for (const auto& ty : NumericTypes()) {
    base.signature = KernelSignature::Make({InputType(ty)}, OutputType(ResolveOutput));
    // output type is determined at runtime, set template argument to nulltype
    base.exec = GenerateNumeric<QuantileExecutor, NullType>(*ty);
    base.exec_chunked =
        GenerateNumeric<QuantileExecutorChunked, NullType, VectorKernel::ChunkedExec>(
            *ty);
    DCHECK_OK(func->AddKernel(base));
  }
  {
    base.signature =
        KernelSignature::Make({InputType(Type::DECIMAL128)}, OutputType(ResolveOutput));
    base.exec = QuantileExecutor<NullType, Decimal128Type>::Exec;
    base.exec_chunked = QuantileExecutorChunked<NullType, Decimal128Type>::Exec;
    DCHECK_OK(func->AddKernel(base));
  }
  {
    base.signature =
        KernelSignature::Make({InputType(Type::DECIMAL256)}, OutputType(ResolveOutput));
    base.exec = QuantileExecutor<NullType, Decimal256Type>::Exec;
    base.exec_chunked = QuantileExecutorChunked<NullType, Decimal256Type>::Exec;
    DCHECK_OK(func->AddKernel(base));
  }
}

const FunctionDoc quantile_doc{
    "Compute an array of quantiles of a numeric array or chunked array",
    ("By default, 0.5 quantile (median) is returned.\n"
     "If quantile lies between two data points, an interpolated value is\n"
     "returned based on selected interpolation method.\n"
     "Nulls and NaNs are ignored.\n"
     "An array of nulls is returned if there is no valid data point."),
    {"array"},
    "QuantileOptions"};

}  // namespace

void RegisterScalarAggregateQuantile(FunctionRegistry* registry) {
  static QuantileOptions default_options;
  auto func = std::make_shared<VectorFunction>("quantile", Arity::Unary(), quantile_doc,
                                               &default_options);
  AddQuantileKernels(func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
