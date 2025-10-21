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

#include <functional>
#include <memory>
#include <optional>
#include <utility>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"

namespace arrow::compute::internal {

using ::arrow::internal::checked_cast;

namespace {

Status ValidateOptions(const WinsorizeOptions& options) {
  if (!(options.lower_limit >= 0 && options.lower_limit <= 1) ||
      !(options.upper_limit >= 0 && options.upper_limit <= 1)) {
    return Status::Invalid("winsorize limits must be between 0 and 1");
  }
  if (options.lower_limit > options.upper_limit) {
    return Status::Invalid(
        "winsorize upper limit must be equal or greater than lower limit");
  }
  return Status::OK();
}

using WinsorizeState = internal::OptionsWrapper<WinsorizeOptions>;

// We have a first unused template parameter for compatibility with GenerateNumeric.
template <typename Unused, typename Type>
struct Winsorize {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using CType = typename TypeTraits<Type>::CType;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = WinsorizeState::Get(ctx);
    RETURN_NOT_OK(ValidateOptions(options));
    auto data = batch.values[0].array.ToArrayData();
    ARROW_ASSIGN_OR_RAISE(auto maybe_quantiles, GetQuantileValues(ctx, data, options));
    auto out_data = out->array_data_mutable();
    if (!maybe_quantiles.has_value()) {
      // Only nulls and NaNs => return input as-is
      out_data->null_count = data->null_count.load();
      out_data->length = data->length;
      out_data->buffers = data->buffers;
      return Status::OK();
    }
    return ClipValues(*data, maybe_quantiles.value(), out_data, ctx);
  }

  static Status ExecChunked(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = WinsorizeState::Get(ctx);
    RETURN_NOT_OK(ValidateOptions(options));
    const auto& chunked_array = batch.values[0].chunked_array();
    ARROW_ASSIGN_OR_RAISE(auto maybe_quantiles,
                          GetQuantileValues(ctx, chunked_array, options));
    if (!maybe_quantiles.has_value()) {
      // Only nulls and NaNs => return input as-is
      *out = chunked_array;
      return Status::OK();
    }
    ArrayVector out_chunks;
    out_chunks.reserve(chunked_array->num_chunks());
    for (const auto& chunk : chunked_array->chunks()) {
      auto out_data = chunk->data()->Copy();
      RETURN_NOT_OK(
          ClipValues(*chunk->data(), maybe_quantiles.value(), out_data.get(), ctx));
      out_chunks.push_back(MakeArray(out_data));
    }
    return ChunkedArray::Make(std::move(out_chunks)).Value(out);
  }

  struct QuantileValues {
    CType lower_bound, upper_bound;
  };

  static Result<std::optional<QuantileValues>> GetQuantileValues(
      KernelContext* ctx, const Datum& input, const WinsorizeOptions& options) {
    // We use "nearest" to avoid the conversion of quantile values to double.
    QuantileOptions quantile_options(/*q=*/{options.lower_limit, options.upper_limit},
                                     QuantileOptions::NEAREST);
    ARROW_ASSIGN_OR_RAISE(
        auto quantile,
        CallFunction("quantile", {input}, &quantile_options, ctx->exec_context()));
    auto quantile_array = quantile.array_as<ArrayType>();
    DCHECK_EQ(quantile_array->length(), 2);
    // The quantile function outputs either all nulls or no nulls at all.
    if (quantile_array->null_count() == 2) {
      return std::nullopt;
    }
    DCHECK_EQ(quantile_array->null_count(), 0);
    return QuantileValues{CType(quantile_array->Value(0)),
                          CType(quantile_array->Value(1))};
  }

  static Status ClipValues(const ArrayData& data, QuantileValues quantiles,
                           ArrayData* out, KernelContext* ctx) {
    DCHECK_EQ(out->buffers.size(), data.buffers.size());
    out->null_count = data.null_count.load();
    out->length = data.length;
    out->buffers[0] = data.buffers[0];
    ARROW_ASSIGN_OR_RAISE(out->buffers[1], ctx->Allocate(out->length * sizeof(CType)));
    // Avoid leaving uninitialized memory under null entries
    std::memset(out->buffers[1]->mutable_data(), 0, out->length * sizeof(CType));

    const CType* in_values = data.GetValues<CType>(1);
    CType* out_values = out->GetMutableValues<CType>(1);

    auto visit = [&](int64_t position, int64_t length) {
      for (int64_t i = position; i < position + length; ++i) {
        if (in_values[i] < quantiles.lower_bound) {
          out_values[i] = quantiles.lower_bound;
        } else if (in_values[i] > quantiles.upper_bound) {
          out_values[i] = quantiles.upper_bound;
        } else {
          // NaNs also fall here
          out_values[i] = in_values[i];
        }
      }
    };
    arrow::internal::VisitSetBitRunsVoid(data.buffers[0], data.offset, data.length,
                                         visit);
    return Status::OK();
  }
};

template <typename Unused, typename Type>
struct WinsorizeChunked {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return Winsorize<Unused, Type>::ExecChunked(ctx, batch, out);
  }
};

Result<TypeHolder> ResolveWinsorizeOutput(KernelContext* ctx,
                                          const std::vector<TypeHolder>& in_types) {
  DCHECK_EQ(in_types.size(), 1);
  return in_types[0];
}

const FunctionDoc winsorize_doc(
    "Winsorize an array",
    ("This function applies a winsorization transform to the input array\n"
     "so as to reduce the influence of potential outliers.\n"
     "NaNs and nulls in the input are ignored for the purpose of computing\n"
     "the lower and upper quantiles.\n"
     "The quantile limits can be changed in WinsorizeOptions."),
    {"array"}, "WinsorizeOptions", /*options_required=*/true);

}  // namespace

void RegisterVectorStatistics(FunctionRegistry* registry) {
  static const auto default_winsorize_options = WinsorizeOptions();

  auto winsorize = std::make_shared<VectorFunction>(
      "winsorize", Arity::Unary(), winsorize_doc, &default_winsorize_options);

  VectorKernel base;
  base.init = WinsorizeState::Init;
  base.mem_allocation = MemAllocation::NO_PREALLOCATE;
  base.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  base.can_execute_chunkwise = false;
  // The variable is ill-named, but since we output a ChunkedArray ourselves,
  // the function execution logic shouldn't try to wrap it again.
  base.output_chunked = false;

  for (const auto& ty : NumericTypes()) {
    base.signature = KernelSignature::Make({ty->id()}, &ResolveWinsorizeOutput);
    base.exec = GenerateNumeric<Winsorize, /*Unused*/ void>(ty->id());
    base.exec_chunked = GenerateNumeric<WinsorizeChunked, /*Unused*/ void>(ty->id());
    DCHECK_OK(winsorize->AddKernel(base));
  }
  for (auto type_id : DecimalTypeIds()) {
    base.signature = KernelSignature::Make({type_id}, &ResolveWinsorizeOutput);
    base.exec = GenerateDecimal<Winsorize, /*Unused*/ void>(type_id);
    base.exec_chunked = GenerateDecimal<WinsorizeChunked, /*Unused*/ void>(type_id);
    DCHECK_OK(winsorize->AddKernel(base));
  }
  DCHECK_OK(registry->AddFunction(std::move(winsorize)));
}

}  // namespace arrow::compute::internal
