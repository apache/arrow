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

// Vector kernels for pairwise computation

#include <memory>
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {

template <typename InputType>
Result<std::shared_ptr<DataType>> GetDiffOutputType(
    const std::shared_ptr<arrow::DataType>& type) {
  std::shared_ptr<DataType> output_type;
  if constexpr (is_timestamp_type<InputType>::value) {  // timestamp -> duration with same
                                                        // time unit
    const auto* real_type = checked_cast<const TimestampType*>(type.get());
    return std::make_shared<DurationType>(real_type->unit());
  } else if constexpr (is_time_type<InputType>::value) {  // time -> duration with same
                                                          // time unit
    const auto* real_type = checked_cast<const InputType*>(type.get());
    return std::make_shared<DurationType>(real_type->unit());
  } else if constexpr (is_date_type<InputType>::value) {  // date -> duration
    if constexpr (InputType::type_id == Type::DATE32) {   // date32 -> second
      return duration(TimeUnit::SECOND);
    } else {  // date64 -> millisecond
      return duration(TimeUnit::MILLI);
    }
  } else if constexpr (is_decimal_type<InputType>::value) {  // decimal -> decimal with
                                                             // precision + 1
    const auto* real_type = checked_cast<const InputType*>(type.get());
    if constexpr (InputType::type_id == Type::DECIMAL128) {
      return Decimal128Type::Make(real_type->precision() + 1, real_type->scale());
    } else {
      return Decimal256Type::Make(real_type->precision() + 1, real_type->scale());
    }
  } else {
    return type;
  }
}

/// A generic pairwise implementation that can be reused by different Ops.
template <typename InputType, typename OutputType, typename Op>
Status PairwiseKernelImpl(const ArraySpan& input, int64_t periods,
                          const std::shared_ptr<DataType>& output_type,
                          std::shared_ptr<ArrayData>* result) {
  typename TypeTraits<OutputType>::BuilderType builder(output_type,
                                                       default_memory_pool());
  RETURN_NOT_OK(builder.Reserve(input.length));

  Status status;
  auto valid_func = [&](typename GetViewType<InputType>::T left,
                        typename GetViewType<InputType>::T right) {
    auto result = Op::template Call<typename GetOutputType<OutputType>::T>(
        nullptr, left, right, &status);
    builder.UnsafeAppend(result);
  };
  auto null_func = [&]() { builder.UnsafeAppendNull(); };

  if (periods > 0) {
    periods = std::min(periods, input.length);
    RETURN_NOT_OK(builder.AppendNulls(periods));
    ArraySpan left(input);
    left.SetSlice(periods, input.length - periods);
    ArraySpan right(input);
    right.SetSlice(0, input.length - periods);
    VisitTwoArrayValuesInline<InputType, InputType>(left, right, valid_func, null_func);
    RETURN_NOT_OK(status);
  } else {
    periods = std::max(periods, -input.length);
    ArraySpan left(input);
    left.SetSlice(0, input.length + periods);
    ArraySpan right(input);
    right.SetSlice(-periods, input.length + periods);
    VisitTwoArrayValuesInline<InputType, InputType>(left, right, valid_func, null_func);
    RETURN_NOT_OK(status);
    RETURN_NOT_OK(builder.AppendNulls(-periods));
  }
  RETURN_NOT_OK(builder.FinishInternal(result));
  return Status::OK();
}

template <typename InputType, typename OutputType, typename Op>
Status PairwiseDiffKernel(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const PairwiseOptions& options = OptionsWrapper<PairwiseOptions>::Get(ctx);
  std::shared_ptr<ArrayData> result;
  auto input = batch[0].array;
  ARROW_ASSIGN_OR_RAISE(auto output_type,
                        GetDiffOutputType<InputType>(input.type->GetSharedPtr()));
  RETURN_NOT_OK((PairwiseKernelImpl<InputType, OutputType, Op>(
      batch[0].array, options.periods, output_type, &result)));

  out->value = std::move(result);
  return Status::OK();
}

const FunctionDoc pairwise_diff_doc(
    "Compute first order difference of an array",
    ("Computes the first order difference of an array, It internally calls \n"
     "the scalar function \"subtract\" to compute \n differences, so its \n"
     "behavior and supported types are the same as \n"
     "\"subtract\". The period can be specified in :struct:`PairwiseOptions`.\n"
     "\n"
     "Results will wrap around on integer overflow. Use function \n"
     "\"pairwise_diff_checked\" if you want overflow to return an error."),
    {"input"}, "PairwiseOptions");

const FunctionDoc pairwise_diff_checked_doc(
    "Compute first order difference of an array",
    ("Computes the first order difference of an array, It internally calls \n"
     "the scalar function \"subtract_checked\" (or the checked variant) to compute \n"
     "differences, so its behavior and supported types are the same as \n"
     "\"subtract_checked\". The period can be specified in :struct:`PairwiseOptions`.\n"
     "\n"
     "This function returns an error on overflow. For a variant that doesn't \n"
     "fail on overflow, use function \"pairwise_diff\"."),
    {"input"}, "PairwiseOptions");

const PairwiseOptions* GetDefaultPairwiseOptions() {
  static const auto kDefaultPairwiseOptions = PairwiseOptions::Defaults();
  return &kDefaultPairwiseOptions;
}

struct PairwiseKernelData {
  InputType input;
  OutputType output;
  ArrayKernelExec exec;
};

template <typename Op, typename OpDate32>
void RegisterPairwiseDiffKernels(std::string_view func_name, const FunctionDoc& doc,
                                 FunctionRegistry* registry) {
  std::vector<PairwiseKernelData> pairwise_diff_kernels = {
      {int8(), int8(), PairwiseDiffKernel<Int8Type, Int8Type, Op>},
      {uint8(), uint8(), PairwiseDiffKernel<UInt8Type, UInt8Type, Op>},
      {int16(), int16(), PairwiseDiffKernel<Int16Type, Int16Type, Op>},
      {uint16(), uint16(), PairwiseDiffKernel<UInt16Type, UInt16Type, Op>},
      {int32(), int32(), PairwiseDiffKernel<Int32Type, Int32Type, Op>},
      {uint32(), uint32(), PairwiseDiffKernel<UInt32Type, UInt32Type, Op>},
      {int64(), int64(), PairwiseDiffKernel<Int64Type, Int64Type, Op>},
      {uint64(), uint64(), PairwiseDiffKernel<UInt64Type, UInt64Type, Op>},
      {float32(), float32(), PairwiseDiffKernel<FloatType, FloatType, Op>},
      {float64(), float64(), PairwiseDiffKernel<DoubleType, DoubleType, Op>},
  };

  auto decimal_resolver = [](KernelContext*,
                             const std::vector<TypeHolder>& types) -> Result<TypeHolder> {
    // Subtract increase decimal precision by one
    DCHECK(is_decimal(types[0].id()));
    auto decimal_type = checked_cast<const DecimalType*>(types[0].type);
    return decimal(decimal_type->precision() + 1, decimal_type->scale());
  };

  pairwise_diff_kernels.emplace_back(
      PairwiseKernelData{Type::DECIMAL128, OutputType(decimal_resolver),
                         PairwiseDiffKernel<Decimal128Type, Decimal128Type, Op>});
  pairwise_diff_kernels.emplace_back(
      PairwiseKernelData{Type::DECIMAL256, OutputType(decimal_resolver),
                         PairwiseDiffKernel<Decimal256Type, Decimal256Type, Op>});

  auto identity_resolver =
      [](KernelContext*, const std::vector<TypeHolder>& types) -> Result<TypeHolder> {
    return types[0];
  };

  // timestamp -> duration
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    OutputType out_type(duration(unit));
    auto exec = PairwiseDiffKernel<TimestampType, DurationType, Op>;
    pairwise_diff_kernels.emplace_back(PairwiseKernelData{in_type, out_type, exec});
  }

  // duration -> duration
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::DurationTypeUnit(unit));
    OutputType out_type(identity_resolver);
    auto exec = PairwiseDiffKernel<DurationType, DurationType, Op>;
    pairwise_diff_kernels.emplace_back(
        PairwiseKernelData{in_type, out_type, std::move(exec)});
  }

  // time32 -> duration
  for (auto unit : {TimeUnit::SECOND, TimeUnit::MILLI}) {
    InputType in_type(match::Time32TypeUnit(unit));
    OutputType out_type(duration(unit));
    auto exec = PairwiseDiffKernel<Time32Type, DurationType, Op>;
    pairwise_diff_kernels.emplace_back(
        PairwiseKernelData{in_type, out_type, std::move(exec)});
  }

  // time64 -> duration
  for (auto unit : {TimeUnit::MICRO, TimeUnit::NANO}) {
    InputType in_type(match::Time64TypeUnit(unit));
    OutputType out_type(duration(unit));
    auto exec = PairwiseDiffKernel<Time64Type, DurationType, Op>;
    pairwise_diff_kernels.emplace_back(
        PairwiseKernelData{in_type, out_type, std::move(exec)});
  }

  // date32 -> duration(TimeUnit::SECOND)
  {
    InputType in_type(date32());
    OutputType out_type(duration(TimeUnit::SECOND));
    auto exec = PairwiseDiffKernel<Date32Type, DurationType, OpDate32>;
    pairwise_diff_kernels.emplace_back(
        PairwiseKernelData{in_type, out_type, std::move(exec)});
  }

  // date64 -> duration(TimeUnit::MILLI)
  {
    InputType in_type(date64());
    OutputType out_type(duration(TimeUnit::MILLI));
    auto exec = PairwiseDiffKernel<Date64Type, DurationType, Op>;
    pairwise_diff_kernels.emplace_back(
        PairwiseKernelData{in_type, out_type, std::move(exec)});
  }

  VectorKernel base_kernel;
  base_kernel.can_execute_chunkwise = false;
  base_kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  base_kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  base_kernel.init = OptionsWrapper<PairwiseOptions>::Init;
  auto func = std::make_shared<VectorFunction>(std::string(func_name), Arity::Unary(),
                                               doc, GetDefaultPairwiseOptions());

  for (const auto& kernel_data : pairwise_diff_kernels) {
    base_kernel.signature =
        KernelSignature::Make({kernel_data.input}, kernel_data.output);
    base_kernel.exec = kernel_data.exec;
    DCHECK_OK(func->AddKernel(base_kernel));
  }

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void RegisterVectorPairwise(FunctionRegistry* registry) {
  RegisterPairwiseDiffKernels<Subtract, SubtractDate32>("pairwise_diff",
                                                        pairwise_diff_doc, registry);
  RegisterPairwiseDiffKernels<SubtractChecked, SubtractCheckedDate32>(
      "pairwise_diff_checked", pairwise_diff_checked_doc, registry);
}

}  // namespace arrow::compute::internal
