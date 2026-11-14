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

#include "benchmark/benchmark.h"

#include "arrow/compute/exec_internal.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/testing/generator.h"
#include "arrow/util/logging.h"

namespace arrow::compute {

namespace {

// A trivial kernel that just keeps the CPU busy for a specified number of iterations per
// input row. Has both regular and selective variants. Used to benchmark the overhead of
// the execution framework.

struct SpinOptions : public FunctionOptions {
  explicit SpinOptions(int64_t count = 0);
  static constexpr char kTypeName[] = "SpinOptions";
  static SpinOptions Defaults() { return SpinOptions(); }
  int64_t count = 0;
};

static auto kSpinOptionsType = internal::GetFunctionOptionsType<SpinOptions>(
    arrow::internal::DataMember("count", &SpinOptions::count));

SpinOptions::SpinOptions(int64_t count)
    : FunctionOptions(kSpinOptionsType), count(count) {}

const SpinOptions* GetDefaultSpinOptions() {
  static const auto kDefaultSpinOptions = SpinOptions::Defaults();
  return &kDefaultSpinOptions;
}

using SpinState = internal::OptionsWrapper<SpinOptions>;

inline void Spin(volatile int64_t count) {
  while (count-- > 0) {
    // Do nothing, just burn CPU cycles.
  }
}

Status SpinExec(KernelContext* ctx, const ExecSpan& span, ExecResult* out) {
  ARROW_CHECK_EQ(span.num_values(), 1);
  const auto& arg = span[0];
  ARROW_CHECK(arg.is_array());

  int64_t count = SpinState::Get(ctx).count;
  for (int64_t i = 0; i < arg.length(); ++i) {
    Spin(count);
  }
  *out->array_data_mutable() = *arg.array.ToArrayData();
  return Status::OK();
}

Status SpinSelectiveExec(KernelContext* ctx, const ExecSpan& span,
                         const SelectionVectorSpan& selection_span, ExecResult* out) {
  ARROW_CHECK_EQ(span.num_values(), 1);
  const auto& arg = span[0];
  ARROW_CHECK(arg.is_array());

  int64_t count = SpinState::Get(ctx).count;
  detail::VisitSelectionVectorSpanInline(selection_span, [&](int64_t i) { Spin(count); });
  *out->array_data_mutable() = *arg.array.ToArrayData();
  return Status::OK();
}

Status RegisterSpinFunction() {
  auto registry = GetFunctionRegistry();

  if (registry->CanAddFunctionOptionsType(kSpinOptionsType).ok()) {
    RETURN_NOT_OK(registry->AddFunctionOptionsType(kSpinOptionsType));
  }

  auto register_spin_function = [&](std::string name, ArrayKernelExec exec,
                                    ArrayKernelSelectiveExec selective_exec) {
    auto func = std::make_shared<ScalarFunction>(
        std::move(name), Arity::Unary(), FunctionDoc::Empty(), GetDefaultSpinOptions());
    ScalarKernel kernel({InputType::Any()}, internal::FirstType, exec, selective_exec,
                        SpinState::Init);
    kernel.can_write_into_slices = false;
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    RETURN_NOT_OK(func->AddKernel(kernel));
    if (registry->CanAddFunction(func, /*allow_overwrite=*/false).ok()) {
      RETURN_NOT_OK(registry->AddFunction(std::move(func)));
    }
    return Status::OK();
  };

  // Register two variants, one with selective exec and one without.
  RETURN_NOT_OK(register_spin_function("spin_selective", SpinExec, SpinSelectiveExec));
  RETURN_NOT_OK(register_spin_function("spin", SpinExec, /*selective_exec=*/nullptr));

  return Status::OK();
}

std::shared_ptr<SelectionVector> MakeSelectionVectorTo(int64_t length) {
  auto res = gen::Step<int32_t>()->Generate(length);
  ARROW_CHECK_OK(res.status());
  auto arr = res.ValueUnsafe();
  return std::make_shared<SelectionVector>(*arr);
}

}  // namespace

void BenchmarkExec(benchmark::State& state, std::string spin_function,
                   int64_t kernel_intensity, std::shared_ptr<Array> input,
                   std::shared_ptr<SelectionVector> selection = nullptr) {
  static auto registered = RegisterSpinFunction();
  ARROW_CHECK_OK(registered);

  auto expr =
      call(std::move(spin_function), {field_ref(0)}, SpinOptions(kernel_intensity));
  auto bound = expr.Bind(*schema({field("", input->type())})).ValueOrDie();
  auto length = input->length();
  auto batch = ExecBatch{{std::move(input)}, length, std::move(selection)};

  for (auto _ : state) {
    ARROW_CHECK_OK(ExecuteScalarExpression(bound, batch).status());
  }

  state.SetItemsProcessed(state.iterations() * length);
}

// Baseline: Run the spin kernel without selection vector.
static void BM_ExecBaseline(benchmark::State& state) {
  const int64_t kernel_intensity = state.range(0);
  const int64_t num_rows = state.range(1);

  auto input = ConstantArrayGenerator::Int32(num_rows, 0);
  BenchmarkExec(state, "spin", kernel_intensity, std::move(input));
}

// Selective: Run the spin kernel with a selection vector, either sparsely or densely,
// depending on whether the spin kernel has a selective exec implementation.
static void BM_ExecSelective(benchmark::State& state, std::string spin_function) {
  const int64_t selectivity = state.range(0);
  ARROW_CHECK(selectivity >= 0 && selectivity <= 100);
  const int64_t kernel_intensity = state.range(1);
  const int64_t num_rows = state.range(2);

  auto input = ConstantArrayGenerator::Int32(num_rows, 0);
  auto selection =
      MakeSelectionVectorTo(static_cast<int64_t>(num_rows * selectivity / 100));
  BenchmarkExec(state, std::move(spin_function), kernel_intensity, std::move(input),
                std::move(selection));
}

const char* kSelectivityArgName = "selectivity";
const std::vector<int64_t> kSelectivityArg{0, 20, 50, 100};
const char* kKernelIntensityArgName = "kernel_intensity";
const std::vector<int64_t> kKernelIntensityArg = benchmark::CreateDenseRange(0, 100, 20);
const char* kNumRowsArgName = "num_rows";
const std::vector<int64_t> kNumRowsArg{4096};

BENCHMARK(BM_ExecBaseline)
    ->ArgNames({kKernelIntensityArgName, kNumRowsArgName})
    ->ArgsProduct({kKernelIntensityArg, kNumRowsArg});

BENCHMARK_CAPTURE(BM_ExecSelective, sparse, "spin_selective")
    ->ArgNames({kSelectivityArgName, kKernelIntensityArgName, kNumRowsArgName})
    ->ArgsProduct({kSelectivityArg, kKernelIntensityArg, kNumRowsArg});

BENCHMARK_CAPTURE(BM_ExecSelective, dense, "spin")
    ->ArgNames({kSelectivityArgName, kKernelIntensityArgName, kNumRowsArgName})
    ->ArgsProduct({kSelectivityArg, kKernelIntensityArg, kNumRowsArg});

}  // namespace arrow::compute
