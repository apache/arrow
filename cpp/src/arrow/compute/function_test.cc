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

#include "arrow/compute/function.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

TEST(FunctionOptions, Equality) {
  std::vector<std::shared_ptr<FunctionOptions>> options;
  options.emplace_back(new ScalarAggregateOptions());
  options.emplace_back(new ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1));
  options.emplace_back(new CountOptions());
  options.emplace_back(new CountOptions(CountOptions::ALL));
  options.emplace_back(new ModeOptions());
  options.emplace_back(new ModeOptions(/*n=*/2));
  options.emplace_back(new VarianceOptions());
  options.emplace_back(new VarianceOptions(/*ddof=*/2));
  options.emplace_back(new QuantileOptions());
  options.emplace_back(
      new QuantileOptions(/*q=*/0.75, QuantileOptions::Interpolation::MIDPOINT));
  options.emplace_back(new TDigestOptions());
  options.emplace_back(
      new TDigestOptions(/*q=*/0.75, /*delta=*/50, /*buffer_size=*/1024));
  options.emplace_back(new IndexOptions(ScalarFromJSON(int64(), "16")));
  options.emplace_back(new IndexOptions(ScalarFromJSON(boolean(), "true")));
  options.emplace_back(new IndexOptions(ScalarFromJSON(boolean(), "null")));
  options.emplace_back(new ArithmeticOptions());
  options.emplace_back(new ArithmeticOptions(/*check_overflow=*/true));
  options.emplace_back(new RoundOptions());
  options.emplace_back(
      new RoundOptions(/*ndigits=*/2, /*round_mode=*/RoundMode::TOWARDS_INFINITY));
  options.emplace_back(new RoundTemporalOptions());
  options.emplace_back(new RoundTemporalOptions(
      /*multiple=*/2,
      /*unit=*/CalendarUnit::WEEK, /*week_starts_monday*/ true));
  options.emplace_back(new RoundToMultipleOptions());
  options.emplace_back(new RoundToMultipleOptions(
      /*multiple=*/100, /*round_mode=*/RoundMode::TOWARDS_INFINITY));
  options.emplace_back(new ElementWiseAggregateOptions());
  options.emplace_back(new ElementWiseAggregateOptions(/*skip_nulls=*/false));
  options.emplace_back(new JoinOptions());
  options.emplace_back(new JoinOptions(JoinOptions::REPLACE, "replacement"));
  options.emplace_back(new MatchSubstringOptions("pattern"));
  options.emplace_back(new MatchSubstringOptions("pattern", /*ignore_case=*/true));
  options.emplace_back(new SplitOptions());
  options.emplace_back(new SplitOptions(/*max_splits=*/2, /*reverse=*/true));
  options.emplace_back(new SplitPatternOptions("pattern"));
  options.emplace_back(
      new SplitPatternOptions("pattern", /*max_splits=*/2, /*reverse=*/true));
  options.emplace_back(new ReplaceSubstringOptions("pattern", "replacement"));
  options.emplace_back(
      new ReplaceSubstringOptions("pattern", "replacement", /*max_replacements=*/2));
  options.emplace_back(new ReplaceSliceOptions(0, 1, "foo"));
  options.emplace_back(new ReplaceSliceOptions(1, -1, "bar"));
  options.emplace_back(new ExtractRegexOptions("pattern"));
  options.emplace_back(new ExtractRegexOptions("pattern2"));
  options.emplace_back(new SetLookupOptions(ArrayFromJSON(int64(), "[1, 2, 3, 4]")));
  options.emplace_back(new SetLookupOptions(ArrayFromJSON(boolean(), "[true, false]")));
  options.emplace_back(new StrptimeOptions("%Y", TimeUnit::type::MILLI, true));
  options.emplace_back(new StrptimeOptions("%Y", TimeUnit::type::NANO));
  options.emplace_back(new StrftimeOptions("%Y-%m-%dT%H:%M:%SZ", "C"));
#ifndef _WIN32
  options.emplace_back(new AssumeTimezoneOptions(
      "Europe/Amsterdam", AssumeTimezoneOptions::Ambiguous::AMBIGUOUS_RAISE,
      AssumeTimezoneOptions::Nonexistent::NONEXISTENT_RAISE));
#endif
  options.emplace_back(new PadOptions(5, " "));
  options.emplace_back(new PadOptions(10, "A"));
  options.emplace_back(new TrimOptions(" "));
  options.emplace_back(new TrimOptions("abc"));
  options.emplace_back(new SliceOptions(/*start=*/1));
  options.emplace_back(new SliceOptions(/*start=*/1, /*stop=*/-5, /*step=*/-2));
  // N.B. we never actually use field_nullability or field_metadata in Arrow
  options.emplace_back(new MakeStructOptions({"col1"}, {true}, {}));
  options.emplace_back(new MakeStructOptions({"col1"}, {false}, {}));
  options.emplace_back(
      new MakeStructOptions({"col1"}, {false}, {key_value_metadata({{"key", "val"}})}));
  options.emplace_back(new DayOfWeekOptions(false, 1));
  options.emplace_back(new WeekOptions(true, false, false));
  options.emplace_back(new CastOptions(CastOptions::Safe(boolean())));
  options.emplace_back(new CastOptions(CastOptions::Unsafe(int64())));
  options.emplace_back(new FilterOptions());
  options.emplace_back(
      new FilterOptions(FilterOptions::NullSelectionBehavior::EMIT_NULL));
  options.emplace_back(new TakeOptions());
  options.emplace_back(new TakeOptions(/*boundscheck=*/false));
  options.emplace_back(new DictionaryEncodeOptions());
  options.emplace_back(
      new DictionaryEncodeOptions(DictionaryEncodeOptions::NullEncodingBehavior::ENCODE));
  options.emplace_back(new ArraySortOptions());
  options.emplace_back(new ArraySortOptions(SortOrder::Descending));
  options.emplace_back(new SortOptions());
  options.emplace_back(new SortOptions({SortKey("key", SortOrder::Ascending)}));
  options.emplace_back(new SortOptions(
      {SortKey("key", SortOrder::Descending), SortKey("value", SortOrder::Descending)}));
  options.emplace_back(new PartitionNthOptions(/*pivot=*/0));
  options.emplace_back(new PartitionNthOptions(/*pivot=*/42));
  options.emplace_back(new SelectKOptions(0, {}));
  options.emplace_back(new SelectKOptions(5, {{SortKey("key", SortOrder::Ascending)}}));
  options.emplace_back(new Utf8NormalizeOptions());
  options.emplace_back(new Utf8NormalizeOptions(Utf8NormalizeOptions::NFD));

  for (size_t i = 0; i < options.size(); i++) {
    const size_t prev_i = i == 0 ? options.size() - 1 : i - 1;
    const FunctionOptions& cur = *options[i];
    const FunctionOptions& prev = *options[prev_i];
    SCOPED_TRACE(cur.type_name());
    SCOPED_TRACE(cur.ToString());
    ASSERT_EQ(cur, cur);
    ASSERT_NE(cur, prev);
    ASSERT_NE(prev, cur);
    ASSERT_NE("", cur.ToString());

    ASSERT_OK_AND_ASSIGN(auto serialized, cur.Serialize());
    const auto* type_name = cur.type_name();
    ASSERT_OK_AND_ASSIGN(
        auto deserialized,
        FunctionOptions::Deserialize(std::string(type_name, std::strlen(type_name)),
                                     *serialized));
    ASSERT_TRUE(cur.Equals(*deserialized));
  }
}

struct ExecSpan;

TEST(Arity, Basics) {
  auto nullary = Arity::Nullary();
  ASSERT_EQ(0, nullary.num_args);
  ASSERT_FALSE(nullary.is_varargs);

  auto unary = Arity::Unary();
  ASSERT_EQ(1, unary.num_args);

  auto binary = Arity::Binary();
  ASSERT_EQ(2, binary.num_args);

  auto ternary = Arity::Ternary();
  ASSERT_EQ(3, ternary.num_args);

  auto varargs = Arity::VarArgs();
  ASSERT_EQ(0, varargs.num_args);
  ASSERT_TRUE(varargs.is_varargs);

  auto varargs2 = Arity::VarArgs(2);
  ASSERT_EQ(2, varargs2.num_args);
  ASSERT_TRUE(varargs2.is_varargs);
}

TEST(ScalarFunction, Basics) {
  ScalarFunction func("scalar_test", Arity::Binary(), /*doc=*/FunctionDoc::Empty());
  ScalarFunction varargs_func("varargs_test", Arity::VarArgs(1),
                              /*doc=*/FunctionDoc::Empty());

  ASSERT_EQ("scalar_test", func.name());
  ASSERT_EQ(2, func.arity().num_args);
  ASSERT_FALSE(func.arity().is_varargs);
  ASSERT_EQ(Function::SCALAR, func.kind());

  ASSERT_EQ("varargs_test", varargs_func.name());
  ASSERT_EQ(1, varargs_func.arity().num_args);
  ASSERT_TRUE(varargs_func.arity().is_varargs);
  ASSERT_EQ(Function::SCALAR, varargs_func.kind());
}

TEST(VectorFunction, Basics) {
  VectorFunction func("vector_test", Arity::Binary(), /*doc=*/FunctionDoc::Empty());
  VectorFunction varargs_func("varargs_test", Arity::VarArgs(1),
                              /*doc=*/FunctionDoc::Empty());

  ASSERT_EQ("vector_test", func.name());
  ASSERT_EQ(2, func.arity().num_args);
  ASSERT_FALSE(func.arity().is_varargs);
  ASSERT_EQ(Function::VECTOR, func.kind());

  ASSERT_EQ("varargs_test", varargs_func.name());
  ASSERT_EQ(1, varargs_func.arity().num_args);
  ASSERT_TRUE(varargs_func.arity().is_varargs);
  ASSERT_EQ(Function::VECTOR, varargs_func.kind());
}

auto ExecNYI = [](KernelContext* ctx, const ExecSpan& args, ExecResult* out) {
  return Status::NotImplemented("NYI");
};

template <typename FunctionType, typename ExecType>
void CheckAddDispatch(FunctionType* func, ExecType exec) {
  using KernelType = typename FunctionType::KernelType;

  ASSERT_EQ(0, func->num_kernels());
  ASSERT_EQ(0, func->kernels().size());

  std::vector<InputType> in_types1 = {int32(), int32()};
  OutputType out_type1 = int32();

  ASSERT_OK(func->AddKernel(in_types1, out_type1, exec));
  ASSERT_OK(func->AddKernel({int32(), int8()}, int32(), exec));

  // Duplicate sig is okay
  ASSERT_OK(func->AddKernel(in_types1, out_type1, exec));

  // Add a kernel
  KernelType kernel({float64(), float64()}, float64(), exec);
  ASSERT_OK(func->AddKernel(kernel));

  ASSERT_EQ(4, func->num_kernels());
  ASSERT_EQ(4, func->kernels().size());

  // Try adding some invalid kernels
  ASSERT_RAISES(Invalid, func->AddKernel({}, int32(), exec));
  ASSERT_RAISES(Invalid, func->AddKernel({int32()}, int32(), exec));
  ASSERT_RAISES(Invalid, func->AddKernel({int8(), int8(), int8()}, int32(), exec));

  // Add valid and invalid kernel using kernel struct directly
  KernelType valid_kernel({boolean(), boolean()}, boolean(), exec);
  ASSERT_OK(func->AddKernel(valid_kernel));

  KernelType invalid_kernel({boolean()}, boolean(), exec);
  ASSERT_RAISES(Invalid, func->AddKernel(invalid_kernel));

  ASSERT_OK_AND_ASSIGN(const Kernel* dispatched, func->DispatchExact({int32(), int32()}));
  KernelSignature expected_sig(in_types1, out_type1);
  ASSERT_TRUE(dispatched->signature->Equals(expected_sig));

  // No kernel available
  ASSERT_RAISES(NotImplemented, func->DispatchExact({utf8(), utf8()}));

  // Wrong arity
  ASSERT_RAISES(Invalid, func->DispatchExact({}));
  ASSERT_RAISES(Invalid, func->DispatchExact({int32(), int32(), int32()}));
}

TEST(ScalarVectorFunction, DispatchExact) {
  ScalarFunction func1("scalar_test", Arity::Binary(), /*doc=*/FunctionDoc::Empty());
  VectorFunction func2("vector_test", Arity::Binary(), /*doc=*/FunctionDoc::Empty());

  CheckAddDispatch(&func1, ExecNYI);

  // ARROW-16576: will migrate later to new span-based kernel exec API
  CheckAddDispatch(&func2, ExecNYI);
}

TEST(ArrayFunction, VarArgs) {
  ScalarFunction va_func("va_test", Arity::VarArgs(1), /*doc=*/FunctionDoc::Empty());

  std::vector<InputType> va_args = {int8()};

  ASSERT_OK(va_func.AddKernel(va_args, int8(), ExecNYI));

  // No input type passed
  ASSERT_RAISES(Invalid, va_func.AddKernel({}, int8(), ExecNYI));

  // VarArgs function expect a single input type
  ASSERT_RAISES(Invalid, va_func.AddKernel({int8(), int8()}, int8(), ExecNYI));

  // Invalid sig
  ScalarKernel non_va_kernel(std::make_shared<KernelSignature>(va_args, int8()), ExecNYI);
  ASSERT_RAISES(Invalid, va_func.AddKernel(non_va_kernel));

  std::vector<TypeHolder> args = {int8(), int8(), int8()};
  ASSERT_OK_AND_ASSIGN(const Kernel* kernel, va_func.DispatchExact(args));
  ASSERT_TRUE(kernel->signature->MatchesInputs(args));

  // No dispatch possible because args incompatible
  args[2] = int32();
  ASSERT_RAISES(NotImplemented, va_func.DispatchExact(args));
}

TEST(ScalarAggregateFunction, Basics) {
  ScalarAggregateFunction func("agg_test", Arity::Unary(), /*doc=*/FunctionDoc::Empty());

  ASSERT_EQ("agg_test", func.name());
  ASSERT_EQ(1, func.arity().num_args);
  ASSERT_FALSE(func.arity().is_varargs);
  ASSERT_EQ(Function::SCALAR_AGGREGATE, func.kind());
}

Result<std::unique_ptr<KernelState>> NoopInit(KernelContext*, const KernelInitArgs&) {
  return nullptr;
}

Status NoopConsume(KernelContext*, const ExecSpan&) { return Status::OK(); }
Status NoopMerge(KernelContext*, KernelState&&, KernelState*) { return Status::OK(); }
Status NoopFinalize(KernelContext*, Datum*) { return Status::OK(); }

TEST(ScalarAggregateFunction, DispatchExact) {
  ScalarAggregateFunction func("agg_test", Arity::Unary(), FunctionDoc::Empty());

  std::vector<InputType> in_args = {int8()};
  ScalarAggregateKernel kernel(std::move(in_args), int64(), NoopInit, NoopConsume,
                               NoopMerge, NoopFinalize, /*ordered=*/false);
  ASSERT_OK(func.AddKernel(kernel));

  in_args = {float64()};
  kernel.signature = std::make_shared<KernelSignature>(in_args, float64());
  ASSERT_OK(func.AddKernel(kernel));

  ASSERT_EQ(2, func.num_kernels());
  ASSERT_EQ(2, func.kernels().size());
  ASSERT_TRUE(func.kernels()[1]->signature->Equals(*kernel.signature));

  // Invalid arity
  in_args = {};
  kernel.signature = std::make_shared<KernelSignature>(in_args, float64());
  ASSERT_RAISES(Invalid, func.AddKernel(kernel));

  in_args = {float32(), float64()};
  kernel.signature = std::make_shared<KernelSignature>(in_args, float64());
  ASSERT_RAISES(Invalid, func.AddKernel(kernel));

  std::vector<TypeHolder> dispatch_args = {int8()};
  ASSERT_OK_AND_ASSIGN(const Kernel* selected_kernel, func.DispatchExact(dispatch_args));
  ASSERT_EQ(func.kernels()[0], selected_kernel);
  ASSERT_TRUE(selected_kernel->signature->MatchesInputs(dispatch_args));

  // Didn't qualify the float64() kernel so this actually dispatches (even
  // though that may not be what you want)
  dispatch_args[0] = {float64()};
  ASSERT_OK_AND_ASSIGN(selected_kernel, func.DispatchExact(dispatch_args));
  ASSERT_TRUE(selected_kernel->signature->MatchesInputs(dispatch_args));
}

namespace {

struct TestFunctionOptions : public FunctionOptions {
  TestFunctionOptions();

  static const char* kTypeName;

  int value;
};

static auto kTestFunctionOptionsType =
    internal::GetFunctionOptionsType<TestFunctionOptions>();

TestFunctionOptions::TestFunctionOptions() : FunctionOptions(kTestFunctionOptionsType) {}

const char* TestFunctionOptions::kTypeName = "test_options";

}  // namespace

TEST(FunctionExecutor, Basics) {
  VectorFunction func("vector_test", Arity::Binary(), /*doc=*/FunctionDoc::Empty());
  int init_calls = 0;
  int expected_optval = 0;
  ExecContext exec_ctx;
  TestFunctionOptions options;
  options.value = 1;
  auto init =
      [&](KernelContext* kernel_ctx,
          const KernelInitArgs& init_args) -> Result<std::unique_ptr<KernelState>> {
    if (&exec_ctx != kernel_ctx->exec_context()) {
      return Status::Invalid("expected exec context not found in kernel context");
    }
    if (init_args.options != nullptr) {
      const auto* test_opts = checked_cast<const TestFunctionOptions*>(init_args.options);
      if (test_opts->value != expected_optval) {
        return Status::Invalid("bad options value");
      }
    }
    if (&options != init_args.options) {
      return Status::Invalid("expected options not found in kernel init args");
    }
    ++init_calls;
    return nullptr;
  };
  auto exec = [](KernelContext* ctx, const ExecSpan& args, ExecResult* out) -> Status {
    [&]() {  // gtest ASSERT macros require a void function
      ASSERT_EQ(2, args.values.size());
      const int32_t* vals[2];
      for (size_t i = 0; i < 2; i++) {
        ASSERT_TRUE(args.values[i].is_array());
        const ArraySpan& array = args.values[i].array;
        ASSERT_EQ(array.type->id(), Type::INT32);
        vals[i] = array.GetValues<int32_t>(1);
      }
      ASSERT_TRUE(out->is_array_data());
      auto out_data = out->array_data();
      Int32Builder builder;
      for (int64_t i = 0; i < args.length; i++) {
        ASSERT_OK(builder.Append(vals[0][i] + vals[1][i]));
      }
      ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());
      *out_data.get() = *array->data();
    }();
    return Status::OK();
  };
  std::vector<InputType> in_types = {int32(), int32()};
  OutputType out_type = int32();
  ASSERT_OK(func.AddKernel(in_types, out_type, exec, init));

  ASSERT_OK_AND_ASSIGN(const Kernel* dispatched, func.DispatchExact({int32(), int32()}));
  ASSERT_EQ(exec, static_cast<const ScalarKernel*>(dispatched)->exec);
  std::vector<TypeHolder> inputs = {int32(), int32()};

  ASSERT_OK_AND_ASSIGN(auto func_exec, func.GetBestExecutor(inputs));
  ASSERT_EQ(0, init_calls);
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("options not found"),
                                  func_exec->Init(nullptr, &exec_ctx));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("bad options value"),
                                  func_exec->Init(&options, &exec_ctx));
  ExecContext other_exec_ctx;
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("exec context not found"),
                                  func_exec->Init(&options, &other_exec_ctx));

  ArrayVector arrays = {ArrayFromJSON(int32(), "[1]"), ArrayFromJSON(int32(), "[2]"),
                        ArrayFromJSON(int32(), "[3]"), ArrayFromJSON(int32(), "[4]")};
  ArrayVector expected = {ArrayFromJSON(int32(), "[3]"), ArrayFromJSON(int32(), "[5]"),
                          ArrayFromJSON(int32(), "[7]")};
  for (int n = 1; n <= 3; n++) {
    expected_optval = options.value = n;
    ASSERT_OK(func_exec->Init(&options, &exec_ctx));
    ASSERT_EQ(n, init_calls);
    for (int32_t i = 1; i <= 3; i++) {
      std::vector<Datum> values = {arrays[i - 1], arrays[i]};
      ASSERT_OK_AND_ASSIGN(auto result, func_exec->Execute(values, 1));
      ASSERT_TRUE(result.is_array());
      auto actual = result.make_array();
      AssertArraysEqual(*expected[i - 1], *actual);
    }
  }
}

}  // namespace compute
}  // namespace arrow
