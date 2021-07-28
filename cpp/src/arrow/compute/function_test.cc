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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace compute {

TEST(FunctionOptions, Equality) {
  std::vector<std::shared_ptr<FunctionOptions>> options;
  options.emplace_back(new ScalarAggregateOptions());
  options.emplace_back(new ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1));
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
  options.emplace_back(new StrptimeOptions("%Y", TimeUnit::type::MILLI));
  options.emplace_back(new StrptimeOptions("%Y", TimeUnit::type::NANO));
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

struct ExecBatch;

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
  ScalarFunction func("scalar_test", Arity::Binary(), /*doc=*/nullptr);
  ScalarFunction varargs_func("varargs_test", Arity::VarArgs(1), /*doc=*/nullptr);

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
  VectorFunction func("vector_test", Arity::Binary(), /*doc=*/nullptr);
  VectorFunction varargs_func("varargs_test", Arity::VarArgs(1), /*doc=*/nullptr);

  ASSERT_EQ("vector_test", func.name());
  ASSERT_EQ(2, func.arity().num_args);
  ASSERT_FALSE(func.arity().is_varargs);
  ASSERT_EQ(Function::VECTOR, func.kind());

  ASSERT_EQ("varargs_test", varargs_func.name());
  ASSERT_EQ(1, varargs_func.arity().num_args);
  ASSERT_TRUE(varargs_func.arity().is_varargs);
  ASSERT_EQ(Function::VECTOR, varargs_func.kind());
}

auto ExecNYI = [](KernelContext* ctx, const ExecBatch& args, Datum* out) {
  return Status::NotImplemented("NYI");
};

template <typename FunctionType>
void CheckAddDispatch(FunctionType* func) {
  using KernelType = typename FunctionType::KernelType;

  ASSERT_EQ(0, func->num_kernels());
  ASSERT_EQ(0, func->kernels().size());

  std::vector<InputType> in_types1 = {int32(), int32()};
  OutputType out_type1 = int32();

  ASSERT_OK(func->AddKernel(in_types1, out_type1, ExecNYI));
  ASSERT_OK(func->AddKernel({int32(), int8()}, int32(), ExecNYI));

  // Duplicate sig is okay
  ASSERT_OK(func->AddKernel(in_types1, out_type1, ExecNYI));

  // Add given a descr
  KernelType descr({float64(), float64()}, float64(), ExecNYI);
  ASSERT_OK(func->AddKernel(descr));

  ASSERT_EQ(4, func->num_kernels());
  ASSERT_EQ(4, func->kernels().size());

  // Try adding some invalid kernels
  ASSERT_RAISES(Invalid, func->AddKernel({}, int32(), ExecNYI));
  ASSERT_RAISES(Invalid, func->AddKernel({int32()}, int32(), ExecNYI));
  ASSERT_RAISES(Invalid, func->AddKernel({int8(), int8(), int8()}, int32(), ExecNYI));

  // Add valid and invalid kernel using kernel struct directly
  KernelType valid_kernel({boolean(), boolean()}, boolean(), ExecNYI);
  ASSERT_OK(func->AddKernel(valid_kernel));

  KernelType invalid_kernel({boolean()}, boolean(), ExecNYI);
  ASSERT_RAISES(Invalid, func->AddKernel(invalid_kernel));

  ASSERT_OK_AND_ASSIGN(const Kernel* kernel, func->DispatchExact({int32(), int32()}));
  KernelSignature expected_sig(in_types1, out_type1);
  ASSERT_TRUE(kernel->signature->Equals(expected_sig));

  // No kernel available
  ASSERT_RAISES(NotImplemented, func->DispatchExact({utf8(), utf8()}));

  // Wrong arity
  ASSERT_RAISES(Invalid, func->DispatchExact({}));
  ASSERT_RAISES(Invalid, func->DispatchExact({int32(), int32(), int32()}));
}

TEST(ScalarVectorFunction, DispatchExact) {
  ScalarFunction func1("scalar_test", Arity::Binary(), /*doc=*/nullptr);
  VectorFunction func2("vector_test", Arity::Binary(), /*doc=*/nullptr);

  CheckAddDispatch(&func1);
  CheckAddDispatch(&func2);
}

TEST(ArrayFunction, VarArgs) {
  ScalarFunction va_func("va_test", Arity::VarArgs(1), /*doc=*/nullptr);

  std::vector<InputType> va_args = {int8()};

  ASSERT_OK(va_func.AddKernel(va_args, int8(), ExecNYI));

  // No input type passed
  ASSERT_RAISES(Invalid, va_func.AddKernel({}, int8(), ExecNYI));

  // VarArgs function expect a single input type
  ASSERT_RAISES(Invalid, va_func.AddKernel({int8(), int8()}, int8(), ExecNYI));

  // Invalid sig
  ScalarKernel non_va_kernel(std::make_shared<KernelSignature>(va_args, int8()), ExecNYI);
  ASSERT_RAISES(Invalid, va_func.AddKernel(non_va_kernel));

  std::vector<ValueDescr> args = {ValueDescr::Scalar(int8()), int8(), int8()};
  ASSERT_OK_AND_ASSIGN(const Kernel* kernel, va_func.DispatchExact(args));
  ASSERT_TRUE(kernel->signature->MatchesInputs(args));

  // No dispatch possible because args incompatible
  args[2] = int32();
  ASSERT_RAISES(NotImplemented, va_func.DispatchExact(args));
}

TEST(ScalarAggregateFunction, Basics) {
  ScalarAggregateFunction func("agg_test", Arity::Unary(), /*doc=*/nullptr);

  ASSERT_EQ("agg_test", func.name());
  ASSERT_EQ(1, func.arity().num_args);
  ASSERT_FALSE(func.arity().is_varargs);
  ASSERT_EQ(Function::SCALAR_AGGREGATE, func.kind());
}

Result<std::unique_ptr<KernelState>> NoopInit(KernelContext*, const KernelInitArgs&) {
  return nullptr;
}

Status NoopConsume(KernelContext*, const ExecBatch&) { return Status::OK(); }
Status NoopMerge(KernelContext*, const KernelState&, KernelState*) {
  return Status::OK();
}
Status NoopFinalize(KernelContext*, Datum*) { return Status::OK(); }

TEST(ScalarAggregateFunction, DispatchExact) {
  ScalarAggregateFunction func("agg_test", Arity::Unary(), /*doc=*/nullptr);

  std::vector<InputType> in_args = {ValueDescr::Array(int8())};
  ScalarAggregateKernel kernel(std::move(in_args), int64(), NoopInit, NoopConsume,
                               NoopMerge, NoopFinalize);
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

  std::vector<ValueDescr> dispatch_args = {ValueDescr::Array(int8())};
  ASSERT_OK_AND_ASSIGN(const Kernel* selected_kernel, func.DispatchExact(dispatch_args));
  ASSERT_EQ(func.kernels()[0], selected_kernel);
  ASSERT_TRUE(selected_kernel->signature->MatchesInputs(dispatch_args));

  // We declared that only arrays are accepted
  dispatch_args[0] = {ValueDescr::Scalar(int8())};
  ASSERT_RAISES(NotImplemented, func.DispatchExact(dispatch_args));

  // Didn't qualify the float64() kernel so this actually dispatches (even
  // though that may not be what you want)
  dispatch_args[0] = {ValueDescr::Scalar(float64())};
  ASSERT_OK_AND_ASSIGN(selected_kernel, func.DispatchExact(dispatch_args));
  ASSERT_TRUE(selected_kernel->signature->MatchesInputs(dispatch_args));
}

}  // namespace compute
}  // namespace arrow
