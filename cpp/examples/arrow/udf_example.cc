// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// Demonstrate registering a user-defined Arrow compute function outside of the Arrow
// source tree

namespace cp = ::arrow::compute;

template <typename TYPE,
          typename = typename std::enable_if<arrow::is_number_type<TYPE>::value |
                                             arrow::is_boolean_type<TYPE>::value |
                                             arrow::is_temporal_type<TYPE>::value>::type>
arrow::Result<std::shared_ptr<arrow::Array>> GetArrayDataSample(
    const std::vector<typename TYPE::c_type>& values) {
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  return builder.Finish();
}

const cp::FunctionDoc func_doc{
    "User-defined-function usage to demonstrate registering an out-of-tree function",
    "returns x + y + z",
    {"x", "y", "z"},
    "UDFOptions"};

arrow::Status SampleFunction(cp::KernelContext* ctx, const cp::ExecSpan& batch,
                             cp::ExecResult* out) {
  // return x + y + z
  const int64_t* x = batch[0].array.GetValues<int64_t>(1);
  const int64_t* y = batch[1].array.GetValues<int64_t>(1);
  const int64_t* z = batch[2].array.GetValues<int64_t>(1);
  int64_t* out_values = out->array_span()->GetValues<int64_t>(1);
  for (int64_t i = 0; i < batch.length; ++i) {
    *out_values++ = *x++ + *y++ + *z++;
  }
  return arrow::Status::OK();
}

arrow::Status Execute() {
  const std::string name = "add_three";
  auto func = std::make_shared<cp::ScalarFunction>(name, cp::Arity::Ternary(), func_doc);
  cp::ScalarKernel kernel({arrow::int64(), arrow::int64(), arrow::int64()},
                          arrow::int64(), SampleFunction);

  kernel.mem_allocation = cp::MemAllocation::PREALLOCATE;
  kernel.null_handling = cp::NullHandling::INTERSECTION;

  ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));

  auto registry = cp::GetFunctionRegistry();
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

  ARROW_ASSIGN_OR_RAISE(auto x, GetArrayDataSample<arrow::Int64Type>({1, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(auto y, GetArrayDataSample<arrow::Int64Type>({4, 5, 6}));
  ARROW_ASSIGN_OR_RAISE(auto z, GetArrayDataSample<arrow::Int64Type>({7, 8, 9}));

  ARROW_ASSIGN_OR_RAISE(auto res, cp::CallFunction(name, {x, y, z}));
  auto res_array = res.make_array();
  std::cout << "Scalar UDF Result" << std::endl;
  std::cout << res_array->ToString() << std::endl;
  return arrow::Status::OK();
}

// User-defined Scalar Aggregate Function Example
struct ScalarUdfAggregator : public cp::KernelState {
  virtual arrow::Status Consume(cp::KernelContext* ctx, const cp::ExecSpan& batch) = 0;
  virtual arrow::Status MergeFrom(cp::KernelContext* ctx, cp::KernelState&& src) = 0;
  virtual arrow::Status Finalize(cp::KernelContext* ctx, arrow::Datum* out) = 0;
};

class SimpleCountFunctionOptionsType : public cp::FunctionOptionsType {
  const char* type_name() const override { return "SimpleCountFunctionOptionsType"; }
  std::string Stringify(const cp::FunctionOptions&) const override {
    return "SimpleCountFunctionOptionsType";
  }
  bool Compare(const cp::FunctionOptions&, const cp::FunctionOptions&) const override {
    return true;
  }
  std::unique_ptr<cp::FunctionOptions> Copy(const cp::FunctionOptions&) const override;
};

cp::FunctionOptionsType* GetSimpleCountFunctionOptionsType() {
  static SimpleCountFunctionOptionsType options_type;
  return &options_type;
}

class SimpleCountOptions : public cp::FunctionOptions {
 public:
  SimpleCountOptions() : cp::FunctionOptions(GetSimpleCountFunctionOptionsType()) {}
  static constexpr char const kTypeName[] = "SimpleCountOptions";
  static SimpleCountOptions Defaults() { return SimpleCountOptions{}; }
};

std::unique_ptr<cp::FunctionOptions> SimpleCountFunctionOptionsType::Copy(
    const cp::FunctionOptions&) const {
  return std::make_unique<SimpleCountOptions>();
}

const cp::FunctionDoc simple_count_doc{
    "SimpleCount the number of null / non-null values",
    ("By default, only non-null values are counted.\n"
     "This can be changed through SimpleCountOptions."),
    {"array"},
    "SimpleCountOptions"};

// Need Python interface for this Class
struct SimpleCountImpl : public ScalarUdfAggregator {
  explicit SimpleCountImpl(SimpleCountOptions options) : options(std::move(options)) {}

  arrow::Status Consume(cp::KernelContext*, const cp::ExecSpan& batch) override {
    if (batch[0].is_array()) {
      const arrow::ArraySpan& input = batch[0].array;
      const int64_t nulls = input.GetNullCount();
      this->non_nulls += input.length - nulls;
    } else {
      const arrow::Scalar& input = *batch[0].scalar;
      this->non_nulls += input.is_valid * batch.length;
    }
    return arrow::Status::OK();
  }

  arrow::Status MergeFrom(cp::KernelContext*, cp::KernelState&& src) override {
    const auto& other_state = arrow::internal::checked_cast<const SimpleCountImpl&>(src);
    this->non_nulls += other_state.non_nulls;
    return arrow::Status::OK();
  }

  arrow::Status Finalize(cp::KernelContext* ctx, arrow::Datum* out) override {
    const auto& state =
        arrow::internal::checked_cast<const SimpleCountImpl&>(*ctx->state());
    *out = arrow::Datum(state.non_nulls);
    return arrow::Status::OK();
  }

  SimpleCountOptions options;
  int64_t non_nulls = 0;
};

// TODO: need a Python interface for this function
arrow::Result<std::unique_ptr<cp::KernelState>> SimpleCountInit(
    cp::KernelContext*, const cp::KernelInitArgs& args) {
  return std::make_unique<SimpleCountImpl>(
      static_cast<const SimpleCountOptions&>(*args.options));
}

arrow::Status AggregateUdfConsume(cp::KernelContext* ctx, const cp::ExecSpan& batch) {
  return arrow::internal::checked_cast<ScalarUdfAggregator*>(ctx->state())
      ->Consume(ctx, batch);
}

arrow::Status AggregateUdfMerge(cp::KernelContext* ctx, cp::KernelState&& src,
                                cp::KernelState* dst) {
  return arrow::internal::checked_cast<ScalarUdfAggregator*>(dst)->MergeFrom(
      ctx, std::move(src));
}

arrow::Status AggregateUdfFinalize(cp::KernelContext* ctx, arrow::Datum* out) {
  return arrow::internal::checked_cast<ScalarUdfAggregator*>(ctx->state())
      ->Finalize(ctx, out);
}

arrow::Status AddAggKernel(std::shared_ptr<cp::KernelSignature> sig, cp::KernelInit init,
                           cp::ScalarAggregateFunction* func) {
  cp::ScalarAggregateKernel kernel(std::move(sig), std::move(init), AggregateUdfConsume,
                                   AggregateUdfMerge, AggregateUdfFinalize);
  ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  return arrow::Status::OK();
}

arrow::Status ExecuteAggregate() {
  auto registry = cp::GetFunctionRegistry();
  static auto default_scalar_aggregate_options = cp::ScalarAggregateOptions::Defaults();
  static auto default_count_options = SimpleCountOptions::Defaults();
  const std::string name = "simple_count";
  auto func = std::make_shared<cp::ScalarAggregateFunction>(
      name, cp::Arity::Unary(), simple_count_doc, &default_count_options);

  // Takes any input, outputs int64 scalar
  ARROW_RETURN_NOT_OK(
      AddAggKernel(cp::KernelSignature::Make({arrow::int64()}, arrow::int64()),
                   SimpleCountInit, func.get()));
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

  ARROW_ASSIGN_OR_RAISE(auto x, GetArrayDataSample<arrow::Int64Type>({1, 2, 3, 4, 5, 6}));

  ARROW_ASSIGN_OR_RAISE(auto res, cp::CallFunction(name, {x}));
  auto res_scalar = res.scalar();
  std::cout << "Aggregate UDF Result" << std::endl;
  std::cout << res_scalar->ToString() << std::endl;

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  std::cout << "Sample Scalar UDF Execution" << std::endl;
  auto s1 = Execute();
  if (!s1.ok()) {
    std::cerr << "Error occurred : " << s1.message() << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << "Sample Aggregate UDF Execution" << std::endl;
  auto s2 = ExecuteAggregate();
  if (!s2.ok()) {
    std::cerr << "Error occurred : " << s2.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
