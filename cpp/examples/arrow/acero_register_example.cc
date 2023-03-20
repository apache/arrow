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

#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/expression.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/future.h>

#include <cstdlib>
#include <iostream>
#include <memory>

// Demonstrate registering an Arrow compute function outside of the Arrow source tree

namespace cp = ::arrow::compute;
namespace ac = ::arrow::acero;

class ExampleFunctionOptionsType : public cp::FunctionOptionsType {
  const char* type_name() const override { return "ExampleFunctionOptionsType"; }
  std::string Stringify(const cp::FunctionOptions&) const override {
    return "ExampleFunctionOptionsType";
  }
  bool Compare(const cp::FunctionOptions&, const cp::FunctionOptions&) const override {
    return true;
  }
  std::unique_ptr<cp::FunctionOptions> Copy(const cp::FunctionOptions&) const override;
  // optional: support for serialization
  // Result<std::shared_ptr<Buffer>> Serialize(const FunctionOptions&) const override;
  // Result<std::unique_ptr<FunctionOptions>> Deserialize(const Buffer&) const override;
};

cp::FunctionOptionsType* GetExampleFunctionOptionsType() {
  static ExampleFunctionOptionsType options_type;
  return &options_type;
}

class ExampleFunctionOptions : public cp::FunctionOptions {
 public:
  ExampleFunctionOptions() : cp::FunctionOptions(GetExampleFunctionOptionsType()) {}
};

std::unique_ptr<cp::FunctionOptions> ExampleFunctionOptionsType::Copy(
    const cp::FunctionOptions&) const {
  return std::make_unique<ExampleFunctionOptions>();
}

arrow::Status ExampleFunctionImpl(cp::KernelContext* ctx, const cp::ExecSpan& batch,
                                  cp::ExecResult* out) {
  out->value = batch[0].array.ToArrayData();
  return arrow::Status::OK();
}

class ExampleNodeOptions : public ac::ExecNodeOptions {};

// a basic ExecNode which ignores all input batches
class ExampleNode : public ac::ExecNode {
 public:
  ExampleNode(ExecNode* input, const ExampleNodeOptions&)
      : ExecNode(/*plan=*/input->plan(), /*inputs=*/{input},
                 /*input_labels=*/{"ignored"},
                 /*output_schema=*/input->output_schema()) {}

  const char* kind_name() const override { return "ExampleNode"; }

  arrow::Status StartProducing() override { return output_->InputFinished(this, 0); }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }
  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }

  arrow::Status StopProducingImpl() override { return arrow::Status::OK(); }

  arrow::Status InputReceived(ExecNode* input, cp::ExecBatch batch) override {
    return arrow::Status::OK();
  }
  arrow::Status InputFinished(ExecNode* input, int total_batches) override {
    return arrow::Status::OK();
  }
};

arrow::Result<ac::ExecNode*> ExampleExecNodeFactory(ac::ExecPlan* plan,
                                                    std::vector<ac::ExecNode*> inputs,
                                                    const ac::ExecNodeOptions& options) {
  const auto& example_options =
      arrow::internal::checked_cast<const ExampleNodeOptions&>(options);

  return plan->EmplaceNode<ExampleNode>(inputs[0], example_options);
}

const cp::FunctionDoc func_doc{
    "Example function to demonstrate registering an out-of-tree function",
    "",
    {"x"},
    "ExampleFunctionOptions"};

arrow::Status RunComputeRegister(int argc, char** argv) {
  const std::string name = "compute_register_example";
  auto func = std::make_shared<cp::ScalarFunction>(name, cp::Arity::Unary(), func_doc);
  cp::ScalarKernel kernel({arrow::int64()}, arrow::int64(), ExampleFunctionImpl);
  kernel.mem_allocation = cp::MemAllocation::NO_PREALLOCATE;
  ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));

  auto registry = cp::GetFunctionRegistry();
  ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

  arrow::Int64Builder builder(arrow::default_memory_pool());
  std::shared_ptr<arrow::Array> arr;
  ARROW_RETURN_NOT_OK(builder.Append(42));
  ARROW_RETURN_NOT_OK(builder.Finish(&arr));
  auto options = std::make_shared<ExampleFunctionOptions>();
  auto maybe_result = cp::CallFunction(name, {arr}, options.get());
  ARROW_RETURN_NOT_OK(maybe_result.status());

  std::cout << maybe_result->make_array()->ToString() << std::endl;

  // Expression serialization will raise NotImplemented if an expression includes
  // FunctionOptions for which serialization is not supported.
  auto expr = cp::call(name, {}, options);
  auto maybe_serialized = cp::Serialize(expr);
  std::cerr << maybe_serialized.status().ToString() << std::endl;

  auto exec_registry = ac::default_exec_factory_registry();
  ARROW_RETURN_NOT_OK(
      exec_registry->AddFactory("compute_register_example", ExampleExecNodeFactory));

  auto maybe_plan = ac::ExecPlan::Make();
  ARROW_RETURN_NOT_OK(maybe_plan.status());
  ARROW_ASSIGN_OR_RAISE(auto plan, maybe_plan);

  arrow::AsyncGenerator<std::optional<cp::ExecBatch>> source_gen, sink_gen;
  ARROW_RETURN_NOT_OK(
      ac::Declaration::Sequence(
          {
              {"source", ac::SourceNodeOptions{arrow::schema({}), source_gen}},
              {"compute_register_example", ExampleNodeOptions{}},
              {"sink", ac::SinkNodeOptions{&sink_gen}},
          })
          .AddToPlan(plan.get())
          .status());
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto status = RunComputeRegister(argc, argv);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
