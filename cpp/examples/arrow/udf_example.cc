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
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/compute/exec/options.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/future.h>
#include <arrow/util/vector.h>

#include <cstdlib>
#include <iostream>
#include <memory>

// Demonstrate registering an Arrow compute function outside of the Arrow source tree

namespace cp = ::arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

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
  return std::unique_ptr<cp::FunctionOptions>(new ExampleFunctionOptions());
}

arrow::Status ExampleFunctionImpl(cp::KernelContext* ctx, const cp::ExecBatch& batch,
                                  arrow::Datum* out) {
  auto result = cp::CallFunction("add", {batch[0].array(), batch[0].array()});
  *out->mutable_array() = *result.ValueOrDie().array();
  return arrow::Status::OK();
}

struct BatchesWithSchema {
  std::vector<cp::ExecBatch> batches;
  std::shared_ptr<arrow::Schema> schema;
  // // This method uses internal arrow utilities to
  // // convert a vector of record batches to an AsyncGenerator of optional batches
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> gen() const {
    auto opt_batches = ::arrow::internal::MapVector(
        [](cp::ExecBatch batch) { return arrow::util::make_optional(std::move(batch)); },
        batches);
    arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> gen;
    gen = arrow::MakeVectorGenerator(std::move(opt_batches));
    return gen;
  }
};

template <typename TYPE,
          typename = typename std::enable_if<arrow::is_number_type<TYPE>::value |
                                             arrow::is_boolean_type<TYPE>::value |
                                             arrow::is_temporal_type<TYPE>::value>::type>
arrow::Result<std::shared_ptr<arrow::Array>> GetArrayDataSample(
    const std::vector<typename TYPE::c_type>& values) {
  using ARROW_ARRAY_TYPE = typename arrow::TypeTraits<TYPE>::ArrayType;
  using ARROW_BUILDER_TYPE = typename arrow::TypeTraits<TYPE>::BuilderType;
  ARROW_BUILDER_TYPE builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  std::shared_ptr<ARROW_ARRAY_TYPE> array;
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetSampleRecordBatch(
    const arrow::ArrayVector array_vector, const arrow::FieldVector& field_vector) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(auto struct_result,
                        arrow::StructArray::Make(array_vector, field_vector));
  return record_batch->FromStructArray(struct_result);
}

arrow::Result<cp::ExecBatch> GetExecBatchFromVectors(
    const arrow::FieldVector& field_vector, const arrow::ArrayVector& array_vector) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(auto res_batch, GetSampleRecordBatch(array_vector, field_vector));
  cp::ExecBatch batch{*res_batch};
  return batch;
}

arrow::Result<BatchesWithSchema> MakeBasicBatches() {
  BatchesWithSchema out;
  auto field_vector = {arrow::field("a", arrow::int32()),
                       arrow::field("b", arrow::boolean())};
  ARROW_ASSIGN_OR_RAISE(auto b1_int, GetArrayDataSample<arrow::Int32Type>({0, 4}));
  ARROW_ASSIGN_OR_RAISE(auto b2_int, GetArrayDataSample<arrow::Int32Type>({5, 6, 7}));
  ARROW_ASSIGN_OR_RAISE(auto b3_int, GetArrayDataSample<arrow::Int32Type>({8, 9, 10}));

  ARROW_ASSIGN_OR_RAISE(auto b1_bool,
                        GetArrayDataSample<arrow::BooleanType>({false, true}));
  ARROW_ASSIGN_OR_RAISE(auto b2_bool,
                        GetArrayDataSample<arrow::BooleanType>({true, false, true}));
  ARROW_ASSIGN_OR_RAISE(auto b3_bool,
                        GetArrayDataSample<arrow::BooleanType>({false, true, false}));

  ARROW_ASSIGN_OR_RAISE(auto b1,
                        GetExecBatchFromVectors(field_vector, {b1_int, b1_bool}));
  ARROW_ASSIGN_OR_RAISE(auto b2,
                        GetExecBatchFromVectors(field_vector, {b2_int, b2_bool}));
  ARROW_ASSIGN_OR_RAISE(auto b3,
                        GetExecBatchFromVectors(field_vector, {b3_int, b3_bool}));

  out.batches = {b1, b2, b3};
  out.schema = arrow::schema(field_vector);
  return out;
}

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  std::shared_ptr<arrow::Table> out;

  return out;
}

class ExampleNodeOptions : public cp::ExecNodeOptions {};

// a basic ExecNode which ignores all input batches
class ExampleNode : public cp::ExecNode {
 public:
  ExampleNode(ExecNode* input, const ExampleNodeOptions&)
      : ExecNode(/*plan=*/input->plan(), /*inputs=*/{input},
                 /*input_labels=*/{"ignored"},
                 /*output_schema=*/input->output_schema(), /*num_outputs=*/1) {}

  const char* kind_name() const override { return "ExampleNode"; }

  arrow::Status StartProducing() override {
    outputs_[0]->InputFinished(this, 0);
    return arrow::Status::OK();
  }

  void ResumeProducing(ExecNode* output) override {}
  void PauseProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override { inputs_[0]->StopProducing(this); }
  void StopProducing() override { inputs_[0]->StopProducing(); }

  void InputReceived(ExecNode* input, cp::ExecBatch batch) override {}
  void ErrorReceived(ExecNode* input, arrow::Status error) override {}
  void InputFinished(ExecNode* input, int total_batches) override {}

  arrow::Future<> finished() override { return inputs_[0]->finished(); }
};

arrow::Result<cp::ExecNode*> ExampleExecNodeFactory(cp::ExecPlan* plan,
                                                    std::vector<cp::ExecNode*> inputs,
                                                    const cp::ExecNodeOptions& options) {
  const auto& example_options =
      arrow::internal::checked_cast<const ExampleNodeOptions&>(options);

  return plan->EmplaceNode<ExampleNode>(inputs[0], example_options);
}

const cp::FunctionDoc func_doc{
    "Example function to demonstrate registering an out-of-tree function",
    "",
    {"x"},
    "ExampleFunctionOptions"};

arrow::Status Execute() {
  const std::string name = "x+x";
  auto func = std::make_shared<cp::ScalarFunction>(name, cp::Arity::Unary(), &func_doc);
  cp::ScalarKernel kernel({cp::InputType::Array(arrow::int32())}, arrow::int32(),
                          ExampleFunctionImpl);
  kernel.mem_allocation = cp::MemAllocation::NO_PREALLOCATE;
  ABORT_ON_FAILURE(func->AddKernel(std::move(kernel)));

  auto registry = cp::GetFunctionRegistry();
  ABORT_ON_FAILURE(registry->AddFunction(std::move(func)));

  arrow::Int32Builder builder(arrow::default_memory_pool());
  std::shared_ptr<arrow::Array> arr;
  ABORT_ON_FAILURE(builder.Append(42));
  ABORT_ON_FAILURE(builder.Finish(&arr));
  auto options = std::make_shared<ExampleFunctionOptions>();
  auto maybe_result = cp::CallFunction(name, {arr}, options.get());
  ABORT_ON_FAILURE(maybe_result.status());

  std::cout << "Result 1: " << maybe_result->make_array()->ToString() << std::endl;

  // Expression serialization will raise NotImplemented if an expression includes
  // FunctionOptions for which serialization is not supported.
  //   auto expr = cp::call(name, {}, options);
  //   auto maybe_serialized = cp::Serialize(expr);
  //   std::cerr << maybe_serialized.status().ToString() << std::endl;

  auto exec_registry = cp::default_exec_factory_registry();
  ABORT_ON_FAILURE(
      exec_registry->AddFactory("compute_register_example", ExampleExecNodeFactory));

  auto maybe_plan = cp::ExecPlan::Make();
  ABORT_ON_FAILURE(maybe_plan.status());
  auto plan = maybe_plan.ValueOrDie();
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> source_gen, sink_gen;
  ARROW_ASSIGN_OR_RAISE(auto basic_data, MakeBasicBatches());

  cp::Expression a_times_10 = cp::call("multiply", {cp::field_ref("a"), cp::literal(10)});
  cp::Expression custom_exp = cp::call(name, {cp::field_ref("a")}, options);

  auto source_node_options = cp::SourceNodeOptions{basic_data.schema, basic_data.gen()};
  auto project_node_options = cp::ProjectNodeOptions{{
      cp::field_ref("a"),
      custom_exp,
      cp::field_ref("b"),
  }};
  auto output_schema = arrow::schema({arrow::field("a", arrow::int32()),
                                      arrow::field("a + a", arrow::int32()),
                                      arrow::field("b", arrow::boolean())});
  std::shared_ptr<arrow::Table> out;
  ABORT_ON_FAILURE(cp::Declaration::Sequence(
                       {
                           {"source", source_node_options},
                           {"project", project_node_options},
                           {"table_sink", cp::TableSinkNodeOptions{&out, output_schema}},
                       })
                       .AddToPlan(plan.get())
                       .status());

  ARROW_RETURN_NOT_OK(plan->StartProducing());

  std::cout << "Output Table Data : " << std::endl;
  std::cout << out->ToString() << std::endl;

  auto future = plan->finished();

  return future.status();
}

int main(int argc, char** argv) {
  auto status = Execute();
  if (!status.ok()) {
    std::cerr << "Error occurred : " << status.message() << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
