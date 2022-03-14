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
#include <arrow/datum.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/future.h>
#include <arrow/util/vector.h>

#include <cstdlib>
#include <iostream>
#include <memory>

// Demonstrate registering an user-defined Arrow compute function outside of the Arrow
// source tree

namespace cp = ::arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

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

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  std::shared_ptr<arrow::Table> table;

  auto field_vector = {
      arrow::field("a", arrow::int64()), arrow::field("x", arrow::int64()),
      arrow::field("y", arrow::int64()), arrow::field("z", arrow::int64()),
      arrow::field("b", arrow::boolean())};

  ARROW_ASSIGN_OR_RAISE(auto int_array,
                        GetArrayDataSample<arrow::Int64Type>({1, 2, 3, 4, 5, 6}));
  ARROW_ASSIGN_OR_RAISE(auto x,
                        GetArrayDataSample<arrow::Int64Type>({21, 22, 23, 24, 25, 26}));
  ARROW_ASSIGN_OR_RAISE(auto y,
                        GetArrayDataSample<arrow::Int64Type>({31, 32, 33, 34, 35, 36}));
  ARROW_ASSIGN_OR_RAISE(auto z,
                        GetArrayDataSample<arrow::Int64Type>({41, 42, 43, 44, 45, 46}));
  ARROW_ASSIGN_OR_RAISE(auto bool_array, GetArrayDataSample<arrow::BooleanType>(
                                             {false, true, false, true, true, false}));

  auto schema = arrow::schema(field_vector);
  auto data_vector = {int_array, x, y, z, bool_array};

  table = arrow::Table::Make(schema, data_vector, 6);

  return table;
}

class UDFOptionsType : public cp::FunctionOptionsType {
  const char* type_name() const override { return "UDFOptionsType"; }
  std::string Stringify(const cp::FunctionOptions&) const override {
    return "UDFOptionsType";
  }
  bool Compare(const cp::FunctionOptions&, const cp::FunctionOptions&) const override {
    return true;
  }
  std::unique_ptr<cp::FunctionOptions> Copy(const cp::FunctionOptions&) const override;
};

cp::FunctionOptionsType* GetUDFOptionsType() {
  static UDFOptionsType options_type;
  return &options_type;
}

class UDFOptions : public cp::FunctionOptions {
 public:
  UDFOptions() : cp::FunctionOptions(GetUDFOptionsType()) {}
};

std::unique_ptr<cp::FunctionOptions> UDFOptionsType::Copy(
    const cp::FunctionOptions&) const {
  return std::unique_ptr<cp::FunctionOptions>(new UDFOptions());
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
    "User-defined-function usage to demonstrate registering an out-of-tree function",
    "returns x + y + z",
    {"x", "y", "z"},
    "UDFOptions"};

arrow::Status Execute() {
  const std::string name = "x+x";
  auto func = std::make_shared<cp::ScalarFunction>(name, cp::Arity::Ternary(), &func_doc);

  auto exec_func = [](cp::KernelContext* ctx, const cp::ExecBatch& batch,
                      arrow::Datum* out) -> arrow::Status {
    auto in_res = cp::CallFunction("add", {batch[0].array(), batch[1].array()});
    auto in_arr = in_res.ValueOrDie().make_array();
    auto final_res = cp::CallFunction("add", {in_arr, batch[2].array()});
    auto final_arr = final_res.ValueOrDie().array();
    auto datum = new arrow::Datum(final_arr);
    *out = *datum;
    return arrow::Status::OK();
  };

  auto options = std::make_shared<UDFOptions>();
  cp::ScalarKernel kernel(
      {
        cp::InputType::Array(arrow::int64()),
        cp::InputType::Array(arrow::int64()),
        cp::InputType::Array(arrow::int64())
       },
      arrow::int64(), exec_func);

  kernel.mem_allocation = cp::MemAllocation::NO_PREALLOCATE;
  kernel.null_handling = cp::NullHandling::COMPUTED_NO_PREALLOCATE;

  ABORT_ON_FAILURE(func->AddKernel(std::move(kernel)));

  auto registry = cp::GetFunctionRegistry();
  ABORT_ON_FAILURE(registry->AddFunction(std::move(func)));

  auto exec_registry = cp::default_exec_factory_registry();
  ABORT_ON_FAILURE(
      exec_registry->AddFactory("udf_register_example", ExampleExecNodeFactory));

  auto maybe_plan = cp::ExecPlan::Make();
  ABORT_ON_FAILURE(maybe_plan.status());
  auto plan = maybe_plan.ValueOrDie();
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> source_gen, sink_gen;

  cp::Expression custom_exp = cp::call(
      name, {cp::field_ref("x"), cp::field_ref("y"), cp::field_ref("z")}, options);

  ARROW_ASSIGN_OR_RAISE(auto table, GetTable());
  auto table_source_node_options = cp::TableSourceNodeOptions{table, 2};
  auto project_node_options = cp::ProjectNodeOptions{{
      cp::field_ref("a"),
      custom_exp,
      cp::field_ref("b"),
  }};
  auto output_schema = arrow::schema({arrow::field("a", arrow::int64()),
                                      arrow::field("x + y + z", arrow::int64()),
                                      arrow::field("b", arrow::boolean())});
  std::shared_ptr<arrow::Table> out;
  auto table_sink_node_options = cp::TableSinkNodeOptions{&out, output_schema};
  ABORT_ON_FAILURE(
      cp::Declaration::Sequence({
                                    {"table_source", table_source_node_options},
                                    {"project", project_node_options},
                                    {"table_sink", table_sink_node_options},
                                })
          .AddToPlan(plan.get())
          .status());

  ARROW_RETURN_NOT_OK(plan->StartProducing());
  constexpr int print_len = 25;
  std::cout << std::string(print_len, '#') << std::endl;
  std::cout << "Input Table Data : " << std::endl;
  std::cout << std::string(print_len, '#') << std::endl;

  std::cout << table->ToString() << std::endl;

  std::cout << std::string(print_len, '#') << std::endl;
  std::cout << "Output Table Data : " << std::endl;
  std::cout << std::string(print_len, '#') << std::endl;
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
