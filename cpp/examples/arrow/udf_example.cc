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

#include <arrow/python/udf.h>

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

PyObject* SimpleFunction() {
  PyObject* out = Py_BuildValue("s", "hello");
  std::cout << "HELLO" << std::endl;
  return std::move(out);
}

// PyObject* objectsRepresentation = PyObject_Repr(yourObject);
// const char* s = PyString_AsString(objectsRepresentation);

arrow::Status ExampleFunctionImpl(cp::KernelContext* ctx, const cp::ExecBatch& batch,
                                  arrow::Datum* out) {
  std::cout << "calling udf :" << batch.length << std::endl;
  Py_Initialize();
  PyObject* res = SimpleFunction();
  PyObject* objectsRepresentation = PyObject_Repr(res);
  const char* s = PyUnicode_AsUTF8(objectsRepresentation);
  std::cout << "Message :: " << s << std::endl;
  Py_Finalize();
  auto result = cp::CallFunction("add", {batch[0].array(), batch[0].array()});
  *out->mutable_array() = *result.ValueOrDie().array();
  return arrow::Status::OK();
}
// cp::KernelContext*, const cp::ExecBatch&, Datum*, PyObject* func
arrow::Status ExamplePyFunctionImpl(cp::KernelContext* ctx, const cp::ExecBatch& batch,
                                    arrow::Datum* out, PyObject* func) {
  std::cout << "H" << std::endl;
  auto result = cp::CallFunction("add", {batch[0].array(), batch[0].array()});
  *out->mutable_array() = *result.ValueOrDie().array();
  // PyObject* res = SimpleFunction();
  // PyObject* objectsRepresentation = PyObject_Repr(res);
  // const char* s = PyUnicode_AsUTF8(objectsRepresentation);
  std::cout << "Message :: "
            << "s" << std::endl;
            
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
  auto field_vector = {arrow::field("a", arrow::int64()),
                       arrow::field("b", arrow::boolean())};
  ARROW_ASSIGN_OR_RAISE(auto b1_int, GetArrayDataSample<arrow::Int64Type>({0, 4}));
  ARROW_ASSIGN_OR_RAISE(auto b2_int, GetArrayDataSample<arrow::Int64Type>({5, 6, 7}));
  ARROW_ASSIGN_OR_RAISE(auto b3_int, GetArrayDataSample<arrow::Int64Type>({8, 9, 10}));

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
    {"x", "y"},
    "ExampleFunctionOptions"};

const cp::FunctionDoc func_doc2{
    "Example function to demonstrate registering an out-of-tree function",
    "",
    {"x"},
    "ExampleFunctionOptions2"};

PyObject* MultiplyFunction(PyObject* scalar) {
  PyObject* constant = PyLong_FromLong(2);
  PyObject* res = PyNumber_Multiply(constant, scalar);
  return std::move(res);
}

class ScalarUDF {

  public:
    ScalarUDF();
    explicit ScalarUDF(cp::Arity arity, std::vector<cp::InputType> input_types,
    cp::OutputType output_type, PyObject* (*function)(PyObject*)) : arity_(std::move(arity)), input_types_(std::move(input_types)),
     output_type_(output_type), function_(function) {} 

    arrow::Status Make(cp::KernelContext* ctx, const cp::ExecBatch& batch,
                                  arrow::Datum* out) {
      Py_Initialize();                                    
      PyObject* args = PyTuple_Pack(1,PyLong_FromLong(2));                              
      PyObject* myResult = function_(args);
      int64_t result = PyLong_AsLong(myResult);
      Py_Finalize();
      std::cout << "Value : " << result << std::endl;
      arrow::Result<arrow::Datum> maybe_result;
      arrow::Int64Builder builder(arrow::default_memory_pool());
      std::shared_ptr<arrow::Array> arr;
      ABORT_ON_FAILURE(builder.Append(result));
      ABORT_ON_FAILURE(builder.Finish(&arr));
      maybe_result = cp::CallFunction("add", {batch[0].array(), arr});
      *out->mutable_array() = *maybe_result.ValueOrDie().array();
      return arrow::Status::OK();
    }

  private:
    cp::Arity arity_;
    std::vector<cp::InputType> input_types_;
    cp::OutputType output_type_;
    PyObject* (*function_)(PyObject*);

};

arrow::Status Execute() {
  const std::string name = "x+x";
  
  ScalarUDF func_gen(cp::Arity::Unary(), {cp::InputType::Array(arrow::int64())}, arrow::int64(), &MultiplyFunction);
  

  auto func = std::make_shared<cp::ScalarFunction>(name, cp::Arity::Unary(), &func_doc2);
  cp::ScalarKernel kernel({cp::InputType::Array(arrow::int64())}, arrow::int64(), ExampleFunctionImpl);
  
  kernel.mem_allocation = cp::MemAllocation::NO_PREALLOCATE;

  ABORT_ON_FAILURE(func->AddKernel(std::move(kernel)));

  auto registry = cp::GetFunctionRegistry();
  ABORT_ON_FAILURE(registry->AddFunction(std::move(func)));

  arrow::Int64Builder builder(arrow::default_memory_pool());
  std::shared_ptr<arrow::Array> arr1, arr2;
  ABORT_ON_FAILURE(builder.Append(42));
  ABORT_ON_FAILURE(builder.Finish(&arr1));
  ABORT_ON_FAILURE(builder.Append(58));
  ABORT_ON_FAILURE(builder.Finish(&arr2));
  auto options = std::make_shared<ExampleFunctionOptions>();
  auto maybe_result = cp::CallFunction(name, {arr1}, options.get());
  ABORT_ON_FAILURE(maybe_result.status());

  std::cout << "Result 1: " << maybe_result->make_array()->ToString() << std::endl;

  // Expression serialization will raise NotImplemented if an expression includes
  // FunctionOptions for which serialization is not supported.
  // auto expr = cp::call(name, {}, options);
  // auto maybe_serialized = cp::Serialize(expr);
  // std::cerr << maybe_serialized.status().ToString() << std::endl;

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
  auto output_schema = arrow::schema({arrow::field("a", arrow::int64()),
                                      arrow::field("a + a", arrow::int64()),
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

arrow::Status ExecutePy() {
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());
  const std::string name = "simple_func";
  auto func2 =
      std::make_shared<arrow::py::UDFScalarFunction>(name, cp::Arity::Unary(), &func_doc2);
  arrow::py::UDFScalarKernel kernel2({cp::InputType::Array(arrow::int64())},
                                     arrow::int64(), ExamplePyFunctionImpl);

  kernel2.mem_allocation = cp::MemAllocation::NO_PREALLOCATE;
  ABORT_ON_FAILURE(func2->AddKernel(std::move(kernel2)));

  auto registry = cp::GetFunctionRegistry();

  auto size_before_registration = registry->GetFunctionNames().size();

  std::cout << "[Before] Func Reg Size: " << size_before_registration << ", " << registry->num_functions() << std::endl;

  ABORT_ON_FAILURE(registry->AddFunction(std::move(func2)));

  auto size_after_registration = registry->GetFunctionNames().size();

  std::cout << "[After] Func Reg Size: " << size_after_registration << ", " << registry->num_functions() << std::endl;

  arrow::Int64Builder builder(arrow::default_memory_pool());
  std::shared_ptr<arrow::Array> arr;
  ABORT_ON_FAILURE(builder.Append(42));
  ABORT_ON_FAILURE(builder.Finish(&arr));
  auto options = std::make_shared<ExampleFunctionOptions>();

  std::cout << "Calling function :" << arr->ToString() << std::endl;

  auto maybe_result = cp::CallFunction(name, {arr}, options.get());
  ABORT_ON_FAILURE(maybe_result.status());

  std::cout << "Result 1: " << maybe_result->make_array()->ToString() << std::endl;

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  auto status = Execute();
  if (!status.ok()) {
    std::cerr << "Error occurred : " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
