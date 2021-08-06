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
#include <arrow/compute/exec/expression.h>

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

arrow::Status ExampleFunctionImpl(cp::KernelContext* ctx, const cp::ExecBatch& batch,
                                  arrow::Datum* out) {
  *out->mutable_array() = *batch[0].array();
  return arrow::Status::OK();
}

const cp::FunctionDoc func_doc{
    "Example function to demonstrate registering an out-of-tree function",
    "",
    {"x"},
    "ExampleFunctionOptions"};

int main(int argc, char** argv) {
  const std::string name = "compute_register_example";
  auto func = std::make_shared<cp::ScalarFunction>(name, cp::Arity::Unary(), &func_doc);
  ABORT_ON_FAILURE(func->AddKernel({cp::InputType::Array(arrow::int64())}, arrow::int64(),
                                   ExampleFunctionImpl));

  auto registry = cp::GetFunctionRegistry();
  ABORT_ON_FAILURE(registry->AddFunction(std::move(func)));

  arrow::Int64Builder builder(arrow::default_memory_pool());
  std::shared_ptr<arrow::Array> arr;
  ABORT_ON_FAILURE(builder.Append(42));
  ABORT_ON_FAILURE(builder.Finish(&arr));
  auto options = std::make_shared<ExampleFunctionOptions>();
  auto maybe_result = cp::CallFunction(name, {arr}, options.get());
  ABORT_ON_FAILURE(maybe_result.status());

  std::cout << maybe_result->make_array()->ToString() << std::endl;

  // Expression serialization will raise NotImplemented if an expression includes
  // FunctionOptions for which serialization is not supported.
  auto expr = cp::call(name, {}, options);
  auto maybe_serialized = cp::Serialize(expr);
  std::cerr << maybe_serialized.status().ToString() << std::endl;

  return EXIT_SUCCESS;
}
