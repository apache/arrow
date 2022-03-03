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

// This example showcases various ways to work with Datasets. It's
// intended to be paired with the documentation.

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

namespace cp = arrow::compute;

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

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  std::shared_ptr<arrow::Table> table;

  auto field_vector = {arrow::field("a", arrow::int64()),
                       arrow::field("b", arrow::boolean()),
                       arrow::field("c", arrow::int64())};
  ARROW_ASSIGN_OR_RAISE(auto int_array,
                        GetArrayDataSample<arrow::Int64Type>({0, 1, 2, 0, 4, 1, 0, 5}));
  ARROW_ASSIGN_OR_RAISE(auto bool_array, GetArrayDataSample<arrow::BooleanType>(
                                             {false, true, false, true, true, false, true, false}));
  ARROW_ASSIGN_OR_RAISE(auto data_array,
                        GetArrayDataSample<arrow::Int64Type>({10, 11, 12, 10, 11, 11, 10, 15}));                                             

  auto schema = arrow::schema(field_vector);
  auto data_vector = {int_array, bool_array, data_array};

  table = arrow::Table::Make(schema, data_vector, 8);

  return table;
}

arrow::Status DoAggregate() {
  auto maybe_plan = cp::ExecPlan::Make();
  ABORT_ON_FAILURE(maybe_plan.status());
  auto plan = maybe_plan.ValueOrDie();
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());

  ARROW_ASSIGN_OR_RAISE(auto table, GetTable());

  std::cout << "Source Table" << std::endl;

  std::cout << table->ToString() << std::endl;

  std::shared_ptr<arrow::Table> out;
  cp::CountOptions options(cp::CountOptions::ONLY_VALID);
  auto aggregate_options =
      cp::AggregateNodeOptions{/*aggregates=*/{{"sum", &options}},
                               /*targets=*/{"c"},
                               /*names=*/{"count(c)"},
                               /*keys=*/{}};
  auto schema = arrow::schema({
      arrow::field("count(c)", arrow::int64())
      //arrow::field("a", arrow::int64())
  });

  ABORT_ON_FAILURE(cp::Declaration::Sequence(
                       {
                           {"table_source", cp::TableSourceNodeOptions{table, 2}},
                           {"aggregate", aggregate_options},
                           {"table_sink", cp::TableSinkNodeOptions{&out, schema}},
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
  auto status = DoAggregate();
  if (!status.ok()) {
    std::cerr << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
