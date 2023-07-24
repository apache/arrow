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

#include "arrow/api.h"
#include "arrow/compute/api_vector.h"
#include "arrow/status.h"

#include "gandiva/filter.h"
#include "gandiva/projector.h"
#include "gandiva/selection_vector.h"
#include "gandiva/tree_expr_builder.h"

#include <iostream>

using arrow::Datum;
using arrow::Status;
using arrow::compute::TakeOptions;
using gandiva::Condition;
using gandiva::ConfigurationBuilder;
using gandiva::Expression;
using gandiva::Filter;
using gandiva::Node;
using gandiva::Projector;
using gandiva::SelectionVector;
using gandiva::TreeExprBuilder;

Status Example() {
  //(Doc section: Create expressions)
  std::shared_ptr<arrow::Field> field_x_raw = arrow::field("x", arrow::int32());
  std::shared_ptr<Node> field_x = TreeExprBuilder::MakeField(field_x_raw);
  std::shared_ptr<Node> literal_3 = TreeExprBuilder::MakeLiteral(3);
  std::shared_ptr<arrow::Field> field_result = arrow::field("result", arrow::int32());

  std::shared_ptr<Node> add_node =
      TreeExprBuilder::MakeFunction("add", {field_x, literal_3}, arrow::int32());
  std::shared_ptr<Expression> expression =
      TreeExprBuilder::MakeExpression(add_node, field_result);

  std::shared_ptr<Node> less_than_node =
      TreeExprBuilder::MakeFunction("less_than", {field_x, literal_3}, arrow::boolean());
  std::shared_ptr<Condition> condition = TreeExprBuilder::MakeCondition(less_than_node);
  //(Doc section: Create expressions)

  //(Doc section: Create projector and filter)
  std::shared_ptr<arrow::Schema> input_schema = arrow::schema({field_x_raw});
  std::shared_ptr<arrow::Schema> output_schema = arrow::schema({field_result});
  std::shared_ptr<Projector> projector;
  Status status;
  std::vector<std::shared_ptr<Expression>> expressions = {expression};
  status = Projector::Make(input_schema, expressions, &projector);
  ARROW_RETURN_NOT_OK(status);

  std::shared_ptr<Filter> filter;
  status = Filter::Make(input_schema, condition, &filter);
  ARROW_RETURN_NOT_OK(status);
  //(Doc section: Create projector and filter)

  //(Doc section: Evaluate projection)
  auto pool = arrow::default_memory_pool();
  int num_records = 4;
  arrow::Int32Builder builder;
  int32_t values[4] = {1, 2, 3, 4};
  ARROW_RETURN_NOT_OK(builder.AppendValues(values, 4));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array, builder.Finish());
  auto in_batch = arrow::RecordBatch::Make(input_schema, num_records, {array});

  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool, &outputs);
  ARROW_RETURN_NOT_OK(status);
  std::shared_ptr<arrow::RecordBatch> result =
      arrow::RecordBatch::Make(output_schema, outputs[0]->length(), outputs);
  //(Doc section: Evaluate projection)

  std::cout << "Project result:" << std::endl;
  std::cout << result->ToString() << std::endl;

  //(Doc section: Evaluate filter)
  std::shared_ptr<gandiva::SelectionVector> result_indices;
  // Use 16-bit integers for indices. Result can be no longer than input size,
  // so use batch num_rows as max_slots.
  status = gandiva::SelectionVector::MakeInt16(/*max_slots=*/in_batch->num_rows(), pool,
                                               &result_indices);
  ARROW_RETURN_NOT_OK(status);
  status = filter->Evaluate(*in_batch, result_indices);
  ARROW_RETURN_NOT_OK(status);
  std::shared_ptr<arrow::Array> take_indices = result_indices->ToArray();
  Datum maybe_batch;
  ARROW_ASSIGN_OR_RAISE(maybe_batch,
                        arrow::compute::Take(Datum(in_batch), Datum(take_indices),
                                             TakeOptions::NoBoundsCheck()));
  result = maybe_batch.record_batch();
  //(Doc section: Evaluate filter)

  std::cout << "Filter result:" << std::endl;
  std::cout << result->ToString() << std::endl;

  //(Doc section: Evaluate filter and projection)
  // Make sure the projector is compiled for the appropriate selection vector mode
  status = Projector::Make(input_schema, expressions, result_indices->GetMode(),
                           ConfigurationBuilder::DefaultConfiguration(), &projector);
  ARROW_RETURN_NOT_OK(status);

  arrow::ArrayVector outputs_filtered;
  status = projector->Evaluate(*in_batch, result_indices.get(), pool, &outputs_filtered);
  ARROW_RETURN_NOT_OK(status);

  result =
      arrow::RecordBatch::Make(output_schema, outputs[0]->length(), outputs_filtered);
  //(Doc section: Evaluate filter and projection)

  std::cout << "Project + filter result:" << std::endl;
  std::cout << result->ToString() << std::endl;

  return Status::OK();
}

int main(int argc, char** argv) {
  arrow::Status status = Example();

  if (!status.ok()) {
    std::cerr << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
