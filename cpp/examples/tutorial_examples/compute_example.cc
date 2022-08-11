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

#include <iostream>

arrow::Status RunMain() {
  //Create a couple 32-bit integer arrays.
  arrow::Int32Builder int32builder;
  int32_t some_nums_raw[5] = {34, 624, 2223, 5654, 4356};
  ARROW_RETURN_NOT_OK(int32builder.AppendValues(some_nums_raw, 5));
  std::shared_ptr<arrow::Array> some_nums;
  ARROW_ASSIGN_OR_RAISE(some_nums, int32builder.Finish());

  int32_t more_nums_raw[5] = {75342, 23, 64, 17, 736};
  ARROW_RETURN_NOT_OK(int32builder.AppendValues(more_nums_raw, 5));
  std::shared_ptr<arrow::Array> more_nums;
  ARROW_ASSIGN_OR_RAISE(more_nums, int32builder.Finish());

  //Make a table out of our pair of arrays.
  std::shared_ptr<arrow::Field> field_a, field_b;
  std::shared_ptr<arrow::Schema> schema;

  field_a = arrow::field("A", arrow::int32());
  field_b = arrow::field("B", arrow::int32());

  schema = arrow::schema({field_a, field_b});

  std::shared_ptr<arrow::Table> table; 
  table = arrow::Table::Make(schema, {some_nums, more_nums}, 5);

  //The Datum class is what all compute functions output to, and they can take Datums
  //as inputs, as well.
  arrow::Datum sum;
  //Here, we can use arrow::compute::Sum. This is a convenience function, and the next
  //computation won't be so simple. However, using these where possible helps readability.
  ARROW_ASSIGN_OR_RAISE(sum,
                      arrow::compute::Sum({table->GetColumnByName("A")}));
  //Get the kind of Datum and what it holds -- this is a ChunkedArray, with int32.
  std::cout << "Datum kind: " << sum.ToString() << " content type: " <<
  sum.type()->ToString() << std::endl;
  //Note that we explicitly request a scalar -- the Datum cannot simply give what it is,
  //you must ask for the correct type.
  std::cout << sum.scalar_as<arrow::Int64Scalar>().value << std::endl;

  arrow::Datum element_wise_sum;
  //Get element-wise sum of both columns A and B in our Table. Note that here we use
  //CallFunction(), which takes the name of the function as the first argument.
  ARROW_ASSIGN_OR_RAISE(element_wise_sum,
                      arrow::compute::CallFunction("add", {table->GetColumnByName("A"),
                                                           table->GetColumnByName("B")}));
  //Get the kind of Datum and what it holds -- this is a ChunkedArray, with int32.
  std::cout << "Datum kind: " << element_wise_sum.ToString() << " content type: " <<
  element_wise_sum.type()->ToString() << std::endl;
  //This time, we get a ChunkedArray, not a scalar.
  std::cout << element_wise_sum.chunked_array()->ToString() << std::endl;

  return arrow::Status::OK();
}

int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
