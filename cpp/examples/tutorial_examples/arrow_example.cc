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

// (Doc section: Basic Example)

// (Doc section: Includes)
#include <arrow/api.h>

#include <iostream>
// (Doc section: Includes)

// (Doc section: RunMain Start)
arrow::Status RunMain() {
  // (Doc section: RunMain Start)
  // (Doc section: int8builder 1 Append)
  // Builders are the main way to create Arrays in Arrow from existing values that are not
  // on-disk. In this case, we'll make a simple array, and feed that in.
  // Data types are important as ever, and there is a Builder for each compatible type;
  // in this case, int8.
  arrow::Int8Builder int8builder;
  int8_t days_raw[5] = {1, 12, 17, 23, 28};
  // AppendValues, as called, puts 5 values from days_raw into our Builder object.
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
  // (Doc section: int8builder 1 Append)

  // (Doc section: int8builder 1 Finish)
  // We only have a Builder though, not an Array -- the following code pushes out the
  // built up data into a proper Array.
  std::shared_ptr<arrow::Array> days;
  ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());
  // (Doc section: int8builder 1 Finish)

  // (Doc section: int8builder 2)
  // Builders clear their state every time they fill an Array, so if the type is the same,
  // we can re-use the builder. We do that here for month values.
  int8_t months_raw[5] = {1, 3, 5, 7, 1};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
  std::shared_ptr<arrow::Array> months;
  ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());
  // (Doc section: int8builder 2)

  // (Doc section: int16builder)
  // Now that we change to int16, we use the Builder for that data type instead.
  arrow::Int16Builder int16builder;
  int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
  ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
  std::shared_ptr<arrow::Array> years;
  ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());
  // (Doc section: int16builder)

  // (Doc section: Schema)
  // Now, we want a RecordBatch, which has columns and labels for said columns.
  // This gets us to the 2d data structures we want in Arrow.
  // These are defined by schema, which have fields -- here we get both those object types
  // ready.
  std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

  // Every field needs its name and data type.
  field_day = arrow::field("Day", arrow::int8());
  field_month = arrow::field("Month", arrow::int8());
  field_year = arrow::field("Year", arrow::int16());

  // The schema can be built from a vector of fields, and we do so here.
  schema = arrow::schema({field_day, field_month, field_year});
  // (Doc section: Schema)

  // (Doc section: RBatch)
  // With the schema and Arrays full of data, we can make our RecordBatch! Here,
  // each column is internally contiguous. This is in opposition to Tables, which we'll
  // see next.
  std::shared_ptr<arrow::RecordBatch> rbatch;
  // The RecordBatch needs the schema, length for columns, which all must match,
  // and the actual data itself.
  rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});

  std::cout << rbatch->ToString();
  // (Doc section: RBatch)

  // (Doc section: More Arrays)
  // Now, let's get some new arrays! It'll be the same datatypes as above, so we re-use
  // Builders.
  int8_t days_raw2[5] = {6, 12, 3, 30, 22};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw2, 5));
  std::shared_ptr<arrow::Array> days2;
  ARROW_ASSIGN_OR_RAISE(days2, int8builder.Finish());

  int8_t months_raw2[5] = {5, 4, 11, 3, 2};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw2, 5));
  std::shared_ptr<arrow::Array> months2;
  ARROW_ASSIGN_OR_RAISE(months2, int8builder.Finish());

  int16_t years_raw2[5] = {1980, 2001, 1915, 2020, 1996};
  ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw2, 5));
  std::shared_ptr<arrow::Array> years2;
  ARROW_ASSIGN_OR_RAISE(years2, int16builder.Finish());
  // (Doc section: More Arrays)

  // (Doc section: ArrayVector)
  // ChunkedArrays let us have a list of arrays, which aren't contiguous
  // with each other. First, we get a vector of arrays.
  arrow::ArrayVector day_vecs{days, days2};
  // (Doc section: ArrayVector)
  // (Doc section: ChunkedArray Day)
  // Then, we use that to initialize a ChunkedArray, which can be used with other
  // functions in Arrow! This is good, since having a normal vector of arrays wouldn't
  // get us far.
  std::shared_ptr<arrow::ChunkedArray> day_chunks =
      std::make_shared<arrow::ChunkedArray>(day_vecs);
  // (Doc section: ChunkedArray Day)

  // (Doc section: ChunkedArray Month Year)
  // Repeat for months.
  arrow::ArrayVector month_vecs{months, months2};
  std::shared_ptr<arrow::ChunkedArray> month_chunks =
      std::make_shared<arrow::ChunkedArray>(month_vecs);

  // Repeat for years.
  arrow::ArrayVector year_vecs{years, years2};
  std::shared_ptr<arrow::ChunkedArray> year_chunks =
      std::make_shared<arrow::ChunkedArray>(year_vecs);
  // (Doc section: ChunkedArray Month Year)

  // (Doc section: Table)
  // A Table is the structure we need for these non-contiguous columns, and keeps them
  // all in one place for us so we can use them as if they were normal arrays.
  std::shared_ptr<arrow::Table> table;
  table = arrow::Table::Make(schema, {day_chunks, month_chunks, year_chunks}, 10);

  std::cout << table->ToString();
  // (Doc section: Table)

  // (Doc section: Ret)
  return arrow::Status::OK();
}
// (Doc section: Ret)

// (Doc section: Main)
int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}

// (Doc section: Main)
// (Doc section: Basic Example)
