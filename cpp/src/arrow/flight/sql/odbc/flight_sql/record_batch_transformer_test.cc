// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/flight/sql/odbc/flight_sql/record_batch_transformer.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/record_batch.h"
#include "arrow/testing/builder.h"
#include "gtest/gtest.h"

using arrow::Array;
using arrow::Int32Type;
using arrow::RecordBatch;

using arrow::ArrayFromVector;
namespace {
std::shared_ptr<RecordBatch> CreateOriginalRecordBatch() {
  std::vector<int> values = {1, 2, 3, 4, 5};
  std::shared_ptr<Array> array;

  ArrayFromVector<Int32Type, int32_t>(values, &array);

  auto schema = arrow::schema({field("test", arrow::int32(), false)});

  return RecordBatch::Make(schema, 4, {array});
}
}  // namespace

namespace driver {
namespace flight_sql {

TEST(Transformer, TransformerRenameTest) {
  // Prepare the Original Record Batch
  auto original_record_batch = CreateOriginalRecordBatch();
  auto schema = original_record_batch->schema();

  // Execute the transformation of the Record Batch
  std::string original_name("test");
  std::string transformed_name("test1");

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField(original_name, transformed_name)
                         .Build();

  auto transformed_record_batch = transformer->Transform(original_record_batch);

  auto transformed_array_ptr =
      transformed_record_batch->GetColumnByName(transformed_name);

  auto original_array_ptr = original_record_batch->GetColumnByName(original_name);

  // Assert that the arrays are being the same and we are not creating new
  // buffers
  ASSERT_EQ(transformed_array_ptr, original_array_ptr);

  // Assert if the schema is not the same
  ASSERT_NE(original_record_batch->schema(), transformed_record_batch->schema());
  // Assert if the data is not changed
  ASSERT_EQ(original_record_batch->GetColumnByName(original_name),
            transformed_record_batch->GetColumnByName(transformed_name));
}

TEST(Transformer, TransformerAddEmptyVectorTest) {
  // Prepare the Original Record Batch
  auto original_record_batch = CreateOriginalRecordBatch();
  auto schema = original_record_batch->schema();

  std::string original_name("test");
  std::string transformed_name("test1");
  auto emptyField = std::string("empty");

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField(original_name, transformed_name)
                         .AddFieldOfNulls(emptyField, arrow::int32())
                         .Build();

  auto transformed_schema = transformer->GetTransformedSchema();

  ASSERT_EQ(transformed_schema->num_fields(), 2);
  ASSERT_EQ(transformed_schema->GetFieldIndex(transformed_name), 0);
  ASSERT_EQ(transformed_schema->GetFieldIndex(emptyField), 1);

  auto transformed_record_batch = transformer->Transform(original_record_batch);

  auto transformed_array_ptr =
      transformed_record_batch->GetColumnByName(transformed_name);

  auto original_array_ptr = original_record_batch->GetColumnByName(original_name);

  // Assert that the arrays are being the same and we are not creating new
  // buffers
  ASSERT_EQ(transformed_array_ptr, original_array_ptr);

  // Assert if the schema is not the same
  ASSERT_NE(original_record_batch->schema(), transformed_record_batch->schema());
  // Assert if the data is not changed
  ASSERT_EQ(original_record_batch->GetColumnByName(original_name),
            transformed_record_batch->GetColumnByName(transformed_name));
}

TEST(Transformer, TransformerChangingOrderOfArrayTest) {
  std::vector<int> first_array_value = {1, 2, 3, 4, 5};
  std::vector<int> second_array_value = {6, 7, 8, 9, 10};
  std::vector<int> third_array_value = {2, 4, 6, 8, 10};
  std::shared_ptr<Array> first_array;
  std::shared_ptr<Array> second_array;
  std::shared_ptr<Array> third_array;

  ArrayFromVector<Int32Type, int32_t>(first_array_value, &first_array);
  ArrayFromVector<Int32Type, int32_t>(second_array_value, &second_array);
  ArrayFromVector<Int32Type, int32_t>(third_array_value, &third_array);

  auto schema = arrow::schema({field("first_array", arrow::int32(), false),
                               field("second_array", arrow::int32(), false),
                               field("third_array", arrow::int32(), false)});

  auto original_record_batch =
      RecordBatch::Make(schema, 5, {first_array, second_array, third_array});

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("third_array", "test3")
                         .RenameField("second_array", "test2")
                         .RenameField("first_array", "test1")
                         .AddFieldOfNulls("empty", arrow::int32())
                         .Build();

  const std::shared_ptr<RecordBatch>& transformed_record_batch =
      transformer->Transform(original_record_batch);

  auto transformed_schema = transformed_record_batch->schema();

  // Assert to check if the empty fields was added
  ASSERT_EQ(transformed_record_batch->num_columns(), 4);

  // Assert to make sure that the elements changed his order.
  ASSERT_EQ(transformed_schema->GetFieldIndex("test3"), 0);
  ASSERT_EQ(transformed_schema->GetFieldIndex("test2"), 1);
  ASSERT_EQ(transformed_schema->GetFieldIndex("test1"), 2);
  ASSERT_EQ(transformed_schema->GetFieldIndex("empty"), 3);

  // Assert to make sure that the data didn't change after renaming the arrays
  ASSERT_EQ(transformed_record_batch->GetColumnByName("test3"), third_array);
  ASSERT_EQ(transformed_record_batch->GetColumnByName("test2"), second_array);
  ASSERT_EQ(transformed_record_batch->GetColumnByName("test1"), first_array);
}
}  // namespace flight_sql
}  // namespace driver
