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

#include "arrow/flight/sql/example/sqlite_type_info.h"

#include "arrow/flight/sql/types.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

std::shared_ptr<RecordBatch> DoGetTypeInfoResult(const std::shared_ptr<Schema>& schema) {
  auto type_name_array =
      ArrayFromJSON(utf8(), R"(["bit", "tinyint", "bigint", "longvarbinary",
                            "varbinary", "text", "longvarchar", "char",
                            "integer", "smallint", "float", "double",
                            "numeric", "varchar", "date", "time",
                            "timestamp"])");
  auto data_type = ArrayFromJSON(
      int32(), R"([-7, -6, -5, -4, -3, -1, -1, 1, 4, 5, 6, 8, 8, 12, 91, 92, 93])");
  auto column_size = ArrayFromJSON(
      int32(),
      R"([1, 3, 19, 65536, 255, 65536, 65536, 255, 9, 5, 7, 15, 15, 255, 10, 8, 32])");
  auto literal_prefix = ArrayFromJSON(
      utf8(),
      R"([null, null, null, null, null, "'", "'", "'", null, null, null, null, null, "'", "'", "'", "'"])");
  auto literal_suffix = ArrayFromJSON(
      utf8(),
      R"([null, null, null, null, null, "'", "'", "'", null, null, null, null, null, "'", "'", "'", "'"])");
  auto create_params = ArrayFromJSON(
      utf8(),
      R"([null, null, null, null, null, "length", "length", "length", null, null, null, null, null, "length", null, null, null])");
  auto nullable =
      ArrayFromJSON(int32(), R"([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])");
  auto case_sensitive =
      ArrayFromJSON(boolean(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  auto serachable =
      ArrayFromJSON(int32(), R"([3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3])");
  auto unsigned_attribute =
      ArrayFromJSON(int32(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  auto fixed_prec_scale =
      ArrayFromJSON(boolean(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  auto auto_unique_value =
      ArrayFromJSON(boolean(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  auto local_type_name =
      ArrayFromJSON(utf8(), R"(["bit", "tinyint", "bigint", "longvarbinary",
                          "varbinary", "text", "longvarchar", "char",
                          "integer", "smallint", "float", "double",
                          "numeric", "varchar", "date", "time",
                          "timestamp"])");
  auto minimal_scale =
      ArrayFromJSON(int32(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  auto maximum_scale =
      ArrayFromJSON(int32(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  auto sql_data_type =
      ArrayFromJSON(int32(), R"([-7,-6,-5,-4,-3,-1,-1,1,4,5,6,8,8,12,91,92,93])");
  auto sql_datetime_sub =
      ArrayFromJSON(int32(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  auto num_prec_radix =
      ArrayFromJSON(int32(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  auto interval_precision =
      ArrayFromJSON(int32(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");

  return RecordBatch::Make(
      schema, 17,
      {type_name_array, data_type, column_size, literal_prefix, literal_suffix,
       create_params, nullable, case_sensitive, serachable, unsigned_attribute,
       fixed_prec_scale, auto_unique_value, local_type_name, minimal_scale, maximum_scale,
       sql_data_type, sql_datetime_sub, num_prec_radix, interval_precision});
}

std::shared_ptr<RecordBatch> DoGetTypeInfoResult(const std::shared_ptr<Schema>& schema,
                                                 int data_type_filter) {
  auto record_batch = DoGetTypeInfoResult(schema);

  std::vector<int16_t> data_type_vector{-7, -6, -5, -4, -3, -1, -1, 1, 4,
                                        5,  6,  8,  8,  12, 91, 92, 93};

  // Checking if the data_type is in the vector with the sqlite3 data types
  auto it = std::find(data_type_vector.begin(), data_type_vector.end(), data_type_filter);

  int64_t begin_offset = std::distance(data_type_vector.begin(), it);

  // Check if there is more than one of the same data type in the vector, if there
  // is more than one we increase the counter.
  int16_t counter = 1;
  while (begin_offset + counter < static_cast<int64_t>(data_type_vector.size()) &&
         data_type_vector[begin_offset + counter] == data_type_filter) {
    counter++;
  }

  // The complete record batch with all the data type is sliced using the
  // begin_offset and the counter as the length,
  return record_batch->Slice(begin_offset, counter);
}
}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
