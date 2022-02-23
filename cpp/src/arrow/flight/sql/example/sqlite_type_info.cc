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

#include "arrow/flight/sql/server.h"
#include "arrow/flight/sql/types.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

std::shared_ptr<RecordBatch> DoGetTypeInfoResult() {
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
      list(field("item", utf8(), false)),
      R"([[], [], [], [], [], ["length"], ["length"], ["length"], [], [], [], [], [], ["length"], [], [], []])");
  auto nullable =
      ArrayFromJSON(int32(), R"([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])");
  // Reference for creating a boolean() array only with zero.
  auto zero_bool_array =
      ArrayFromJSON(boolean(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  const auto& case_sensitive = zero_bool_array;
  auto searchable =
      ArrayFromJSON(int32(), R"([3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3])");
  const auto& unsigned_attribute = zero_bool_array;
  const auto& fixed_prec_scale = zero_bool_array;
  const auto& auto_unique_value = zero_bool_array;
  auto local_type_name =
      ArrayFromJSON(utf8(), R"(["bit", "tinyint", "bigint", "longvarbinary",
                          "varbinary", "text", "longvarchar", "char",
                          "integer", "smallint", "float", "double",
                          "numeric", "varchar", "date", "time",
                          "timestamp"])");
  // Reference for creating an int32() array only with zero.
  auto zero_int_array =
      ArrayFromJSON(int32(), R"([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
  const auto& minimal_scale = zero_int_array;
  const auto& maximum_scale = zero_int_array;
  const auto& sql_data_type = data_type;
  const auto& sql_datetime_sub = zero_int_array;
  const auto& num_prec_radix = zero_int_array;
  const auto& interval_precision = zero_int_array;

  return RecordBatch::Make(
      SqlSchema::GetXdbcTypeInfoSchema(), 17,
      {type_name_array, data_type, column_size, literal_prefix, literal_suffix,
       create_params, nullable, case_sensitive, searchable, unsigned_attribute,
       fixed_prec_scale, auto_unique_value, local_type_name, minimal_scale, maximum_scale,
       sql_data_type, sql_datetime_sub, num_prec_radix, interval_precision});
}

std::shared_ptr<RecordBatch> DoGetTypeInfoResult(int data_type_filter) {
  auto record_batch = DoGetTypeInfoResult();

  std::vector<int> data_type_vector{-7, -6, -5, -4, -3, -1, -1, 1, 4,
                                    5,  6,  8,  8,  12, 91, 92, 93};

  // Checking if the data_type is in the vector with the sqlite3 data types
  // and returning a slice from the vector containing the filtered values.
  auto pair = std::equal_range(data_type_vector.begin(), data_type_vector.end(),
                               data_type_filter);

  return record_batch->Slice(pair.first - data_type_vector.begin(),
                             pair.second - pair.first);
}
}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
