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

#include "arrow/compute/kernels/test_util.h"

#include "arrow/compute/api_vector.h"
#include "arrow/datum.h"
#include "arrow/util/logging.h"

namespace arrow::compute {

Result<std::shared_ptr<Table>> RunEndEncodeTableColumns(
    const Table& table, const std::vector<int>& column_indices) {
  const int num_columns = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> encoded_columns;
  encoded_columns.reserve(num_columns);
  std::vector<std::shared_ptr<Field>> encoded_fields;
  encoded_fields.reserve(num_columns);
  for (int i = 0; i < num_columns; i++) {
    const auto& field = table.schema()->field(i);
    if (std::find(column_indices.begin(), column_indices.end(), i) !=
        column_indices.end()) {
      ARROW_ASSIGN_OR_RAISE(auto run_end_encoded, RunEndEncode(table.column(i)));
      ARROW_DCHECK_EQ(run_end_encoded.kind(), Datum::CHUNKED_ARRAY);
      encoded_columns.push_back(run_end_encoded.chunked_array());
      auto encoded_type = arrow::run_end_encoded(arrow::int32(), field->type());
      encoded_fields.push_back(field->WithType(encoded_type));
    } else {
      encoded_columns.push_back(table.column(i));
      encoded_fields.push_back(field);
    }
  }
  auto updated_schema = arrow::schema(std::move(encoded_fields));
  return Table::Make(std::move(updated_schema), std::move(encoded_columns));
}

}  // namespace arrow::compute
