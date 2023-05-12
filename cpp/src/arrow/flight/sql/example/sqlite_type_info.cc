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

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/sql/types.h"
#include "arrow/record_batch.h"
#include "arrow/util/rows_to_batches.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

arrow::Result<std::shared_ptr<RecordBatch>> DoGetTypeInfoResult() {
  auto schema = SqlSchema::GetXdbcTypeInfoSchema();
  using ValueType =
      std::variant<bool, int32_t, std::nullptr_t, const char*, std::vector<const char*>>;
  auto VariantConverter = [](ArrayBuilder& array_builder, const ValueType& value) {
    if (std::holds_alternative<bool>(value)) {
      return dynamic_cast<BooleanBuilder&>(array_builder).Append(std::get<bool>(value));
    } else if (std::holds_alternative<int32_t>(value)) {
      return dynamic_cast<Int32Builder&>(array_builder).Append(std::get<int32_t>(value));
    } else if (std::holds_alternative<std::nullptr_t>(value)) {
      return array_builder.AppendNull();
    } else if (std::holds_alternative<const char*>(value)) {
      return dynamic_cast<StringBuilder&>(array_builder)
          .Append(std::get<const char*>(value));
    } else {
      auto& list_builder = dynamic_cast<ListBuilder&>(array_builder);
      ARROW_RETURN_NOT_OK(list_builder.Append());
      auto value_builder = dynamic_cast<StringBuilder*>(list_builder.value_builder());
      for (const auto& v : std::get<std::vector<const char*>>(value)) {
        ARROW_RETURN_NOT_OK(value_builder->Append(v));
      }
      return Status::OK();
    }
  };
  std::vector<std::vector<ValueType>> rows = {
      {
          "bit", -7,    1, nullptr, nullptr, std::vector<const char*>({}),
          1,     false, 3, false,   false,   false,
          "bit", 0,     0, -7,      0,       0,
          0,
      },
      {
          "tinyint", -6,    3, nullptr, nullptr, std::vector<const char*>({}),
          1,         false, 3, false,   false,   false,
          "tinyint", 0,     0, -6,      0,       0,
          0,
      },
      {
          "bigint", -5,    19, nullptr, nullptr, std::vector<const char*>({}),
          1,        false, 3,  false,   false,   false,
          "bigint", 0,     0,  -5,      0,       0,
          0,
      },
      {
          "longvarbinary",
          -4,
          65536,
          nullptr,
          nullptr,
          std::vector<const char*>({}),
          1,
          false,
          3,
          false,
          false,
          false,
          "longvarbinary",
          0,
          0,
          -4,
          0,
          0,
          0,
      },
      {
          "varbinary", -3,    255, nullptr, nullptr, std::vector<const char*>({}),
          1,           false, 3,   false,   false,   false,

          "varbinary", 0,     0,   -3,      0,       0,
          0,
      },
      {
          "text", -1,    65536, "'",   "'",   std::vector<const char*>({"length"}),
          1,      false, 3,     false, false, false,
          "text", 0,     0,     -1,    0,     0,
          0,
      },
      {
          "longvarchar",
          -1,
          65536,
          "'",
          "'",
          std::vector<const char*>({"length"}),
          1,
          false,
          3,
          false,
          false,
          false,
          "longvarchar",
          0,
          0,
          -1,
          0,
          0,
          0,
      },
      {
          "char", 1,     255, "'",   "'",   std::vector<const char*>({"length"}),
          1,      false, 3,   false, false, false,
          "char", 0,     0,   1,     0,     0,
          0,
      },
      {
          "integer", 4,     9, nullptr, nullptr, std::vector<const char*>({}),
          1,         false, 3, false,   false,   false,
          "integer", 0,     0, 4,       0,       0,
          0,
      },
      {
          "smallint", 5,     5, nullptr, nullptr, std::vector<const char*>({}),
          1,          false, 3, false,   false,   false,
          "smallint", 0,     0, 5,       0,       0,
          0,
      },
      {
          "float", 6,     7, nullptr, nullptr, std::vector<const char*>({}),
          1,       false, 3, false,   false,   false,
          "float", 0,     0, 6,       0,       0,
          0,
      },
      {
          "double", 8,     15, nullptr, nullptr, std::vector<const char*>({}),
          1,        false, 3,  false,   false,   false,
          "double", 0,     0,  8,       0,       0,
          0,
      },
      {
          "numeric", 8,     15, nullptr, nullptr, std::vector<const char*>({}),
          1,         false, 3,  false,   false,   false,
          "numeric", 0,     0,  8,       0,       0,
          0,
      },
      {
          "varchar", 12,    255, "'",   "'",   std::vector<const char*>({"length"}),
          1,         false, 3,   false, false, false,
          "varchar", 0,     0,   12,    0,     0,
          0,
      },
      {
          "date", 91,    10, "'",   "'",   std::vector<const char*>({}),
          1,      false, 3,  false, false, false,
          "date", 0,     0,  91,    0,     0,
          0,
      },
      {
          "time", 92,    8, "'",   "'",   std::vector<const char*>({}),
          1,      false, 3, false, false, false,
          "time", 0,     0, 92,    0,     0,
          0,
      },
      {
          "timestamp", 93,    32, "'",   "'",   std::vector<const char*>({}),
          1,           false, 3,  false, false, false,
          "timestamp", 0,     0,  93,    0,     0,
          0,
      },
  };
  ARROW_ASSIGN_OR_RAISE(auto reader, RowsToBatches(schema, rows, VariantConverter));
  return reader->Next();
}

arrow::Result<std::shared_ptr<RecordBatch>> DoGetTypeInfoResult(int data_type_filter) {
  ARROW_ASSIGN_OR_RAISE(auto record_batch, DoGetTypeInfoResult());

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
