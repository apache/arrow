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

// Tests for GH-50578: FlightSqlResultSetMetadata must not crash or throw when
// the server omits dataset_schema in ActionCreatePreparedStatementResult.
// This happens with servers like InfluxDB 3 that don't return schema at
// prepare time — the Windows ODBC driver uses CommandBehavior.SchemaOnly
// which calls SQLPrepare without SQLExecute, so if Prepare() crashes the
// resulting is_prepared_=false state causes a HY010 Function sequence error
// on the next SQLExecute call.

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set_metadata.h"

#include <gtest/gtest.h>

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"
#include "arrow/type.h"

namespace driver {
namespace flight_sql {

using odbcabstraction::MetadataSettings;

static MetadataSettings DefaultMetadataSettings() {
  MetadataSettings s;
  s.chunk_buffer_capacity = 10;
  s.use_wide_char = false;
  return s;
}

// ---------------------------------------------------------------------------
// GH-50578: null schema (server omitted dataset_schema)
// ---------------------------------------------------------------------------

// When a FlightSQL server doesn't return dataset_schema in its
// ActionCreatePreparedStatementResult the Arrow C++ client leaves
// PreparedStatement::dataset_schema() as nullptr.
// FlightSqlResultSetMetadata must handle this gracefully: GetColumnCount()
// should return 0, not crash.
TEST(FlightSqlResultSetMetadataTest, NullSchemaGetColumnCountReturnsZero) {
  MetadataSettings settings = DefaultMetadataSettings();

  // Construct with nullptr schema — simulates a server that doesn't return
  // dataset_schema in ActionCreatePreparedStatementResult.
  FlightSqlResultSetMetadata metadata(std::shared_ptr<arrow::Schema>{nullptr}, settings);

  // Before the fix this crashed with a null-pointer dereference inside
  // schema_->num_fields().
  EXPECT_EQ(0u, metadata.GetColumnCount());
}

// After GetColumnCount() returns 0 the ODBC layer should not attempt to
// read individual column attributes, but be defensive and verify that
// FlightSqlResultSetMetadata does not crash on construction with nullptr.
TEST(FlightSqlResultSetMetadataTest, NullSchemaConstructionDoesNotThrow) {
  MetadataSettings settings = DefaultMetadataSettings();
  EXPECT_NO_THROW({
    FlightSqlResultSetMetadata metadata(std::shared_ptr<arrow::Schema>{nullptr}, settings);
  });
}

// ---------------------------------------------------------------------------
// Baseline: a real schema works correctly
// ---------------------------------------------------------------------------

TEST(FlightSqlResultSetMetadataTest, ValidSchemaGetColumnCount) {
  MetadataSettings settings = DefaultMetadataSettings();

  auto schema = arrow::schema({arrow::field("a", arrow::int32()),
                               arrow::field("b", arrow::utf8()),
                               arrow::field("c", arrow::float64())});

  FlightSqlResultSetMetadata metadata(schema, settings);
  EXPECT_EQ(3u, metadata.GetColumnCount());
}

TEST(FlightSqlResultSetMetadataTest, ValidSchemaGetColumnName) {
  MetadataSettings settings = DefaultMetadataSettings();

  auto schema =
      arrow::schema({arrow::field("id", arrow::int64()), arrow::field("name", arrow::utf8())});

  FlightSqlResultSetMetadata metadata(schema, settings);
  EXPECT_EQ("id", metadata.GetColumnName(1));
  EXPECT_EQ("name", metadata.GetColumnName(2));
}

}  // namespace flight_sql
}  // namespace driver
