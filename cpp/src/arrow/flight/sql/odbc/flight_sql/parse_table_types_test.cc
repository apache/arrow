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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_statement_get_tables.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

void AssertParseTest(const std::string& input_string,
                     const std::vector<std::string>& assert_vector) {
  std::vector<std::string> table_types;

  ParseTableTypes(input_string, table_types);
  ASSERT_EQ(table_types, assert_vector);
}

TEST(TableTypeParser, ParsingWithoutSingleQuotesWithLeadingWhiteSpace) {
  AssertParseTest("TABLE, VIEW", {"TABLE", "VIEW"});
}

TEST(TableTypeParser, ParsingWithoutSingleQuotesWithoutLeadingWhiteSpace) {
  AssertParseTest("TABLE,VIEW", {"TABLE", "VIEW"});
}

TEST(TableTypeParser, ParsingWithSingleQuotesWithLeadingWhiteSpace) {
  AssertParseTest("'TABLE', 'VIEW'", {"TABLE", "VIEW"});
}

TEST(TableTypeParser, ParsingWithSingleQuotesWithoutLeadingWhiteSpace) {
  AssertParseTest("'TABLE','VIEW'", {"TABLE", "VIEW"});
}

TEST(TableTypeParser, ParsingWithCommaInsideSingleQuotes) {
  AssertParseTest("'TABLE, TEST', 'VIEW, TEMPORARY'", {"TABLE, TEST", "VIEW, TEMPORARY"});
}
}  // namespace flight_sql
}  // namespace driver
