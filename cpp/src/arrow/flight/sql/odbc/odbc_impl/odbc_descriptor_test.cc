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

#include "arrow/flight/sql/odbc/odbc_impl/odbc_descriptor.h"

#include "arrow/flight/sql/odbc/odbc_impl/diagnostics.h"
#include "arrow/flight/sql/odbc/odbc_impl/encoding.h"
#include "arrow/flight/sql/odbc/odbc_impl/encoding_utils.h"

#include <sql.h>
#include <sqlext.h>

#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

using ODBC::ODBCDescriptor;

TEST(ODBCDescriptorTest, SetGetNameWideRoundTrips) {
  arrow::flight::sql::odbc::Diagnostics diagnostics("vendor", "component",
                                                    OdbcVersion::V_3);
  ODBCDescriptor desc(diagnostics, nullptr, nullptr, /*is_app_descriptor=*/true,
                      /*is_writable=*/true, /*is_2x_connection=*/false);

  SQLSMALLINT count = 1;
  desc.SetHeaderField(SQL_DESC_COUNT, reinterpret_cast<SQLPOINTER>(&count), 0);

  std::vector<uint8_t> wide;
  Utf8ToWcs("my_column", &wide);
  desc.SetField(1, SQL_DESC_NAME, wide.data(), static_cast<SQLINTEGER>(wide.size()));

  SQLWCHAR out[256];
  SQLINTEGER out_len = 0;
  desc.GetField(1, SQL_DESC_NAME, out, sizeof(out), &out_len);
  std::string name =
      ODBC::SqlWcharToString(out, static_cast<SQLSMALLINT>(out_len / GetSqlWCharSize()));
  EXPECT_EQ("my_column", name);
}

}  // namespace arrow::flight::sql::odbc
