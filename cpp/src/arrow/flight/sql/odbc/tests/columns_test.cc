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
#include "arrow/flight/sql/odbc/tests/odbc_test_suite.h"

#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

template <typename T>
class ColumnsTest : public T {};

class ColumnsMockTest : public FlightSQLODBCMockTestBase {};
class ColumnsRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<ColumnsMockTest, ColumnsRemoteTest>;
TYPED_TEST_SUITE(ColumnsTest, TestTypes);

template <typename T>
class ColumnsOdbcV2Test : public T {};

class ColumnsOdbcV2MockTest : public FlightSQLOdbcV2MockTestBase {};
class ColumnsOdbcV2RemoteTest : public FlightSQLOdbcV2RemoteTestBase {};
using TestTypesOdbcV2 = ::testing::Types<ColumnsOdbcV2MockTest, ColumnsOdbcV2RemoteTest>;
TYPED_TEST_SUITE(ColumnsOdbcV2Test, TestTypesOdbcV2);

namespace {
// Helper functions

void CheckSQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT idx,
                          const std::wstring& expected_column_name,
                          SQLLEN expected_data_type, SQLLEN expected_concise_type,
                          SQLLEN expected_display_size, SQLLEN expected_prec_scale,
                          SQLLEN expected_length,
                          const std::wstring& expected_literal_prefix,
                          const std::wstring& expected_literal_suffix,
                          SQLLEN expected_column_size, SQLLEN expected_column_scale,
                          SQLLEN expected_column_nullability,
                          SQLLEN expected_num_prec_radix, SQLLEN expected_octet_length,
                          SQLLEN expected_searchable, SQLLEN expected_unsigned_column) {
  std::vector<SQLWCHAR> name(kOdbcBufferSize);
  SQLSMALLINT name_len = 0;
  std::vector<SQLWCHAR> base_column_name(kOdbcBufferSize);
  SQLSMALLINT column_name_len = 0;
  std::vector<SQLWCHAR> label(kOdbcBufferSize);
  SQLSMALLINT label_len = 0;
  std::vector<SQLWCHAR> prefix(kOdbcBufferSize);
  SQLSMALLINT prefix_len = 0;
  std::vector<SQLWCHAR> suffix(kOdbcBufferSize);
  SQLSMALLINT suffix_len = 0;
  SQLLEN data_type = 0;
  SQLLEN concise_type = 0;
  SQLLEN display_size = 0;
  SQLLEN prec_scale = 0;
  SQLLEN length = 0;
  SQLLEN size = 0;
  SQLLEN scale = 0;
  SQLLEN nullability = 0;
  SQLLEN num_prec_radix = 0;
  SQLLEN octet_length = 0;
  SQLLEN searchable = 0;
  SQLLEN unsigned_col = 0;

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_NAME, &name[0],
                                         (SQLSMALLINT)name.size(), &name_len, 0));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_BASE_COLUMN_NAME, &base_column_name[0],
                            (SQLSMALLINT)base_column_name.size(), &column_name_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_LABEL, &label[0],
                                         (SQLSMALLINT)label.size(), &label_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_TYPE, 0, 0, 0, &data_type));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_CONCISE_TYPE, 0, 0, 0, &concise_type));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_DISPLAY_SIZE, 0, 0, 0, &display_size));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_FIXED_PREC_SCALE, 0, 0, 0, &prec_scale));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_LENGTH, 0, 0, 0, &length));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_LITERAL_PREFIX, &prefix[0],
                                         (SQLSMALLINT)prefix.size(), &prefix_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_LITERAL_SUFFIX, &suffix[0],
                                         (SQLSMALLINT)suffix.size(), &suffix_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_PRECISION, 0, 0, 0, &size));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_SCALE, 0, 0, 0, &scale));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_NULLABLE, 0, 0, 0, &nullability));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_NUM_PREC_RADIX, 0, 0, 0,
                                         &num_prec_radix));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_OCTET_LENGTH, 0, 0, 0, &octet_length));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_SEARCHABLE, 0, 0, 0, &searchable));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_UNSIGNED, 0, 0, 0, &unsigned_col));

  std::wstring name_str = ConvertToWString(name, name_len);
  std::wstring base_column_name_str = ConvertToWString(base_column_name, column_name_len);
  std::wstring label_str = ConvertToWString(label, label_len);
  std::wstring prefixStr = ConvertToWString(prefix, prefix_len);

  // Assume column name, base column name, and label are equivalent in the result set
  EXPECT_EQ(expected_column_name, name_str);
  EXPECT_EQ(expected_column_name, base_column_name_str);
  EXPECT_EQ(expected_column_name, label_str);
  EXPECT_EQ(expected_data_type, data_type);
  EXPECT_EQ(expected_concise_type, concise_type);
  EXPECT_EQ(expected_display_size, display_size);
  EXPECT_EQ(expected_prec_scale, prec_scale);
  EXPECT_EQ(expected_length, length);
  EXPECT_EQ(expected_literal_prefix, prefixStr);
  EXPECT_EQ(expected_column_size, size);
  EXPECT_EQ(expected_column_scale, scale);
  EXPECT_EQ(expected_column_nullability, nullability);
  EXPECT_EQ(expected_num_prec_radix, num_prec_radix);
  EXPECT_EQ(expected_octet_length, octet_length);
  EXPECT_EQ(expected_searchable, searchable);
  EXPECT_EQ(expected_unsigned_column, unsigned_col);
}

void CheckSQLColAttributes(SQLHSTMT stmt, SQLUSMALLINT idx,
                           const std::wstring& expected_column_name,
                           SQLLEN expected_data_type, SQLLEN expected_display_size,
                           SQLLEN expected_prec_scale, SQLLEN expected_length,
                           SQLLEN expected_column_size, SQLLEN expected_column_scale,
                           SQLLEN expected_column_nullability, SQLLEN expected_searchable,
                           SQLLEN expected_unsigned_column) {
  std::vector<SQLWCHAR> name(kOdbcBufferSize);
  SQLSMALLINT name_len = 0;
  std::vector<SQLWCHAR> label(kOdbcBufferSize);
  SQLSMALLINT label_len = 0;
  SQLLEN data_type = 0;
  SQLLEN display_size = 0;
  SQLLEN prec_scale = 0;
  SQLLEN length = 0;
  SQLLEN size = 0;
  SQLLEN scale = 0;
  SQLLEN nullability = 0;
  SQLLEN searchable = 0;
  SQLLEN unsigned_col = 0;

  EXPECT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, SQL_COLUMN_NAME, &name[0],
                                          (SQLSMALLINT)name.size(), &name_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, SQL_COLUMN_LABEL, &label[0],
                                          (SQLSMALLINT)label.size(), &label_len, 0));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_TYPE, 0, 0, 0, &data_type));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_DISPLAY_SIZE, 0, 0, 0, &display_size));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_COLUMN_MONEY, 0, 0, 0, &prec_scale));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_LENGTH, 0, 0, 0, &length));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_PRECISION, 0, 0, 0, &size));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, SQL_COLUMN_SCALE, 0, 0, 0, &scale));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_NULLABLE, 0, 0, 0, &nullability));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_SEARCHABLE, 0, 0, 0, &searchable));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_UNSIGNED, 0, 0, 0, &unsigned_col));

  std::wstring name_str = ConvertToWString(name, name_len);
  std::wstring label_str = ConvertToWString(label, label_len);

  EXPECT_EQ(expected_column_name, name_str);
  EXPECT_EQ(expected_column_name, label_str);
  EXPECT_EQ(expected_data_type, data_type);
  EXPECT_EQ(expected_display_size, display_size);
  EXPECT_EQ(expected_length, length);
  EXPECT_EQ(expected_column_size, size);
  EXPECT_EQ(expected_column_scale, scale);
  EXPECT_EQ(expected_column_nullability, nullability);
  EXPECT_EQ(expected_searchable, searchable);
  EXPECT_EQ(expected_unsigned_column, unsigned_col);
}

void GetSQLColAttributeString(SQLHSTMT stmt, const std::wstring& wsql, SQLUSMALLINT idx,
                              SQLUSMALLINT field_identifier, std::wstring& value) {
  if (!wsql.empty()) {
    // Execute query
    std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
    ASSERT_EQ(SQL_SUCCESS,
              SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

    ASSERT_EQ(SQL_SUCCESS, SQLFetch(stmt));
  }

  // check SQLColAttribute string attribute
  std::vector<SQLWCHAR> str_val(kOdbcBufferSize);
  SQLSMALLINT str_len = 0;

  ASSERT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, field_identifier, &str_val[0],
                                         (SQLSMALLINT)str_val.size(), &str_len, 0));

  value = ConvertToWString(str_val, str_len);
}

void GetSQLColAttributesString(SQLHSTMT stmt, const std::wstring& wsql, SQLUSMALLINT idx,
                               SQLUSMALLINT field_identifier, std::wstring& value) {
  if (!wsql.empty()) {
    // Execute query
    std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
    ASSERT_EQ(SQL_SUCCESS,
              SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

    ASSERT_EQ(SQL_SUCCESS, SQLFetch(stmt));
  }

  // check SQLColAttribute string attribute
  std::vector<SQLWCHAR> str_val(kOdbcBufferSize);
  SQLSMALLINT str_len = 0;

  ASSERT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, field_identifier, &str_val[0],
                                          (SQLSMALLINT)str_val.size(), &str_len, 0));

  value = ConvertToWString(str_val, str_len);
}

void GetSQLColAttributeNumeric(SQLHSTMT stmt, const std::wstring& wsql, SQLUSMALLINT idx,
                               SQLUSMALLINT field_identifier, SQLLEN* value) {
  // Execute query and check SQLColAttribute numeric attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(stmt));

  SQLLEN num_val = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, field_identifier, 0, 0, 0, value));
}

void GetSQLColAttributesNumeric(SQLHSTMT stmt, const std::wstring& wsql, SQLUSMALLINT idx,
                                SQLUSMALLINT field_identifier, SQLLEN* value) {
  // Execute query and check SQLColAttribute numeric attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(stmt));

  SQLLEN num_val = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, field_identifier, 0, 0, 0, value));
}
}  // namespace

TYPED_TEST(ColumnsTest, SQLColAttributeTestInputData) {
  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLUSMALLINT idx = 1;
  std::vector<SQLWCHAR> character_attr(kOdbcBufferSize);
  SQLSMALLINT character_attr_len = 0;
  SQLLEN numeric_attr = 0;

  // All character values populated
  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(this->stmt, idx, SQL_DESC_NAME, &character_attr[0],
                            (SQLSMALLINT)character_attr.size(), &character_attr_len, 0));

  // All numeric values populated
  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(this->stmt, idx, SQL_DESC_COUNT, 0, 0, 0, &numeric_attr));

  // Pass null values, driver should not throw error
  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(this->stmt, idx, SQL_COLUMN_TABLE_NAME, 0, 0, 0, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(this->stmt, idx, SQL_DESC_COUNT, 0, 0, 0, 0));
}

TYPED_TEST(ColumnsTest, SQLColAttributeGetCharacterLen) {
  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLSMALLINT character_attr_len = 0;

  // Check length of character attribute
  ASSERT_EQ(SQL_SUCCESS, SQLColAttribute(this->stmt, 1, SQL_DESC_BASE_COLUMN_NAME, 0, 0,
                                         &character_attr_len, 0));
  EXPECT_EQ(4 * ODBC::GetSqlWCharSize(), character_attr_len);
}

TYPED_TEST(ColumnsTest, SQLColAttributeInvalidFieldId) {
  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLUSMALLINT invalid_field_id = -100;
  SQLUSMALLINT idx = 1;
  std::vector<SQLWCHAR> character_attr(kOdbcBufferSize);
  SQLSMALLINT character_attr_len = 0;
  SQLLEN numeric_attr = 0;

  ASSERT_EQ(SQL_ERROR,
            SQLColAttribute(this->stmt, idx, invalid_field_id, &character_attr[0],
                            (SQLSMALLINT)character_attr.size(), &character_attr_len, 0));
  // Verify invalid descriptor field identifier error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY091);
}

TYPED_TEST(ColumnsTest, SQLColAttributeInvalidColId) {
  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLUSMALLINT invalid_col_id = 2;
  std::vector<SQLWCHAR> character_attr(kOdbcBufferSize);
  SQLSMALLINT character_attr_len = 0;

  ASSERT_EQ(SQL_ERROR,
            SQLColAttribute(this->stmt, invalid_col_id, SQL_DESC_BASE_COLUMN_NAME,
                            &character_attr[0], (SQLSMALLINT)character_attr.size(),
                            &character_attr_len, 0));
  // Verify invalid descriptor index error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState07009);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeAllTypes) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckSQLColAttribute(this->stmt, 1,
                       std::wstring(L"bigint_col"),  // expected_column_name
                       SQL_BIGINT,                   // expected_data_type
                       SQL_BIGINT,                   // expected_concise_type
                       20,                           // expected_display_size
                       SQL_FALSE,                    // expected_prec_scale
                       8,                            // expected_length
                       std::wstring(L""),            // expected_literal_prefix
                       std::wstring(L""),            // expected_literal_suffix
                       8,                            // expected_column_size
                       0,                            // expected_column_scale
                       SQL_NULLABLE,                 // expected_column_nullability
                       10,                           // expected_num_prec_radix
                       8,                            // expected_octet_length
                       SQL_PRED_NONE,                // expected_searchable
                       SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 2,
                       std::wstring(L"char_col"),  // expected_column_name
                       SQL_WVARCHAR,               // expected_data_type
                       SQL_WVARCHAR,               // expected_concise_type
                       0,                          // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       0,                          // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       0,                          // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       0,                          // expected_octet_length
                       SQL_PRED_NONE,              // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 3,
                       std::wstring(L"varbinary_col"),  // expected_column_name
                       SQL_BINARY,                      // expected_data_type
                       SQL_BINARY,                      // expected_concise_type
                       0,                               // expected_display_size
                       SQL_FALSE,                       // expected_prec_scale
                       0,                               // expected_length
                       std::wstring(L""),               // expected_literal_prefix
                       std::wstring(L""),               // expected_literal_suffix
                       0,                               // expected_column_size
                       0,                               // expected_column_scale
                       SQL_NULLABLE,                    // expected_column_nullability
                       0,                               // expected_num_prec_radix
                       0,                               // expected_octet_length
                       SQL_PRED_NONE,                   // expected_searchable
                       SQL_TRUE);                       // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 4,
                       std::wstring(L"double_col"),  // expected_column_name
                       SQL_DOUBLE,                   // expected_data_type
                       SQL_DOUBLE,                   // expected_concise_type
                       24,                           // expected_display_size
                       SQL_FALSE,                    // expected_prec_scale
                       8,                            // expected_length
                       std::wstring(L""),            // expected_literal_prefix
                       std::wstring(L""),            // expected_literal_suffix
                       8,                            // expected_column_size
                       0,                            // expected_column_scale
                       SQL_NULLABLE,                 // expected_column_nullability
                       2,                            // expected_num_prec_radix
                       8,                            // expected_octet_length
                       SQL_PRED_NONE,                // expected_searchable
                       SQL_FALSE);                   // expected_unsigned_column
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesAllTypesODBCVer2) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));
  CheckSQLColAttributes(this->stmt, 1,
                        std::wstring(L"bigint_col"),  // expected_column_name
                        SQL_BIGINT,                   // expected_data_type
                        20,                           // expected_display_size
                        SQL_FALSE,                    // expected_prec_scale
                        8,                            // expected_length
                        8,                            // expected_column_size
                        0,                            // expected_column_scale
                        SQL_NULLABLE,                 // expected_column_nullability
                        SQL_PRED_NONE,                // expected_searchable
                        SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 2,
                        std::wstring(L"char_col"),  // expected_column_name
                        SQL_WVARCHAR,               // expected_data_type
                        0,                          // expected_display_size
                        SQL_FALSE,                  // expected_prec_scale
                        0,                          // expected_length
                        0,                          // expected_column_size
                        0,                          // expected_column_scale
                        SQL_NULLABLE,               // expected_column_nullability
                        SQL_PRED_NONE,              // expected_searchable
                        SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 3,
                        std::wstring(L"varbinary_col"),  // expected_column_name
                        SQL_BINARY,                      // expected_data_type
                        0,                               // expected_display_size
                        SQL_FALSE,                       // expected_prec_scale
                        0,                               // expected_length
                        0,                               // expected_column_size
                        0,                               // expected_column_scale
                        SQL_NULLABLE,                    // expected_column_nullability
                        SQL_PRED_NONE,                   // expected_searchable
                        SQL_TRUE);                       // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 4,
                        std::wstring(L"double_col"),  // expected_column_name
                        SQL_DOUBLE,                   // expected_data_type
                        24,                           // expected_display_size
                        SQL_FALSE,                    // expected_prec_scale
                        8,                            // expected_length
                        8,                            // expected_column_size
                        0,                            // expected_column_scale
                        SQL_NULLABLE,                 // expected_column_nullability
                        SQL_PRED_NONE,                // expected_searchable
                        SQL_FALSE);                   // expected_unsigned_column
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeAllTypes) {
  // Test assumes there is a table $scratch.ODBCTest in remote server

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckSQLColAttribute(this->stmt, 1,
                       std::wstring(L"sinteger_max"),  // expected_column_name
                       SQL_INTEGER,                    // expected_data_type
                       SQL_INTEGER,                    // expected_concise_type
                       11,                             // expected_display_size
                       SQL_FALSE,                      // expected_prec_scale
                       4,                              // expected_length
                       std::wstring(L""),              // expected_literal_prefix
                       std::wstring(L""),              // expected_literal_suffix
                       4,                              // expected_column_size
                       0,                              // expected_column_scale
                       SQL_NULLABLE,                   // expected_column_nullability
                       10,                             // expected_num_prec_radix
                       4,                              // expected_octet_length
                       SQL_SEARCHABLE,                 // expected_searchable
                       SQL_FALSE);                     // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 2,
                       std::wstring(L"sbigint_max"),  // expected_column_name
                       SQL_BIGINT,                    // expected_data_type
                       SQL_BIGINT,                    // expected_concise_type
                       20,                            // expected_display_size
                       SQL_FALSE,                     // expected_prec_scale
                       8,                             // expected_length
                       std::wstring(L""),             // expected_literal_prefix
                       std::wstring(L""),             // expected_literal_suffix
                       8,                             // expected_column_size
                       0,                             // expected_column_scale
                       SQL_NULLABLE,                  // expected_column_nullability
                       10,                            // expected_num_prec_radix
                       8,                             // expected_octet_length
                       SQL_SEARCHABLE,                // expected_searchable
                       SQL_FALSE);                    // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 3,
                       std::wstring(L"decimal_positive"),  // expected_column_name
                       SQL_DECIMAL,                        // expected_data_type
                       SQL_DECIMAL,                        // expected_concise_type
                       40,                                 // expected_display_size
                       SQL_FALSE,                          // expected_prec_scale
                       19,                                 // expected_length
                       std::wstring(L""),                  // expected_literal_prefix
                       std::wstring(L""),                  // expected_literal_suffix
                       19,                                 // expected_column_size
                       0,                                  // expected_column_scale
                       SQL_NULLABLE,                       // expected_column_nullability
                       10,                                 // expected_num_prec_radix
                       40,                                 // expected_octet_length
                       SQL_SEARCHABLE,                     // expected_searchable
                       SQL_FALSE);                         // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 4,
                       std::wstring(L"float_max"),  // expected_column_name
                       SQL_FLOAT,                   // expected_data_type
                       SQL_FLOAT,                   // expected_concise_type
                       24,                          // expected_display_size
                       SQL_FALSE,                   // expected_prec_scale
                       8,                           // expected_length
                       std::wstring(L""),           // expected_literal_prefix
                       std::wstring(L""),           // expected_literal_suffix
                       8,                           // expected_column_size
                       0,                           // expected_column_scale
                       SQL_NULLABLE,                // expected_column_nullability
                       2,                           // expected_num_prec_radix
                       8,                           // expected_octet_length
                       SQL_SEARCHABLE,              // expected_searchable
                       SQL_FALSE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 5,
                       std::wstring(L"double_max"),  // expected_column_name
                       SQL_DOUBLE,                   // expected_data_type
                       SQL_DOUBLE,                   // expected_concise_type
                       24,                           // expected_display_size
                       SQL_FALSE,                    // expected_prec_scale
                       8,                            // expected_length
                       std::wstring(L""),            // expected_literal_prefix
                       std::wstring(L""),            // expected_literal_suffix
                       8,                            // expected_column_size
                       0,                            // expected_column_scale
                       SQL_NULLABLE,                 // expected_column_nullability
                       2,                            // expected_num_prec_radix
                       8,                            // expected_octet_length
                       SQL_SEARCHABLE,               // expected_searchable
                       SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 6,
                       std::wstring(L"bit_true"),  // expected_column_name
                       SQL_BIT,                    // expected_data_type
                       SQL_BIT,                    // expected_concise_type
                       1,                          // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       1,                          // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       1,                          // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       1,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 7,
                       std::wstring(L"date_max"),  // expected_column_name
                       SQL_DATETIME,               // expected_data_type
                       SQL_TYPE_DATE,              // expected_concise_type
                       10,                         // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       10,                         // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       10,                         // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       6,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 8,
                       std::wstring(L"time_max"),  // expected_column_name
                       SQL_DATETIME,               // expected_data_type
                       SQL_TYPE_TIME,              // expected_concise_type
                       12,                         // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       12,                         // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       12,                         // expected_column_size
                       3,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       6,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 9,
                       std::wstring(L"timestamp_max"),  // expected_column_name
                       SQL_DATETIME,                    // expected_data_type
                       SQL_TYPE_TIMESTAMP,              // expected_concise_type
                       23,                              // expected_display_size
                       SQL_FALSE,                       // expected_prec_scale
                       23,                              // expected_length
                       std::wstring(L""),               // expected_literal_prefix
                       std::wstring(L""),               // expected_literal_suffix
                       23,                              // expected_column_size
                       3,                               // expected_column_scale
                       SQL_NULLABLE,                    // expected_column_nullability
                       0,                               // expected_num_prec_radix
                       16,                              // expected_octet_length
                       SQL_SEARCHABLE,                  // expected_searchable
                       SQL_TRUE);                       // expected_unsigned_column
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributeAllTypesODBCVer2) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckSQLColAttribute(this->stmt, 1,
                       std::wstring(L"sinteger_max"),  // expected_column_name
                       SQL_INTEGER,                    // expected_data_type
                       SQL_INTEGER,                    // expected_concise_type
                       11,                             // expected_display_size
                       SQL_FALSE,                      // expected_prec_scale
                       4,                              // expected_length
                       std::wstring(L""),              // expected_literal_prefix
                       std::wstring(L""),              // expected_literal_suffix
                       4,                              // expected_column_size
                       0,                              // expected_column_scale
                       SQL_NULLABLE,                   // expected_column_nullability
                       10,                             // expected_num_prec_radix
                       4,                              // expected_octet_length
                       SQL_SEARCHABLE,                 // expected_searchable
                       SQL_FALSE);                     // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 2,
                       std::wstring(L"sbigint_max"),  // expected_column_name
                       SQL_BIGINT,                    // expected_data_type
                       SQL_BIGINT,                    // expected_concise_type
                       20,                            // expected_display_size
                       SQL_FALSE,                     // expected_prec_scale
                       8,                             // expected_length
                       std::wstring(L""),             // expected_literal_prefix
                       std::wstring(L""),             // expected_literal_suffix
                       8,                             // expected_column_size
                       0,                             // expected_column_scale
                       SQL_NULLABLE,                  // expected_column_nullability
                       10,                            // expected_num_prec_radix
                       8,                             // expected_octet_length
                       SQL_SEARCHABLE,                // expected_searchable
                       SQL_FALSE);                    // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 3,
                       std::wstring(L"decimal_positive"),  // expected_column_name
                       SQL_DECIMAL,                        // expected_data_type
                       SQL_DECIMAL,                        // expected_concise_type
                       40,                                 // expected_display_size
                       SQL_FALSE,                          // expected_prec_scale
                       19,                                 // expected_length
                       std::wstring(L""),                  // expected_literal_prefix
                       std::wstring(L""),                  // expected_literal_suffix
                       19,                                 // expected_column_size
                       0,                                  // expected_column_scale
                       SQL_NULLABLE,                       // expected_column_nullability
                       10,                                 // expected_num_prec_radix
                       40,                                 // expected_octet_length
                       SQL_SEARCHABLE,                     // expected_searchable
                       SQL_FALSE);                         // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 4,
                       std::wstring(L"float_max"),  // expected_column_name
                       SQL_FLOAT,                   // expected_data_type
                       SQL_FLOAT,                   // expected_concise_type
                       24,                          // expected_display_size
                       SQL_FALSE,                   // expected_prec_scale
                       8,                           // expected_length
                       std::wstring(L""),           // expected_literal_prefix
                       std::wstring(L""),           // expected_literal_suffix
                       8,                           // expected_column_size
                       0,                           // expected_column_scale
                       SQL_NULLABLE,                // expected_column_nullability
                       2,                           // expected_num_prec_radix
                       8,                           // expected_octet_length
                       SQL_SEARCHABLE,              // expected_searchable
                       SQL_FALSE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 5,
                       std::wstring(L"double_max"),  // expected_column_name
                       SQL_DOUBLE,                   // expected_data_type
                       SQL_DOUBLE,                   // expected_concise_type
                       24,                           // expected_display_size
                       SQL_FALSE,                    // expected_prec_scale
                       8,                            // expected_length
                       std::wstring(L""),            // expected_literal_prefix
                       std::wstring(L""),            // expected_literal_suffix
                       8,                            // expected_column_size
                       0,                            // expected_column_scale
                       SQL_NULLABLE,                 // expected_column_nullability
                       2,                            // expected_num_prec_radix
                       8,                            // expected_octet_length
                       SQL_SEARCHABLE,               // expected_searchable
                       SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 6,
                       std::wstring(L"bit_true"),  // expected_column_name
                       SQL_BIT,                    // expected_data_type
                       SQL_BIT,                    // expected_concise_type
                       1,                          // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       1,                          // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       1,                          // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       1,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 7,
                       std::wstring(L"date_max"),  // expected_column_name
                       SQL_DATETIME,               // expected_data_type
                       SQL_DATE,                   // expected_concise_type
                       10,                         // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       10,                         // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       10,                         // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       6,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 8,
                       std::wstring(L"time_max"),  // expected_column_name
                       SQL_DATETIME,               // expected_data_type
                       SQL_TIME,                   // expected_concise_type
                       12,                         // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       12,                         // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       12,                         // expected_column_size
                       3,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       6,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 9,
                       std::wstring(L"timestamp_max"),  // expected_column_name
                       SQL_DATETIME,                    // expected_data_type
                       SQL_TIMESTAMP,                   // expected_concise_type
                       23,                              // expected_display_size
                       SQL_FALSE,                       // expected_prec_scale
                       23,                              // expected_length
                       std::wstring(L""),               // expected_literal_prefix
                       std::wstring(L""),               // expected_literal_suffix
                       23,                              // expected_column_size
                       3,                               // expected_column_scale
                       SQL_NULLABLE,                    // expected_column_nullability
                       0,                               // expected_num_prec_radix
                       16,                              // expected_octet_length
                       SQL_SEARCHABLE,                  // expected_searchable
                       SQL_TRUE);                       // expected_unsigned_column
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributesAllTypesODBCVer2) {
  // Tests ODBC 2.0 API SQLColAttributes
  // Test assumes there is a table $scratch.ODBCTest in remote server
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckSQLColAttributes(this->stmt, 1,
                        std::wstring(L"sinteger_max"),  // expected_column_name
                        SQL_INTEGER,                    // expected_data_type
                        11,                             // expected_display_size
                        SQL_FALSE,                      // expected_prec_scale
                        4,                              // expected_length
                        4,                              // expected_column_size
                        0,                              // expected_column_scale
                        SQL_NULLABLE,                   // expected_column_nullability
                        SQL_SEARCHABLE,                 // expected_searchable
                        SQL_FALSE);                     // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 2,
                        std::wstring(L"sbigint_max"),  // expected_column_name
                        SQL_BIGINT,                    // expected_data_type
                        20,                            // expected_display_size
                        SQL_FALSE,                     // expected_prec_scale
                        8,                             // expected_length
                        8,                             // expected_column_size
                        0,                             // expected_column_scale
                        SQL_NULLABLE,                  // expected_column_nullability
                        SQL_SEARCHABLE,                // expected_searchable
                        SQL_FALSE);                    // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 3,
                        std::wstring(L"decimal_positive"),  // expected_column_name
                        SQL_DECIMAL,                        // expected_data_type
                        40,                                 // expected_display_size
                        SQL_FALSE,                          // expected_prec_scale
                        19,                                 // expected_length
                        19,                                 // expected_column_size
                        0,                                  // expected_column_scale
                        SQL_NULLABLE,                       // expected_column_nullability
                        SQL_SEARCHABLE,                     // expected_searchable
                        SQL_FALSE);                         // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 4,
                        std::wstring(L"float_max"),  // expected_column_name
                        SQL_FLOAT,                   // expected_data_type
                        24,                          // expected_display_size
                        SQL_FALSE,                   // expected_prec_scale
                        8,                           // expected_length
                        8,                           // expected_column_size
                        0,                           // expected_column_scale
                        SQL_NULLABLE,                // expected_column_nullability
                        SQL_SEARCHABLE,              // expected_searchable
                        SQL_FALSE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 5,
                        std::wstring(L"double_max"),  // expected_column_name
                        SQL_DOUBLE,                   // expected_data_type
                        24,                           // expected_display_size
                        SQL_FALSE,                    // expected_prec_scale
                        8,                            // expected_length
                        8,                            // expected_column_size
                        0,                            // expected_column_scale
                        SQL_NULLABLE,                 // expected_column_nullability
                        SQL_SEARCHABLE,               // expected_searchable
                        SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 6,
                        std::wstring(L"bit_true"),  // expected_column_name
                        SQL_BIT,                    // expected_data_type
                        1,                          // expected_display_size
                        SQL_FALSE,                  // expected_prec_scale
                        1,                          // expected_length
                        1,                          // expected_column_size
                        0,                          // expected_column_scale
                        SQL_NULLABLE,               // expected_column_nullability
                        SQL_SEARCHABLE,             // expected_searchable
                        SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 7,
                        std::wstring(L"date_max"),  // expected_column_name
                        SQL_DATE,                   // expected_data_type
                        10,                         // expected_display_size
                        SQL_FALSE,                  // expected_prec_scale
                        10,                         // expected_length
                        10,                         // expected_column_size
                        0,                          // expected_column_scale
                        SQL_NULLABLE,               // expected_column_nullability
                        SQL_SEARCHABLE,             // expected_searchable
                        SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 8,
                        std::wstring(L"time_max"),  // expected_column_name
                        SQL_TIME,                   // expected_data_type
                        12,                         // expected_display_size
                        SQL_FALSE,                  // expected_prec_scale
                        12,                         // expected_length
                        12,                         // expected_column_size
                        3,                          // expected_column_scale
                        SQL_NULLABLE,               // expected_column_nullability
                        SQL_SEARCHABLE,             // expected_searchable
                        SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 9,
                        std::wstring(L"timestamp_max"),  // expected_column_name
                        SQL_TIMESTAMP,                   // expected_data_type
                        23,                              // expected_display_size
                        SQL_FALSE,                       // expected_prec_scale
                        23,                              // expected_length
                        23,                              // expected_column_size
                        3,                               // expected_column_scale
                        SQL_NULLABLE,                    // expected_column_nullability
                        SQL_SEARCHABLE,                  // expected_searchable
                        SQL_TRUE);                       // expected_unsigned_column
}

TYPED_TEST(ColumnsTest, TestSQLColAttributeCaseSensitive) {
  // Arrow limitation: returns SQL_FALSE for case sensitive column

  std::wstring wsql = this->GetQueryAllDataTypes();
  // Int column
  SQLLEN value;
  GetSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_CASE_SENSITIVE, &value);
  ASSERT_EQ(SQL_FALSE, value);
  SQLFreeStmt(this->stmt, SQL_CLOSE);
  // Varchar column
  GetSQLColAttributeNumeric(this->stmt, wsql, 28, SQL_DESC_CASE_SENSITIVE, &value);
  ASSERT_EQ(SQL_FALSE, value);
}

TYPED_TEST(ColumnsOdbcV2Test, TestSQLColAttributesCaseSensitive) {
  // Arrow limitation: returns SQL_FALSE for case sensitive column
  // Tests ODBC 2.0 API SQLColAttributes

  std::wstring wsql = this->GetQueryAllDataTypes();
  // Int column
  SQLLEN value;
  GetSQLColAttributesNumeric(this->stmt, wsql, 1, SQL_COLUMN_CASE_SENSITIVE, &value);
  ASSERT_EQ(SQL_FALSE, value);
  SQLFreeStmt(this->stmt, SQL_CLOSE);
  // Varchar column
  GetSQLColAttributesNumeric(this->stmt, wsql, 28, SQL_COLUMN_CASE_SENSITIVE, &value);
  ASSERT_EQ(SQL_FALSE, value);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeUniqueValue) {
  // Mock server limitation: returns false for auto-increment column
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  SQLLEN value;
  GetSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_AUTO_UNIQUE_VALUE, &value);
  ASSERT_EQ(SQL_FALSE, value);
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesAutoIncrement) {
  // Tests ODBC 2.0 API SQLColAttributes
  // Mock server limitation: returns false for auto-increment column
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  SQLLEN value;
  GetSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_COLUMN_AUTO_INCREMENT, &value);
  ASSERT_EQ(SQL_FALSE, value);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeBaseTableName) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_BASE_TABLE_NAME, value);
  ASSERT_EQ(std::wstring(L"AllTypesTable"), value);
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesTableName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::wstring value;
  GetSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TABLE_NAME, value);
  ASSERT_EQ(std::wstring(L"AllTypesTable"), value);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeCatalogName) {
  // Mock server limitattion: mock doesn't return catalog for result metadata,
  // and the defautl catalog should be 'main'
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_CATALOG_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeCatalogName) {
  // Remote server does not have catalogs

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_CATALOG_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesQualifierName) {
  // Mock server limitattion: mock doesn't return catalog for result metadata,
  // and the defautl catalog should be 'main'
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_COLUMN_QUALIFIER_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributesQualifierName) {
  // Remote server does not have catalogs
  // Tests ODBC 2.0 API SQLColAttributes
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_COLUMN_QUALIFIER_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TYPED_TEST(ColumnsTest, TestSQLColAttributeCount) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  // Pass 0 as column number, driver should ignore it
  SQLLEN value;
  GetSQLColAttributeNumeric(this->stmt, wsql, 0, SQL_DESC_COUNT, &value);
  ASSERT_EQ(32, value);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeLocalTypeName) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  // Mock server doesn't have local type name
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_LOCAL_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeLocalTypeName) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  std::wstring value;
  GetSQLColAttributesString(this->stmt, wsql, 1, SQL_DESC_LOCAL_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"INTEGER"), value);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeSchemaName) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't have schemas
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_SCHEMA_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeSchemaName) {
  // Test assumes there is a table $scratch.ODBCTest in remote server

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  // Remote server limitation: doesn't return schema name, expected schema name is
  // $scratch
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_SCHEMA_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesOwnerName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't have schemas
  std::wstring value;
  GetSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_OWNER_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributesOwnerName) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  // Tests ODBC 2.0 API SQLColAttributes
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  // Remote server limitation: doesn't return schema name, expected schema name is
  // $scratch
  std::wstring value;
  GetSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_OWNER_NAME, value);
  ASSERT_EQ(std::wstring(L""), value);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeTableName) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TABLE_NAME, value);
  ASSERT_EQ(std::wstring(L"AllTypesTable"), value);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeTypeName) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"BIGINT"), value);
  GetSQLColAttributeString(this->stmt, L"", 2, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"WVARCHAR"), value);
  GetSQLColAttributeString(this->stmt, L"", 3, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"BINARY"), value);
  GetSQLColAttributeString(this->stmt, L"", 4, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"DOUBLE"), value);
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeTypeName) {
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::wstring value;
  GetSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"INTEGER"), value);
  GetSQLColAttributeString(this->stmt, L"", 2, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"BIGINT"), value);
  GetSQLColAttributeString(this->stmt, L"", 3, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"DECIMAL"), value);
  GetSQLColAttributeString(this->stmt, L"", 4, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"FLOAT"), value);
  GetSQLColAttributeString(this->stmt, L"", 5, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"DOUBLE"), value);
  GetSQLColAttributeString(this->stmt, L"", 6, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"BOOLEAN"), value);
  GetSQLColAttributeString(this->stmt, L"", 7, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"DATE"), value);
  GetSQLColAttributeString(this->stmt, L"", 8, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"TIME"), value);
  GetSQLColAttributeString(this->stmt, L"", 9, SQL_DESC_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"TIMESTAMP"), value);
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesTypeName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't return data source-dependent data type name
  std::wstring value;
  GetSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"BIGINT"), value);
  GetSQLColAttributesString(this->stmt, L"", 2, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"WVARCHAR"), value);
  GetSQLColAttributesString(this->stmt, L"", 3, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"BINARY"), value);
  GetSQLColAttributesString(this->stmt, L"", 4, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"DOUBLE"), value);
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributesTypeName) {
  // Tests ODBC 2.0 API SQLColAttributes
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::wstring value;
  GetSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"INTEGER"), value);
  GetSQLColAttributesString(this->stmt, L"", 2, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"BIGINT"), value);
  GetSQLColAttributesString(this->stmt, L"", 3, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"DECIMAL"), value);
  GetSQLColAttributesString(this->stmt, L"", 4, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"FLOAT"), value);
  GetSQLColAttributesString(this->stmt, L"", 5, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"DOUBLE"), value);
  GetSQLColAttributesString(this->stmt, L"", 6, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"BOOLEAN"), value);
  GetSQLColAttributesString(this->stmt, L"", 7, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"DATE"), value);
  GetSQLColAttributesString(this->stmt, L"", 8, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"TIME"), value);
  GetSQLColAttributesString(this->stmt, L"", 9, SQL_COLUMN_TYPE_NAME, value);
  ASSERT_EQ(std::wstring(L"TIMESTAMP"), value);
}

TYPED_TEST(ColumnsTest, TestSQLColAttributeUnnamed) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  SQLLEN value;
  GetSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_UNNAMED, &value);
  ASSERT_EQ(SQL_NAMED, value);
}

TYPED_TEST(ColumnsTest, TestSQLColAttributeUpdatable) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  // Mock server and remote server do not return updatable information
  SQLLEN value;
  GetSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_UPDATABLE, &value);
  ASSERT_EQ(SQL_ATTR_READWRITE_UNKNOWN, value);
}

TYPED_TEST(ColumnsOdbcV2Test, TestSQLColAttributesUpdatable) {
  // Tests ODBC 2.0 API SQLColAttributes
  std::wstring wsql = this->GetQueryAllDataTypes();
  // Mock server and remote server do not return updatable information
  SQLLEN value;
  GetSQLColAttributesNumeric(this->stmt, wsql, 1, SQL_COLUMN_UPDATABLE, &value);
  ASSERT_EQ(SQL_ATTR_READWRITE_UNKNOWN, value);
}

}  // namespace arrow::flight::sql::odbc
