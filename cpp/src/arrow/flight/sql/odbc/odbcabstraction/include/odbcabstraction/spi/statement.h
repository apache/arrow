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

#pragma once

#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <map>
#include <vector>

namespace driver {
namespace odbcabstraction {

using boost::optional;

class ResultSet;

class ResultSetMetadata;

/// \brief High-level representation of an ODBC statement.
class Statement {
 protected:
  Statement() = default;

 public:
  virtual ~Statement() = default;

  /// \brief Statement attributes that can be called at anytime.
  ////TODO: Document attributes
  enum StatementAttributeId {
    MAX_LENGTH,   // size_t - The maximum length when retrieving variable length data. 0
                  // means no limit.
    METADATA_ID,  // size_t - Modifies catalog function arguments to be identifiers.
                  // SQL_TRUE or SQL_FALSE.
    NOSCAN,  // size_t - Indicates that the driver does not scan for escape sequences.
             // Default to SQL_NOSCAN_OFF
    QUERY_TIMEOUT,  // size_t - The time to wait in seconds for queries to execute. 0 to
                    // have no timeout.
  };

  typedef boost::variant<size_t> Attribute;

  /// \brief Set a statement attribute (may be called at any time)
  ///
  /// NOTE: Meant to be bound with SQLSetStmtAttr.
  ///
  /// \param attribute Attribute identifier to set.
  /// \param value Value to be associated with the attribute.
  /// \return true if the value was set successfully or false if it was substituted with
  /// a similar value.
  virtual bool SetAttribute(StatementAttributeId attribute, const Attribute& value) = 0;

  /// \brief Retrieve a statement attribute.
  ///
  /// NOTE: Meant to be bound with SQLGetStmtAttr.
  ///
  /// \param attribute Attribute identifier to be retrieved.
  /// \return Value associated with the attribute.
  virtual optional<Statement::Attribute> GetAttribute(
      Statement::StatementAttributeId attribute) = 0;

  /// \brief Prepares the statement.
  /// Returns ResultSetMetadata if query returns a result set,
  /// otherwise it returns `boost::none`.
  /// \param query The SQL query to prepare.
  virtual boost::optional<std::shared_ptr<ResultSetMetadata>> Prepare(
      const std::string& query) = 0;

  /// \brief Execute the prepared statement.
  ///
  /// NOTE: Must call `Prepare(const std::string &query)` before, otherwise it
  /// will throw an exception.
  ///
  /// \returns true if the first result is a ResultSet object;
  ///         false if it is an update count or there are no results.
  virtual bool ExecutePrepared() = 0;

  /// \brief Execute the statement if it is prepared or not.
  /// \param query The SQL query to execute.
  /// \returns true if the first result is a ResultSet object;
  ///         false if it is an update count or there are no results.
  virtual bool Execute(const std::string& query) = 0;

  /// \brief Returns the current result as a ResultSet object.
  virtual std::shared_ptr<ResultSet> GetResultSet() = 0;

  /// \brief Retrieves the current result as an update count;
  /// if the result is a ResultSet object or there are no more results, -1 is
  /// returned.
  virtual int64_t GetUpdateCount() = 0;

  /// \brief Returns the list of table, catalog, or schema names, and table
  /// types, stored in a specific data source. The driver returns the
  /// information as a result set.
  ///
  /// NOTE: This is meant to be used by ODBC 2.x binding.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  /// \param table_type The table type.
  virtual std::shared_ptr<ResultSet> GetTables_V2(const std::string* catalog_name,
                                                  const std::string* schema_name,
                                                  const std::string* table_name,
                                                  const std::string* table_type) = 0;

  /// \brief Returns the list of table, catalog, or schema names, and table
  /// types, stored in a specific data source. The driver returns the
  /// information as a result set.
  ///
  /// NOTE: This is meant to be used by ODBC 3.x binding.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  /// \param table_type The table type.
  virtual std::shared_ptr<ResultSet> GetTables_V3(const std::string* catalog_name,
                                                  const std::string* schema_name,
                                                  const std::string* table_name,
                                                  const std::string* table_type) = 0;

  /// \brief Returns the list of column names in specified tables. The driver
  /// returns this information as a result set..
  ///
  /// NOTE: This is meant to be used by ODBC 2.x binding.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  /// \param column_name The column name.
  virtual std::shared_ptr<ResultSet> GetColumns_V2(const std::string* catalog_name,
                                                   const std::string* schema_name,
                                                   const std::string* table_name,
                                                   const std::string* column_name) = 0;

  /// \brief Returns the list of column names in specified tables. The driver
  /// returns this information as a result set..
  ///
  /// NOTE: This is meant to be used by ODBC 3.x binding.
  ///
  /// \param catalog_name The catalog name.
  /// \param schema_name The schema name.
  /// \param table_name The table name.
  /// \param column_name The column name.
  virtual std::shared_ptr<ResultSet> GetColumns_V3(const std::string* catalog_name,
                                                   const std::string* schema_name,
                                                   const std::string* table_name,
                                                   const std::string* column_name) = 0;

  /// \brief Returns information about data types supported by the data source.
  /// The driver returns the information in the form of an SQL result set. The
  /// data types are intended for use in Data Definition Language (DDL)
  /// statements.
  ///
  /// NOTE: This is meant to be used by ODBC 2.x binding.
  ///
  /// \param data_type The SQL data type.
  virtual std::shared_ptr<ResultSet> GetTypeInfo_V2(int16_t data_type) = 0;

  /// \brief Returns information about data types supported by the data source.
  /// The driver returns the information in the form of an SQL result set. The
  /// data types are intended for use in Data Definition Language (DDL)
  /// statements.
  ///
  /// NOTE: This is meant to be used by ODBC 3.x binding.
  ///
  /// \param data_type The SQL data type.
  virtual std::shared_ptr<ResultSet> GetTypeInfo_V3(int16_t data_type) = 0;

  /// \brief Gets the diagnostics for this statement.
  /// \return the diagnostics
  virtual Diagnostics& GetDiagnostics() = 0;

  /// \brief Cancels the processing of this statement.
  virtual void Cancel() = 0;
};

}  // namespace odbcabstraction
}  // namespace driver
