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

#include <boost/algorithm/string.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h>

namespace driver {
namespace odbcabstraction {

/// \brief Case insensitive comparator
struct CaseInsensitiveComparator {
  bool operator()(const std::string_view& s1, const std::string_view& s2) const {
    return boost::lexicographical_compare(s1, s2, boost::is_iless());
  }
};

// PropertyMap is case-insensitive for keys.
typedef std::map<std::string_view, std::string, CaseInsensitiveComparator> PropertyMap;

class Statement;

/// \brief High-level representation of an ODBC connection.
class Connection {
 protected:
  Connection() = default;

 public:
  virtual ~Connection() = default;

  /// \brief Connection attributes
  enum AttributeId {
    ACCESS_MODE,         // uint32_t - Tells if it should support write operations
    CONNECTION_DEAD,     // uint32_t - Tells if connection is still alive
    CONNECTION_TIMEOUT,  // uint32_t - The timeout for connection functions after
                         // connecting.
    CURRENT_CATALOG,     // std::string - The current catalog
    LOGIN_TIMEOUT,       // uint32_t - The timeout for the initial connection
    PACKET_SIZE,         // uint32_t - The Packet Size
  };

  typedef boost::variant<std::string, void*, uint64_t, uint32_t> Attribute;
  typedef boost::variant<std::string, uint32_t, uint16_t> Info;
  typedef PropertyMap ConnPropertyMap;

  /// \brief Establish the connection.
  /// \param properties [in] properties used to establish the connection.
  /// \param missing_properties [out] vector of missing properties (if any).
  virtual void Connect(const ConnPropertyMap& properties,
                       std::vector<std::string_view>& missing_properties) = 0;

  /// \brief Close the connection.
  virtual void Close() = 0;

  /// \brief Create a statement.
  virtual std::shared_ptr<Statement> CreateStatement() = 0;

  /// \brief Set a connection attribute (may be called at any time).
  /// \param attribute [in] Which attribute to set.
  /// \param value The value to be set.
  /// \return true if the value was set successfully or false if it was substituted with
  /// a similar value.
  virtual bool SetAttribute(AttributeId attribute, const Attribute& value) = 0;

  /// \brief Retrieve a connection attribute
  /// \param attribute [in] Attribute to be retrieved.
  virtual boost::optional<Connection::Attribute> GetAttribute(
      Connection::AttributeId attribute) = 0;

  /// \brief Retrieves info from the database (see ODBC's SQLGetInfo).
  virtual Info GetInfo(uint16_t info_type) = 0;

  /// \brief Gets the diagnostics for this connection.
  /// \return the diagnostics
  virtual Diagnostics& GetDiagnostics() = 0;
};

}  // namespace odbcabstraction
}  // namespace driver
