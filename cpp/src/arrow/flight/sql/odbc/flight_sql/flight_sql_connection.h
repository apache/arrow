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

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h"

#include <vector>
#include "arrow/flight/api.h"
#include "arrow/flight/sql/api.h"

#include "arrow/flight/sql/odbc/flight_sql/get_info_cache.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

namespace driver {
namespace flight_sql {

class FlightSqlSslConfig;

/// \brief Create an instance of the FlightSqlSslConfig class, from the properties passed
///        into the map.
/// \param connPropertyMap the map with the Connection properties.
/// \return                An instance of the FlightSqlSslConfig.
std::shared_ptr<FlightSqlSslConfig> LoadFlightSslConfigs(
    const odbcabstraction::Connection::ConnPropertyMap& connPropertyMap);

class FlightSqlConnection : public odbcabstraction::Connection {
 private:
  odbcabstraction::MetadataSettings metadata_settings_;
  std::map<AttributeId, Attribute> attribute_;
  arrow::flight::FlightClientOptions client_options_;
  arrow::flight::FlightCallOptions call_options_;
  std::unique_ptr<arrow::flight::sql::FlightSqlClient> sql_client_;
  GetInfoCache info_;
  odbcabstraction::Diagnostics diagnostics_;
  odbcabstraction::OdbcVersion odbc_version_;
  bool closed_;

  void PopulateMetadataSettings(const Connection::ConnPropertyMap& connPropertyMap);

 public:
  static const std::vector<std::string_view> ALL_KEYS;
  static constexpr std::string_view DSN = "dsn";
  static constexpr std::string_view DRIVER = "driver";
  static constexpr std::string_view HOST = "host";
  static constexpr std::string_view PORT = "port";
  static constexpr std::string_view USER = "user";
  static constexpr std::string_view USER_ID = "user id";
  static constexpr std::string_view UID = "uid";
  static constexpr std::string_view PASSWORD = "password";
  static constexpr std::string_view PWD = "pwd";
  static constexpr std::string_view TOKEN = "token";
  static constexpr std::string_view USE_ENCRYPTION = "useEncryption";
  static constexpr std::string_view DISABLE_CERTIFICATE_VERIFICATION =
      "disableCertificateVerification";
  static constexpr std::string_view TRUSTED_CERTS = "trustedCerts";
  static constexpr std::string_view USE_SYSTEM_TRUST_STORE = "useSystemTrustStore";
  static constexpr std::string_view STRING_COLUMN_LENGTH = "StringColumnLength";
  static constexpr std::string_view USE_WIDE_CHAR = "UseWideChar";
  static constexpr std::string_view CHUNK_BUFFER_CAPACITY = "ChunkBufferCapacity";

  explicit FlightSqlConnection(odbcabstraction::OdbcVersion odbc_version,
                               const std::string& driver_version = "0.9.0.0");

  void Connect(const ConnPropertyMap& properties,
               std::vector<std::string_view>& missing_attr) override;

  void Close() override;

  std::shared_ptr<odbcabstraction::Statement> CreateStatement() override;

  bool SetAttribute(AttributeId attribute, const Attribute& value) override;

  boost::optional<Connection::Attribute> GetAttribute(
      Connection::AttributeId attribute) override;

  Info GetInfo(uint16_t info_type) override;

  /// \brief Builds a Location used for FlightClient connection.
  /// \note Visible for testing
  static arrow::flight::Location BuildLocation(
      const ConnPropertyMap& properties, std::vector<std::string_view>& missing_attr,
      const std::shared_ptr<FlightSqlSslConfig>& ssl_config);

  /// \brief Builds a FlightClientOptions used for FlightClient connection.
  /// \note Visible for testing
  static arrow::flight::FlightClientOptions BuildFlightClientOptions(
      const ConnPropertyMap& properties, std::vector<std::string_view>& missing_attr,
      const std::shared_ptr<FlightSqlSslConfig>& ssl_config);

  /// \brief Builds a FlightCallOptions used on gRPC calls.
  /// \note Visible for testing
  const arrow::flight::FlightCallOptions& PopulateCallOptions(
      const ConnPropertyMap& properties);

  odbcabstraction::Diagnostics& GetDiagnostics() override;

  /// \brief A setter to the field closed_.
  /// \note Visible for testing
  void SetClosed(bool is_closed);

  boost::optional<int32_t> GetStringColumnLength(const ConnPropertyMap& connPropertyMap);

  bool GetUseWideChar(const ConnPropertyMap& connPropertyMap);

  size_t GetChunkBufferCapacity(const ConnPropertyMap& connPropertyMap);
};
}  // namespace flight_sql
}  // namespace driver
