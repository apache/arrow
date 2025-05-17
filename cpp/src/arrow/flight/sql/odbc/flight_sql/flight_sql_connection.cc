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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/utils.h"

#include "arrow/flight/client_cookie_middleware.h"
#include "arrow/flight/sql/odbc/flight_sql/address_info.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_auth_method.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_ssl_config.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_statement.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/types.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"

#include <sql.h>
#include <sqlext.h>

#include "arrow/flight/sql/odbc/flight_sql/system_trust_store.h"

#ifndef NI_MAXHOST
#  define NI_MAXHOST 1025
#endif

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::Status;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClient;
using arrow::flight::FlightClientOptions;
using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using arrow::flight::sql::FlightSqlClient;
using driver::odbcabstraction::AsBool;
using driver::odbcabstraction::CommunicationException;
using driver::odbcabstraction::Connection;
using driver::odbcabstraction::DriverException;
using driver::odbcabstraction::OdbcVersion;
using driver::odbcabstraction::Statement;

const std::vector<std::string_view> FlightSqlConnection::ALL_KEYS = {
    FlightSqlConnection::DSN,
    FlightSqlConnection::DRIVER,
    FlightSqlConnection::HOST,
    FlightSqlConnection::PORT,
    FlightSqlConnection::TOKEN,
    FlightSqlConnection::UID,
    FlightSqlConnection::USER_ID,
    FlightSqlConnection::PWD,
    FlightSqlConnection::USE_ENCRYPTION,
    FlightSqlConnection::TRUSTED_CERTS,
    FlightSqlConnection::USE_SYSTEM_TRUST_STORE,
    FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
    FlightSqlConnection::STRING_COLUMN_LENGTH,
    FlightSqlConnection::USE_WIDE_CHAR,
    FlightSqlConnection::CHUNK_BUFFER_CAPACITY};

namespace {

#if _WIN32 || _WIN64
constexpr auto SYSTEM_TRUST_STORE_DEFAULT = true;
constexpr auto STORES = {"CA", "MY", "ROOT", "SPC"};

inline std::string GetCerts() {
  std::string certs;

  for (auto store : STORES) {
    std::shared_ptr<SystemTrustStore> cert_iterator =
        std::make_shared<SystemTrustStore>(store);

    if (!cert_iterator->SystemHasStore()) {
      // If the system does not have the specific store, we skip it using the continue.
      continue;
    }
    while (cert_iterator->HasNext()) {
      certs += cert_iterator->GetNext();
    }
  }

  return certs;
}

#else

constexpr auto SYSTEM_TRUST_STORE_DEFAULT = false;
inline std::string GetCerts() { return ""; }

#endif

const std::set<std::string_view, odbcabstraction::CaseInsensitiveComparator>
    BUILT_IN_PROPERTIES = {FlightSqlConnection::HOST,
                           FlightSqlConnection::PORT,
                           FlightSqlConnection::USER,
                           FlightSqlConnection::USER_ID,
                           FlightSqlConnection::UID,
                           FlightSqlConnection::PASSWORD,
                           FlightSqlConnection::PWD,
                           FlightSqlConnection::TOKEN,
                           FlightSqlConnection::USE_ENCRYPTION,
                           FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
                           FlightSqlConnection::TRUSTED_CERTS,
                           FlightSqlConnection::USE_SYSTEM_TRUST_STORE,
                           FlightSqlConnection::STRING_COLUMN_LENGTH,
                           FlightSqlConnection::USE_WIDE_CHAR};

Connection::ConnPropertyMap::const_iterator TrackMissingRequiredProperty(
    const std::string_view& property, const Connection::ConnPropertyMap& properties,
    std::vector<std::string_view>& missing_attr) {
  auto prop_iter = properties.find(property);
  if (properties.end() == prop_iter) {
    missing_attr.push_back(property);
  }
  return prop_iter;
}
}  // namespace

std::shared_ptr<FlightSqlSslConfig> LoadFlightSslConfigs(
    const Connection::ConnPropertyMap& connPropertyMap) {
  bool use_encryption =
      AsBool(connPropertyMap, FlightSqlConnection::USE_ENCRYPTION).value_or(true);
  bool disable_cert =
      AsBool(connPropertyMap, FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION)
          .value_or(false);
  bool use_system_trusted =
      AsBool(connPropertyMap, FlightSqlConnection::USE_SYSTEM_TRUST_STORE)
          .value_or(SYSTEM_TRUST_STORE_DEFAULT);

  auto trusted_certs_iterator = connPropertyMap.find(FlightSqlConnection::TRUSTED_CERTS);
  auto trusted_certs = trusted_certs_iterator != connPropertyMap.end()
                           ? trusted_certs_iterator->second
                           : "";

  return std::make_shared<FlightSqlSslConfig>(disable_cert, trusted_certs,
                                              use_system_trusted, use_encryption);
}

void FlightSqlConnection::Connect(const ConnPropertyMap& properties,
                                  std::vector<std::string_view>& missing_attr) {
  try {
    auto flight_ssl_configs = LoadFlightSslConfigs(properties);

    Location location = BuildLocation(properties, missing_attr, flight_ssl_configs);
    FlightClientOptions client_options =
        BuildFlightClientOptions(properties, missing_attr, flight_ssl_configs);

    const std::shared_ptr<arrow::flight::ClientMiddlewareFactory>& cookie_factory =
        arrow::flight::GetCookieFactory();
    client_options.middleware.push_back(cookie_factory);

    std::unique_ptr<FlightClient> flight_client;
    ThrowIfNotOK(FlightClient::Connect(location, client_options).Value(&flight_client));

    std::unique_ptr<FlightSqlAuthMethod> auth_method =
        FlightSqlAuthMethod::FromProperties(flight_client, properties);
    auth_method->Authenticate(*this, call_options_);

    sql_client_.reset(new FlightSqlClient(std::move(flight_client)));
    closed_ = false;

    // Note: This should likely come from Flight instead of being from the
    // connection properties to allow reporting a user for other auth mechanisms
    // and also decouple the database user from user credentials.

    info_.SetProperty(SQL_USER_NAME, auth_method->GetUser());
    attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_FALSE);

    PopulateMetadataSettings(properties);
    PopulateCallOptions(properties);
  } catch (...) {
    attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
    sql_client_.reset();

    throw;
  }
}

void FlightSqlConnection::PopulateMetadataSettings(
    const Connection::ConnPropertyMap& conn_property_map) {
  metadata_settings_.string_column_length_ = GetStringColumnLength(conn_property_map);
  metadata_settings_.use_wide_char_ = GetUseWideChar(conn_property_map);
  metadata_settings_.chunk_buffer_capacity_ = GetChunkBufferCapacity(conn_property_map);
}

boost::optional<int32_t> FlightSqlConnection::GetStringColumnLength(
    const Connection::ConnPropertyMap& conn_property_map) {
  const int32_t min_string_column_length = 1;

  try {
    return AsInt32(min_string_column_length, conn_property_map,
                   FlightSqlConnection::STRING_COLUMN_LENGTH);
  } catch (const std::exception& e) {
    diagnostics_.AddWarning(
        std::string("Invalid value for connection property " +
                    std::string(FlightSqlConnection::STRING_COLUMN_LENGTH) +
                    ". Please ensure it has a valid numeric value. Message: " + e.what()),
        "01000", odbcabstraction::ODBCErrorCodes_GENERAL_WARNING);
  }

  return boost::none;
}

bool FlightSqlConnection::GetUseWideChar(const ConnPropertyMap& connPropertyMap) {
#if defined _WIN32 || defined _WIN64
  // Windows should use wide chars by default
  bool default_value = true;
#else
  // Mac and Linux should not use wide chars by default
  bool default_value = false;
#endif
  return AsBool(connPropertyMap, FlightSqlConnection::USE_WIDE_CHAR)
      .value_or(default_value);
}

size_t FlightSqlConnection::GetChunkBufferCapacity(
    const ConnPropertyMap& connPropertyMap) {
  size_t default_value = 5;
  try {
    return AsInt32(1, connPropertyMap, FlightSqlConnection::CHUNK_BUFFER_CAPACITY)
        .value_or(default_value);
  } catch (const std::exception& e) {
    diagnostics_.AddWarning(
        std::string("Invalid value for connection property " +
                    std::string(FlightSqlConnection::CHUNK_BUFFER_CAPACITY) +
                    ". Please ensure it has a valid numeric value. Message: " + e.what()),
        "01000", odbcabstraction::ODBCErrorCodes_GENERAL_WARNING);
  }

  return default_value;
}

const FlightCallOptions& FlightSqlConnection::PopulateCallOptions(
    const ConnPropertyMap& props) {
  // Set CONNECTION_TIMEOUT attribute or LOGIN_TIMEOUT depending on if this
  // is the first request.
  const boost::optional<Connection::Attribute>& connection_timeout =
      closed_ ? GetAttribute(LOGIN_TIMEOUT) : GetAttribute(CONNECTION_TIMEOUT);
  if (connection_timeout && boost::get<uint32_t>(*connection_timeout) > 0) {
    call_options_.timeout =
        TimeoutDuration{static_cast<double>(boost::get<uint32_t>(*connection_timeout))};
  }

  for (auto prop : props) {
    if (BUILT_IN_PROPERTIES.count(prop.first) != 0) {
      continue;
    }

    if (prop.first.find(' ') != std::string::npos) {
      // Connection properties containing spaces will crash gRPC, but some tools
      // such as the OLE DB to ODBC bridge generate unused properties containing spaces.
      diagnostics_.AddWarning(
          std::string("Ignoring connection option " + std::string(prop.first)) +
              ". Server-specific options must be valid HTTP header names and " +
              "cannot contain spaces.",
          "01000", odbcabstraction::ODBCErrorCodes_GENERAL_WARNING);
      continue;
    }

    // Note: header names must be lower case for gRPC.
    // gRPC will crash if they are not lower-case.
    std::string key_lc = boost::algorithm::to_lower_copy(std::string(prop.first));
    call_options_.headers.emplace_back(std::make_pair(key_lc, prop.second));
  }

  return call_options_;
}

FlightClientOptions FlightSqlConnection::BuildFlightClientOptions(
    const ConnPropertyMap& properties, std::vector<std::string_view>& missing_attr,
    const std::shared_ptr<FlightSqlSslConfig>& ssl_config) {
  FlightClientOptions options;
  // Persist state information using cookies if the FlightProducer supports it.
  options.middleware.push_back(arrow::flight::GetCookieFactory());

  if (ssl_config->useEncryption()) {
    if (ssl_config->shouldDisableCertificateVerification()) {
      options.disable_server_verification =
          ssl_config->shouldDisableCertificateVerification();
    } else {
      if (ssl_config->useSystemTrustStore()) {
        const std::string certs = GetCerts();

        options.tls_root_certs = certs;
      } else if (!ssl_config->getTrustedCerts().empty()) {
        arrow::flight::CertKeyPair cert_key_pair;
        ssl_config->populateOptionsWithCerts(&cert_key_pair);
        options.tls_root_certs = cert_key_pair.pem_cert;
      }
    }
  }

  return std::move(options);
}

Location FlightSqlConnection::BuildLocation(
    const ConnPropertyMap& properties, std::vector<std::string_view>& missing_attr,
    const std::shared_ptr<FlightSqlSslConfig>& ssl_config) {
  const auto& host_iter = TrackMissingRequiredProperty(HOST, properties, missing_attr);

  const auto& port_iter = TrackMissingRequiredProperty(PORT, properties, missing_attr);

  if (!missing_attr.empty()) {
    std::vector<std::string> missing_attr_string_vec(missing_attr.begin(),
                                                     missing_attr.end());
    std::string missing_attr_str = std::string("Missing required properties: ") +
                                   boost::algorithm::join(missing_attr_string_vec, ", ");
    throw DriverException(missing_attr_str);
  }

  const std::string& host = host_iter->second;
  const int& port = boost::lexical_cast<int>(port_iter->second);

  Location location;
  if (ssl_config->useEncryption()) {
    AddressInfo address_info;
    char host_name_info[NI_MAXHOST] = "";
    bool operation_result = false;

    try {
      auto ip_address = boost::asio::ip::make_address(host);
      // We should only attempt to resolve the hostname from the IP if the given
      // HOST input is an IP address.
      if (ip_address.is_v4() || ip_address.is_v6()) {
        operation_result = address_info.GetAddressInfo(host, host_name_info, NI_MAXHOST);
        if (operation_result) {
          ThrowIfNotOK(Location::ForGrpcTls(host_name_info, port).Value(&location));
          return location;
        }
        // TODO: We should log that we could not convert an IP to hostname here.
      }
    } catch (...) {
      // This is expected. The Host attribute can be an IP or name, but make_address will
      // throw if it is not an IP.
    }

    ThrowIfNotOK(Location::ForGrpcTls(host, port).Value(&location));
    return location;
  }

  ThrowIfNotOK(Location::ForGrpcTcp(host, port).Value(&location));
  return location;
}

void FlightSqlConnection::Close() {
  if (closed_) {
    throw DriverException("Connection already closed.");
  }

  sql_client_.reset();
  closed_ = true;
  attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
}

std::shared_ptr<Statement> FlightSqlConnection::CreateStatement() {
  return std::shared_ptr<Statement>(new FlightSqlStatement(
      diagnostics_, *sql_client_, call_options_, metadata_settings_));
}

bool FlightSqlConnection::SetAttribute(Connection::AttributeId attribute,
                                       const Connection::Attribute& value) {
  switch (attribute) {
    case ACCESS_MODE:
      // We will always return read-write.
      return CheckIfSetToOnlyValidValue(value,
                                        static_cast<uint32_t>(SQL_MODE_READ_WRITE));
    case PACKET_SIZE:
      return CheckIfSetToOnlyValidValue(value, static_cast<uint32_t>(0));
    default:
      attribute_[attribute] = value;
      return true;
  }
}

boost::optional<Connection::Attribute> FlightSqlConnection::GetAttribute(
    Connection::AttributeId attribute) {
  switch (attribute) {
    case ACCESS_MODE:
      // FlightSQL does not provide this metadata.
      return boost::make_optional(Attribute(static_cast<uint32_t>(SQL_MODE_READ_WRITE)));
    case PACKET_SIZE:
      return boost::make_optional(Attribute(static_cast<uint32_t>(0)));
    default:
      const auto& it = attribute_.find(attribute);
      return boost::make_optional(it != attribute_.end(), it->second);
  }
}

Connection::Info FlightSqlConnection::GetInfo(uint16_t info_type) {
  auto result = info_.GetInfo(info_type);
  if (info_type == SQL_DBMS_NAME || info_type == SQL_SERVER_NAME) {
    // Update the database component reported in error messages.
    // We do this lazily for performance reasons.
    diagnostics_.SetDataSourceComponent(boost::get<std::string>(result));
  }
  return result;
}

FlightSqlConnection::FlightSqlConnection(OdbcVersion odbc_version,
                                         const std::string& driver_version)
    : diagnostics_("Apache Arrow", "Flight SQL", odbc_version),
      odbc_version_(odbc_version),
      info_(call_options_, sql_client_, driver_version),
      closed_(true) {
  attribute_[CONNECTION_DEAD] = static_cast<uint32_t>(SQL_TRUE);
  attribute_[LOGIN_TIMEOUT] = static_cast<uint32_t>(0);
  attribute_[CONNECTION_TIMEOUT] = static_cast<uint32_t>(0);
  attribute_[CURRENT_CATALOG] = "";
}
odbcabstraction::Diagnostics& FlightSqlConnection::GetDiagnostics() {
  return diagnostics_;
}

void FlightSqlConnection::SetClosed(bool is_closed) { closed_ = is_closed; }

}  // namespace flight_sql
}  // namespace driver
