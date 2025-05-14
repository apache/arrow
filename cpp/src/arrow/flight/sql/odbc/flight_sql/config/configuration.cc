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

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"

#include <odbcinst.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <iterator>
#include <sstream>

namespace driver {
namespace flight_sql {
namespace config {

static const char DEFAULT_DSN[] = "Apache Arrow Flight SQL";
static const char DEFAULT_ENABLE_ENCRYPTION[] = TRUE_STR;
static const char DEFAULT_USE_CERT_STORE[] = TRUE_STR;
static const char DEFAULT_DISABLE_CERT_VERIFICATION[] = FALSE_STR;

namespace {
std::string ReadDsnString(const std::string& dsn, const std::string_view& key,
                          const std::string& dflt = "") {
#define BUFFER_SIZE (1024)
  std::vector<char> buf(BUFFER_SIZE);

  std::string key_str = std::string(key);
  int ret =
      SQLGetPrivateProfileString(dsn.c_str(), key_str.c_str(), dflt.c_str(), buf.data(),
                                 static_cast<int>(buf.size()), "ODBC.INI");

  if (ret > BUFFER_SIZE) {
    // If there wasn't enough space, try again with the right size buffer.
    buf.resize(ret + 1);
    ret =
        SQLGetPrivateProfileString(dsn.c_str(), key_str.c_str(), dflt.c_str(), buf.data(),
                                   static_cast<int>(buf.size()), "ODBC.INI");
  }

  return std::string(buf.data(), ret);
}

void RemoveAllKnownKeys(std::vector<std::string>& keys) {
  // Remove all known DSN keys from the passed in set of keys, case insensitively.
  keys.erase(std::remove_if(keys.begin(), keys.end(),
                            [&](auto& x) {
                              return std::find_if(
                                         FlightSqlConnection::ALL_KEYS.begin(),
                                         FlightSqlConnection::ALL_KEYS.end(),
                                         [&](auto& s) { return boost::iequals(x, s); }) !=
                                     FlightSqlConnection::ALL_KEYS.end();
                            }),
             keys.end());
}

std::vector<std::string> ReadAllKeys(const std::string& dsn) {
  std::vector<char> buf(BUFFER_SIZE);

  int ret = SQLGetPrivateProfileString(dsn.c_str(), NULL, "", buf.data(),
                                       static_cast<int>(buf.size()), "ODBC.INI");

  if (ret > BUFFER_SIZE) {
    // If there wasn't enough space, try again with the right size buffer.
    buf.resize(ret + 1);
    ret = SQLGetPrivateProfileString(dsn.c_str(), NULL, "", buf.data(),
                                     static_cast<int>(buf.size()), "ODBC.INI");
  }

  // When you pass NULL to SQLGetPrivateProfileString it gives back a \0 delimited list of
  // all the keys. The below loop simply tokenizes all the keys and places them into a
  // vector.
  std::vector<std::string> keys;
  char* begin = buf.data();
  while (begin && *begin != '\0') {
    char* cur;
    for (cur = begin; *cur != '\0'; ++cur) {
    }
    keys.emplace_back(begin, cur);
    begin = ++cur;
  }
  return keys;
}
}  // namespace

Configuration::Configuration() {
  // No-op.
}

Configuration::~Configuration() {
  // No-op.
}

void Configuration::LoadDefaults() {
  Set(FlightSqlConnection::DSN, DEFAULT_DSN);
  Set(FlightSqlConnection::USE_ENCRYPTION, DEFAULT_ENABLE_ENCRYPTION);
  Set(FlightSqlConnection::USE_SYSTEM_TRUST_STORE, DEFAULT_USE_CERT_STORE);
  Set(FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
      DEFAULT_DISABLE_CERT_VERIFICATION);
}

void Configuration::LoadDsn(const std::string& dsn) {
  Set(FlightSqlConnection::DSN, dsn);
  Set(FlightSqlConnection::HOST, ReadDsnString(dsn, FlightSqlConnection::HOST));
  Set(FlightSqlConnection::PORT, ReadDsnString(dsn, FlightSqlConnection::PORT));
  Set(FlightSqlConnection::TOKEN, ReadDsnString(dsn, FlightSqlConnection::TOKEN));
  Set(FlightSqlConnection::UID, ReadDsnString(dsn, FlightSqlConnection::UID));
  Set(FlightSqlConnection::PWD, ReadDsnString(dsn, FlightSqlConnection::PWD));
  Set(FlightSqlConnection::USE_ENCRYPTION,
      ReadDsnString(dsn, FlightSqlConnection::USE_ENCRYPTION, DEFAULT_ENABLE_ENCRYPTION));
  Set(FlightSqlConnection::TRUSTED_CERTS,
      ReadDsnString(dsn, FlightSqlConnection::TRUSTED_CERTS));
  Set(FlightSqlConnection::USE_SYSTEM_TRUST_STORE,
      ReadDsnString(dsn, FlightSqlConnection::USE_SYSTEM_TRUST_STORE,
                    DEFAULT_USE_CERT_STORE));
  Set(FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
      ReadDsnString(dsn, FlightSqlConnection::DISABLE_CERTIFICATE_VERIFICATION,
                    DEFAULT_DISABLE_CERT_VERIFICATION));

  auto customKeys = ReadAllKeys(dsn);
  RemoveAllKnownKeys(customKeys);
  for (auto key : customKeys) {
    std::string_view key_sv(key);
    Set(key, ReadDsnString(dsn, key_sv));
  }
}

void Configuration::Clear() { this->properties.clear(); }

bool Configuration::IsSet(const std::string_view& key) const {
  return 0 != this->properties.count(key);
}

const std::string& Configuration::Get(const std::string_view& key) const {
  const auto itr = this->properties.find(key);
  if (itr == this->properties.cend()) {
    static const std::string empty("");
    return empty;
  }
  return itr->second;
}

void Configuration::Set(const std::string_view& key, const std::string& value) {
  const std::string copy = boost::trim_copy(value);
  if (!copy.empty()) {
    this->properties[key] = value;
  }
}

const driver::odbcabstraction::Connection::ConnPropertyMap& Configuration::GetProperties()
    const {
  return this->properties;
}

std::vector<std::string_view> Configuration::GetCustomKeys() const {
  driver::odbcabstraction::Connection::ConnPropertyMap copyProps(properties);
  for (auto& key : FlightSqlConnection::ALL_KEYS) {
    copyProps.erase(key);
  }
  std::vector<std::string_view> keys;
  boost::copy(copyProps | boost::adaptors::map_keys, std::back_inserter(keys));
  return keys;
}

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
