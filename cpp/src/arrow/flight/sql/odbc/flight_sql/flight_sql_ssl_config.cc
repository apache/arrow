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

#include <fstream>
#include <sstream>

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_ssl_config.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"

namespace driver {
namespace flight_sql {

FlightSqlSslConfig::FlightSqlSslConfig(bool disableCertificateVerification,
                                       const std::string& trustedCerts,
                                       bool systemTrustStore, bool useEncryption)
    : trustedCerts_(trustedCerts),
      useEncryption_(useEncryption),
      disableCertificateVerification_(disableCertificateVerification),
      systemTrustStore_(systemTrustStore) {}

bool FlightSqlSslConfig::useEncryption() const { return useEncryption_; }

bool FlightSqlSslConfig::shouldDisableCertificateVerification() const {
  return disableCertificateVerification_;
}

const std::string& FlightSqlSslConfig::getTrustedCerts() const { return trustedCerts_; }

bool FlightSqlSslConfig::useSystemTrustStore() const { return systemTrustStore_; }

void FlightSqlSslConfig::populateOptionsWithCerts(arrow::flight::CertKeyPair* out) {
  try {
    std::ifstream cert_file(trustedCerts_);
    if (!cert_file) {
      throw odbcabstraction::DriverException("Could not open certificate: " +
                                             trustedCerts_);
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();
    out->pem_cert = cert.str();
  } catch (const std::ifstream::failure& e) {
    throw odbcabstraction::DriverException(e.what());
  }
}
}  // namespace flight_sql
}  // namespace driver
