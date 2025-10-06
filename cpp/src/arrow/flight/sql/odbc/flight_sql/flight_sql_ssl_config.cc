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

FlightSqlSslConfig::FlightSqlSslConfig(bool disable_certificate_verification,
                                       const std::string& trusted_certs,
                                       bool system_trust_store, bool use_encryption)
    : trusted_certs_(trusted_certs),
      use_encryption_(use_encryption),
      disable_certificate_verification_(disable_certificate_verification),
      system_trust_store_(system_trust_store) {}

bool FlightSqlSslConfig::UseEncryption() const { return use_encryption_; }

bool FlightSqlSslConfig::ShouldDisableCertificateVerification() const {
  return disable_certificate_verification_;
}

const std::string& FlightSqlSslConfig::GetTrustedCerts() const { return trusted_certs_; }

bool FlightSqlSslConfig::UseSystemTrustStore() const { return system_trust_store_; }

void FlightSqlSslConfig::PopulateOptionsWithCerts(arrow::flight::CertKeyPair* out) {
  try {
    std::ifstream cert_file(trusted_certs_);
    if (!cert_file) {
      throw odbcabstraction::DriverException("Could not open certificate: " +
                                             trusted_certs_);
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
