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

#include <string>
#include "arrow/flight/types.h"
#include "arrow/status.h"

namespace arrow::flight::sql::odbc {

/// \brief An Auxiliary class that holds all the information to perform
///        a SSL connection.
class FlightSqlSslConfig {
 public:
  FlightSqlSslConfig(bool disable_certificate_verification,
                     const std::string& trusted_certs, bool system_trust_store,
                     bool use_encryption);

  /// \brief  Tells if ssl is enabled. By default it will be true.
  /// \return Whether ssl is enabled.
  bool UseEncryption() const;

  /// \brief  Tells if disable certificate verification is enabled.
  /// \return Whether disable certificate verification is enabled.
  bool ShouldDisableCertificateVerification() const;

  /// \brief  The path to the trusted certificate.
  /// \return Certificate path.
  const std::string& GetTrustedCerts() const;

  /// \brief  Tells if we need to check if the certificate is in the system trust store.
  /// \return Whether to use the system trust store.
  bool UseSystemTrustStore() const;

  /// \brief Loads the certificate file and extract the certificate file from it
  ///        and create the object CertKeyPair with it on.
  /// \param out A CertKeyPair with the cert on it.
  void PopulateOptionsWithCerts(arrow::flight::CertKeyPair* out);

 private:
  const std::string trusted_certs_;
  const bool use_encryption_;
  const bool disable_certificate_verification_;
  const bool system_trust_store_;
};
}  // namespace arrow::flight::sql::odbc
