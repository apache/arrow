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

#include "arrow/flight/sql/odbc/flight_sql/system_trust_store.h"

#if defined _WIN32 || defined _WIN64

namespace driver {
namespace flight_sql {
bool SystemTrustStore::HasNext() {
  p_context_ = CertEnumCertificatesInStore(h_store_, p_context_);

  return p_context_ != nullptr;
}

std::string SystemTrustStore::GetNext() const {
  DWORD size = 0;
  CryptBinaryToString(p_context_->pbCertEncoded, p_context_->cbCertEncoded,
                      CRYPT_STRING_BASE64HEADER, nullptr, &size);

  std::string cert;
  cert.resize(size);
  CryptBinaryToString(p_context_->pbCertEncoded, p_context_->cbCertEncoded,
                      CRYPT_STRING_BASE64HEADER, &cert[0], &size);
  cert.resize(size);

  return cert;
}

bool SystemTrustStore::SystemHasStore() { return h_store_ != nullptr; }

SystemTrustStore::SystemTrustStore(const char* store)
    : stores_(store), h_store_(CertOpenSystemStore(NULL, store)), p_context_(nullptr) {}

SystemTrustStore::~SystemTrustStore() {
  if (p_context_) {
    CertFreeCertificateContext(p_context_);
  }
  if (h_store_) {
    CertCloseStore(h_store_, 0);
  }
}
}  // namespace flight_sql
}  // namespace driver

#endif
