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

#if defined _WIN32 || defined _WIN64

#  include <windows.h>

#  include <wincrypt.h>

#  include <bcrypt.h>
#  include <cryptuiapi.h>

#  include <tchar.h>
#  include <string>
#  include <vector>

namespace arrow::flight::sql::odbc {

/// Load the certificates from the windows system trust store. Part of the logic
/// was based in the drill connector
/// https://github.com/apache/drill/blob/master/contrib/native/client/src/clientlib/wincert.ipp.
class SystemTrustStore {
 private:
  const wchar_t* stores_;
  HCERTSTORE h_store_;
  PCCERT_CONTEXT p_context_;

 public:
  explicit SystemTrustStore(const wchar_t* store);

  ~SystemTrustStore();

  /// Check if there is a certificate inside the system trust store to be extracted
  /// \return   If there is a valid cert in the store.
  bool HasNext();

  /// Get the next certificate from the store.
  /// \return   the certificate.
  std::string GetNext() const;

  /// Check if the system has the specify store.
  /// \return  If the specific store exist in the system.
  bool SystemHasStore();
};
}  // namespace arrow::flight::sql::odbc

#else  // Not Windows
namespace arrow::flight::sql::odbc {
class SystemTrustStore;
}  // namespace arrow::flight::sql::odbc

#endif
