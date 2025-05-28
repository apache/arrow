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

#include "arrow/flight/sql/odbc/flight_sql/address_info.h"

namespace driver {

bool AddressInfo::GetAddressInfo(const std::string& host, char* host_name_info,
                                 int64_t max_host) {
  if (addrinfo_result_) {
    freeaddrinfo(addrinfo_result_);
    addrinfo_result_ = nullptr;
  }

  int error;
  error = getaddrinfo(host.c_str(), NULL, NULL, &addrinfo_result_);

  if (error != 0) {
    return false;
  }

  error = getnameinfo(addrinfo_result_->ai_addr, addrinfo_result_->ai_addrlen,
                      host_name_info, static_cast<DWORD>(max_host), NULL, 0, 0);
  return error == 0;
}

AddressInfo::~AddressInfo() {
  if (addrinfo_result_) {
    freeaddrinfo(addrinfo_result_);
    addrinfo_result_ = nullptr;
  }
}

AddressInfo::AddressInfo() : addrinfo_result_(nullptr) {}
}  // namespace driver
