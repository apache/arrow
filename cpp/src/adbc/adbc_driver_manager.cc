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

#include "adbc/adbc_driver_manager.h"

#include <dlfcn.h>
#include <algorithm>
#include <string>
#include <unordered_map>

namespace {
std::unordered_map<std::string, std::string> ParseConnectionString(
    const std::string& target) {
  // TODO: this does not properly implement the ODBC connection string format.
  std::unordered_map<std::string, std::string> option_pairs;
  size_t cur = 0;

  while (cur < target.size()) {
    auto divider = target.find('=', cur);
    if (divider == std::string::npos) break;

    std::string key = target.substr(cur, divider - cur);
    cur = divider + 1;
    auto end = target.find(';', cur);
    if (end == std::string::npos) {
      option_pairs.insert({std::move(key), target.substr(cur)});
      break;
    } else {
      option_pairs.insert({std::string(key), target.substr(cur, end - cur)});
      cur = end + 1;
    }
  }
  return option_pairs;
}
}  // namespace

AdbcStatusCode AdbcLoadDriver(const char* connection, size_t count,
                              struct AdbcDriver* driver, size_t* initialized) {
  auto params = ParseConnectionString(connection);

  auto driver_str = params.find("Driver");
  if (driver_str == params.end()) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  auto entrypoint_str = params.find("Entrypoint");
  if (entrypoint_str == params.end()) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  void* handle = dlopen(driver_str->second.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle) {
    return ADBC_STATUS_UNKNOWN;
  }

  auto* load =
      reinterpret_cast<AdbcDriverInitFunc>(dlsym(handle, entrypoint_str->second.c_str()));
  if (!load) {
    return ADBC_STATUS_INTERNAL;
  }
  // TODO: we could do things here like filling in default stubs for
  // unimplemented functions
  return load(count, driver, initialized);
}
#undef GET_FUNC
#undef GET_OPTIONAL
