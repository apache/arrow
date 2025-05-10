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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/encoding.h>

#if defined(__APPLE__)
#  include <dlfcn.h>
#  include <boost/algorithm/string/predicate.hpp>
#  include <mutex>
#endif

namespace driver {
namespace odbcabstraction {

#if defined(__APPLE__)
std::atomic<size_t> SqlWCharSize{0};

namespace {
std::mutex SqlWCharSizeMutex;

bool IsUsingIODBC() {
  // Detects iODBC by looking up by symbol iodbc_version
  void* handle = dlsym(RTLD_DEFAULT, "iodbc_version");
  bool using_iodbc = handle != nullptr;
  dlclose(handle);

  return using_iodbc;
}
}  // namespace

void ComputeSqlWCharSize() {
  std::unique_lock<std::mutex> lock(SqlWCharSizeMutex);
  if (SqlWCharSize != 0) return;  // double-checked locking

  const char* env_p = std::getenv("WCHAR_ENCODING");
  if (env_p) {
    if (boost::iequals(env_p, "UTF-16")) {
      SqlWCharSize = sizeof(char16_t);
      return;
    } else if (boost::iequals(env_p, "UTF-32")) {
      SqlWCharSize = sizeof(char32_t);
      return;
    }
  }

  SqlWCharSize = IsUsingIODBC() ? sizeof(char32_t) : sizeof(char16_t);
}
#endif

}  // namespace odbcabstraction
}  // namespace driver
