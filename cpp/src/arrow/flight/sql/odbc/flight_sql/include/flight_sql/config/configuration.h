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

#include <stdint.h>
#include <string>
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h"

#if defined _WIN32 || defined _WIN64
// winuser.h needs to be included after windows.h, which is defined in platform.h
#  include <winuser.h>
#endif

namespace arrow::flight::sql::odbc {
namespace config {

#define TRUE_STR "true"
#define FALSE_STR "false"

/**
 * ODBC configuration abstraction.
 */
class Configuration {
 public:
  /**
   * Default constructor.
   */
  Configuration();

  /**
   * Destructor.
   */
  ~Configuration();

  void LoadDefaults();
  void LoadDsn(const std::string& dsn);

  void Clear();
  bool IsSet(const std::string_view& key) const;
  const std::string& Get(const std::string_view& key) const;
  void Set(const std::string_view& key, const std::wstring& wvalue);
  void Set(const std::string_view& key, const std::string& value);
  void Emplace(const std::string_view& key, std::string&& value);
  /**
   * Get properties map.
   */
  const arrow::flight::sql::odbc::Connection::ConnPropertyMap& GetProperties() const;

  std::vector<std::string> GetCustomKeys() const;

 private:
  arrow::flight::sql::odbc::Connection::ConnPropertyMap properties_;
};

}  // namespace config
}  // namespace arrow::flight::sql::odbc
