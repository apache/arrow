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

#include "config/configuration.h"

namespace driver {
namespace flight_sql {
namespace config {

/**
 * ODBC configuration parser abstraction.
 */
class ConnectionStringParser {
 public:
  /**
   * Constructor.
   *
   * @param cfg Configuration.
   */
  explicit ConnectionStringParser(Configuration& cfg);

  /**
   * Destructor.
   */
  ~ConnectionStringParser();

  /**
   * Parse connect string.
   *
   * @param str String to parse.
   * @param len String length.
   * @param delimiter delimiter.
   */
  void ParseConnectionString(const char* str, size_t len, char delimiter);

  /**
   * Parse connect string.
   *
   * @param str String to parse.
   */
  void ParseConnectionString(const std::string& str);

  /**
   * Parse config attributes.
   *
   * @param str String to parse.
   */
  void ParseConfigAttributes(const char* str);

 private:
  ConnectionStringParser(const ConnectionStringParser& parser) = delete;
  ConnectionStringParser& operator=(const ConnectionStringParser&) = delete;

  /** Configuration. */
  Configuration& cfg;
};

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
