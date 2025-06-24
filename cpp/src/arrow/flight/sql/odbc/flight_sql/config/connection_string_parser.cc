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

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/connection_string_parser.h"

#include <stdint.h>
#include <iostream>
#include <string>
#include <vector>

#include <algorithm>
#include <cstring>
#include <iterator>
#include <sstream>

namespace driver {
namespace flight_sql {
namespace config {

ConnectionStringParser::ConnectionStringParser(Configuration& cfg) : cfg(cfg) {
  // No-op.
}

ConnectionStringParser::~ConnectionStringParser() {
  // No-op.
}

void ConnectionStringParser::ParseConnectionString(const char* str, size_t len,
                                                   char delimiter) {
  std::string connect_str(str, len);

  while (connect_str.rbegin() != connect_str.rend() && *connect_str.rbegin() == 0)
    connect_str.erase(connect_str.size() - 1);

  while (!connect_str.empty()) {
    size_t attr_begin = connect_str.rfind(delimiter);

    if (attr_begin == std::string::npos)
      attr_begin = 0;
    else
      ++attr_begin;

    size_t attr_eq_pos = connect_str.rfind('=');

    if (attr_eq_pos == std::string::npos) attr_eq_pos = 0;

    if (attr_begin < attr_eq_pos) {
      const char* key_begin = connect_str.data() + attr_begin;
      const char* key_end = connect_str.data() + attr_eq_pos;
      std::string key(key_begin, key_end);
      boost::algorithm::trim(key);

      const char* value_begin = connect_str.data() + attr_eq_pos + 1;
      const char* value_end = connect_str.data() + connect_str.size();
      std::string value(value_begin, value_end);
      boost::algorithm::trim(value);

      if (value[0] == '{' && value[value.size() - 1] == '}') {
        value = value.substr(1, value.size() - 2);
      }

      cfg.Set(key, value);
    }

    if (!attr_begin) break;

    connect_str.erase(attr_begin - 1);
  }
}

void ConnectionStringParser::ParseConnectionString(const std::string& str) {
  ParseConnectionString(str.data(), str.size(), ';');
}

void ConnectionStringParser::ParseConfigAttributes(const char* str) {
  size_t len = 0;

  // Getting list length. List is terminated by two '\0'.
  while (str[len] || str[len + 1]) ++len;

  ++len;

  ParseConnectionString(str, len, '\0');
}

}  // namespace config
}  // namespace flight_sql
}  // namespace driver
