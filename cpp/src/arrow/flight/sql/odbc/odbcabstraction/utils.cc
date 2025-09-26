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

#include "arrow/vendored/whereami/whereami.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/utils.h"

#include <boost/algorithm/string/predicate.hpp>

namespace driver {
namespace odbcabstraction {

boost::optional<bool> AsBool(const std::string& value) {
  if (boost::iequals(value, "true") || boost::iequals(value, "1")) {
    return true;
  } else if (boost::iequals(value, "false") || boost::iequals(value, "0")) {
    return false;
  } else {
    return boost::none;
  }
}

boost::optional<bool> AsBool(const Connection::ConnPropertyMap& conn_property_map,
                             const std::string_view& property_name) {
  auto extracted_property = conn_property_map.find(std::string(property_name));

  if (extracted_property != conn_property_map.end()) {
    return AsBool(extracted_property->second);
  }

  return boost::none;
}

boost::optional<int32_t> AsInt32(int32_t min_value,
                                 const Connection::ConnPropertyMap& conn_property_map,
                                 const std::string_view& property_name) {
  auto extracted_property = conn_property_map.find(std::string(property_name));

  if (extracted_property != conn_property_map.end()) {
    const int32_t string_column_length = std::stoi(extracted_property->second);

    if (string_column_length >= min_value && string_column_length <= INT32_MAX) {
      return string_column_length;
    }
  }
  return boost::none;
}

std::string GetModulePath() {
  std::vector<char> path;
  int length, dirname_length;
  length = wai_getModulePath(NULL, 0, &dirname_length);

  if (length != 0) {
    path.resize(length);
    wai_getModulePath(path.data(), length, &dirname_length);
  } else {
    throw DriverException("Could not find module path.");
  }

  return std::string(path.begin(), path.begin() + dirname_length);
}

}  // namespace odbcabstraction
}  // namespace driver
