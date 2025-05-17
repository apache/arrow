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

#include <fstream>
#include <sstream>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <iostream>

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

boost::optional<bool> AsBool(const Connection::ConnPropertyMap& connPropertyMap,
                             const std::string_view& property_name) {
  auto extracted_property = connPropertyMap.find(property_name);

  if (extracted_property != connPropertyMap.end()) {
    return AsBool(extracted_property->second);
  }

  return boost::none;
}

boost::optional<int32_t> AsInt32(int32_t min_value,
                                 const Connection::ConnPropertyMap& connPropertyMap,
                                 const std::string_view& property_name) {
  auto extracted_property = connPropertyMap.find(property_name);

  if (extracted_property != connPropertyMap.end()) {
    const int32_t stringColumnLength = std::stoi(extracted_property->second);

    if (stringColumnLength >= min_value && stringColumnLength <= INT32_MAX) {
      return stringColumnLength;
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

void ReadConfigFile(PropertyMap& properties, const std::string& config_file_name) {
  auto config_path = GetModulePath();

  std::ifstream config_file;
  auto config_file_path = config_path + "/" + config_file_name;
  config_file.open(config_file_path);

  if (config_file.fail()) {
    auto error_msg = "Arrow Flight SQL ODBC driver config file not found on \"" +
                     config_file_path + "\"";
    std::cerr << error_msg << std::endl;

    throw DriverException(error_msg);
  }

  std::string temp_config;

  boost::char_separator<char> separator("=");
  while (config_file.good()) {
    config_file >> temp_config;
    boost::tokenizer<boost::char_separator<char>> tokenizer(temp_config, separator);

    auto iterator = tokenizer.begin();

    std::string key = *iterator;
    std::string value = *++iterator;

    properties[key] = std::move(value);
  }
}

}  // namespace odbcabstraction
}  // namespace driver
