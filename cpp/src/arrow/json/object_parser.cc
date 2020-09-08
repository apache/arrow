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

#include <sstream>

#include "arrow/json/object_parser.h"

namespace arrow {

namespace json {

bool ObjectParser::Parse(arrow::util::string_view json) {
  _document.Parse(json.data());

  if (_document.HasParseError() || !_document.IsObject()) {
    return false;
  }
  return true;
}

Result<std::string> ObjectParser::GetString(const char* key) const {
  std::ostringstream ss;
  if (!_document.HasMember(key)) {
    ss << key << " does not exist";
    return Result<std::string>(Status(StatusCode::KeyError, ss.str()));
  }
  if (!_document[key].IsString()) {
    ss << key << " is not a string";
    return Result<std::string>(Status(StatusCode::TypeError, ss.str()));
  }
  return Result<std::string>(_document[key].GetString());
}

Result<bool> ObjectParser::GetBool(const char* key) const {
  std::ostringstream ss;
  if (!_document.HasMember(key)) {
    ss << key << " does not exist";
    return Result<bool>(Status(StatusCode::KeyError, ss.str()));
  }
  if (!_document[key].IsBool()) {
    ss << key << " is not a boolean";
    return Result<bool>(Status(StatusCode::TypeError, ss.str()));
  }
  return Result<bool>(_document[key].GetBool());
}

}  // namespace json

}  // namespace arrow
