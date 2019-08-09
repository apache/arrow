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

#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

ARROW_EXPORT std::string HexEncode(const uint8_t* data, size_t length);

ARROW_EXPORT std::string Escape(const char* data, size_t length);

ARROW_EXPORT std::string HexEncode(const char* data, size_t length);

ARROW_EXPORT std::string HexEncode(util::string_view str);

ARROW_EXPORT std::string Escape(util::string_view str);

ARROW_EXPORT Status ParseHexValue(const char* data, uint8_t* out);

}  // namespace arrow
