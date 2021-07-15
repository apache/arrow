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

#include <sstream>
#include <string>

#include "gandiva/arrow.h"
#include "gandiva/visibility.h"

namespace gandiva {

class GANDIVA_EXPORT StringUtil {
 public:
  static std::vector<std::string> explode(std::string str, std::string delimiter) {
    std::vector<std::string> vector;
    size_t last = 0;
    size_t next = 0;
    while ((next = str.find(delimiter, last)) != std::string::npos) {
      vector.push_back(str.substr(last, next - last));
      last = next + 1;
    }
    // get last item
    vector.push_back(str.substr(last));

    return vector;
  };
};

}  // namespace gandiva
