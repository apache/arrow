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

#ifndef GANDIVA_REGEX_UTIL_H
#define GANDIVA_REGEX_UTIL_H

#include <set>
#include <sstream>
#include <string>

#include "gandiva/arrow.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Utility class for converting sql patterns to pcre patterns.
class GANDIVA_EXPORT RegexUtil {
 public:
  // Convert an sql pattern to a pcre pattern
  static Status SqlLikePatternToPcre(const std::string& like_pattern, char escape_char,
                                     std::string& pcre_pattern);

  static Status SqlLikePatternToPcre(const std::string& like_pattern,
                                     std::string& pcre_pattern) {
    return SqlLikePatternToPcre(like_pattern, 0 /*escape_char*/, pcre_pattern);
  }

 private:
  static const std::set<char> pcre_regex_specials_;
};

}  // namespace gandiva

#endif  // GANDIVA_REGEX_UTIL_H
