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

#include "gandiva/regex_util.h"

namespace gandiva {

const std::set<char> RegexUtil::pcre_regex_specials_ = {
    '[', ']', '(', ')', '|', '^', '-', '+', '*', '?', '{', '}', '$', '\\'};

Status RegexUtil::SqlLikePatternToPcre(const std::string& sql_pattern, char escape_char,
                                       std::string& pcre_pattern) {
  /// Characters that are considered special by pcre regex. These needs to be
  /// escaped with '\\'.
  pcre_pattern.clear();
  for (size_t idx = 0; idx < sql_pattern.size(); ++idx) {
    auto cur = sql_pattern.at(idx);

    // Escape any char that is special for pcre regex
    if (pcre_regex_specials_.find(cur) != pcre_regex_specials_.end()) {
      pcre_pattern += "\\";
    }

    if (cur == escape_char) {
      // escape char must be followed by '_', '%' or the escape char itself.
      ++idx;
      if (idx == sql_pattern.size()) {
        std::stringstream msg;
        msg << "unexpected escape char at the end of pattern " << sql_pattern;
        return Status::Invalid(msg.str());
      }

      cur = sql_pattern.at(idx);
      if (cur == '_' || cur == '%' || cur == escape_char) {
        pcre_pattern += cur;
      } else {
        std::stringstream msg;
        msg << "invalid escape sequence in pattern " << sql_pattern << " at offset "
            << idx;
        return Status::Invalid(msg.str());
      }
    } else if (cur == '_') {
      pcre_pattern += '.';
    } else if (cur == '%') {
      pcre_pattern += ".*";
    } else {
      pcre_pattern += cur;
    }
  }
  return Status::OK();
}

}  // namespace gandiva
