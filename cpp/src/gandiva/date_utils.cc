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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sstream>
#include <vector>

#include "gandiva/date_utils.h"

namespace gandiva {

std::vector<std::string> DateUtils::GetMatches(std::string pattern, bool exactMatch) {
  // we are case insensitive
  std::transform(pattern.begin(), pattern.end(), pattern.begin(), ::tolower);
  std::vector<std::string> matches;

  for (const auto& it : sql_date_format_to_boost_map_) {
    if (it.first.find(pattern) != std::string::npos &&
        (!exactMatch || (it.first.length() == pattern.length()))) {
      matches.push_back(it.first);
    }
  }

  return matches;
}

std::vector<std::string> DateUtils::GetPotentialMatches(const std::string& pattern) {
  return GetMatches(pattern, false);
}

std::vector<std::string> DateUtils::GetExactMatches(const std::string& pattern) {
  return GetMatches(pattern, true);
}

/**
 * Validates and converts format to the strptime equivalent
 *
 */
Status DateUtils::ToInternalFormat(const std::string& format,
                                   std::shared_ptr<std::string>* internal_format) {
  std::stringstream builder;
  std::stringstream buffer;
  bool is_in_quoted_text = false;

  for (size_t i = 0; i < format.size(); i++) {
    char currentChar = format[i];

    // logic before we append to the buffer
    if (currentChar == '"') {
      if (is_in_quoted_text) {
        // we are done with a quoted block
        is_in_quoted_text = false;

        // use ' for quoting
        builder << '\'';
        builder << buffer.str();
        builder << '\'';

        // clear buffer
        buffer.str("");
        continue;
      } else {
        ARROW_RETURN_IF(buffer.str().length() > 0,
                        Status::Invalid("Invalid date format string '", format, "'"));

        is_in_quoted_text = true;
        continue;
      }
    }

    // handle special characters we want to simply pass through, but only if not in quoted
    // and the buffer is empty
    std::string special_characters = "*-/,.;: ";
    if (!is_in_quoted_text && buffer.str().length() == 0 &&
        (special_characters.find_first_of(currentChar) != std::string::npos)) {
      builder << currentChar;
      continue;
    }

    // append to the buffer
    buffer << currentChar;

    // nothing else to do if we are in quoted text
    if (is_in_quoted_text) {
      continue;
    }

    // check how many matches we have for our buffer
    std::vector<std::string> potentialList = GetPotentialMatches(buffer.str());
    int64_t potentialCount = potentialList.size();

    if (potentialCount >= 1) {
      // one potential and the length match
      if (potentialCount == 1 && potentialList[0].length() == buffer.str().length()) {
        // we have a match!
        builder << sql_date_format_to_boost_map_[potentialList[0]];
        buffer.str("");
      } else {
        // Some patterns (like MON, MONTH) can cause ambiguity, such as "MON:".  "MON"
        // will have two potential matches, but "MON:" will match nothing, so we want to
        // look ahead when we match "MON" and check if adding the next char leads to 0
        // potentials.  If it does, we go ahead and treat the buffer as matched (if a
        // potential match exists that matches the buffer)
        if (format.length() - 1 > i) {
          std::string lookAheadPattern = (buffer.str() + format.at(i + 1));
          std::transform(lookAheadPattern.begin(), lookAheadPattern.end(),
                         lookAheadPattern.begin(), ::tolower);
          bool lookAheadMatched = false;

          // we can query potentialList to see if it has anything that matches the
          // lookahead pattern
          for (std::string potential : potentialList) {
            if (potential.find(lookAheadPattern) != std::string::npos) {
              lookAheadMatched = true;
              break;
            }
          }

          if (!lookAheadMatched) {
            // check if any of the potential matches are the same length as our buffer, we
            // do not want to match "MO:"
            bool matched = false;
            for (std::string potential : potentialList) {
              if (potential.length() == buffer.str().length()) {
                matched = true;
                break;
              }
            }

            if (matched) {
              std::string match = buffer.str();
              std::transform(match.begin(), match.end(), match.begin(), ::tolower);
              builder << sql_date_format_to_boost_map_[match];
              buffer.str("");
              continue;
            }
          }
        }
      }
    } else {
      return Status::Invalid("Invalid date format string '", format, "'");
    }
  }

  if (buffer.str().length() > 0) {
    // Some patterns (like MON, MONTH) can cause us to reach this point with a valid
    // buffer value as MON has 2 valid potential matches, so double check here
    std::vector<std::string> exactMatches = GetExactMatches(buffer.str());
    if (exactMatches.size() == 1 && exactMatches[0].length() == buffer.str().length()) {
      builder << sql_date_format_to_boost_map_[exactMatches[0]];
    } else {
      // Format partially parsed
      int64_t pos = format.length() - buffer.str().length();
      return Status::Invalid("Invalid date format string '", format, "' at position ",
                             pos);
    }
  }
  std::string final_pattern = builder.str();
  internal_format->reset(new std::string(final_pattern));
  return Status::OK();
}

DateUtils::date_format_converter DateUtils::sql_date_format_to_boost_map_ = InitMap();

DateUtils::date_format_converter DateUtils::InitMap() {
  date_format_converter map;

  // Era
  map["ad"] = "%EC";
  map["bc"] = "%EC";
  // Meridian
  map["am"] = "%p";
  map["pm"] = "%p";
  // Century
  map["cc"] = "%C";
  // Week of year
  map["ww"] = "%W";
  // Day of week
  map["d"] = "%u";
  // Day name of week
  map["dy"] = "%a";
  map["day"] = "%a";
  // Year
  map["yyyy"] = "%Y";
  map["yy"] = "%y";
  // Day of year
  map["ddd"] = "%j";
  // Month
  map["mm"] = "%m";
  map["mon"] = "%b";
  map["month"] = "%b";
  // Day of month
  map["dd"] = "%d";
  // Hour of day
  map["hh"] = "%I";
  map["hh12"] = "%I";
  map["hh24"] = "%H";
  // Minutes
  map["mi"] = "%M";
  // Seconds
  map["ss"] = "%S";
  // Milliseconds
  map["f"] = "S";
  map["ff"] = "SS";
  map["fff"] = "SSS";
  /*
  // Timezone not tested/supported yet fully.
  map["tzd"] = "%Z";
  map["tzo"] = "%z";
  map["tzh:tzm"] = "%z";
  */

  return map;
}

}  // namespace gandiva
