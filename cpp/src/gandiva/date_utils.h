// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TO_DATE_HELPER_H
#define TO_DATE_HELPER_H

#include <unordered_map>
#include <vector>

#include "gandiva/status.h"

namespace gandiva {

#ifdef GDV_HELPERS
namespace helpers {
#endif

/// \brief Utility class for converting sql date patterns to internal date patterns.
class DateUtils {
 public:
  static Status ToInternalFormat(const std::string& format,
                                 std::shared_ptr<std::string>* internal_format);

 private:
  using date_format_converter = std::unordered_map<std::string, std::string>;

  static date_format_converter sql_date_format_to_boost_map_;

  static date_format_converter InitMap();

  static std::vector<std::string> GetMatches(std::string pattern, bool exactMatch);

  static std::vector<std::string> GetPotentialMatches(std::string pattern);

  static std::vector<std::string> GetExactMatches(std::string pattern);
};

#ifdef GDV_HELPERS
}  // namespace helpers
#endif

}  // namespace gandiva

#endif  // TO_DATE_HELPER_H
