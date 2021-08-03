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

#include "gandiva/arrow.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Utility class for formatting numbers.
class GANDIVA_EXPORT FormatNumberUtil {
 public:
  /// \brief Add thousand separators with support for positive and negative numbers
  ///
  /// \param value number as string to be formatted
  /// \param thousandSep separator for the thousands, default is ','
  /// \param decimalSep decimal point separator, default is '.'
  /// \param sourceDecimalSep decimal point separator from value source, default is '.'
  /// \return the formatted number following with thousand separator, default pattern
  /// is ###,###,###.##
  static std::string AddThousandSeparators(std::string value, char thousandSep = ',',
                                           char decimalSep = '.',
                                           char sourceDecimalSep = '.') {
    {
      size_t len = value.length();
      size_t negative = ((len && value[0] == '-') ? 1 : 0);
      size_t dpos = value.find_last_of(sourceDecimalSep);
      size_t dlen = 3 + (dpos == std::string::npos ? 0 : (len - dpos));

      if (dpos != std::string::npos && decimalSep != sourceDecimalSep) {
        value[dpos] = decimalSep;
      }

      while ((len - negative) > dlen) {
        value.insert(len - dlen, 1, thousandSep);
        dlen += 4;
        len += 1;
      }
      return value;
    }
  }
};

}  // namespace gandiva
