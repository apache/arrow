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

#include <memory>
#include <string>
#include <unordered_map>

#include "arrow/vendored/datetime/tz.h"
#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

using arrow_vendored::date::local_time;
using arrow_vendored::date::locate_zone;
using arrow_vendored::date::make_zoned;
using arrow_vendored::date::time_zone;
using std::chrono::milliseconds;

namespace gandiva {

/// Function Holder for SQL 'CONVERT_TIMEZONE'
class GANDIVA_EXPORT ConvertTimezoneHolder : public FunctionHolder {
 public:
  ~ConvertTimezoneHolder() override = default;

  static Status Make(const FunctionNode& node,
                     std::shared_ptr<ConvertTimezoneHolder>* holder);

  static Status Make(const std::string& srcTz, const std::string& destTz,
                     std::shared_ptr<ConvertTimezoneHolder>* holder);

  /// Return the converted timestamp
  int64_t convert(const int64_t src_timestamp) {
    auto src_maked_zone =
        make_zoned(src_timezone, local_time<milliseconds>(milliseconds(src_timestamp)));
    auto dest_maked_zone = make_zoned(dest_timezone, src_maked_zone);
    return dest_maked_zone.get_local_time().time_since_epoch().count();
  }

  // Tracks if the timezones given could be found in IANA Timezone DB.
  bool ok = true;

 private:
  explicit ConvertTimezoneHolder(const std::string& srcTz, const std::string& destTz) {
    auto srcTz_abbrv = abbrv_tz.find(srcTz);
    auto destTz_abbrv = abbrv_tz.find(destTz);

    try {
      if (srcTz_abbrv != abbrv_tz.end()) {
        src_timezone = locate_zone(srcTz_abbrv->second);
      } else {
        src_timezone = locate_zone(srcTz);
      }

      if (destTz_abbrv != abbrv_tz.end()) {
        dest_timezone = locate_zone(destTz_abbrv->second);
      } else {
        dest_timezone = locate_zone(destTz);
      }
    } catch (...) {
      ok = false;
    }
  }

  const time_zone* src_timezone;
  const time_zone* dest_timezone;

  static std::unordered_map<std::string, std::string> abbrv_tz;
};

}  // namespace gandiva
