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

#include "arrow/compute/kernels/temporal_internal.h"

namespace arrow::compute::internal {

Result<ArrowTimeZone> LocateZone(const std::string_view timezone) {
  if (timezone[0] == '+' || timezone[0] == '-') {
    // Valid offset strings have to have 4 digits and a sign prefix.
    // Valid examples: +01:23 and -0123.
    // Invalid examples: 1:23, 123, 0123, 01:23, +25:00, -12:34:45, +090000.
    auto offset = std::string(timezone.substr(1));
    std::chrono::minutes zone_offset;
    switch (timezone.length()) {
      case 6:
        if (arrow::internal::detail::ParseHH_MM(offset.c_str(), &zone_offset)) {
          break;
        }
        [[fallthrough]];
      case 5:
        if (arrow::internal::detail::ParseHHMM(offset.c_str(), &zone_offset)) {
          break;
        }
        [[fallthrough]];
      default:
        return Status::Invalid("Cannot locate or parse timezone '", timezone, "'");
    }
    zone_offset = timezone[0] == '-' ? -zone_offset : zone_offset;
    return OffsetZone(zone_offset);
  }

  try {
    return locate_zone(timezone);
  } catch (const std::runtime_error& ex) {
    return Status::Invalid("Cannot locate or parse timezone '", timezone,
                           "': ", ex.what());
  }
}

}  // namespace arrow::compute::internal
