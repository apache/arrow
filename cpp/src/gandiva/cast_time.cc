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

#include <cstdint>

#include "arrow/vendored/datetime.h"

#include "gandiva/precompiled/time_fields.h"

#ifndef GANDIVA_UNIT_TEST
#include "gandiva/exported_funcs.h"
#include "gandiva/gdv_function_stubs.h"

#include "gandiva/engine.h"

namespace gandiva {

void ExportedTimeFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_fn_time_with_zone
  args = {types->ptr_type(types->i32_type()),  // time fields
          types->i8_ptr_type(),                // const char* zone
          types->i32_type(),                   // int data_len
          types->i64_type()};                  // timestamp *ret_time

  engine->AddGlobalMappingForFunc("gdv_fn_time_with_zone",
                                  types->i32_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_time_with_zone));
}

}  // namespace gandiva
#endif  // !GANDIVA_UNIT_TEST

extern "C" {

// TODO : Do input validation or make sure the callers do that ?
int gdv_fn_time_with_zone(int* time_fields, const char* zone, int zone_len,
                          int64_t* ret_time) {
  using arrow_vendored::date::day;
  using arrow_vendored::date::local_days;
  using arrow_vendored::date::locate_zone;
  using arrow_vendored::date::month;
  using arrow_vendored::date::time_zone;
  using arrow_vendored::date::year;
  using std::chrono::hours;
  using std::chrono::milliseconds;
  using std::chrono::minutes;
  using std::chrono::seconds;

  using gandiva::TimeFields;
  try {
    const time_zone* tz = locate_zone(std::string(zone, zone_len));
    *ret_time = tz->to_sys(local_days(year(time_fields[TimeFields::kYear]) /
                                      month(time_fields[TimeFields::kMonth]) /
                                      day(time_fields[TimeFields::kDay])) +
                           hours(time_fields[TimeFields::kHours]) +
                           minutes(time_fields[TimeFields::kMinutes]) +
                           seconds(time_fields[TimeFields::kSeconds]) +
                           milliseconds(time_fields[TimeFields::kSubSeconds]))
                    .time_since_epoch()
                    .count();
  } catch (...) {
    return EINVAL;
  }

  return 0;
}

}  // extern "C"
