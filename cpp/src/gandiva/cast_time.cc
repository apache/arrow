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

#include "arrow/util/chrono_internal.h"

#include "gandiva/precompiled/time_fields.h"

#ifndef GANDIVA_UNIT_TEST
#  include "gandiva/exported_funcs.h"
#  include "gandiva/gdv_function_stubs.h"

#  include "gandiva/engine.h"

namespace gandiva {

arrow::Status ExportedTimeFunctions::AddMappings(Engine* engine) const {
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
  return arrow::Status::OK();
}

}  // namespace gandiva
#endif  // !GANDIVA_UNIT_TEST

namespace chrono = arrow::internal::chrono;

extern "C" {

// TODO : Do input validation or make sure the callers do that ?
int gdv_fn_time_with_zone(int* time_fields, const char* zone, int zone_len,
                          int64_t* ret_time) {
  using chrono::day;
  using chrono::local_days;
  using chrono::locate_zone;
  using chrono::month;
  using chrono::time_zone;
  using chrono::year;
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
