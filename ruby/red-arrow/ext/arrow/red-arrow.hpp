/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow/api.h>

#ifdef _WIN32
#  define gmtime_r gmtime_r_ruby_win32
#  define localtime_r localtime_r_ruby_win32
#  include <ruby.h>
#  undef gmtime_r
#  undef localtime_r
#endif

#include <arrow-glib/arrow-glib.hpp>
#include <rbgobject.h>

namespace red_arrow {
  extern VALUE cDate;

  extern ID id_BigDecimal;
  extern ID id_jd;
  extern ID id_to_datetime;

  VALUE record_batch_raw_records(VALUE obj);

  inline VALUE time_unit_to_scale(arrow::TimeUnit::type unit) {
    switch (unit) {
    case arrow::TimeUnit::SECOND:
      return INT2FIX(1);
    case arrow::TimeUnit::MILLI:
      return INT2FIX(1000);
    case arrow::TimeUnit::MICRO:
      return INT2FIX(1000 * 1000);
    case arrow::TimeUnit::NANO:
      // NOTE: INT2FIX works for 1e+9 because: FIXNUM_MAX >= (1<<30) - 1 > 1e+9
      return INT2FIX(1000 * 1000 * 1000);
    default:
      break; // NOT REACHED
    }
    return Qnil;
  }
}
