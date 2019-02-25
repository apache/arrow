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

#include <arrow-glib/arrow-glib.hpp>
#include <rbgobject.h>

namespace red_arrow {
  extern VALUE mArrow;
  extern VALUE cDate;

  extern ID id_BigDecimal;
  extern ID id_jd;
  extern ID id_to_datetime;

  VALUE record_batch_raw_records(int argc, VALUE* argv, VALUE obj);

  extern VALUE timeunit_scale_second;
  extern VALUE timeunit_scale_milli;
  extern VALUE timeunit_scale_micro;
  extern VALUE timeunit_scale_nano;

  inline VALUE time_unit_to_scale(arrow::TimeUnit::type unit) {
    switch (unit) {
      case arrow::TimeUnit::SECOND:
        return timeunit_scale_second;
      case arrow::TimeUnit::MILLI:
        return timeunit_scale_milli;
      case arrow::TimeUnit::MICRO:
        return timeunit_scale_micro;
      case arrow::TimeUnit::NANO:
        return timeunit_scale_nano;
      default:
        break; // NOT REACHED
    }
    return Qnil;
  }
}
