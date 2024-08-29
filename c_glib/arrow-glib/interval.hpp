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

#include <memory>

#include <arrow/type.h>

#include <arrow-glib/interval.h>

GArrowDayMillisecond *
garrow_day_millisecond_new_raw(
  arrow::DayTimeIntervalType::DayMilliseconds *arrow_day_millisecond);
arrow::DayTimeIntervalType::DayMilliseconds *
garrow_day_millisecond_get_raw(GArrowDayMillisecond *day_millisecond);
const arrow::DayTimeIntervalType::DayMilliseconds *
garrow_day_millisecond_get_raw(const GArrowDayMillisecond *day_millisecond);

GArrowMonthDayNano *
garrow_month_day_nano_new_raw(
  arrow::MonthDayNanoIntervalType::MonthDayNanos *arrow_month_day_nano);
arrow::MonthDayNanoIntervalType::MonthDayNanos *
garrow_month_day_nano_get_raw(GArrowMonthDayNano *month_day_nano);
const arrow::MonthDayNanoIntervalType::MonthDayNanos *
garrow_month_day_nano_get_raw(const GArrowMonthDayNano *month_day_nano);

