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

#include <arrow-glib/gobject-type.h>
#include <arrow-glib/version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_DAY_MILLISECOND (garrow_day_millisecond_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDayMillisecond,
                         garrow_day_millisecond,
                         GARROW,
                         DAY_MILLISECOND,
                         GObject)

struct _GArrowDayMillisecondClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowDayMillisecond *
garrow_day_millisecond_new(gint32 day, gint32 millisecond);
GARROW_AVAILABLE_IN_8_0
gboolean
garrow_day_millisecond_equal(GArrowDayMillisecond *day_millisecond,
                             GArrowDayMillisecond *other_day_millisecond);
GARROW_AVAILABLE_IN_8_0
gboolean
garrow_day_millisecond_less_than(GArrowDayMillisecond *day_millisecond,
                                 GArrowDayMillisecond *other_day_millisecond);


#define GARROW_TYPE_MONTH_DAY_NANO (garrow_month_day_nano_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMonthDayNano,
                         garrow_month_day_nano,
                         GARROW,
                         MONTH_DAY_NANO,
                         GObject)

struct _GArrowMonthDayNanoClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowMonthDayNano *
garrow_month_day_nano_new(gint32 month, gint32 day, gint64 nanosecond);
GARROW_AVAILABLE_IN_8_0
gboolean
garrow_month_day_nano_equal(GArrowMonthDayNano *month_nano_day,
                            GArrowMonthDayNano *other_month_nano_day);


G_END_DECLS
