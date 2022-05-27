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

#include <arrow-glib/arrow-glib.h>

G_BEGIN_DECLS


#define GPARQUET_TYPE_STATISTICS                \
  (gparquet_statistics_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetStatistics,
                         gparquet_statistics,
                         GPARQUET,
                         STATISTICS,
                         GObject)
struct _GParquetStatisticsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
gboolean
gparquet_statistics_equal(GParquetStatistics *statistics,
                          GParquetStatistics *other_statistics);
GARROW_AVAILABLE_IN_8_0
gboolean
gparquet_statistics_has_n_nulls(GParquetStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
gint64
gparquet_statistics_get_n_nulls(GParquetStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
gboolean
gparquet_statistics_has_n_distinct_values(GParquetStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
gint64
gparquet_statistics_get_n_distinct_values(GParquetStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
gint64
gparquet_statistics_get_n_values(GParquetStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
gboolean
gparquet_statistics_has_min_max(GParquetStatistics *statistics);


#define GPARQUET_TYPE_BOOLEAN_STATISTICS        \
  (gparquet_boolean_statistics_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetBooleanStatistics,
                         gparquet_boolean_statistics,
                         GPARQUET,
                         BOOLEAN_STATISTICS,
                         GParquetStatistics)
struct _GParquetBooleanStatisticsClass
{
  GParquetStatisticsClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
gboolean
gparquet_boolean_statistics_get_min(GParquetBooleanStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
gboolean
gparquet_boolean_statistics_get_max(GParquetBooleanStatistics *statistics);


#define GPARQUET_TYPE_INT32_STATISTICS          \
  (gparquet_int32_statistics_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetInt32Statistics,
                         gparquet_int32_statistics,
                         GPARQUET,
                         INT32_STATISTICS,
                         GParquetStatistics)
struct _GParquetInt32StatisticsClass
{
  GParquetStatisticsClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
gint32
gparquet_int32_statistics_get_min(GParquetInt32Statistics *statistics);
GARROW_AVAILABLE_IN_8_0
gint32
gparquet_int32_statistics_get_max(GParquetInt32Statistics *statistics);


#define GPARQUET_TYPE_INT64_STATISTICS          \
  (gparquet_int64_statistics_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetInt64Statistics,
                         gparquet_int64_statistics,
                         GPARQUET,
                         INT64_STATISTICS,
                         GParquetStatistics)
struct _GParquetInt64StatisticsClass
{
  GParquetStatisticsClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
gint64
gparquet_int64_statistics_get_min(GParquetInt64Statistics *statistics);
GARROW_AVAILABLE_IN_8_0
gint64
gparquet_int64_statistics_get_max(GParquetInt64Statistics *statistics);


#define GPARQUET_TYPE_FLOAT_STATISTICS          \
  (gparquet_float_statistics_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetFloatStatistics,
                         gparquet_float_statistics,
                         GPARQUET,
                         FLOAT_STATISTICS,
                         GParquetStatistics)
struct _GParquetFloatStatisticsClass
{
  GParquetStatisticsClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
gfloat
gparquet_float_statistics_get_min(GParquetFloatStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
gfloat
gparquet_float_statistics_get_max(GParquetFloatStatistics *statistics);


#define GPARQUET_TYPE_DOUBLE_STATISTICS          \
  (gparquet_double_statistics_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetDoubleStatistics,
                         gparquet_double_statistics,
                         GPARQUET,
                         DOUBLE_STATISTICS,
                         GParquetStatistics)
struct _GParquetDoubleStatisticsClass
{
  GParquetStatisticsClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
gdouble
gparquet_double_statistics_get_min(GParquetDoubleStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
gdouble
gparquet_double_statistics_get_max(GParquetDoubleStatistics *statistics);


#define GPARQUET_TYPE_BYTE_ARRAY_STATISTICS          \
  (gparquet_byte_array_statistics_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetByteArrayStatistics,
                         gparquet_byte_array_statistics,
                         GPARQUET,
                         BYTE_ARRAY_STATISTICS,
                         GParquetStatistics)
struct _GParquetByteArrayStatisticsClass
{
  GParquetStatisticsClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GBytes *
gparquet_byte_array_statistics_get_min(GParquetByteArrayStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
GBytes *
gparquet_byte_array_statistics_get_max(GParquetByteArrayStatistics *statistics);


#define GPARQUET_TYPE_FIXED_LENGTH_BYTE_ARRAY_STATISTICS        \
  (gparquet_fixed_length_byte_array_statistics_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetFixedLengthByteArrayStatistics,
                         gparquet_fixed_length_byte_array_statistics,
                         GPARQUET,
                         FIXED_LENGTH_BYTE_ARRAY_STATISTICS,
                         GParquetStatistics)
struct _GParquetFixedLengthByteArrayStatisticsClass
{
  GParquetStatisticsClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GBytes *
gparquet_fixed_length_byte_array_statistics_get_min(
  GParquetFixedLengthByteArrayStatistics *statistics);
GARROW_AVAILABLE_IN_8_0
GBytes *
gparquet_fixed_length_byte_array_statistics_get_max(
  GParquetFixedLengthByteArrayStatistics *statistics);


G_END_DECLS
