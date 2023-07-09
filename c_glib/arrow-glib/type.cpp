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

#include <arrow-glib/type.hpp>

/**
 * SECTION: type
 * @title: GArrowType
 * @short_description: Type mapping between Arrow and arrow-glib
 *
 * #GArrowType provides types corresponding to `arrow::Type::type`
 * values.
 */

GArrowType
garrow_type_from_raw(arrow::Type::type type)
{
  switch (type) {
  case arrow::Type::type::NA:
    return GARROW_TYPE_NA;
  case arrow::Type::type::BOOL:
    return GARROW_TYPE_BOOLEAN;
  case arrow::Type::type::UINT8:
    return GARROW_TYPE_UINT8;
  case arrow::Type::type::INT8:
    return GARROW_TYPE_INT8;
  case arrow::Type::type::UINT16:
    return GARROW_TYPE_UINT16;
  case arrow::Type::type::INT16:
    return GARROW_TYPE_INT16;
  case arrow::Type::type::UINT32:
    return GARROW_TYPE_UINT32;
  case arrow::Type::type::INT32:
    return GARROW_TYPE_INT32;
  case arrow::Type::type::UINT64:
    return GARROW_TYPE_UINT64;
  case arrow::Type::type::INT64:
    return GARROW_TYPE_INT64;
  case arrow::Type::type::HALF_FLOAT:
    return GARROW_TYPE_HALF_FLOAT;
  case arrow::Type::type::FLOAT:
    return GARROW_TYPE_FLOAT;
  case arrow::Type::type::DOUBLE:
    return GARROW_TYPE_DOUBLE;
  case arrow::Type::type::STRING:
    return GARROW_TYPE_STRING;
  case arrow::Type::type::BINARY:
    return GARROW_TYPE_BINARY;
  case arrow::Type::type::FIXED_SIZE_BINARY:
    return GARROW_TYPE_FIXED_SIZE_BINARY;
  case arrow::Type::type::DATE32:
    return GARROW_TYPE_DATE32;
  case arrow::Type::type::DATE64:
    return GARROW_TYPE_DATE64;
  case arrow::Type::type::TIMESTAMP:
    return GARROW_TYPE_TIMESTAMP;
  case arrow::Type::type::TIME32:
    return GARROW_TYPE_TIME32;
  case arrow::Type::type::TIME64:
    return GARROW_TYPE_TIME64;
  case arrow::Type::type::INTERVAL_MONTHS:
    return GARROW_TYPE_MONTH_INTERVAL;
  case arrow::Type::type::INTERVAL_DAY_TIME:
    return GARROW_TYPE_DAY_TIME_INTERVAL;
  case arrow::Type::type::DECIMAL128:
    return GARROW_TYPE_DECIMAL128;
  case arrow::Type::type::DECIMAL256:
    return GARROW_TYPE_DECIMAL256;
  case arrow::Type::type::LIST:
    return GARROW_TYPE_LIST;
  case arrow::Type::type::STRUCT:
    return GARROW_TYPE_STRUCT;
  case arrow::Type::type::SPARSE_UNION:
    return GARROW_TYPE_SPARSE_UNION;
  case arrow::Type::type::DENSE_UNION:
    return GARROW_TYPE_DENSE_UNION;
  case arrow::Type::type::DICTIONARY:
    return GARROW_TYPE_DICTIONARY;
  case arrow::Type::type::MAP:
    return GARROW_TYPE_MAP;
  case arrow::Type::type::EXTENSION:
    return GARROW_TYPE_EXTENSION;
  case arrow::Type::type::FIXED_SIZE_LIST:
    return GARROW_TYPE_FIXED_SIZE_LIST;
  case arrow::Type::type::DURATION:
    return GARROW_TYPE_DURATION;
  case arrow::Type::type::LARGE_STRING:
    return GARROW_TYPE_LARGE_STRING;
  case arrow::Type::type::LARGE_BINARY:
    return GARROW_TYPE_LARGE_BINARY;
  case arrow::Type::type::LARGE_LIST:
    return GARROW_TYPE_LARGE_LIST;
  case arrow::Type::type::INTERVAL_MONTH_DAY_NANO:
    return GARROW_TYPE_MONTH_DAY_NANO_INTERVAL;
  case arrow::Type::type::RUN_END_ENCODED:
    return GARROW_TYPE_RUN_END_ENCODED;
  default:
    return GARROW_TYPE_NA;
  }
}

GArrowTimeUnit
garrow_time_unit_from_raw(arrow::TimeUnit::type unit)
{
  switch (unit) {
  case arrow::TimeUnit::type::SECOND:
    return GARROW_TIME_UNIT_SECOND;
  case arrow::TimeUnit::type::MILLI:
    return GARROW_TIME_UNIT_MILLI;
  case arrow::TimeUnit::type::MICRO:
    return GARROW_TIME_UNIT_MICRO;
  case arrow::TimeUnit::type::NANO:
    return GARROW_TIME_UNIT_NANO;
  default:
    return GARROW_TIME_UNIT_SECOND;
  }
}

arrow::TimeUnit::type
garrow_time_unit_to_raw(GArrowTimeUnit unit)
{
  switch (unit) {
  case GARROW_TIME_UNIT_SECOND:
    return arrow::TimeUnit::type::SECOND;
  case GARROW_TIME_UNIT_MILLI:
    return arrow::TimeUnit::type::MILLI;
  case GARROW_TIME_UNIT_MICRO:
    return arrow::TimeUnit::type::MICRO;
  case GARROW_TIME_UNIT_NANO:
    return arrow::TimeUnit::type::NANO;
  default:
    return arrow::TimeUnit::type::SECOND;
  }
}

GArrowIntervalType
garrow_interval_type_from_raw(arrow::IntervalType::type type)
{
  switch (type) {
  case arrow::IntervalType::type::MONTHS:
    return GARROW_INTERVAL_TYPE_MONTH;
  case arrow::IntervalType::type::DAY_TIME:
    return GARROW_INTERVAL_TYPE_DAY_TIME;
  case arrow::IntervalType::type::MONTH_DAY_NANO:
    return GARROW_INTERVAL_TYPE_MONTH_DAY_NANO;
  default:
    return GARROW_INTERVAL_TYPE_MONTH;
  }
}
