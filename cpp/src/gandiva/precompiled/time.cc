/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include<time.h>
#include<stdlib.h>
#include "./types.h"

#define MILLIS_TO_SEC(millis) (millis / 1000)

/*
 * Expand inner macro for all date types.
 */
#define DATE_TYPES(INNER) \
  INNER(date) \
  INNER(time64) \
  INNER(timestamp64)

/*
 * Extract  year.
 */
#define EXTRACT_YEAR(TYPE) \
  __attribute__((always_inline)) \
  int64 extractYear##_##TYPE(TYPE millis) { \
    time_t tsec = (time_t) MILLIS_TO_SEC(millis); \
    struct tm tm; \
    gmtime_r(&tsec, &tm); \
    return 1900 + tm.tm_year; \
  }

DATE_TYPES(EXTRACT_YEAR)

#define EXTRACT_MONTH(TYPE) \
  __attribute__((always_inline)) \
  int64 extractMonth##_##TYPE(TYPE millis) { \
    time_t tsec = (time_t) MILLIS_TO_SEC(millis); \
    struct tm tm; \
    gmtime_r(&tsec, &tm); \
    return 1 + tm.tm_mon; \
  }

DATE_TYPES(EXTRACT_MONTH)

#define EXTRACT_DAY(TYPE) \
  __attribute__((always_inline)) \
  int64 extractDay##_##TYPE(TYPE millis) { \
    time_t tsec = (time_t) MILLIS_TO_SEC(millis); \
    struct tm tm; \
    gmtime_r(&tsec, &tm); \
    return tm.tm_mday; \
  }

DATE_TYPES(EXTRACT_DAY)

#define EXTRACT_HOUR(TYPE) \
  __attribute__((always_inline)) \
  int64 extractHour##_##TYPE(TYPE millis) { \
    time_t tsec = (time_t) MILLIS_TO_SEC(millis); \
    struct tm tm; \
    gmtime_r(&tsec, &tm); \
    return tm.tm_hour; \
  }

DATE_TYPES(EXTRACT_HOUR)

#define EXTRACT_MINUTE(TYPE) \
  __attribute__((always_inline)) \
  int64 extractMinute##_##TYPE(TYPE millis) { \
    time_t tsec = (time_t) MILLIS_TO_SEC(millis); \
    struct tm tm; \
    gmtime_r(&tsec, &tm); \
    return tm.tm_min; \
  }

DATE_TYPES(EXTRACT_MINUTE)

