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

#ifndef ARROW_TYPES_DATETIME_H
#define ARROW_TYPES_DATETIME_H

#include "arrow/type.h"

namespace arrow {

struct DateType : public DataType {
  enum class Unit: char {
    DAY = 0,
    MONTH = 1,
    YEAR = 2
  };

  Unit unit;

  explicit DateType(Unit unit = Unit::DAY, bool nullable = true)
      : DataType(LogicalType::DATE, nullable),
        unit(unit) {}

  DateType(const DateType& other)
      : DateType(other.unit) {}

  static char const *name() {
    return "date";
  }

  // virtual std::string ToString() {
  //   return name();
  // }
};


struct TimestampType : public DataType {
  enum class Unit: char {
    SECOND = 0,
    MILLI = 1,
    MICRO = 2,
    NANO = 3
  };

  Unit unit;

  explicit TimestampType(Unit unit = Unit::MILLI, bool nullable = true)
      : DataType(LogicalType::TIMESTAMP, nullable),
        unit(unit) {}

  TimestampType(const TimestampType& other)
      : TimestampType(other.unit) {}

  static char const *name() {
    return "timestamp";
  }

  // virtual std::string ToString() {
  //   return name();
  // }
};

} // namespace arrow

#endif // ARROW_TYPES_DATETIME_H
