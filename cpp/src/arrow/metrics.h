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

#pragma once

#include <cstdint>
#include <limits>

#include "arrow/util/visibility.h"

namespace arrow {

enum ARROW_EXPORT Unit {
  NONE,
  TIME_NANO,
  TIME_MICRO,
  TIME_MILLI,
  TIME_SECOND,
  SIZE_BYTE,
  SIZE_KB,
  SIZE_MB,
  UNIT_MAX
};

struct ARROW_EXPORT Counter {
  Unit unit{NONE};
  int64_t value{0};

  inline void Update(int64_t v) { value += v; }

  inline void Merge(const Counter& other) { value += other.value; }

  inline void Reset(int64_t v, Unit u = NONE) {
    value = v;
    unit = u;
  }
};

struct ARROW_EXPORT Metric {
  Unit unit{TIME_NANO};

  int64_t sum{0};
  int32_t count{0};
  int64_t min{std::numeric_limits<int64_t>::max()};
  int64_t max{std::numeric_limits<int64_t>::min()};

  inline void Update(int64_t value) {
    sum += value;
    count++;

    if (min > value) {
      min = value;
    }
    if (max < value) {
      max = value;
    }
  };

  inline void Merge(const Metric& other) {
    sum += other.sum;
    count += other.count;
    if (min > other.min) {
      min = other.min;
    }
    if (max < other.max) {
      max = other.max;
    }
  }

  inline void Reset(int64_t o_sum, int32_t o_count, int64_t o_min, int64_t o_max,
                    Unit o_unit) {
    unit = o_unit;
    sum = o_sum;
    count = o_count;
    min = o_min;
    max = o_max;
  }
};

}  // namespace arrow
