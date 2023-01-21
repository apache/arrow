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

// approximate quantiles from arbitrary length dataset with O(1) space
// based on 'Computing Extremely Accurate Quantiles Using t-Digests' from Dunning & Ertl
// - https://arxiv.org/abs/1902.04023
// - https://github.com/tdunning/t-digest

#pragma once

#include <cmath>
#include <cstring>

#include <array>

#include <datasketches-cpp/hll/include/hll.hpp>

#include "arrow/type.h"
#include "arrow/util/decimal.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace internal {

struct ARROW_EXPORT HllImpl {
  explicit HllImpl(uint8_t lg_config_k) : hll{lg_config_k} {}

  template <typename T>
  void Update(T value) {
    hll.update(value);
  }

  void Update(Decimal128 x) {
    auto bytes = x.ToBytes();
    hll.update(bytes.data(), bytes.size());
  }

  void Update(Decimal256 x) {
    auto bytes = x.ToBytes();
    hll.update(bytes.data(), bytes.size());
  }

  void Update(std::string_view x) { hll.update(x.data(), x.size()); }

  void Update(MonthDayNanoIntervalType::MonthDayNanos mdn) {
    std::array<char, sizeof(mdn.months) + sizeof(mdn.days) + sizeof(mdn.nanoseconds)>
        data;
    std::memcpy(&data[0], &mdn.months, sizeof(mdn.months));
    std::memcpy(&data[sizeof(mdn.months)], &mdn.days, sizeof(mdn.days));
    std::memcpy(&data[sizeof(mdn.months) + sizeof(mdn.days)], &mdn.nanoseconds,
                sizeof(mdn.nanoseconds));
    hll.update(data.data(), data.size());
  }

  void Update(DayTimeIntervalType::DayMilliseconds dm) {
    std::array<char, sizeof(dm.days) + sizeof(dm.milliseconds)> data;
    std::memcpy(&data[0], &dm.days, sizeof(dm.days));
    std::memcpy(&data[sizeof(dm.days)], &dm.milliseconds, sizeof(dm.milliseconds));
    hll.update(data.data(), data.size());
  }

  void Merge(const HllImpl& that) { hll.update(that.hll.get_result()); }

  double Finalize() const { return hll.get_estimate(); }

  datasketches::hll_union hll;
};

}  // namespace internal
}  // namespace arrow
