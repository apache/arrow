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

#include <unordered_map>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_basic_internal.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace compute {

/// \brief Configure a grouped aggregation
struct ARROW_EXPORT GroupByOptions {
  struct Aggregate {
    /// the name of the aggregation function
    std::string function;

    /// options for the aggregation function
    const FunctionOptions* options;
  };

  GroupByOptions() = default;

  GroupByOptions(std::initializer_list<Aggregate> aggregates) : aggregates(aggregates) {}

  explicit GroupByOptions(std::vector<Aggregate> aggregates)
      : aggregates(std::move(aggregates)) {}

  std::vector<Aggregate> aggregates;
};

/// Internal use only: helper function for testing HashAggregateKernels.
/// This will be replaced by streaming execution operators.
ARROW_EXPORT
Result<Datum> GroupBy(const std::vector<Datum>& aggregands,
                      const std::vector<Datum>& keys, const GroupByOptions& options,
                      ExecContext* ctx = nullptr);

}  // namespace compute
}  // namespace arrow
