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
struct ARROW_EXPORT GroupByOptions : public FunctionOptions {
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

ARROW_EXPORT
Result<Datum> GroupBy(const std::vector<Datum>& aggregands,
                      const std::vector<Datum>& keys, const GroupByOptions& options,
                      ExecContext* ctx = nullptr);

struct GroupedAggregator {
  virtual ~GroupedAggregator() = default;

  virtual void Init(KernelContext*, const FunctionOptions*,
                    const std::shared_ptr<DataType>&) = 0;

  virtual void Consume(KernelContext*, const Datum& aggregand,
                       const uint32_t* group_ids) = 0;

  virtual void Finalize(KernelContext* ctx, Datum* out) = 0;

  virtual void Resize(KernelContext* ctx, int64_t new_num_groups) = 0;

  virtual int64_t num_groups() const = 0;

  void MaybeResize(KernelContext* ctx, int64_t length, const uint32_t* group_ids) {
    if (length == 0) return;

    // maybe a batch of group_ids should include the min/max group id
    int64_t max_group = *std::max_element(group_ids, group_ids + length);
    auto old_size = num_groups();

    if (max_group >= old_size) {
      auto new_size = BufferBuilder::GrowByFactor(old_size, max_group + 1);
      Resize(ctx, new_size);
    }
  }

  virtual std::shared_ptr<DataType> out_type() const = 0;
};

}  // namespace compute
}  // namespace arrow
