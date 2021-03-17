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

#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/function.h"
#include "arrow/datum.h"

namespace arrow {
namespace compute {
namespace internal {

/// Internal use only: streaming group identifier.
/// Consumes batches of keys and yields batches of the group ids.
class ARROW_EXPORT GroupIdentifier {
 public:
  virtual ~GroupIdentifier() = default;

  /// Construct a GroupIdentifier which receives the specified key types
  static Result<std::unique_ptr<GroupIdentifier>> Make(
      ExecContext* ctx, const std::vector<ValueDescr>& descrs);

  /// Consume a batch of keys, producing an array of the corresponding
  /// group ids as an integer column.
  virtual Result<ExecBatch> Consume(const ExecBatch& batch) = 0;

  /// Get current unique keys. May be called repeatedly.
  virtual Result<ExecBatch> GetUniqueKeys() = 0;
};

/// \brief Configure a grouped aggregation
struct ARROW_EXPORT Aggregate {
  /// the name of the aggregation function
  std::string function;

  /// options for the aggregation function
  const FunctionOptions* options;
};

/// Internal use only: helper function for testing HashAggregateKernels.
/// This will be replaced by streaming execution operators.
ARROW_EXPORT
Result<Datum> GroupBy(const std::vector<Datum>& aggregands,
                      const std::vector<Datum>& keys,
                      const std::vector<Aggregate>& aggregates,
                      ExecContext* ctx = nullptr);

}  // namespace internal
}  // namespace compute
}  // namespace arrow
