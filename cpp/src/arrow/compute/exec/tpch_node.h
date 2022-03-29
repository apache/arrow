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

#include <string>
#include <vector>
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/pcg_random.h"

namespace arrow {
namespace compute {
namespace internal {
class OrdersAndLineItemGenerator;
class PartAndPartSupplierGenerator;

class ARROW_EXPORT TpchGen {
 public:
  /*
   * \brief Create a factory for nodes that generate TPC-H data
   *
   * Note: Individual tables will reference each other.  It is important that you only
   * create a single TpchGen instance for each plan and then you can create nodes for each
   * table from that single TpchGen instance. Note: Every batch will be scheduled as a new
   * task using the ExecPlan's scheduler.
   */
  static Result<TpchGen> Make(ExecPlan* plan, double scale_factor = 1.0,
                              int64_t batch_size = 4096,
                              util::optional<int64_t> seed = util::nullopt);

  // The below methods will create and add an ExecNode to the plan that generates
  // data for the desired table. If columns is empty, all columns will be generated.
  // The methods return the added ExecNode, which should be used for inputs.
  Result<ExecNode*> Supplier(std::vector<std::string> columns = {});
  Result<ExecNode*> Part(std::vector<std::string> columns = {});
  Result<ExecNode*> PartSupp(std::vector<std::string> columns = {});
  Result<ExecNode*> Customer(std::vector<std::string> columns = {});
  Result<ExecNode*> Orders(std::vector<std::string> columns = {});
  Result<ExecNode*> Lineitem(std::vector<std::string> columns = {});
  Result<ExecNode*> Nation(std::vector<std::string> columns = {});
  Result<ExecNode*> Region(std::vector<std::string> columns = {});

 private:
  TpchGen(ExecPlan* plan, double scale_factor, int64_t batch_size, int64_t seed)
      : plan_(plan),
        scale_factor_(scale_factor),
        batch_size_(batch_size),
        seed_rng_(seed) {}

  template <typename Generator>
  Result<ExecNode*> CreateNode(const char* name, std::vector<std::string> columns);

  ExecPlan* plan_;
  double scale_factor_;
  int64_t batch_size_;
  random::pcg64_fast seed_rng_;

  std::shared_ptr<PartAndPartSupplierGenerator> part_and_part_supp_generator_{};
  std::shared_ptr<OrdersAndLineItemGenerator> orders_and_line_item_generator_{};
};
}  // namespace internal
}  // namespace compute
}  // namespace arrow
