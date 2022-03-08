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

#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/pcg_random.h"
#include <vector>
#include <string>

namespace arrow
{
    namespace compute
    {
        class OrdersAndLineItemGenerator;
        class PartAndPartSupplierGenerator;

        class TpchGen
        {
        public:
            static Result<TpchGen> Make(ExecPlan *plan, float scale_factor = 1.0f, int64_t batch_size = 4096);

            Result<ExecNode *> Supplier(std::vector<std::string> columns = {});
            Result<ExecNode *> Part(std::vector<std::string> columns = {});
            Result<ExecNode *> PartSupp(std::vector<std::string> columns = {});
            Result<ExecNode *> Customer(std::vector<std::string> columns = {});
            Result<ExecNode *> Orders(std::vector<std::string> columns = {});
            Result<ExecNode *> Lineitem(std::vector<std::string> columns = {});
            Result<ExecNode *> Nation(std::vector<std::string> columns = {});
            Result<ExecNode *> Region(std::vector<std::string> columns = {});

        private:
            TpchGen(ExecPlan *plan, float scale_factor, int64_t batch_size)
                : plan_(plan),
                  scale_factor_(scale_factor),
                  batch_size_(batch_size),
                  orders_and_line_item_generator_(nullptr)
            {}

            template <typename Generator>
            Result<ExecNode *> CreateNode(std::vector<std::string> columns);

            ExecPlan *plan_;
            float scale_factor_;
            int64_t batch_size_;

            std::shared_ptr<PartAndPartSupplierGenerator> part_and_part_supp_generator_;
            std::shared_ptr<OrdersAndLineItemGenerator> orders_and_line_item_generator_;
        };
    }
}
