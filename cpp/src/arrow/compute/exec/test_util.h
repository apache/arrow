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

#include <functional>
#include <string>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/testing/visibility.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace compute {

using StartProducingFunc = std::function<Status(ExecNode*)>;
using StopProducingFunc = std::function<void(ExecNode*)>;

// Make a dummy node that has no execution behaviour
ARROW_TESTING_EXPORT
ExecNode* MakeDummyNode(ExecPlan* plan, std::string label, std::vector<ExecNode*> inputs,
                        int num_outputs, StartProducingFunc = {}, StopProducingFunc = {});

ARROW_TESTING_EXPORT
ExecBatch ExecBatchFromJSON(const std::vector<ValueDescr>& descrs,
                            util::string_view json);

}  // namespace compute
}  // namespace arrow
