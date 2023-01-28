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

#include "arrow/compute/exec/test_nodes.h"

#include <deque>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/util.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

AsyncGenerator<std::optional<ExecBatch>> MakeDelayedGen(
    Iterator<std::optional<ExecBatch>> src, std::string label, double delay_sec,
    bool noisy) {
  struct DelayedIoGenState {
    DelayedIoGenState(Iterator<std::optional<ExecBatch>> batch_it, double delay_sec,
                      std::string label, bool noisy)
        : batch_it(std::move(batch_it)),
          delay_sec(delay_sec),
          label(std::move(label)),
          noisy(noisy) {}
    std::optional<ExecBatch> Next() {
      Result<std::optional<ExecBatch>> opt_batch_res = batch_it.Next();
      if (!opt_batch_res.ok()) {
        return std::nullopt;
      }
      std::optional<ExecBatch> opt_batch = opt_batch_res.ValueOrDie();
      if (!opt_batch) {
        return std::nullopt;
      }
      if (noisy) {
        std::cout << label + ": asking for batch(" + std::to_string(index) + ")\n";
      }
      SleepFor(delay_sec);
      ++index;
      return *opt_batch;
    }

    Iterator<std::optional<ExecBatch>> batch_it;
    double delay_sec;
    std::string label;
    bool noisy;
    std::size_t index = 0;
  };
  auto state = std::make_shared<DelayedIoGenState>(std::move(src), delay_sec,
                                                   std::move(label), noisy);
  return [state]() {
    return DeferNotOk(::arrow::io::default_io_context().executor()->Submit(
        [state]() { return state->Next(); }));
  };
}

AsyncGenerator<std::optional<ExecBatch>> MakeDelayedGen(
    AsyncGenerator<std::optional<ExecBatch>> src, std::string label, double delay_sec,
    bool noisy) {
  return MakeDelayedGen(MakeGeneratorIterator(src), label, delay_sec, noisy);
}

AsyncGenerator<std::optional<ExecBatch>> MakeDelayedGen(BatchesWithSchema src,
                                                        std::string label,
                                                        double delay_sec, bool noisy) {
  std::vector<std::optional<ExecBatch>> opt_batches = ::arrow::internal::MapVector(
      [](ExecBatch batch) { return std::make_optional(std::move(batch)); }, src.batches);
  return MakeDelayedGen(MakeVectorIterator(opt_batches), label, delay_sec, noisy);
}

}  // namespace compute
}  // namespace arrow
