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
#include <random>

#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/registry.h"
#include "arrow/util/pcg_random.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

// Generates a random floating point number in range [0, 1).
double generate_uniform(random::pcg64& rng) {
  return (rng() >> 11) * (1.0 / 9007199254740992.0);
}

Status ExecRandom(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  static thread_local std::random_device rd;
  static thread_local random::pcg64 gen(rd());
  double value = generate_uniform(gen);
  BoxScalar<DoubleType>::Box(value, out->scalar().get());
  return Status::OK();
}

const FunctionDoc random_doc{
    "Generates a number between 0 and 1",
    "Generates an uniformly-distributed double-precision number between 0 and 1.",
    {}};

}  // namespace

void RegisterRandom(FunctionRegistry* registry) {
  auto random_func =
      std::make_shared<ScalarFunction>("random", Arity::Nullary(), &random_doc);
  DCHECK_OK(random_func->AddKernel({}, float64(), ExecRandom));
  DCHECK_OK(registry->AddFunction(std::move(random_func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
