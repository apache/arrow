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

#include "arrow/builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/registry.h"
#include "arrow/util/pcg_random.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

// Generates a random floating point number in range [0, 1).
double generate_uniform(random::pcg64_fast& rng) {
  // This equation is copied from numpy. It calculates `rng() / 2^64` and
  // the return value is strictly less than 1.
  return (rng() >> 11) * (1.0 / 9007199254740992.0);
}

using RandomState = OptionsWrapper<RandomOptions>;

Status ExecRandom(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  random::pcg64_fast gen;
  const RandomOptions& options = RandomState::Get(ctx);
  DoubleBuilder builder(ctx->memory_pool());
  if (options.length < 0) {
    return Status::Invalid("Negative number of elements");
  }
  RETURN_NOT_OK(builder.Reserve(options.length));
  if (options.initializer == RandomOptions::Seed) {
    gen.seed(options.seed);
  } else {
    std::random_device rd;
    gen.seed(rd());
  }
  for (int i = 0; i < options.length; ++i) {
    builder.UnsafeAppend(generate_uniform(gen));
  }
  std::shared_ptr<Array> double_array;
  RETURN_NOT_OK(builder.Finish(&double_array));
  *out = *double_array->data();
  return Status::OK();
}

const FunctionDoc random_doc{
    "Generates a number in range [0, 1)",
    ("Generated values are uniformly-distributed, double-precision in range [0, 1).\n"
     "Length of generated data, algorithm and seed can be changed via RandomOptions."),
    {},
    "RandomOptions"};

}  // namespace

void RegisterRandom(FunctionRegistry* registry) {
  static auto random_options = RandomOptions::Defaults();

  auto random_func = std::make_shared<ScalarFunction>("random", Arity::Nullary(),
                                                      &random_doc, &random_options);
  ScalarKernel kernel{
      {}, ValueDescr(float64(), ValueDescr::Shape::ARRAY), ExecRandom, RandomState::Init};
  kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(random_func->AddKernel(kernel));
  DCHECK_OK(registry->AddFunction(std::move(random_func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
