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
#include <mutex>
#include <random>

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
double generate_uniform(random::pcg64_fast* rng) {
  // This equation is copied from numpy. It calculates `rng() / 2^64` and
  // the return value is strictly less than 1.
  static_assert(random::pcg64_fast::min() == 0ULL, "");
  static_assert(random::pcg64_fast::max() == ~0ULL, "");
  return ((*rng)() >> 11) * (1.0 / 9007199254740992.0);
}

using RandomState = OptionsWrapper<RandomOptions>;

random::pcg64_fast MakeSeedGenerator() {
  arrow_vendored::pcg_extras::seed_seq_from<std::random_device> seed_source;
  random::pcg64_fast seed_gen(seed_source);
  return seed_gen;
}

Status ExecRandom(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  static random::pcg64_fast seed_gen = MakeSeedGenerator();
  static std::mutex seed_gen_mutex;

  random::pcg64_fast gen;
  const RandomOptions& options = RandomState::Get(ctx);
  if (options.length < 0) {
    return Status::Invalid("Negative number of elements");
  }

  auto out_data = ArrayData::Make(float64(), options.length, 0);
  out_data->buffers.resize(2, nullptr);

  ARROW_ASSIGN_OR_RAISE(out_data->buffers[1],
                        ctx->Allocate(options.length * sizeof(double)));
  double* out_buffer = out_data->template GetMutableValues<double>(1);

  if (options.initializer == RandomOptions::Seed) {
    gen.seed(options.seed);
  } else {
    std::lock_guard<std::mutex> seed_gen_lock(seed_gen_mutex);
    gen.seed(seed_gen());
  }
  for (int64_t i = 0; i < options.length; ++i) {
    out_buffer[i] = generate_uniform(&gen);
  }
  *out = std::move(out_data);
  return Status::OK();
}

const FunctionDoc random_doc{
    "Generate numbers in the range [0, 1)",
    ("Generated values are uniformly-distributed, double-precision in range [0, 1).\n"
     "Length of generated data, algorithm and seed can be changed via RandomOptions."),
    {},
    "RandomOptions"};

}  // namespace

void RegisterScalarRandom(FunctionRegistry* registry) {
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
