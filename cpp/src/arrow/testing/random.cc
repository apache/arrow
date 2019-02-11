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

#include "arrow/testing/random.h"

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"

namespace arrow {
namespace random {

template <typename ValueType, typename DistributionType>
struct GenerateOptions {
  GenerateOptions(SeedType seed, ValueType min, ValueType max, double probability)
      : min_(min), max_(max), seed_(seed), probability_(probability) {}

  void GenerateData(uint8_t* buffer, size_t n) {
    std::default_random_engine rng(seed_++);
    DistributionType dist(min_, max_);

    ValueType* data = reinterpret_cast<ValueType*>(buffer);

    // A static cast is required due to the int16 -> int8 handling.
    std::generate(data, data + n,
                  [&dist, &rng] { return static_cast<ValueType>(dist(rng)); });
  }

  void GenerateBitmap(uint8_t* buffer, size_t n, int64_t* null_count) {
    int64_t count = 0;
    std::default_random_engine rng(seed_++);
    std::bernoulli_distribution dist(1.0 - probability_);

    for (size_t i = 0; i < n; i++) {
      if (dist(rng)) {
        BitUtil::SetBit(buffer, i);
      } else {
        count++;
      }
    }

    if (null_count != nullptr) *null_count = count;
  }

  ValueType min_;
  ValueType max_;
  SeedType seed_;
  double probability_;
};

std::shared_ptr<Array> RandomArrayGenerator::Boolean(int64_t size, double probability,
                                                     double null_probability) {
  // The boolean generator does not care about the value distribution since it
  // only calls the GenerateBitmap method.
  using GenOpt = GenerateOptions<int, std::uniform_int_distribution<int>>;

  std::vector<std::shared_ptr<Buffer>> buffers{2};
  // Need 2 distinct generators such that probabilities are not shared.
  GenOpt value_gen(seed(), 0, 1, probability);
  GenOpt null_gen(seed(), 0, 1, null_probability);

  int64_t null_count = 0;
  ABORT_NOT_OK(AllocateEmptyBitmap(size, &buffers[0]));
  null_gen.GenerateBitmap(buffers[0]->mutable_data(), size, &null_count);

  ABORT_NOT_OK(AllocateEmptyBitmap(size, &buffers[1]));
  value_gen.GenerateBitmap(buffers[1]->mutable_data(), size, nullptr);

  auto array_data = ArrayData::Make(arrow::boolean(), size, buffers, null_count);
  return std::make_shared<BooleanArray>(array_data);
}

template <typename ArrowType, typename OptionType>
static std::shared_ptr<NumericArray<ArrowType>> GenerateNumericArray(int64_t size,
                                                                     OptionType options) {
  using CType = typename ArrowType::c_type;
  auto type = TypeTraits<ArrowType>::type_singleton();
  std::vector<std::shared_ptr<Buffer>> buffers{2};

  int64_t null_count = 0;
  ABORT_NOT_OK(AllocateEmptyBitmap(size, &buffers[0]));
  options.GenerateBitmap(buffers[0]->mutable_data(), size, &null_count);

  ABORT_NOT_OK(AllocateBuffer(sizeof(CType) * size, &buffers[1]))
  options.GenerateData(buffers[1]->mutable_data(), size);

  auto array_data = ArrayData::Make(type, size, buffers, null_count);
  return std::make_shared<NumericArray<ArrowType>>(array_data);
}

#define PRIMITIVE_RAND_IMPL(Name, CType, ArrowType, Distribution)                       \
  std::shared_ptr<Array> RandomArrayGenerator::Name(int64_t size, CType min, CType max, \
                                                    double probability) {               \
    using OptionType = GenerateOptions<CType, Distribution>;                            \
    OptionType options(seed(), min, max, probability);                                  \
    return GenerateNumericArray<ArrowType, OptionType>(size, options);                  \
  }

#define PRIMITIVE_RAND_INTEGER_IMPL(Name, CType, ArrowType) \
  PRIMITIVE_RAND_IMPL(Name, CType, ArrowType, std::uniform_int_distribution<CType>)

// Visual Studio does not implement uniform_int_distribution for char types.
PRIMITIVE_RAND_IMPL(UInt8, uint8_t, UInt8Type, std::uniform_int_distribution<uint16_t>)
PRIMITIVE_RAND_IMPL(Int8, int8_t, Int8Type, std::uniform_int_distribution<int16_t>)

PRIMITIVE_RAND_INTEGER_IMPL(UInt16, uint16_t, UInt16Type)
PRIMITIVE_RAND_INTEGER_IMPL(Int16, int16_t, Int16Type)
PRIMITIVE_RAND_INTEGER_IMPL(UInt32, uint32_t, UInt32Type)
PRIMITIVE_RAND_INTEGER_IMPL(Int32, int32_t, Int32Type)
PRIMITIVE_RAND_INTEGER_IMPL(UInt64, uint64_t, UInt64Type)
PRIMITIVE_RAND_INTEGER_IMPL(Int64, int64_t, Int64Type)

#define PRIMITIVE_RAND_FLOAT_IMPL(Name, CType, ArrowType) \
  PRIMITIVE_RAND_IMPL(Name, CType, ArrowType, std::uniform_real_distribution<CType>)

PRIMITIVE_RAND_FLOAT_IMPL(Float32, float, FloatType)
PRIMITIVE_RAND_FLOAT_IMPL(Float64, double, DoubleType)

#undef PRIMITIVE_RAND_INTEGER_IMPL
#undef PRIMITIVE_RAND_FLOAT_IMPL
#undef PRIMITIVE_RAND_IMPL

}  // namespace random
}  // namespace arrow
