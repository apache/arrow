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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

namespace arrow {

class ConcatenateTest : public ::testing::Test {
 protected:
  ConcatenateTest()
      : rng_(seed_),
        sizes_({0, 1, 2, 4, 16, 31, 1234}),
        null_probabilities_({0.0, 0.1, 0.5, 0.9, 1.0}) {}

  template <typename OffsetType>
  std::vector<OffsetType> Offsets(int32_t length, int32_t slice_count) {
    std::vector<OffsetType> offsets(static_cast<std::size_t>(slice_count + 1));
    std::default_random_engine gen(seed_);
    std::uniform_int_distribution<OffsetType> dist(0, length);
    std::generate(offsets.begin(), offsets.end(), [&] { return dist(gen); });
    std::sort(offsets.begin(), offsets.end());
    return offsets;
  }

  ArrayVector Slices(const std::shared_ptr<Array>& array,
                     const std::vector<int32_t>& offsets) {
    ArrayVector slices(offsets.size() - 1);
    for (size_t i = 0; i != slices.size(); ++i) {
      slices[i] = array->Slice(offsets[i], offsets[i + 1] - offsets[i]);
    }
    return slices;
  }

  template <typename PrimitiveType>
  std::shared_ptr<Array> GeneratePrimitive(int64_t size, double null_probability) {
    if (std::is_same<PrimitiveType, BooleanType>::value) {
      return rng_.Boolean(size, 0.5, null_probability);
    }
    return rng_.Numeric<PrimitiveType, uint8_t>(size, 0, 127, null_probability);
  }

  void CheckTrailingBitsAreZeroed(const std::shared_ptr<Buffer>& bitmap, int64_t length) {
    if (auto preceding_bits = BitUtil::kPrecedingBitmask[length % 8]) {
      auto last_byte = bitmap->data()[length / 8];
      ASSERT_EQ(static_cast<uint8_t>(last_byte & preceding_bits), last_byte)
          << length << " " << int(preceding_bits);
    }
  }

  template <typename ArrayFactory>
  void Check(ArrayFactory&& factory) {
    for (auto size : this->sizes_) {
      auto offsets = this->Offsets<int32_t>(size, 3);
      for (auto null_probability : this->null_probabilities_) {
        std::shared_ptr<Array> array;
        factory(size, null_probability, &array);
        auto expected = array->Slice(offsets.front(), offsets.back() - offsets.front());
        auto slices = this->Slices(array, offsets);
        std::shared_ptr<Array> actual;
        ASSERT_OK(Concatenate(slices, default_memory_pool(), &actual));
        AssertArraysEqual(*expected, *actual);
        if (actual->data()->buffers[0]) {
          CheckTrailingBitsAreZeroed(actual->data()->buffers[0], actual->length());
        }
        if (actual->type_id() == Type::BOOL) {
          CheckTrailingBitsAreZeroed(actual->data()->buffers[1], actual->length());
        }
      }
    }
  }

  random::SeedType seed_ = 0xdeadbeef;
  random::RandomArrayGenerator rng_;
  std::vector<int32_t> sizes_;
  std::vector<double> null_probabilities_;
};

TEST(ConcatenateEmptyArraysTest, TestValueBuffersNullPtr) {
  ArrayVector inputs;

  std::shared_ptr<Array> binary_array;
  BinaryBuilder builder;
  ASSERT_OK(builder.Finish(&binary_array));
  inputs.push_back(std::move(binary_array));

  builder.Reset();
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Finish(&binary_array));
  inputs.push_back(std::move(binary_array));

  std::shared_ptr<Array> actual;
  ASSERT_OK(Concatenate(inputs, default_memory_pool(), &actual));
  AssertArraysEqual(*actual, *inputs[1]);
}

template <typename PrimitiveType>
class PrimitiveConcatenateTest : public ConcatenateTest {
 public:
};

using PrimitiveTypes =
    ::testing::Types<BooleanType, Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                     UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType>;
TYPED_TEST_CASE(PrimitiveConcatenateTest, PrimitiveTypes);

TYPED_TEST(PrimitiveConcatenateTest, Primitives) {
  this->Check([this](int64_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = this->template GeneratePrimitive<TypeParam>(size, null_probability);
  });
}

TEST_F(ConcatenateTest, StringType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = rng_.String(size, /*min_length =*/0, /*max_length =*/15, null_probability);
    ASSERT_OK(ValidateArray(**out));
  });
}

TEST_F(ConcatenateTest, LargeStringType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out =
        rng_.LargeString(size, /*min_length =*/0, /*max_length =*/15, null_probability);
    ASSERT_OK(ValidateArray(**out));
  });
}

TEST_F(ConcatenateTest, ListType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto values_size = size * 4;
    auto values = this->GeneratePrimitive<Int8Type>(values_size, null_probability);
    auto offsets_vector = this->Offsets<int32_t>(values_size, size);
    // ensure the first offset is 0, which is expected for ListType
    offsets_vector[0] = 0;
    std::shared_ptr<Array> offsets;
    ArrayFromVector<Int32Type>(offsets_vector, &offsets);
    ASSERT_OK(ListArray::FromArrays(*offsets, *values, default_memory_pool(), out));
  });
}

TEST_F(ConcatenateTest, StructType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto foo = this->GeneratePrimitive<Int8Type>(size, null_probability);
    auto bar = this->GeneratePrimitive<DoubleType>(size, null_probability);
    auto baz = this->GeneratePrimitive<BooleanType>(size, null_probability);
    *out = std::make_shared<StructArray>(
        struct_({field("foo", int8()), field("bar", float64()), field("baz", boolean())}),
        size, ArrayVector{foo, bar, baz});
  });
}

TEST_F(ConcatenateTest, DictionaryType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto indices = this->GeneratePrimitive<Int32Type>(size, null_probability);
    auto dict = this->GeneratePrimitive<DoubleType>(128, 0);
    auto type = dictionary(int32(), dict->type());
    *out = std::make_shared<DictionaryArray>(type, indices, dict);
  });
}

TEST_F(ConcatenateTest, DISABLED_UnionType) {
  // sparse mode
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto foo = this->GeneratePrimitive<Int8Type>(size, null_probability);
    auto bar = this->GeneratePrimitive<DoubleType>(size, null_probability);
    auto baz = this->GeneratePrimitive<BooleanType>(size, null_probability);
    auto type_ids = rng_.Numeric<Int8Type>(size, 0, 2, null_probability);
    ASSERT_OK(UnionArray::MakeSparse(*type_ids, {foo, bar, baz}, out));
  });
  // dense mode
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto foo = this->GeneratePrimitive<Int8Type>(size, null_probability);
    auto bar = this->GeneratePrimitive<DoubleType>(size, null_probability);
    auto baz = this->GeneratePrimitive<BooleanType>(size, null_probability);
    auto type_ids = rng_.Numeric<Int8Type>(size, 0, 2, null_probability);
    auto value_offsets = rng_.Numeric<Int32Type>(size, 0, size, 0);
    ASSERT_OK(UnionArray::MakeDense(*type_ids, *value_offsets, {foo, bar, baz}, out));
  });
}

TEST_F(ConcatenateTest, OffsetOverflow) {
  auto fake_long = ArrayFromJSON(utf8(), "[\"\"]");
  fake_long->data()->GetMutableValues<int32_t>(1)[1] =
      std::numeric_limits<int32_t>::max();
  std::shared_ptr<Array> concatenated;
  // XX since the data fake_long claims to own isn't there, this will segfault if
  // Concatenate doesn't detect overflow and raise an error.
  ASSERT_RAISES(
      Invalid, Concatenate({fake_long, fake_long}, default_memory_pool(), &concatenated));
}

}  // namespace arrow
