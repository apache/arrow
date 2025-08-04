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

#include "arrow/chunked_array.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/chunk_resolver.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/endian.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/pcg_random.h"

namespace arrow {

class TestChunkedArray : public ::testing::Test {
 protected:
  virtual void Construct() {
    one_ = std::make_shared<ChunkedArray>(arrays_one_);
    if (!arrays_another_.empty()) {
      another_ = std::make_shared<ChunkedArray>(arrays_another_);
    }
  }

  ArrayVector arrays_one_;
  ArrayVector arrays_another_;

  std::shared_ptr<ChunkedArray> one_;
  std::shared_ptr<ChunkedArray> another_;
};

TEST_F(TestChunkedArray, Make) {
  ASSERT_RAISES(Invalid, ChunkedArray::Make({}));

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> result,
                       ChunkedArray::Make({}, int64()));
  AssertTypeEqual(*int64(), *result->type());
  ASSERT_EQ(result->num_chunks(), 0);
  // Empty chunked arrays are treated as CPU-allocated.
  ASSERT_TRUE(result->is_cpu());

  auto chunk0 = ArrayFromJSON(int8(), "[0, 1, 2]");
  auto chunk1 = ArrayFromJSON(int16(), "[3, 4, 5]");

  ASSERT_OK_AND_ASSIGN(result, ChunkedArray::Make({chunk0, chunk0}));
  ASSERT_OK_AND_ASSIGN(auto result2, ChunkedArray::Make({chunk0, chunk0}, int8()));
  // All chunks are CPU-accessible.
  ASSERT_TRUE(result->is_cpu());
  ASSERT_TRUE(result2->is_cpu());
  AssertChunkedEqual(*result, *result2);

  ASSERT_RAISES(TypeError, ChunkedArray::Make({chunk0, chunk1}));
  ASSERT_RAISES(TypeError, ChunkedArray::Make({chunk0}, int16()));
}

TEST_F(TestChunkedArray, MakeEmpty) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> empty,
                       ChunkedArray::MakeEmpty(int64()));
  AssertTypeEqual(*int64(), *empty->type());
  ASSERT_OK(empty->ValidateFull());
  ASSERT_EQ(empty->length(), 0);
}

TEST_F(TestChunkedArray, BasicEquals) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data, &array);
  arrays_one_.push_back(array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_TRUE(one_->Equals(one_));
  ASSERT_FALSE(one_->Equals(nullptr));
  ASSERT_TRUE(one_->Equals(another_));
  ASSERT_TRUE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingTypes) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data32(100, 1);
  std::vector<int64_t> data64(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data32, &array);
  arrays_one_.push_back(array);
  ArrayFromVector<Int64Type, int64_t>(null_bitmap, data64, &array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_FALSE(one_->Equals(another_));
  ASSERT_FALSE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingLengths) {
  std::vector<bool> null_bitmap100(100, true);
  std::vector<bool> null_bitmap101(101, true);
  std::vector<int32_t> data100(100, 1);
  std::vector<int32_t> data101(101, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap100, data100, &array);
  arrays_one_.push_back(array);
  ArrayFromVector<Int32Type, int32_t>(null_bitmap101, data101, &array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_FALSE(one_->Equals(another_));
  ASSERT_FALSE(one_->Equals(*another_.get()));

  std::vector<bool> null_bitmap1(1, true);
  std::vector<int32_t> data1(1, 1);
  ArrayFromVector<Int32Type, int32_t>(null_bitmap1, data1, &array);
  arrays_one_.push_back(array);

  Construct();
  ASSERT_TRUE(one_->Equals(another_));
  ASSERT_TRUE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingMetadata) {
  auto left_ty = list(field("item", int32()));

  auto metadata = key_value_metadata({"foo"}, {"bar"});
  auto right_ty = list(field("item", int32(), true, metadata));

  std::vector<std::shared_ptr<Array>> left_chunks = {ArrayFromJSON(left_ty, "[[]]")};
  std::vector<std::shared_ptr<Array>> right_chunks = {ArrayFromJSON(right_ty, "[[]]")};

  ChunkedArray left(left_chunks);
  ChunkedArray right(right_chunks);
  ASSERT_TRUE(left.Equals(right));
}

class TestChunkedArrayEqualsSameAddress : public TestChunkedArray {};

TEST_F(TestChunkedArrayEqualsSameAddress, NonFloatType) {
  auto int32_array = ArrayFromJSON(int32(), "[0, 1, 2]");
  ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make({int32_array}));
  ASSERT_TRUE(chunked_array->Equals(chunked_array));
}

TEST_F(TestChunkedArrayEqualsSameAddress, NestedTypeWithoutFloat) {
  auto int32_array = ArrayFromJSON(int32(), "[0, 1]");
  ASSERT_OK_AND_ASSIGN(auto struct_array,
                       StructArray::Make({int32_array}, {"Int32Type"}));
  ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make({struct_array}));

  ASSERT_TRUE(chunked_array->Equals(chunked_array));
}

TEST_F(TestChunkedArrayEqualsSameAddress, FloatType) {
  auto float64_array = ArrayFromJSON(float64(), "[0.0, 1.0, 2.0, NaN]");
  ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make({float64_array}));

  ASSERT_FALSE(chunked_array->Equals(chunked_array));

  // Assert when EqualOptions::nans_equal_ is set
  ASSERT_TRUE(
      chunked_array->Equals(chunked_array, EqualOptions::Defaults().nans_equal(true)));
}

TEST_F(TestChunkedArrayEqualsSameAddress, NestedTypeWithFloat) {
  auto float64_array = ArrayFromJSON(float64(), "[0.0, 1.0, NaN]");
  ASSERT_OK_AND_ASSIGN(auto struct_array,
                       StructArray::Make({float64_array}, {"Float64Type"}));
  ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make({struct_array}));

  ASSERT_FALSE(chunked_array->Equals(chunked_array));

  // Assert when EqualOptions::nans_equal_ is set
  ASSERT_TRUE(
      chunked_array->Equals(chunked_array, EqualOptions::Defaults().nans_equal(true)));
}

TEST_F(TestChunkedArray, ApproxEquals) {
  auto chunk_1 = ArrayFromJSON(float64(), R"([0.0, 0.1, 0.5])");
  auto chunk_2 = ArrayFromJSON(float64(), R"([0.0, 0.1, 0.5001])");
  ASSERT_OK_AND_ASSIGN(auto chunked_array_1, ChunkedArray::Make({chunk_1}));
  ASSERT_OK_AND_ASSIGN(auto chunked_array_2, ChunkedArray::Make({chunk_2}));
  auto options = EqualOptions::Defaults().atol(1e-3);

  ASSERT_FALSE(chunked_array_1->Equals(chunked_array_2));
  ASSERT_TRUE(chunked_array_1->Equals(chunked_array_2, options.use_atol(true)));
  ASSERT_TRUE(chunked_array_1->ApproxEquals(*chunked_array_2, options));
}

TEST_F(TestChunkedArray, SliceEquals) {
  random::RandomArrayGenerator gen(42);

  arrays_one_.push_back(gen.Int32(100, -12345, 12345));
  arrays_one_.push_back(gen.Int32(50, -12345, 12345));
  arrays_one_.push_back(gen.Int32(50, -12345, 12345));
  Construct();

  std::shared_ptr<ChunkedArray> slice = one_->Slice(125, 50);
  ASSERT_EQ(slice->length(), 50);
  AssertChunkedEqual(*one_->Slice(125, 50), *slice);

  std::shared_ptr<ChunkedArray> slice2 = one_->Slice(75)->Slice(25)->Slice(25, 50);
  ASSERT_EQ(slice2->length(), 50);
  AssertChunkedEqual(*slice, *slice2);

  // Making empty slices of a ChunkedArray
  std::shared_ptr<ChunkedArray> slice3 = one_->Slice(one_->length(), 99);
  ASSERT_EQ(slice3->length(), 0);
  ASSERT_EQ(slice3->num_chunks(), 1);
  ASSERT_TRUE(slice3->type()->Equals(one_->type()));

  std::shared_ptr<ChunkedArray> slice4 = one_->Slice(10, 0);
  ASSERT_EQ(slice4->length(), 0);
  ASSERT_EQ(slice4->num_chunks(), 1);
  ASSERT_TRUE(slice4->type()->Equals(one_->type()));

  // Slicing an empty ChunkedArray
  std::shared_ptr<ChunkedArray> slice5 = slice4->Slice(0, 10);
  ASSERT_EQ(slice5->length(), 0);
  ASSERT_EQ(slice5->num_chunks(), 1);
  ASSERT_TRUE(slice5->type()->Equals(one_->type()));
}

TEST_F(TestChunkedArray, ZeroChunksIssues) {
  ArrayVector empty = {};
  auto no_chunks = std::make_shared<ChunkedArray>(empty, int8());

  // ARROW-8911, assert that slicing is a no-op when there are zero-chunks
  auto sliced = no_chunks->Slice(0, 0);
  auto sliced2 = no_chunks->Slice(0, 5);
  AssertChunkedEqual(*no_chunks, *sliced);
  AssertChunkedEqual(*no_chunks, *sliced2);
}

TEST_F(TestChunkedArray, Validate) {
  // Valid if empty
  ArrayVector empty = {};
  auto no_chunks = std::make_shared<ChunkedArray>(empty, utf8());
  ASSERT_OK(no_chunks->ValidateFull());

  random::RandomArrayGenerator gen(0);

  // Valid if non-empty and omitted type
  ArrayVector arrays = {gen.Int64(50, 0, 100, 0.1), gen.Int64(50, 0, 100, 0.1)};
  auto chunks_with_no_type = std::make_shared<ChunkedArray>(arrays, nullptr);
  ASSERT_OK(chunks_with_no_type->ValidateFull());

  arrays_one_.push_back(gen.Int32(50, 0, 100, 0.1));
  Construct();
  ASSERT_OK(one_->ValidateFull());

  arrays_one_.push_back(gen.Int32(50, 0, 100, 0.1));
  Construct();
  ASSERT_OK(one_->ValidateFull());

  // Invalid if different chunk types
  arrays_one_.push_back(gen.String(50, 0, 10, 0.1));
  Construct();
  ASSERT_RAISES(Invalid, one_->ValidateFull());
}

TEST_F(TestChunkedArray, PrintDiff) {
  random::RandomArrayGenerator gen(0);
  arrays_one_.push_back(gen.Int32(50, 0, 100, 0.1));
  Construct();

  auto other = one_->Slice(25);
  ASSERT_OK_AND_ASSIGN(auto diff, PrintArrayDiff(*one_, *other));
  ASSERT_EQ(*diff, "Expected length 50 but was actually 25");

  ASSERT_OK_AND_ASSIGN(diff, PrintArrayDiff(*other, *one_));
  ASSERT_EQ(*diff, "Expected length 25 but was actually 50");
}

TEST_F(TestChunkedArray, View) {
  auto in_ty = int32();
  auto out_ty = fixed_size_binary(4);
#if ARROW_LITTLE_ENDIAN
  auto arr = ArrayFromJSON(in_ty, "[2020568934, 2054316386, null]");
  auto arr2 = ArrayFromJSON(in_ty, "[2020568934, 2054316386]");
#else
  auto arr = ArrayFromJSON(in_ty, "[1718579064, 1650553466, null]");
  auto arr2 = ArrayFromJSON(in_ty, "[1718579064, 1650553466]");
#endif
  auto ex = ArrayFromJSON(out_ty, R"(["foox", "barz", null])");
  auto ex2 = ArrayFromJSON(out_ty, R"(["foox", "barz"])");

  ArrayVector chunks = {arr, arr2};
  ArrayVector ex_chunks = {ex, ex2};
  auto carr = std::make_shared<ChunkedArray>(chunks);
  auto expected = std::make_shared<ChunkedArray>(ex_chunks);

  ASSERT_OK_AND_ASSIGN(auto result, carr->View(out_ty));
  AssertChunkedEqual(*expected, *result);

  // Zero length
  ArrayVector empty = {};
  carr = std::make_shared<ChunkedArray>(empty, in_ty);
  expected = std::make_shared<ChunkedArray>(empty, out_ty);
  ASSERT_OK_AND_ASSIGN(result, carr->View(out_ty));
  AssertChunkedEqual(*expected, *result);
}

TEST_F(TestChunkedArray, GetScalar) {
  auto ty = int32();
  ArrayVector chunks{ArrayFromJSON(ty, "[6, 7, null]"), ArrayFromJSON(ty, "[]"),
                     ArrayFromJSON(ty, "[null]"), ArrayFromJSON(ty, "[3, 4, 5]")};
  ChunkedArray carr(chunks);

  auto check_scalar = [](const ChunkedArray& array, int64_t index,
                         const Scalar& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, array.GetScalar(index));
    AssertScalarsEqual(expected, *actual, /*verbose=*/true);
  };

  check_scalar(carr, 0, **MakeScalar(ty, 6));
  check_scalar(carr, 2, *MakeNullScalar(ty));
  check_scalar(carr, 3, *MakeNullScalar(ty));
  check_scalar(carr, 4, **MakeScalar(ty, 3));
  check_scalar(carr, 6, **MakeScalar(ty, 5));

  ASSERT_RAISES(IndexError, carr.GetScalar(7));
}

// ChunkResolver tests

using IndexTypes = ::testing::Types<uint8_t, uint16_t, uint32_t, uint64_t, int8_t,
                                    int16_t, int32_t, int64_t>;

TEST(TestChunkResolver, Resolve) {
  ChunkResolver empty(std::vector<int64_t>({0}));  // []
  // ChunkLocation::index_in_chunk is undefined when chunk_index==chunks.size(),
  // so only chunk_index is compared in these cases.
  ASSERT_EQ(empty.Resolve(0).chunk_index, 0);
  ASSERT_EQ(empty.Resolve(0).chunk_index, 0);

  ChunkResolver one(std::vector<int64_t>({0, 1}));  // [[0]]
  ASSERT_EQ(one.Resolve(1).chunk_index, 1);
  ASSERT_EQ(one.Resolve(0), (ChunkLocation(0, 0)));
  ASSERT_EQ(one.Resolve(1).chunk_index, 1);

  ChunkResolver one_and_empty(std::vector<int64_t>({0, 1, 1, 1}));  // [[0], [], []]
  ASSERT_EQ(one_and_empty.Resolve(3).chunk_index, 3);
  ASSERT_EQ(one_and_empty.Resolve(2).chunk_index, 3);
  ASSERT_EQ(one_and_empty.Resolve(1).chunk_index, 3);
  ASSERT_EQ(one_and_empty.Resolve(0), (ChunkLocation(0, 0)));
  ASSERT_EQ(one_and_empty.Resolve(1).chunk_index, 3);
  ASSERT_EQ(one_and_empty.Resolve(2).chunk_index, 3);
  ASSERT_EQ(one_and_empty.Resolve(3).chunk_index, 3);

  ChunkResolver one_one_one(std::vector<int64_t>({0, 1, 2, 3}));  // [[0], [1], [2]]
  ASSERT_EQ(one_one_one.Resolve(3).chunk_index, 3);
  ASSERT_EQ(one_one_one.Resolve(2), (ChunkLocation(2, 0)));
  ASSERT_EQ(one_one_one.Resolve(1), (ChunkLocation(1, 0)));
  ASSERT_EQ(one_one_one.Resolve(0), (ChunkLocation(0, 0)));
  ASSERT_EQ(one_one_one.Resolve(1), (ChunkLocation(1, 0)));
  ASSERT_EQ(one_one_one.Resolve(2), (ChunkLocation(2, 0)));
  ASSERT_EQ(one_one_one.Resolve(3).chunk_index, 3);

  ChunkResolver resolver(std::vector<int64_t>({0, 2, 3, 10}));  // [[0, 1], [2], [3..9]]
  ASSERT_EQ(resolver.Resolve(10).chunk_index, 3);
  ASSERT_EQ(resolver.Resolve(9), (ChunkLocation(2, 6)));
  ASSERT_EQ(resolver.Resolve(8), (ChunkLocation(2, 5)));
  ASSERT_EQ(resolver.Resolve(4), (ChunkLocation(2, 1)));
  ASSERT_EQ(resolver.Resolve(3), (ChunkLocation(2, 0)));
  ASSERT_EQ(resolver.Resolve(2), (ChunkLocation(1, 0)));
  ASSERT_EQ(resolver.Resolve(1), (ChunkLocation(0, 1)));
  ASSERT_EQ(resolver.Resolve(0), (ChunkLocation(0, 0)));
  ASSERT_EQ(resolver.Resolve(1), (ChunkLocation(0, 1)));
  ASSERT_EQ(resolver.Resolve(2), (ChunkLocation(1, 0)));
  ASSERT_EQ(resolver.Resolve(3), (ChunkLocation(2, 0)));
  ASSERT_EQ(resolver.Resolve(4), (ChunkLocation(2, 1)));
  ASSERT_EQ(resolver.Resolve(8), (ChunkLocation(2, 5)));
  ASSERT_EQ(resolver.Resolve(9), (ChunkLocation(2, 6)));
  ASSERT_EQ(resolver.Resolve(10).chunk_index, 3);
}

template <class RNG>
std::vector<int64_t> GenChunkedArrayOffsets(RNG& rng, int32_t num_chunks,
                                            int64_t chunked_array_len) {
  std::uniform_int_distribution<int64_t> offset_gen(1, chunked_array_len - 1);
  std::vector<int64_t> offsets;
  offsets.reserve(num_chunks + 1);
  offsets.push_back(0);
  while (offsets.size() < static_cast<size_t>(num_chunks)) {
    offsets.push_back(offset_gen(rng));
  }
  offsets.push_back(chunked_array_len);
  std::sort(offsets.begin() + 1, offsets.end());
  return offsets;
}

template <typename T>
class TestChunkResolverMany : public ::testing::Test {
 public:
  using IndexType = T;
  static constexpr int32_t kMaxInt32 = std::numeric_limits<int32_t>::max();
  static constexpr uint64_t kMaxValidIndex = std::numeric_limits<IndexType>::max();

  Result<std::vector<ChunkLocation>> ResolveMany(
      const ChunkResolver& resolver, const std::vector<IndexType>& logical_index_vec) {
    const size_t n = logical_index_vec.size();
    std::vector<TypedChunkLocation<IndexType>> chunk_location_vec;
    chunk_location_vec.resize(n);
    bool valid = resolver.ResolveMany<IndexType>(
        static_cast<int64_t>(n), logical_index_vec.data(), chunk_location_vec.data(), 0);
    if (ARROW_PREDICT_FALSE(!valid)) {
      return Status::Invalid("index type doesn't fit possible chunk indexes");
    }
    if constexpr (std::is_same<decltype(ChunkLocation::chunk_index), IndexType>::value) {
      return chunk_location_vec;
    } else {
      std::vector<ChunkLocation> locations;
      locations.reserve(n);
      for (size_t i = 0; i < n; i++) {
        auto loc = chunk_location_vec[i];
        auto chunk_index = static_cast<int64_t>(loc.chunk_index);
        auto index_in_chunk = static_cast<int64_t>(loc.index_in_chunk);
        locations.emplace_back(chunk_index, index_in_chunk);
      }
      return locations;
    }
  }

  void CheckResolveMany(const ChunkResolver& resolver,
                        const std::vector<IndexType>& logical_index_vec) {
    ASSERT_OK_AND_ASSIGN(auto locations, ResolveMany(resolver, logical_index_vec));
    EXPECT_EQ(logical_index_vec.size(), locations.size());
    for (size_t i = 0; i < logical_index_vec.size(); i++) {
      IndexType logical_index = logical_index_vec[i];
      const auto expected = resolver.Resolve(logical_index);
      ASSERT_LE(expected.chunk_index, resolver.num_chunks());
      if (expected.chunk_index == resolver.num_chunks()) {
        // index_in_chunk is undefined in this case
        ASSERT_EQ(locations[i].chunk_index, expected.chunk_index);
      } else {
        ASSERT_EQ(locations[i], expected);
      }
    }
  }

  void TestBasics() {
    std::vector<IndexType> logical_index_vec;

    ChunkResolver empty(std::vector<int64_t>({0}));  // []
    logical_index_vec = {0, 0};
    CheckResolveMany(empty, logical_index_vec);

    ChunkResolver one(std::vector<int64_t>({0, 1}));  // [[0]]
    logical_index_vec = {1, 0, 1};
    CheckResolveMany(one, logical_index_vec);

    ChunkResolver one_and_empty(std::vector<int64_t>({0, 1, 1, 1}));  // [[0], [], []]
    logical_index_vec = {3, 2, 1, 0, 1, 2, 3};
    CheckResolveMany(one_and_empty, logical_index_vec);

    ChunkResolver one_one_one(std::vector<int64_t>({0, 1, 2, 3}));  // [[0], [1], [2]]
    logical_index_vec = {3, 2, 1, 0, 1, 2, 3};
    CheckResolveMany(one_one_one, logical_index_vec);

    ChunkResolver resolver(std::vector<int64_t>({0, 2, 3, 10}));  // [[0, 1], [2], [3..9]]
    logical_index_vec = {10, 9, 8, 4, 3, 2, 1, 0, 1, 2, 3, 4, 8, 9, 10};
    CheckResolveMany(resolver, logical_index_vec);
  }

  void TestOutOfBounds() {
    ChunkResolver resolver(std::vector<int64_t>({0, 2, 3, 10}));  // [[0, 1], [2], [3..9]]

    std::vector<IndexType> logical_index_vec = {10, 11, 12, 13, 14, 13, 11, 10};
    ASSERT_OK_AND_ASSIGN(auto locations, ResolveMany(resolver, logical_index_vec));
    EXPECT_EQ(logical_index_vec.size(), locations.size());
    for (size_t i = 0; i < logical_index_vec.size(); i++) {
      ASSERT_EQ(locations[i].chunk_index, resolver.num_chunks());
    }

    if constexpr (std::is_signed_v<IndexType>) {
      std::vector<IndexType> logical_index_vec = {-1, -2, -3, -4, INT8_MIN};

      ChunkResolver resolver(std::vector<int64_t>({0, 2, 128}));  // [[0, 1], [2..127]]
      ASSERT_OK_AND_ASSIGN(auto locations, ResolveMany(resolver, logical_index_vec));
      EXPECT_EQ(logical_index_vec.size(), locations.size());
      for (size_t i = 0; i < logical_index_vec.size(); i++) {
        // All the negative indices are greater than resolver.logical_array_length()-1
        // when cast to uint8_t.
        ASSERT_EQ(locations[i].chunk_index, resolver.num_chunks());
      }

      if constexpr (sizeof(IndexType) == 1) {
        ChunkResolver resolver(std::vector<int64_t>(
            {0, 2, 128, 129, 256}));  // [[0, 1], [2..127], [128], [129, 255]]
        ASSERT_OK_AND_ASSIGN(auto locations, ResolveMany(resolver, logical_index_vec));
        EXPECT_EQ(logical_index_vec.size(), locations.size());
        for (size_t i = 0; i < logical_index_vec.size(); i++) {
          if constexpr (sizeof(IndexType) == 1) {
            // All the negative 8-bit indices are SMALLER than
            // resolver.logical_array_length()=256 when cast to 8-bit unsigned integers.
            // So the resolved locations might look valid, but they should not be trusted.
            ASSERT_LT(locations[i].chunk_index, resolver.num_chunks());
          } else {
            // All the negative indices are greater than resolver.logical_array_length()
            // when cast to 16/32/64-bit unsigned integers.
            ASSERT_EQ(locations[i].chunk_index, resolver.num_chunks());
          }
        }
      }
    }
  }

  void TestOverflow() {
    const int64_t kMaxIndex = std::is_signed_v<IndexType> ? 127 : 255;
    std::vector<IndexType> logical_index_vec = {0, 1, 2,
                                                static_cast<IndexType>(kMaxIndex)};

    // Overflows are rare because to make them possible, we need more chunks
    // than logical elements in the ChunkedArray. That requires at least one
    // empty chunk.
    std::vector<int64_t> offsets;
    for (int64_t i = 0; i <= kMaxIndex; i++) {
      offsets.push_back(i);
    }
    ChunkResolver resolver{offsets};
    ASSERT_OK(ResolveMany(resolver, logical_index_vec));

    offsets.push_back(kMaxIndex);  // adding an empty chunk
    ChunkResolver resolver_with_empty{offsets};
    if (sizeof(IndexType) == 1) {
      ASSERT_NOT_OK(ResolveMany(resolver_with_empty, logical_index_vec));
    } else {
      ASSERT_OK(ResolveMany(resolver_with_empty, logical_index_vec));
    }
  }

  void TestRandomInput(int32_t num_chunks, int64_t chunked_array_len) {
    random::pcg64 rng(42);

    // Generate random chunk offsets...
    auto offsets = GenChunkedArrayOffsets(rng, num_chunks, chunked_array_len);
    ASSERT_EQ(offsets.size(), static_cast<size_t>(num_chunks) + 1);
    // ...and ensure there is at least one empty chunk.
    std::uniform_int_distribution<int32_t> chunk_index_gen(
        1, static_cast<int32_t>(num_chunks - 1));
    auto chunk_index = chunk_index_gen(rng);
    offsets[chunk_index] = offsets[chunk_index - 1];

    // Generate random query array of logical indices...
    const auto num_logical_indices = 3 * static_cast<int64_t>(num_chunks) / 2;
    std::vector<IndexType> logical_index_vec;
    logical_index_vec.reserve(num_logical_indices);
    std::uniform_int_distribution<uint64_t> logical_index_gen(1, kMaxValidIndex);
    for (int64_t i = 0; i < num_logical_indices; i++) {
      const auto index = static_cast<IndexType>(logical_index_gen(rng));
      logical_index_vec.push_back(index);
    }
    // ...and sprinkle some extreme logical index values.
    std::uniform_int_distribution<int64_t> position_gen(0, logical_index_vec.size() - 1);
    for (int i = 0; i < 2; i++) {
      auto max_valid_index =
          std::min(kMaxValidIndex, static_cast<uint64_t>(chunked_array_len));
      // zero and last valid logical index
      logical_index_vec[position_gen(rng)] = 0;
      logical_index_vec[position_gen(rng)] = static_cast<IndexType>(max_valid_index - 1);
      // out of  bounds indices
      logical_index_vec[position_gen(rng)] = static_cast<IndexType>(max_valid_index);
      if (max_valid_index < kMaxValidIndex) {
        logical_index_vec[position_gen(rng)] =
            static_cast<IndexType>(max_valid_index + 1);
      }
    }

    ChunkResolver resolver(std::move(offsets));
    CheckResolveMany(resolver, logical_index_vec);
  }

  void TestRandomInput() {
    const int64_t num_chunks = static_cast<int64_t>(
        std::min(kMaxValidIndex - 1, static_cast<uint64_t>(1) << 16));
    const int64_t avg_chunk_length = 20;
    const int64_t chunked_array_len = num_chunks * 2 * avg_chunk_length;
    TestRandomInput(num_chunks, chunked_array_len);
  }
};

TYPED_TEST_SUITE(TestChunkResolverMany, IndexTypes);

TYPED_TEST(TestChunkResolverMany, Basics) { this->TestBasics(); }
TYPED_TEST(TestChunkResolverMany, OutOfBounds) { this->TestOutOfBounds(); }
TYPED_TEST(TestChunkResolverMany, Overflow) { this->TestOverflow(); }
TYPED_TEST(TestChunkResolverMany, RandomInput) { this->TestRandomInput(); }

}  // namespace arrow
