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

namespace arrow {

using internal::ChunkLocation;
using internal::ChunkResolver;

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

  auto chunk0 = ArrayFromJSON(int8(), "[0, 1, 2]");
  auto chunk1 = ArrayFromJSON(int16(), "[3, 4, 5]");

  ASSERT_OK_AND_ASSIGN(result, ChunkedArray::Make({chunk0, chunk0}));
  ASSERT_OK_AND_ASSIGN(auto result2, ChunkedArray::Make({chunk0, chunk0}, int8()));
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

TEST_F(TestChunkedArray, EqualsSameAddressWithNaNs) {
  auto chunk_with_nan1 = ArrayFromJSON(float64(), "[0, 1, 2, NaN]");
  auto chunk_without_nan1 = ArrayFromJSON(float64(), "[3, 4, 5]");
  ArrayVector chunks1 = {chunk_with_nan1, chunk_without_nan1};
  ASSERT_OK_AND_ASSIGN(auto chunked_array_with_nan1, ChunkedArray::Make(chunks1));
  ASSERT_FALSE(chunked_array_with_nan1->Equals(chunked_array_with_nan1));

  auto chunk_without_nan2 = ArrayFromJSON(float64(), "[6, 7, 8, 9]");
  ArrayVector chunks2 = {chunk_without_nan1, chunk_without_nan2};
  ASSERT_OK_AND_ASSIGN(auto chunked_array_without_nan1, ChunkedArray::Make(chunks2));
  ASSERT_TRUE(chunked_array_without_nan1->Equals(chunked_array_without_nan1));

  auto int32_array = ArrayFromJSON(int32(), "[0, 1, 2]");
  auto float64_array_with_nan = ArrayFromJSON(float64(), "[0, 1, NaN]");
  ArrayVector arrays1 = {int32_array, float64_array_with_nan};
  std::vector<std::string> fieldnames = {"Int32Type", "Float64Type"};
  ASSERT_OK_AND_ASSIGN(auto struct_with_nan, StructArray::Make(arrays1, fieldnames));
  ArrayVector chunks3 = {struct_with_nan};
  ASSERT_OK_AND_ASSIGN(auto chunked_array_with_nan2, ChunkedArray::Make(chunks3));
  ASSERT_FALSE(chunked_array_with_nan2->Equals(chunked_array_with_nan2));

  auto float64_array_without_nan = ArrayFromJSON(float64(), "[0, 1, 2]");
  ArrayVector arrays2 = {int32_array, float64_array_without_nan};
  ASSERT_OK_AND_ASSIGN(auto struct_without_nan, StructArray::Make(arrays2, fieldnames));
  ArrayVector chunks4 = {struct_without_nan};
  ASSERT_OK_AND_ASSIGN(auto chunked_array_without_nan2, ChunkedArray::Make(chunks4));
  ASSERT_TRUE(chunked_array_without_nan2->Equals(chunked_array_without_nan2));
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

template <typename T>
class TestChunkResolverMany : public ::testing::Test {
 public:
  using IndexType = T;

  Result<std::vector<ChunkLocation>> ResolveMany(
      const ChunkResolver& resolver, const std::vector<IndexType>& logical_index_vec) {
    const size_t n = logical_index_vec.size();
    std::vector<IndexType> chunk_index_vec;
    chunk_index_vec.resize(n);
    std::vector<IndexType> index_in_chunk_vec;
    index_in_chunk_vec.resize(n);
    bool valid = resolver.ResolveMany<IndexType>(
        static_cast<int64_t>(n), logical_index_vec.data(), chunk_index_vec.data(), 0,
        index_in_chunk_vec.data());
    if (ARROW_PREDICT_FALSE(!valid)) {
      return Status::Invalid("index type doesn't fit possible chunk indexes");
    }
    std::vector<ChunkLocation> locations;
    locations.reserve(n);
    for (size_t i = 0; i < n; i++) {
      auto chunk_index = static_cast<int64_t>(chunk_index_vec[i]);
      auto index_in_chunk = static_cast<int64_t>(index_in_chunk_vec[i]);
      locations.emplace_back(chunk_index, index_in_chunk);
    }
    return locations;
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
};

TYPED_TEST_SUITE(TestChunkResolverMany, IndexTypes);

TYPED_TEST(TestChunkResolverMany, Basics) { this->TestBasics(); }
TYPED_TEST(TestChunkResolverMany, OutOfBounds) { this->TestOutOfBounds(); }
TYPED_TEST(TestChunkResolverMany, Overflow) { this->TestOverflow(); }

}  // namespace arrow
