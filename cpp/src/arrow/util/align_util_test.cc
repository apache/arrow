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
#include <cstdint>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/align_util.h"

namespace arrow {
namespace internal {

template <int64_t NBYTES>
void CheckBitmapWordAlign(const uint8_t* data, int64_t bit_offset, int64_t length,
                          BitmapWordAlignParams expected) {
  auto p = BitmapWordAlign<static_cast<uint64_t>(NBYTES)>(data, bit_offset, length);

  ASSERT_EQ(p.leading_bits, expected.leading_bits);
  ASSERT_EQ(p.trailing_bits, expected.trailing_bits);
  if (p.trailing_bits > 0) {
    // Only relevant if trailing_bits > 0
    ASSERT_EQ(p.trailing_bit_offset, expected.trailing_bit_offset);
  }
  ASSERT_EQ(p.aligned_bits, expected.aligned_bits);
  ASSERT_EQ(p.aligned_words, expected.aligned_words);
  if (p.aligned_bits > 0) {
    // Only relevant if aligned_bits > 0
    ASSERT_EQ(p.aligned_start, expected.aligned_start);
  }

  // Invariants
  ASSERT_LT(p.leading_bits, NBYTES * 8);
  ASSERT_LT(p.trailing_bits, NBYTES * 8);
  ASSERT_EQ(p.leading_bits + p.aligned_bits + p.trailing_bits, length);
  ASSERT_EQ(p.aligned_bits, NBYTES * 8 * p.aligned_words);
  if (p.aligned_bits > 0) {
    ASSERT_EQ(reinterpret_cast<size_t>(p.aligned_start) & (NBYTES - 1), 0);
  }
  if (p.trailing_bits > 0) {
    ASSERT_EQ(p.trailing_bit_offset, bit_offset + p.leading_bits + p.aligned_bits);
    ASSERT_EQ(p.trailing_bit_offset + p.trailing_bits, bit_offset + length);
  }
}

arrow::Result<std::shared_ptr<Array>> CreateUnalignedArray(const Array& array) {
  // Slicing by 1 would create an invalid array if the type was wider than
  // 1 byte so double-check that the array is a 1-byte type
  EXPECT_EQ(array.type_id(), Type::UINT8);
  BufferVector sliced_buffers(array.data()->buffers.size(), nullptr);
  for (std::size_t i = 0; i < array.data()->buffers.size(); ++i) {
    if (array.data()->buffers[i]) {
      sliced_buffers[i] = SliceBuffer(array.data()->buffers[i], 1, 2);
    }
  }
  auto sliced_array_data =
      ArrayData::Make(array.type(), /*length=*/2, std::move(sliced_buffers));
  return MakeArray(std::move(sliced_array_data));
}

TEST(BitmapWordAlign, AlignedDataStart) {
  alignas(8) char buf[136] = {0};

  // A 8-byte aligned pointer
  const uint8_t* P = reinterpret_cast<const uint8_t*>(buf);
  const uint8_t* A = P;

  // {leading_bits, trailing_bits, trailing_bit_offset,
  //  aligned_start, aligned_bits, aligned_words}
  CheckBitmapWordAlign<8>(P, 0, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 13, {0, 13, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 63, {0, 63, 0, A, 0, 0});

  CheckBitmapWordAlign<8>(P, 0, 64, {0, 0, 0, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 73, {0, 9, 64, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 191, {0, 63, 128, A, 128, 2});

  CheckBitmapWordAlign<8>(P, 5, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 13, {13, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 59, {59, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 60, {59, 1, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 64, {59, 5, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 122, {59, 63, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 5, 123, {59, 0, 64, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 5, 314, {59, 63, 256, A + 8, 192, 3});
  CheckBitmapWordAlign<8>(P, 5, 315, {59, 0, 320, A + 8, 256, 4});

  CheckBitmapWordAlign<8>(P, 63, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 2, {1, 1, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 64, {1, 63, 64, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 63, 65, {1, 0, 128, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 63, 128, {1, 63, 128, A + 8, 64, 1});
  CheckBitmapWordAlign<8>(P, 63, 129, {1, 0, 192, A + 8, 128, 2});

  CheckBitmapWordAlign<8>(P, 1024, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1024, 130, {0, 2, 1152, A + 128, 128, 2});

  CheckBitmapWordAlign<8>(P, 1025, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 63, {63, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 64, {63, 1, 1088, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1025, 128, {63, 1, 1152, A + 136, 64, 1});
}

TEST(BitmapWordAlign, UnalignedDataStart) {
  alignas(8) char buf[136] = {0};

  const uint8_t* P = reinterpret_cast<const uint8_t*>(buf) + 1;
  const uint8_t* A = P + 7;

  // {leading_bits, trailing_bits, trailing_bit_offset,
  //  aligned_start, aligned_bits, aligned_words}
  CheckBitmapWordAlign<8>(P, 0, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 13, {13, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 56, {56, 0, 0, A, 0, 0});

  CheckBitmapWordAlign<8>(P, 0, 57, {56, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 119, {56, 63, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 0, 120, {56, 0, 120, A, 64, 1});
  CheckBitmapWordAlign<8>(P, 0, 184, {56, 0, 184, A, 128, 2});
  CheckBitmapWordAlign<8>(P, 0, 185, {56, 1, 184, A, 128, 2});

  CheckBitmapWordAlign<8>(P, 55, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 2, {1, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 55, 66, {1, 1, 120, A, 64, 1});

  // (P + 56 bits) is 64-bit aligned
  CheckBitmapWordAlign<8>(P, 56, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 1, {0, 1, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 63, {0, 63, 56, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 56, 191, {0, 63, 184, A, 128, 2});

  // (P + 1016 bits) is 64-bit aligned
  CheckBitmapWordAlign<8>(P, 1016, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 5, {0, 5, 1016, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 63, {0, 63, 1016, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1016, 64, {0, 0, 1080, A + 120, 64, 1});
  CheckBitmapWordAlign<8>(P, 1016, 129, {0, 1, 1144, A + 120, 128, 2});

  CheckBitmapWordAlign<8>(P, 1017, 0, {0, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 1, {1, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 63, {63, 0, 0, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 64, {63, 1, 1080, A, 0, 0});
  CheckBitmapWordAlign<8>(P, 1017, 128, {63, 1, 1144, A + 128, 64, 1});
}
}  // namespace internal

TEST(EnsureAlignment, Buffer) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer, AllocateBuffer(/*size=*/1024));
  std::shared_ptr<Buffer> unaligned_view = SliceBuffer(buffer, 1);
  std::shared_ptr<Buffer> aligned_view = SliceBuffer(buffer, 0);

  ASSERT_TRUE(util::CheckAlignment(*aligned_view, kDefaultBufferAlignment));
  ASSERT_FALSE(util::CheckAlignment(*unaligned_view, /*alignment=*/2));

  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Buffer> aligned_dupe,
      util::EnsureAlignment(aligned_view, /*alignment=*/8, default_memory_pool()));

  ASSERT_EQ(aligned_view->data(), aligned_dupe->data());
  ASSERT_TRUE(util::CheckAlignment(*aligned_dupe, kDefaultBufferAlignment));

  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Buffer> realigned,
      util::EnsureAlignment(unaligned_view, /*alignment=*/8, default_memory_pool()));

  ASSERT_NE(realigned->data(), unaligned_view->data());
  // Even though we only asked to check for 8 bytes of alignment, any reallocation will
  // always allocate at least kDefaultBufferAlignment bytes of alignment
  ASSERT_TRUE(util::CheckAlignment(*realigned, /*alignment=*/kDefaultBufferAlignment));

  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Buffer> realigned_large,
      util::EnsureAlignment(unaligned_view, /*alignment=*/256, default_memory_pool()));
  // If the user wants more than kDefaultBufferAlignment they should get it
  ASSERT_TRUE(util::CheckAlignment(*realigned_large, /*alignment=*/128));

  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Buffer> realigned_huge,
      util::EnsureAlignment(unaligned_view, /*alignment=*/2048, default_memory_pool()));
  // It should even be valid for the alignment to be larger than the buffer size itself
  ASSERT_TRUE(util::CheckAlignment(*realigned_huge, /*alignment=*/2048));
}

TEST(EnsureAlignment, BufferInvalid) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer, AllocateBuffer(/*size=*/1024));

  // This is nonsense but not worth introducing a Status return.  We just return true.
  ASSERT_TRUE(util::CheckAlignment(*buffer, 0));
  ASSERT_TRUE(util::CheckAlignment(*buffer, -1));

  ASSERT_THAT(util::EnsureAlignment(buffer, /*alignment=*/0, default_memory_pool()),
              Raises(StatusCode::Invalid,
                     testing::HasSubstr("Alignment must be a positive integer")));

  ASSERT_THAT(
      util::EnsureAlignment(buffer, /*alignment=*/util::kValueAlignment,
                            default_memory_pool()),
      Raises(StatusCode::Invalid,
             testing::HasSubstr(
                 "may only be used to call EnsureAlignment on arrays or tables")));
}

TEST(EnsureAlignment, Array) {
  MemoryPool* pool = default_memory_pool();
  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  auto random_array = rand.UInt8(/*size*/ 50, /*min*/ 0, /*max*/ 100,
                                 /*null_probability*/ 0, /*alignment*/ 512, pool);

  // for having buffers which are not aligned by 2048
  ASSERT_OK_AND_ASSIGN(auto sliced_array,
                       arrow::internal::CreateUnalignedArray(*random_array));

  ASSERT_EQ(util::CheckAlignment(*sliced_array, 2048), false);
  ASSERT_OK_AND_ASSIGN(auto aligned_array,
                       util::EnsureAlignment(std::move(sliced_array), 2048, pool));
  ASSERT_EQ(util::CheckAlignment(*aligned_array, 2048), true);
}

TEST(EnsureAlignment, ChunkedArray) {
  MemoryPool* pool = default_memory_pool();
  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  auto random_array_1 = rand.UInt8(/*size*/ 50, /*min*/ 0, /*max*/ 100,
                                   /*null_probability*/ 0, /*alignment*/ 512, pool);
  auto random_array_2 = rand.UInt8(/*size*/ 100, /*min*/ 10, /*max*/ 50,
                                   /*null_probability*/ 0, /*alignment*/ 1024, pool);

  ASSERT_OK_AND_ASSIGN(auto sliced_array_1,
                       arrow::internal::CreateUnalignedArray(*random_array_1));
  ASSERT_OK_AND_ASSIGN(auto sliced_array_2,
                       arrow::internal::CreateUnalignedArray(*random_array_2));

  ASSERT_OK_AND_ASSIGN(
      auto chunked_array,
      ChunkedArray::Make({sliced_array_1, sliced_array_2}, sliced_array_1->type()));

  std::vector<bool> needs_alignment;
  ASSERT_EQ(util::CheckAlignment(*chunked_array, 2048, &needs_alignment), false);

  ASSERT_OK_AND_ASSIGN(auto aligned_chunked_array,
                       util::EnsureAlignment(std::move(chunked_array), 2048, pool));
  ASSERT_EQ(util::CheckAlignment(*aligned_chunked_array, 2048, &needs_alignment), true);
}

TEST(EnsureAlignment, RecordBatch) {
  MemoryPool* pool = default_memory_pool();
  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  auto random_array_1 = rand.UInt8(/*size*/ 50, /*min*/ 0, /*max*/ 100,
                                   /*null_probability*/ 0, /*alignment*/ 512, pool);
  auto random_array_2 = rand.UInt8(/*size*/ 50, /*min*/ 10, /*max*/ 50,
                                   /*null_probability*/ 0, /*alignment*/ 1024, pool);

  ASSERT_OK_AND_ASSIGN(auto sliced_array_1,
                       arrow::internal::CreateUnalignedArray(*random_array_1));
  ASSERT_OK_AND_ASSIGN(auto sliced_array_2,
                       arrow::internal::CreateUnalignedArray(*random_array_2));

  auto f0 = field("f0", uint8());
  auto f1 = field("f1", uint8());
  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema({f0, f1});

  auto record_batch = RecordBatch::Make(schema, 50, {sliced_array_1, sliced_array_2});

  std::vector<bool> needs_alignment;
  ASSERT_EQ(util::CheckAlignment(*record_batch, 2048, &needs_alignment), false);

  ASSERT_OK_AND_ASSIGN(auto aligned_record_batch,
                       util::EnsureAlignment(std::move(record_batch), 2048, pool));
  ASSERT_EQ(util::CheckAlignment(*aligned_record_batch, 2048, &needs_alignment), true);
}

TEST(EnsureAlignment, Table) {
  MemoryPool* pool = default_memory_pool();
  auto rand = ::arrow::random::RandomArrayGenerator(1923);

  auto random_array_1 = rand.UInt8(/*size*/ 50, /*min*/ 0, /*max*/ 100,
                                   /*null_probability*/ 0, /*alignment*/ 512, pool);
  auto random_array_2 = rand.UInt8(/*size*/ 100, /*min*/ 10, /*max*/ 50,
                                   /*null_probability*/ 0, /*alignment*/ 1024, pool);
  ASSERT_OK_AND_ASSIGN(auto sliced_array_1,
                       arrow::internal::CreateUnalignedArray(*random_array_1));
  ASSERT_OK_AND_ASSIGN(auto sliced_array_2,
                       arrow::internal::CreateUnalignedArray(*random_array_2));
  ASSERT_OK_AND_ASSIGN(
      auto chunked_array_1,
      ChunkedArray::Make({sliced_array_1, sliced_array_2}, sliced_array_1->type()));

  random_array_1 = rand.UInt8(/*size*/ 150, /*min*/ 0, /*max*/ 100,
                              /*null_probability*/ 0, /*alignment*/ 1024, pool);
  random_array_2 = rand.UInt8(/*size*/ 75, /*min*/ 10, /*max*/ 50,
                              /*null_probability*/ 0, /*alignment*/ 512, pool);
  ASSERT_OK_AND_ASSIGN(sliced_array_1,
                       arrow::internal::CreateUnalignedArray(*random_array_1));
  ASSERT_OK_AND_ASSIGN(sliced_array_2,
                       arrow::internal::CreateUnalignedArray(*random_array_2));
  ASSERT_OK_AND_ASSIGN(
      auto chunked_array_2,
      ChunkedArray::Make({sliced_array_1, sliced_array_2}, sliced_array_1->type()));

  auto f0 = field("f0", uint8());
  auto f1 = field("f1", uint8());
  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema({f0, f1});

  auto table = Table::Make(schema, {chunked_array_1, chunked_array_2});

  std::vector<bool> needs_alignment;
  ASSERT_EQ(util::CheckAlignment(*table, 2048, &needs_alignment), false);

  ASSERT_OK_AND_ASSIGN(auto aligned_table,
                       util::EnsureAlignment(std::move(table), 2048, pool));
  ASSERT_EQ(util::CheckAlignment(*aligned_table, 2048, &needs_alignment), true);
}

using TypesRequiringSomeKindOfAlignment =
    testing::Types<Int16Type, Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type,
                   FloatType, DoubleType, Date32Type, Date64Type, Time32Type, Time64Type,
                   Decimal128Type, Decimal256Type, TimestampType, DurationType, MapType,
                   DenseUnionType, LargeBinaryType, LargeListType, LargeStringType,
                   MonthIntervalType, DayTimeIntervalType, MonthDayNanoIntervalType>;

using TypesNotRequiringAlignment =
    testing::Types<NullType, Int8Type, UInt8Type, FixedSizeListType, FixedSizeBinaryType,
                   BooleanType, SparseUnionType>;

template <typename ArrowType>
std::shared_ptr<DataType> sample_type() {
  return TypeTraits<ArrowType>::type_singleton();
}

template <>
std::shared_ptr<DataType> sample_type<FixedSizeBinaryType>() {
  return fixed_size_binary(16);
}

template <>
std::shared_ptr<DataType> sample_type<FixedSizeListType>() {
  return fixed_size_list(uint8(), 16);
}

template <>
std::shared_ptr<DataType> sample_type<Decimal128Type>() {
  return decimal128(32, 6);
}

template <>
std::shared_ptr<DataType> sample_type<Decimal256Type>() {
  return decimal256(60, 10);
}

template <>
std::shared_ptr<DataType> sample_type<LargeListType>() {
  return large_list(int8());
}

template <>
std::shared_ptr<DataType> sample_type<DenseUnionType>() {
  return dense_union({field("x", int8()), field("y", uint8())});
}

template <>
std::shared_ptr<DataType> sample_type<MapType>() {
  return map(utf8(), field("item", utf8()));
}

template <>
std::shared_ptr<DataType> sample_type<DurationType>() {
  return duration(TimeUnit::NANO);
}

template <>
std::shared_ptr<DataType> sample_type<TimestampType>() {
  return timestamp(TimeUnit::NANO);
}

template <>
std::shared_ptr<DataType> sample_type<Time32Type>() {
  return time32(TimeUnit::SECOND);
}

template <>
std::shared_ptr<DataType> sample_type<Time64Type>() {
  return time64(TimeUnit::NANO);
}

template <>
std::shared_ptr<DataType> sample_type<SparseUnionType>() {
  return sparse_union({field("x", uint8()), field("y", int8())}, {1, 2});
}

template <typename ArrowType>
std::shared_ptr<ArrayData> SampleArray() {
  random::RandomArrayGenerator gen(42);
  return gen.ArrayOf(sample_type<ArrowType>(), 100)->data();
}

template <>
std::shared_ptr<ArrayData> SampleArray<SparseUnionType>() {
  auto ty = sparse_union({field("ints", int64()), field("strs", utf8())}, {2, 7});
  auto ints = ArrayFromJSON(int64(), "[0, 1, 2, 3]");
  auto strs = ArrayFromJSON(utf8(), R"(["a", null, "c", "d"])");
  auto ids = ArrayFromJSON(int8(), "[2, 7, 2, 7]")->data()->buffers[1];
  const int length = 4;
  SparseUnionArray arr(ty, length, {ints, strs}, ids);
  return arr.data();
}

class ValueAlignment : public ::testing::Test {
 public:
  void CheckModified(const ArrayData& src, const ArrayData& dst) {
    ASSERT_EQ(src.buffers.size(), dst.buffers.size());
    for (std::size_t i = 0; i < src.buffers.size(); i++) {
      if (!src.buffers[i] || !dst.buffers[i]) {
        continue;
      }
      if (src.buffers[i]->address() != dst.buffers[i]->address()) {
        return;
      }
    }
    FAIL() << "Expected at least one buffer to have been modified by EnsureAlignment";
  }

  void CheckUnmodified(const ArrayData& src, const ArrayData& dst) {
    ASSERT_EQ(src.buffers.size(), dst.buffers.size());
    for (std::size_t i = 0; i < src.buffers.size(); i++) {
      if (!src.buffers[i] || !dst.buffers[i]) {
        continue;
      }
      ASSERT_EQ(src.buffers[i]->address(), dst.buffers[i]->address());
    }
  }
};

template <typename T>
class ValueAlignmentRequired : public ValueAlignment {};
template <typename T>
class ValueAlignmentNotRequired : public ValueAlignment {};

TYPED_TEST_SUITE(ValueAlignmentRequired, TypesRequiringSomeKindOfAlignment);
TYPED_TEST_SUITE(ValueAlignmentNotRequired, TypesNotRequiringAlignment);

// The default buffer alignment should always be large enough for value alignment
TYPED_TEST(ValueAlignmentRequired, DefaultAlignmentSufficient) {
  std::shared_ptr<ArrayData> data = SampleArray<TypeParam>();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> aligned,
      util::EnsureAlignment(data, util::kValueAlignment, default_memory_pool()));

  ASSERT_TRUE(util::CheckAlignment(*aligned, util::kValueAlignment));
  AssertArraysEqual(*MakeArray(data), *MakeArray(aligned));
  this->CheckUnmodified(*data, *aligned);
}

TYPED_TEST(ValueAlignmentRequired, RoundTrip) {
  std::shared_ptr<ArrayData> data = SampleArray<TypeParam>();
  std::shared_ptr<ArrayData> unaligned = UnalignBuffers(*data);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> aligned,
      util::EnsureAlignment(unaligned, util::kValueAlignment, default_memory_pool()));

  ASSERT_TRUE(util::CheckAlignment(*aligned, util::kValueAlignment));
  AssertArraysEqual(*MakeArray(data), *MakeArray(aligned));
  this->CheckModified(*unaligned, *aligned);
}

TYPED_TEST(ValueAlignmentNotRequired, RoundTrip) {
  std::shared_ptr<ArrayData> data = SampleArray<TypeParam>();
  std::shared_ptr<ArrayData> unaligned = UnalignBuffers(*data);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> aligned,
      util::EnsureAlignment(unaligned, util::kValueAlignment, default_memory_pool()));

  ASSERT_TRUE(util::CheckAlignment(*aligned, util::kValueAlignment));
  AssertArraysEqual(*MakeArray(data), *MakeArray(aligned));
  this->CheckUnmodified(*unaligned, *aligned);
}

TYPED_TEST(ValueAlignmentNotRequired, DefaultAlignmentSufficient) {
  std::shared_ptr<ArrayData> data = SampleArray<TypeParam>();
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> aligned,
      util::EnsureAlignment(data, util::kValueAlignment, default_memory_pool()));

  ASSERT_TRUE(util::CheckAlignment(*aligned, util::kValueAlignment));
  AssertArraysEqual(*MakeArray(data), *MakeArray(aligned));
  this->CheckUnmodified(*data, *aligned);
}

TEST_F(ValueAlignment, DenseUnion) {
  std::shared_ptr<ArrayData> data = SampleArray<DenseUnionType>();
  ASSERT_TRUE(util::CheckAlignment(*data, util::kValueAlignment));

  std::shared_ptr<ArrayData> unaligned = UnalignBuffers(*data);
  ASSERT_FALSE(util::CheckAlignment(*unaligned, util::kValueAlignment));
  // Dense union arrays are the only array type where the buffer at index 2 is expected
  // to be aligned (it contains 32-bit offsets and should be 4-byte aligned)
  ASSERT_FALSE(util::CheckAlignment(*unaligned->buffers[2], 4));

  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> realigned,
      util::EnsureAlignment(unaligned, util::kValueAlignment, default_memory_pool()));

  ASSERT_TRUE(util::CheckAlignment(*realigned, util::kValueAlignment));
  ASSERT_TRUE(util::CheckAlignment(*realigned->buffers[2], 4));
  // The buffer at index 1 is the types buffer which does not require realignment
  ASSERT_EQ(unaligned->buffers[1]->data(), realigned->buffers[1]->data());
}

TEST_F(ValueAlignment, RunEndEncoded) {
  // Run end requires alignment, value type does not
  std::shared_ptr<Array> run_ends = ArrayFromJSON(int32(), "[3, 5]");
  std::shared_ptr<Array> values = ArrayFromJSON(int8(), "[50, 100]");
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> array,
                       RunEndEncodedArray::Make(/*logical_length=*/5, std::move(run_ends),
                                                std::move(values), 0));

  std::shared_ptr<ArrayData> unaligned_ree = std::make_shared<ArrayData>(*array->data());
  unaligned_ree->child_data[0] = UnalignBuffers(*unaligned_ree->child_data[0]);
  unaligned_ree->child_data[1] = UnalignBuffers(*unaligned_ree->child_data[1]);

  std::shared_ptr<ArrayData> aligned_ree = std::make_shared<ArrayData>(*unaligned_ree);

  ASSERT_OK_AND_ASSIGN(
      aligned_ree,
      util::EnsureAlignment(aligned_ree, util::kValueAlignment, default_memory_pool()));
  ASSERT_TRUE(util::CheckAlignment(*aligned_ree, util::kValueAlignment));

  this->CheckModified(*unaligned_ree->child_data[0], *aligned_ree->child_data[0]);
  this->CheckUnmodified(*unaligned_ree->child_data[1], *aligned_ree->child_data[1]);
}

TEST_F(ValueAlignment, Dictionary) {
  // Dictionary values require alignment, dictionary indices do not
  std::shared_ptr<DataType> int8_utf8 = dictionary(int8(), utf8());
  std::shared_ptr<Array> array = ArrayFromJSON(int8_utf8, R"(["x", "x", "y"])");

  std::shared_ptr<ArrayData> unaligned_dict = std::make_shared<ArrayData>(*array->data());
  unaligned_dict->dictionary = UnalignBuffers(*unaligned_dict->dictionary);
  unaligned_dict = UnalignBuffers(*unaligned_dict);

  std::shared_ptr<ArrayData> aligned_dict = std::make_shared<ArrayData>(*unaligned_dict);

  ASSERT_OK_AND_ASSIGN(
      aligned_dict,
      util::EnsureAlignment(aligned_dict, util::kValueAlignment, default_memory_pool()));

  ASSERT_TRUE(util::CheckAlignment(*aligned_dict, util::kValueAlignment));
  this->CheckUnmodified(*unaligned_dict, *aligned_dict);
  this->CheckModified(*unaligned_dict->dictionary, *aligned_dict->dictionary);

  // Dictionary values do not require alignment, dictionary indices do
  std::shared_ptr<DataType> int16_int8 = dictionary(int16(), int8());
  array = ArrayFromJSON(int16_int8, R"([7, 11])");

  unaligned_dict = std::make_shared<ArrayData>(*array->data());
  unaligned_dict->dictionary = UnalignBuffers(*unaligned_dict->dictionary);
  unaligned_dict = UnalignBuffers(*unaligned_dict);

  aligned_dict = std::make_shared<ArrayData>(*unaligned_dict);

  ASSERT_OK_AND_ASSIGN(
      aligned_dict,
      util::EnsureAlignment(aligned_dict, util::kValueAlignment, default_memory_pool()));

  ASSERT_TRUE(util::CheckAlignment(*aligned_dict, util::kValueAlignment));
  this->CheckModified(*unaligned_dict, *aligned_dict);
  this->CheckUnmodified(*unaligned_dict->dictionary, *aligned_dict->dictionary);
}

TEST_F(ValueAlignment, Extension) {
  std::shared_ptr<Array> array = ExampleSmallint();

  std::shared_ptr<ArrayData> unaligned = UnalignBuffers(*array->data());

  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<ArrayData> aligned,
      util::EnsureAlignment(unaligned, util::kValueAlignment, default_memory_pool()));

  ASSERT_TRUE(util::CheckAlignment(*aligned, util::kValueAlignment));
  this->CheckModified(*unaligned, *aligned);
}

}  // namespace arrow
