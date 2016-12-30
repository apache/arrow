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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "parquet/column/reader.h"
#include "parquet/column/statistics.h"
#include "parquet/column/test-specialization.h"
#include "parquet/column/test-util.h"
#include "parquet/column/writer.h"
#include "parquet/file/reader.h"
#include "parquet/file/writer.h"
#include "parquet/schema/descriptor.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {

using schema::NodePtr;
using schema::PrimitiveNode;
using schema::GroupNode;

namespace test {

template <typename TestType>
class TestRowGroupStatistics : public PrimitiveTypedTest<TestType> {
 public:
  using T = typename TestType::c_type;
  using TypedStats = TypedRowGroupStatistics<TestType>;

  std::vector<T> GetDeepCopy(
      const std::vector<T>&);  // allocates new memory for FLBA/ByteArray

  T* GetValuesPointer(std::vector<T>&);
  void DeepFree(std::vector<T>&);

  void TestMinMaxEncode() {
    this->GenerateData(1000);

    TypedStats statistics1(this->schema_.Column(0));
    statistics1.Update(this->values_ptr_, this->values_.size(), 0);
    std::string encoded_min = statistics1.EncodeMin();
    std::string encoded_max = statistics1.EncodeMax();

    TypedStats statistics2(this->schema_.Column(0), encoded_min, encoded_max,
        this->values_.size(), 0, 0, true);

    ASSERT_EQ(encoded_min, statistics2.EncodeMin());
    ASSERT_EQ(encoded_max, statistics2.EncodeMax());
    ASSERT_EQ(statistics1.min(), statistics2.min());
    ASSERT_EQ(statistics1.max(), statistics2.max());
  }

  void TestReset() {
    this->GenerateData(1000);

    TypedStats statistics(this->schema_.Column(0));
    statistics.Update(this->values_ptr_, this->values_.size(), 0);
    ASSERT_EQ(this->values_.size(), statistics.num_values());

    statistics.Reset();
    ASSERT_EQ(0, statistics.null_count());
    ASSERT_EQ(0, statistics.num_values());
    ASSERT_EQ("", statistics.EncodeMin());
    ASSERT_EQ("", statistics.EncodeMax());
  }

  void TestMerge() {
    int num_null[2];
    random_numbers(2, 42, 0, 100, num_null);

    TypedStats statistics1(this->schema_.Column(0));
    this->GenerateData(1000);
    statistics1.Update(
        this->values_ptr_, this->values_.size() - num_null[0], num_null[0]);

    TypedStats statistics2(this->schema_.Column(0));
    this->GenerateData(1000);
    statistics2.Update(
        this->values_ptr_, this->values_.size() - num_null[1], num_null[1]);

    TypedStats total(this->schema_.Column(0));
    total.Merge(statistics1);
    total.Merge(statistics2);

    ASSERT_EQ(num_null[0] + num_null[1], total.null_count());
    ASSERT_EQ(this->values_.size() * 2 - num_null[0] - num_null[1], total.num_values());
    ASSERT_EQ(total.min(), std::min(statistics1.min(), statistics2.min()));
    ASSERT_EQ(total.max(), std::max(statistics1.max(), statistics2.max()));
  }

  void TestFullRoundtrip(int64_t num_values, int64_t null_count) {
    this->GenerateData(num_values);

    // compute statistics for the whole batch
    TypedStats expected_stats(this->schema_.Column(0));
    expected_stats.Update(this->values_ptr_, num_values - null_count, null_count);

    auto sink = std::make_shared<InMemoryOutputStream>();
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);
    std::shared_ptr<WriterProperties> writer_properties =
        WriterProperties::Builder().enable_statistics("column")->build();
    auto file_writer = ParquetFileWriter::Open(sink, gnode, writer_properties);
    auto row_group_writer = file_writer->AppendRowGroup(num_values);
    auto column_writer =
        static_cast<TypedColumnWriter<TestType>*>(row_group_writer->NextColumn());

    // simulate the case when data comes from multiple buffers,
    // in which case special care is necessary for FLBA/ByteArray types
    for (int i = 0; i < 2; i++) {
      int batch_num_values = i ? num_values - num_values / 2 : num_values / 2;
      int batch_null_count = i ? null_count : 0;
      DCHECK(null_count <= num_values);  // avoid too much headache
      std::vector<int16_t> definition_levels(batch_null_count, 0);
      definition_levels.insert(
          definition_levels.end(), batch_num_values - batch_null_count, 1);
      auto beg = this->values_.begin() + i * num_values / 2;
      auto end = beg + batch_num_values;
      std::vector<T> batch = GetDeepCopy(std::vector<T>(beg, end));
      T* batch_values_ptr = GetValuesPointer(batch);
      column_writer->WriteBatch(
          batch_num_values, definition_levels.data(), nullptr, batch_values_ptr);
      DeepFree(batch);
    }
    column_writer->Close();
    row_group_writer->Close();
    file_writer->Close();

    auto buffer = sink->GetBuffer();
    auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
    auto file_reader = ParquetFileReader::Open(source);
    auto rg_reader = file_reader->RowGroup(0);
    auto column_chunk = rg_reader->metadata()->ColumnChunk(0);
    std::shared_ptr<RowGroupStatistics> stats = column_chunk->statistics();
    // check values after serialization + deserialization
    ASSERT_EQ(null_count, stats->null_count());
    ASSERT_EQ(num_values - null_count, stats->num_values());
    ASSERT_EQ(expected_stats.EncodeMin(), stats->EncodeMin());
    ASSERT_EQ(expected_stats.EncodeMax(), stats->EncodeMax());
  }
};

template <typename TestType>
typename TestType::c_type* TestRowGroupStatistics<TestType>::GetValuesPointer(
    std::vector<typename TestType::c_type>& values) {
  return values.data();
}

template <>
bool* TestRowGroupStatistics<BooleanType>::GetValuesPointer(std::vector<bool>& values) {
  static std::vector<uint8_t> bool_buffer;
  bool_buffer.clear();
  bool_buffer.resize(values.size());
  std::copy(values.begin(), values.end(), bool_buffer.begin());
  return reinterpret_cast<bool*>(bool_buffer.data());
}

template <typename TestType>
typename std::vector<typename TestType::c_type>
TestRowGroupStatistics<TestType>::GetDeepCopy(
    const std::vector<typename TestType::c_type>& values) {
  return values;
}

template <>
std::vector<FLBA> TestRowGroupStatistics<FLBAType>::GetDeepCopy(
    const std::vector<FLBA>& values) {
  std::vector<FLBA> copy;
  MemoryAllocator* allocator = default_allocator();
  for (const FLBA& flba : values) {
    uint8_t* ptr;
    PARQUET_THROW_NOT_OK(allocator->Allocate(FLBA_LENGTH, &ptr));
    memcpy(ptr, flba.ptr, FLBA_LENGTH);
    copy.emplace_back(ptr);
  }
  return copy;
}

template <>
std::vector<ByteArray> TestRowGroupStatistics<ByteArrayType>::GetDeepCopy(
    const std::vector<ByteArray>& values) {
  std::vector<ByteArray> copy;
  MemoryAllocator* allocator = default_allocator();
  for (const ByteArray& ba : values) {
    uint8_t* ptr;
    PARQUET_THROW_NOT_OK(allocator->Allocate(ba.len, &ptr));
    memcpy(ptr, ba.ptr, ba.len);
    copy.emplace_back(ba.len, ptr);
  }
  return copy;
}

template <typename TestType>
void TestRowGroupStatistics<TestType>::DeepFree(
    std::vector<typename TestType::c_type>& values) {}

template <>
void TestRowGroupStatistics<FLBAType>::DeepFree(std::vector<FLBA>& values) {
  MemoryAllocator* allocator = default_allocator();
  for (FLBA& flba : values) {
    auto ptr = const_cast<uint8_t*>(flba.ptr);
    memset(ptr, 0, FLBA_LENGTH);
    allocator->Free(ptr, FLBA_LENGTH);
  }
}

template <>
void TestRowGroupStatistics<ByteArrayType>::DeepFree(std::vector<ByteArray>& values) {
  MemoryAllocator* allocator = default_allocator();
  for (ByteArray& ba : values) {
    auto ptr = const_cast<uint8_t*>(ba.ptr);
    memset(ptr, 0, ba.len);
    allocator->Free(ptr, ba.len);
  }
}

template <>
void TestRowGroupStatistics<ByteArrayType>::TestMinMaxEncode() {
  this->GenerateData(1000);
  // Test that we encode min max strings correctly
  TypedRowGroupStatistics<ByteArrayType> statistics1(this->schema_.Column(0));
  statistics1.Update(this->values_ptr_, this->values_.size(), 0);
  std::string encoded_min = statistics1.EncodeMin();
  std::string encoded_max = statistics1.EncodeMax();

  // encoded is same as unencoded
  ASSERT_EQ(encoded_min,
      std::string((const char*)statistics1.min().ptr, statistics1.min().len));
  ASSERT_EQ(encoded_max,
      std::string((const char*)statistics1.max().ptr, statistics1.max().len));

  TypedRowGroupStatistics<ByteArrayType> statistics2(this->schema_.Column(0), encoded_min,
      encoded_max, this->values_.size(), 0, 0, true);

  ASSERT_EQ(encoded_min, statistics2.EncodeMin());
  ASSERT_EQ(encoded_max, statistics2.EncodeMax());
  ASSERT_EQ(statistics1.min(), statistics2.min());
  ASSERT_EQ(statistics1.max(), statistics2.max());
}

using TestTypes = ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
    ByteArrayType, FLBAType, BooleanType>;

TYPED_TEST_CASE(TestRowGroupStatistics, TestTypes);

TYPED_TEST(TestRowGroupStatistics, MinMaxEncode) {
  this->SetUpSchema(Repetition::REQUIRED);
  this->TestMinMaxEncode();
}

TYPED_TEST(TestRowGroupStatistics, Reset) {
  this->SetUpSchema(Repetition::OPTIONAL);
  this->TestReset();
}

TYPED_TEST(TestRowGroupStatistics, FullRoundtrip) {
  this->SetUpSchema(Repetition::OPTIONAL);
  this->TestFullRoundtrip(100, 31);
  this->TestFullRoundtrip(1000, 415);
  this->TestFullRoundtrip(10000, 926);
}

template <typename TestType>
class TestNumericRowGroupStatistics : public TestRowGroupStatistics<TestType> {};

using NumericTypes = ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType>;

TYPED_TEST_CASE(TestNumericRowGroupStatistics, NumericTypes);

TYPED_TEST(TestNumericRowGroupStatistics, Merge) {
  this->SetUpSchema(Repetition::OPTIONAL);
  this->TestMerge();
}

}  // namespace test
}  // namespace parquet
