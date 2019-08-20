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

#include "arrow/testing/gtest_util.h"

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/test_util.h"
#include "parquet/thrift.h"
#include "parquet/types.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;

namespace parquet {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

// ----------------------------------------------------------------------
// Test comparators

static ByteArray ByteArrayFromString(const std::string& s) {
  auto ptr = reinterpret_cast<const uint8_t*>(s.data());
  return ByteArray(static_cast<uint32_t>(s.size()), ptr);
}

static FLBA FLBAFromString(const std::string& s) {
  auto ptr = reinterpret_cast<const uint8_t*>(s.data());
  return FLBA(ptr);
}

TEST(Comparison, SignedByteArray) {
  auto comparator = MakeComparator<ByteArrayType>(Type::BYTE_ARRAY, SortOrder::SIGNED);

  std::string s1 = "12345";
  std::string s2 = "12345678";
  ByteArray s1ba = ByteArrayFromString(s1);
  ByteArray s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1ba, s2ba));

  // This is case where signed comparision UTF-8 (PARQUET-686) is incorrect
  // This example is to only check signed comparison and not UTF-8.
  s1 = u8"bügeln";
  s2 = u8"braten";
  s1ba = ByteArrayFromString(s1);
  s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1ba, s2ba));
}

TEST(Comparison, UnsignedByteArray) {
  // Check if UTF-8 is compared using unsigned correctly
  auto comparator = MakeComparator<ByteArrayType>(Type::BYTE_ARRAY, SortOrder::UNSIGNED);

  std::string s1 = "arrange";
  std::string s2 = "arrangement";
  ByteArray s1ba = ByteArrayFromString(s1);
  ByteArray s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1ba, s2ba));

  // Multi-byte UTF-8 characters
  s1 = u8"braten";
  s2 = u8"bügeln";
  s1ba = ByteArrayFromString(s1);
  s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1ba, s2ba));

  s1 = u8"ünk123456";  // ü = 252
  s2 = u8"ănk123456";  // ă = 259
  s1ba = ByteArrayFromString(s1);
  s2ba = ByteArrayFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1ba, s2ba));
}

TEST(Comparison, SignedFLBA) {
  int size = 10;
  auto comparator =
      MakeComparator<FLBAType>(Type::FIXED_LEN_BYTE_ARRAY, SortOrder::SIGNED, size);

  std::string s1 = "Anti123456";
  std::string s2 = "Bunkd123456";
  FLBA s1flba = FLBAFromString(s1);
  FLBA s2flba = FLBAFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1flba, s2flba));

  s1 = "Bünk123456";
  s2 = "Bunk123456";
  s1flba = FLBAFromString(s1);
  s2flba = FLBAFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1flba, s2flba));
}

TEST(Comparison, UnsignedFLBA) {
  int size = 10;
  auto comparator =
      MakeComparator<FLBAType>(Type::FIXED_LEN_BYTE_ARRAY, SortOrder::UNSIGNED, size);

  std::string s1 = "Anti123456";
  std::string s2 = "Bunkd123456";
  FLBA s1flba = FLBAFromString(s1);
  FLBA s2flba = FLBAFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1flba, s2flba));

  s1 = "Bunk123456";
  s2 = "Bünk123456";
  s1flba = FLBAFromString(s1);
  s2flba = FLBAFromString(s2);
  ASSERT_TRUE(comparator->Compare(s1flba, s2flba));
}

TEST(Comparison, SignedInt96) {
  parquet::Int96 a{{1, 41, 14}}, b{{1, 41, 42}};
  parquet::Int96 aa{{1, 41, 14}}, bb{{1, 41, 14}};
  parquet::Int96 aaa{{1, 41, static_cast<uint32_t>(-14)}}, bbb{{1, 41, 42}};

  auto comparator = MakeComparator<Int96Type>(Type::INT96, SortOrder::SIGNED);

  ASSERT_TRUE(comparator->Compare(a, b));
  ASSERT_TRUE(!comparator->Compare(aa, bb) && !comparator->Compare(bb, aa));
  ASSERT_TRUE(comparator->Compare(aaa, bbb));
}

TEST(Comparison, UnsignedInt96) {
  parquet::Int96 a{{1, 41, 14}}, b{{1, static_cast<uint32_t>(-41), 42}};
  parquet::Int96 aa{{1, 41, 14}}, bb{{1, 41, static_cast<uint32_t>(-14)}};
  parquet::Int96 aaa, bbb;

  auto comparator = MakeComparator<Int96Type>(Type::INT96, SortOrder::UNSIGNED);

  ASSERT_TRUE(comparator->Compare(a, b));
  ASSERT_TRUE(comparator->Compare(aa, bb));

  // INT96 Timestamp
  aaa.value[2] = 2451545;  // 2000-01-01
  bbb.value[2] = 2451546;  // 2000-01-02
  // 12 hours + 34 minutes + 56 seconds.
  Int96SetNanoSeconds(aaa, 45296000000000);
  // 12 hours + 34 minutes + 50 seconds.
  Int96SetNanoSeconds(bbb, 45290000000000);
  ASSERT_TRUE(comparator->Compare(aaa, bbb));

  aaa.value[2] = 2451545;  // 2000-01-01
  bbb.value[2] = 2451545;  // 2000-01-01
  // 11 hours + 34 minutes + 56 seconds.
  Int96SetNanoSeconds(aaa, 41696000000000);
  // 12 hours + 34 minutes + 50 seconds.
  Int96SetNanoSeconds(bbb, 45290000000000);
  ASSERT_TRUE(comparator->Compare(aaa, bbb));

  aaa.value[2] = 2451545;  // 2000-01-01
  bbb.value[2] = 2451545;  // 2000-01-01
  // 12 hours + 34 minutes + 55 seconds.
  Int96SetNanoSeconds(aaa, 45295000000000);
  // 12 hours + 34 minutes + 56 seconds.
  Int96SetNanoSeconds(bbb, 45296000000000);
  ASSERT_TRUE(comparator->Compare(aaa, bbb));
}

TEST(Comparison, SignedInt64) {
  int64_t a = 1, b = 4;
  int64_t aa = 1, bb = 1;
  int64_t aaa = -1, bbb = 1;

  NodePtr node = PrimitiveNode::Make("SignedInt64", Repetition::REQUIRED, Type::INT64);
  ColumnDescriptor descr(node, 0, 0);

  auto comparator = MakeComparator<Int64Type>(&descr);

  ASSERT_TRUE(comparator->Compare(a, b));
  ASSERT_TRUE(!comparator->Compare(aa, bb) && !comparator->Compare(bb, aa));
  ASSERT_TRUE(comparator->Compare(aaa, bbb));
}

TEST(Comparison, UnsignedInt64) {
  uint64_t a = 1, b = 4;
  uint64_t aa = 1, bb = 1;
  uint64_t aaa = 1, bbb = -1;

  NodePtr node = PrimitiveNode::Make("UnsignedInt64", Repetition::REQUIRED, Type::INT64,
                                     ConvertedType::UINT_64);
  ColumnDescriptor descr(node, 0, 0);

  ASSERT_EQ(SortOrder::UNSIGNED, descr.sort_order());
  auto comparator = MakeComparator<Int64Type>(&descr);

  ASSERT_TRUE(comparator->Compare(a, b));
  ASSERT_TRUE(!comparator->Compare(aa, bb) && !comparator->Compare(bb, aa));
  ASSERT_TRUE(comparator->Compare(aaa, bbb));
}

TEST(Comparison, UnsignedInt32) {
  uint32_t a = 1, b = 4;
  uint32_t aa = 1, bb = 1;
  uint32_t aaa = 1, bbb = -1;

  NodePtr node = PrimitiveNode::Make("UnsignedInt32", Repetition::REQUIRED, Type::INT32,
                                     ConvertedType::UINT_32);
  ColumnDescriptor descr(node, 0, 0);

  ASSERT_EQ(SortOrder::UNSIGNED, descr.sort_order());
  auto comparator = MakeComparator<Int32Type>(&descr);

  ASSERT_TRUE(comparator->Compare(a, b));
  ASSERT_TRUE(!comparator->Compare(aa, bb) && !comparator->Compare(bb, aa));
  ASSERT_TRUE(comparator->Compare(aaa, bbb));
}

TEST(Comparison, UnknownSortOrder) {
  NodePtr node =
      PrimitiveNode::Make("Unknown", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                          ConvertedType::INTERVAL, 12);
  ColumnDescriptor descr(node, 0, 0);

  ASSERT_THROW(Comparator::Make(&descr), ParquetException);
}

// ----------------------------------------------------------------------

template <typename TestType>
class TestStatistics : public PrimitiveTypedTest<TestType> {
 public:
  using T = typename TestType::c_type;

  std::vector<T> GetDeepCopy(
      const std::vector<T>&);  // allocates new memory for FLBA/ByteArray

  T* GetValuesPointer(std::vector<T>&);
  void DeepFree(std::vector<T>&);

  void TestMinMaxEncode() {
    this->GenerateData(1000);

    auto statistics1 = MakeStatistics<TestType>(this->schema_.Column(0));
    statistics1->Update(this->values_ptr_, this->values_.size(), 0);
    std::string encoded_min = statistics1->EncodeMin();
    std::string encoded_max = statistics1->EncodeMax();

    auto statistics2 =
        MakeStatistics<TestType>(this->schema_.Column(0), encoded_min, encoded_max,
                                 this->values_.size(), 0, 0, true);

    auto statistics3 = MakeStatistics<TestType>(this->schema_.Column(0));
    std::vector<uint8_t> valid_bits(
        BitUtil::BytesForBits(static_cast<uint32_t>(this->values_.size())) + 1, 255);
    statistics3->UpdateSpaced(this->values_ptr_, valid_bits.data(), 0,
                              this->values_.size(), 0);
    std::string encoded_min_spaced = statistics3->EncodeMin();
    std::string encoded_max_spaced = statistics3->EncodeMax();

    ASSERT_EQ(encoded_min, statistics2->EncodeMin());
    ASSERT_EQ(encoded_max, statistics2->EncodeMax());
    ASSERT_EQ(statistics1->min(), statistics2->min());
    ASSERT_EQ(statistics1->max(), statistics2->max());
    ASSERT_EQ(encoded_min_spaced, statistics2->EncodeMin());
    ASSERT_EQ(encoded_max_spaced, statistics2->EncodeMax());
    ASSERT_EQ(statistics3->min(), statistics2->min());
    ASSERT_EQ(statistics3->max(), statistics2->max());
  }

  void TestReset() {
    this->GenerateData(1000);

    auto statistics = MakeStatistics<TestType>(this->schema_.Column(0));
    statistics->Update(this->values_ptr_, this->values_.size(), 0);
    ASSERT_EQ(this->values_.size(), statistics->num_values());

    statistics->Reset();
    ASSERT_EQ(0, statistics->null_count());
    ASSERT_EQ(0, statistics->num_values());
    ASSERT_EQ("", statistics->EncodeMin());
    ASSERT_EQ("", statistics->EncodeMax());
  }

  void TestMerge() {
    int num_null[2];
    random_numbers(2, 42, 0, 100, num_null);

    auto statistics1 = MakeStatistics<TestType>(this->schema_.Column(0));
    this->GenerateData(1000);
    statistics1->Update(this->values_ptr_, this->values_.size() - num_null[0],
                        num_null[0]);

    auto statistics2 = MakeStatistics<TestType>(this->schema_.Column(0));
    this->GenerateData(1000);
    statistics2->Update(this->values_ptr_, this->values_.size() - num_null[1],
                        num_null[1]);

    auto total = MakeStatistics<TestType>(this->schema_.Column(0));
    total->Merge(*statistics1);
    total->Merge(*statistics2);

    ASSERT_EQ(num_null[0] + num_null[1], total->null_count());
    ASSERT_EQ(this->values_.size() * 2 - num_null[0] - num_null[1], total->num_values());
    ASSERT_EQ(total->min(), std::min(statistics1->min(), statistics2->min()));
    ASSERT_EQ(total->max(), std::max(statistics1->max(), statistics2->max()));
  }

  void TestFullRoundtrip(int64_t num_values, int64_t null_count) {
    this->GenerateData(num_values);

    // compute statistics for the whole batch
    auto expected_stats = MakeStatistics<TestType>(this->schema_.Column(0));
    expected_stats->Update(this->values_ptr_, num_values - null_count, null_count);

    auto sink = CreateOutputStream();
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);
    std::shared_ptr<WriterProperties> writer_properties =
        WriterProperties::Builder().enable_statistics("column")->build();
    auto file_writer = ParquetFileWriter::Open(sink, gnode, writer_properties);
    auto row_group_writer = file_writer->AppendRowGroup();
    auto column_writer =
        static_cast<TypedColumnWriter<TestType>*>(row_group_writer->NextColumn());

    // simulate the case when data comes from multiple buffers,
    // in which case special care is necessary for FLBA/ByteArray types
    for (int i = 0; i < 2; i++) {
      int64_t batch_num_values = i ? num_values - num_values / 2 : num_values / 2;
      int64_t batch_null_count = i ? null_count : 0;
      DCHECK(null_count <= num_values);  // avoid too much headache
      std::vector<int16_t> definition_levels(batch_null_count, 0);
      definition_levels.insert(definition_levels.end(),
                               batch_num_values - batch_null_count, 1);
      auto beg = this->values_.begin() + i * num_values / 2;
      auto end = beg + batch_num_values;
      std::vector<T> batch = GetDeepCopy(std::vector<T>(beg, end));
      T* batch_values_ptr = GetValuesPointer(batch);
      column_writer->WriteBatch(batch_num_values, definition_levels.data(), nullptr,
                                batch_values_ptr);
      DeepFree(batch);
    }
    column_writer->Close();
    row_group_writer->Close();
    file_writer->Close();

    std::shared_ptr<Buffer> buffer;
    ASSERT_OK(sink->Finish(&buffer));
    auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
    auto file_reader = ParquetFileReader::Open(source);
    auto rg_reader = file_reader->RowGroup(0);
    auto column_chunk = rg_reader->metadata()->ColumnChunk(0);
    if (!column_chunk->is_stats_set()) return;
    std::shared_ptr<Statistics> stats = column_chunk->statistics();
    // check values after serialization + deserialization
    ASSERT_EQ(null_count, stats->null_count());
    ASSERT_EQ(num_values - null_count, stats->num_values());
    ASSERT_EQ(expected_stats->EncodeMin(), stats->EncodeMin());
    ASSERT_EQ(expected_stats->EncodeMax(), stats->EncodeMax());
  }
};

template <typename TestType>
typename TestType::c_type* TestStatistics<TestType>::GetValuesPointer(
    std::vector<typename TestType::c_type>& values) {
  return values.data();
}

template <>
bool* TestStatistics<BooleanType>::GetValuesPointer(std::vector<bool>& values) {
  static std::vector<uint8_t> bool_buffer;
  bool_buffer.clear();
  bool_buffer.resize(values.size());
  std::copy(values.begin(), values.end(), bool_buffer.begin());
  return reinterpret_cast<bool*>(bool_buffer.data());
}

template <typename TestType>
typename std::vector<typename TestType::c_type> TestStatistics<TestType>::GetDeepCopy(
    const std::vector<typename TestType::c_type>& values) {
  return values;
}

template <>
std::vector<FLBA> TestStatistics<FLBAType>::GetDeepCopy(const std::vector<FLBA>& values) {
  std::vector<FLBA> copy;
  MemoryPool* pool = ::arrow::default_memory_pool();
  for (const FLBA& flba : values) {
    uint8_t* ptr;
    PARQUET_THROW_NOT_OK(pool->Allocate(FLBA_LENGTH, &ptr));
    memcpy(ptr, flba.ptr, FLBA_LENGTH);
    copy.emplace_back(ptr);
  }
  return copy;
}

template <>
std::vector<ByteArray> TestStatistics<ByteArrayType>::GetDeepCopy(
    const std::vector<ByteArray>& values) {
  std::vector<ByteArray> copy;
  MemoryPool* pool = default_memory_pool();
  for (const ByteArray& ba : values) {
    uint8_t* ptr;
    PARQUET_THROW_NOT_OK(pool->Allocate(ba.len, &ptr));
    memcpy(ptr, ba.ptr, ba.len);
    copy.emplace_back(ba.len, ptr);
  }
  return copy;
}

template <typename TestType>
void TestStatistics<TestType>::DeepFree(std::vector<typename TestType::c_type>& values) {}

template <>
void TestStatistics<FLBAType>::DeepFree(std::vector<FLBA>& values) {
  MemoryPool* pool = default_memory_pool();
  for (FLBA& flba : values) {
    auto ptr = const_cast<uint8_t*>(flba.ptr);
    memset(ptr, 0, FLBA_LENGTH);
    pool->Free(ptr, FLBA_LENGTH);
  }
}

template <>
void TestStatistics<ByteArrayType>::DeepFree(std::vector<ByteArray>& values) {
  MemoryPool* pool = default_memory_pool();
  for (ByteArray& ba : values) {
    auto ptr = const_cast<uint8_t*>(ba.ptr);
    memset(ptr, 0, ba.len);
    pool->Free(ptr, ba.len);
  }
}

template <>
void TestStatistics<ByteArrayType>::TestMinMaxEncode() {
  this->GenerateData(1000);
  // Test that we encode min max strings correctly
  auto statistics1 = MakeStatistics<ByteArrayType>(this->schema_.Column(0));
  statistics1->Update(this->values_ptr_, this->values_.size(), 0);
  std::string encoded_min = statistics1->EncodeMin();
  std::string encoded_max = statistics1->EncodeMax();

  // encoded is same as unencoded
  ASSERT_EQ(encoded_min,
            std::string(reinterpret_cast<const char*>(statistics1->min().ptr),
                        statistics1->min().len));
  ASSERT_EQ(encoded_max,
            std::string(reinterpret_cast<const char*>(statistics1->max().ptr),
                        statistics1->max().len));

  auto statistics2 =
      MakeStatistics<ByteArrayType>(this->schema_.Column(0), encoded_min, encoded_max,
                                    this->values_.size(), 0, 0, true);

  ASSERT_EQ(encoded_min, statistics2->EncodeMin());
  ASSERT_EQ(encoded_max, statistics2->EncodeMax());
  ASSERT_EQ(statistics1->min(), statistics2->min());
  ASSERT_EQ(statistics1->max(), statistics2->max());
}

using TestTypes = ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType,
                                   ByteArrayType, FLBAType, BooleanType>;

TYPED_TEST_CASE(TestStatistics, TestTypes);

TYPED_TEST(TestStatistics, MinMaxEncode) {
  this->SetUpSchema(Repetition::REQUIRED);
  ASSERT_NO_FATAL_FAILURE(this->TestMinMaxEncode());
}

TYPED_TEST(TestStatistics, Reset) {
  this->SetUpSchema(Repetition::OPTIONAL);
  ASSERT_NO_FATAL_FAILURE(this->TestReset());
}

TYPED_TEST(TestStatistics, FullRoundtrip) {
  this->SetUpSchema(Repetition::OPTIONAL);
  ASSERT_NO_FATAL_FAILURE(this->TestFullRoundtrip(100, 31));
  ASSERT_NO_FATAL_FAILURE(this->TestFullRoundtrip(1000, 415));
  ASSERT_NO_FATAL_FAILURE(this->TestFullRoundtrip(10000, 926));
}

template <typename TestType>
class TestNumericStatistics : public TestStatistics<TestType> {};

using NumericTypes = ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType>;

TYPED_TEST_CASE(TestNumericStatistics, NumericTypes);

TYPED_TEST(TestNumericStatistics, Merge) {
  this->SetUpSchema(Repetition::OPTIONAL);
  ASSERT_NO_FATAL_FAILURE(this->TestMerge());
}

// Helper for basic statistics tests below
void AssertStatsSet(const ApplicationVersion& version,
                    std::shared_ptr<parquet::WriterProperties> props,
                    const ColumnDescriptor* column, bool expected_is_set) {
  auto metadata_builder = ColumnChunkMetaDataBuilder::Make(props, column);
  auto column_chunk =
      ColumnChunkMetaData::Make(metadata_builder->contents(), column, &version);
  EncodedStatistics stats;
  stats.set_is_signed(false);
  metadata_builder->SetStatistics(stats);
  ASSERT_EQ(column_chunk->is_stats_set(), expected_is_set);
}

// Statistics are restricted for few types in older parquet version
TEST(CorruptStatistics, Basics) {
  std::string created_by = "parquet-mr version 1.8.0";
  ApplicationVersion version(created_by);
  SchemaDescriptor schema;
  schema::NodePtr node;
  std::vector<schema::NodePtr> fields;
  // Test Physical Types
  fields.push_back(schema::PrimitiveNode::Make("col1", Repetition::OPTIONAL, Type::INT32,
                                               ConvertedType::NONE));
  fields.push_back(schema::PrimitiveNode::Make("col2", Repetition::OPTIONAL,
                                               Type::BYTE_ARRAY, ConvertedType::NONE));
  // Test Logical Types
  fields.push_back(schema::PrimitiveNode::Make("col3", Repetition::OPTIONAL, Type::INT32,
                                               ConvertedType::DATE));
  fields.push_back(schema::PrimitiveNode::Make("col4", Repetition::OPTIONAL, Type::INT32,
                                               ConvertedType::UINT_32));
  fields.push_back(schema::PrimitiveNode::Make("col5", Repetition::OPTIONAL,
                                               Type::FIXED_LEN_BYTE_ARRAY,
                                               ConvertedType::INTERVAL, 12));
  fields.push_back(schema::PrimitiveNode::Make("col6", Repetition::OPTIONAL,
                                               Type::BYTE_ARRAY, ConvertedType::UTF8));
  node = schema::GroupNode::Make("schema", Repetition::REQUIRED, fields);
  schema.Init(node);

  parquet::WriterProperties::Builder builder;
  builder.created_by(created_by);
  std::shared_ptr<parquet::WriterProperties> props = builder.build();

  AssertStatsSet(version, props, schema.Column(0), true);
  AssertStatsSet(version, props, schema.Column(1), false);
  AssertStatsSet(version, props, schema.Column(2), true);
  AssertStatsSet(version, props, schema.Column(3), false);
  AssertStatsSet(version, props, schema.Column(4), false);
  AssertStatsSet(version, props, schema.Column(5), false);
}

// Statistics for all types have no restrictions in newer parquet version
TEST(CorrectStatistics, Basics) {
  std::string created_by = "parquet-cpp version 1.3.0";
  ApplicationVersion version(created_by);
  SchemaDescriptor schema;
  schema::NodePtr node;
  std::vector<schema::NodePtr> fields;
  // Test Physical Types
  fields.push_back(schema::PrimitiveNode::Make("col1", Repetition::OPTIONAL, Type::INT32,
                                               ConvertedType::NONE));
  fields.push_back(schema::PrimitiveNode::Make("col2", Repetition::OPTIONAL,
                                               Type::BYTE_ARRAY, ConvertedType::NONE));
  // Test Logical Types
  fields.push_back(schema::PrimitiveNode::Make("col3", Repetition::OPTIONAL, Type::INT32,
                                               ConvertedType::DATE));
  fields.push_back(schema::PrimitiveNode::Make("col4", Repetition::OPTIONAL, Type::INT32,
                                               ConvertedType::UINT_32));
  fields.push_back(schema::PrimitiveNode::Make("col5", Repetition::OPTIONAL,
                                               Type::FIXED_LEN_BYTE_ARRAY,
                                               ConvertedType::INTERVAL, 12));
  fields.push_back(schema::PrimitiveNode::Make("col6", Repetition::OPTIONAL,
                                               Type::BYTE_ARRAY, ConvertedType::UTF8));
  node = schema::GroupNode::Make("schema", Repetition::REQUIRED, fields);
  schema.Init(node);

  parquet::WriterProperties::Builder builder;
  builder.created_by(created_by);
  std::shared_ptr<parquet::WriterProperties> props = builder.build();

  AssertStatsSet(version, props, schema.Column(0), true);
  AssertStatsSet(version, props, schema.Column(1), true);
  AssertStatsSet(version, props, schema.Column(2), true);
  AssertStatsSet(version, props, schema.Column(3), true);
  AssertStatsSet(version, props, schema.Column(4), false);
  AssertStatsSet(version, props, schema.Column(5), true);
}

// Test SortOrder class
static const int NUM_VALUES = 10;

template <typename TestType>
class TestStatisticsSortOrder : public ::testing::Test {
 public:
  typedef typename TestType::c_type T;

  void AddNodes(std::string name) {
    fields_.push_back(schema::PrimitiveNode::Make(
        name, Repetition::REQUIRED, TestType::type_num, ConvertedType::NONE));
  }

  void SetUpSchema() {
    stats_.resize(fields_.size());
    values_.resize(NUM_VALUES);
    schema_ = std::static_pointer_cast<GroupNode>(
        GroupNode::Make("Schema", Repetition::REQUIRED, fields_));

    parquet_sink_ = CreateOutputStream();
  }

  void SetValues();

  void WriteParquet() {
    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    builder.created_by("parquet-cpp version 1.3.0");
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    auto file_writer = parquet::ParquetFileWriter::Open(parquet_sink_, schema_, props);

    // Append a RowGroup with a specific number of rows.
    auto rg_writer = file_writer->AppendRowGroup();

    this->SetValues();

    // Insert Values
    for (int i = 0; i < static_cast<int>(fields_.size()); i++) {
      auto column_writer =
          static_cast<parquet::TypedColumnWriter<TestType>*>(rg_writer->NextColumn());
      column_writer->WriteBatch(NUM_VALUES, nullptr, nullptr, values_.data());
    }
  }

  void VerifyParquetStats() {
    std::shared_ptr<Buffer> pbuffer;
    ASSERT_OK(parquet_sink_->Finish(&pbuffer));

    // Create a ParquetReader instance
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
        parquet::ParquetFileReader::Open(
            std::make_shared<arrow::io::BufferReader>(pbuffer));

    // Get the File MetaData
    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
    std::shared_ptr<parquet::RowGroupMetaData> rg_metadata = file_metadata->RowGroup(0);
    for (int i = 0; i < static_cast<int>(fields_.size()); i++) {
      std::shared_ptr<parquet::ColumnChunkMetaData> cc_metadata =
          rg_metadata->ColumnChunk(i);
      ASSERT_EQ(stats_[i].min(), cc_metadata->statistics()->EncodeMin());
      ASSERT_EQ(stats_[i].max(), cc_metadata->statistics()->EncodeMax());
    }
  }

 protected:
  std::vector<T> values_;
  std::vector<uint8_t> values_buf_;
  std::vector<schema::NodePtr> fields_;
  std::shared_ptr<schema::GroupNode> schema_;
  std::shared_ptr<::arrow::io::BufferOutputStream> parquet_sink_;
  std::vector<EncodedStatistics> stats_;
};

using CompareTestTypes = ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType,
                                          ByteArrayType, FLBAType>;

// TYPE::INT32
template <>
void TestStatisticsSortOrder<Int32Type>::AddNodes(std::string name) {
  // UINT_32 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32,
                                                ConvertedType::UINT_32));
  // INT_32 logical type to set Signed Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32,
                                                ConvertedType::INT_32));
}

template <>
void TestStatisticsSortOrder<Int32Type>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i] = i - 5;  // {-5, -4, -3, -2, -1, 0, 1, 2, 3, 4};
  }

  // Write UINT32 min/max values
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[5]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[4]), sizeof(T)));

  // Write INT32 min/max values
  stats_[1]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[0]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[9]), sizeof(T)));
}

// TYPE::INT64
template <>
void TestStatisticsSortOrder<Int64Type>::AddNodes(std::string name) {
  // UINT_64 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT64,
                                                ConvertedType::UINT_64));
  // INT_64 logical type to set Signed Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT64,
                                                ConvertedType::INT_64));
}

template <>
void TestStatisticsSortOrder<Int64Type>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i] = i - 5;  // {-5, -4, -3, -2, -1, 0, 1, 2, 3, 4};
  }

  // Write UINT64 min/max values
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[5]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[4]), sizeof(T)));

  // Write INT64 min/max values
  stats_[1]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[0]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[9]), sizeof(T)));
}

// TYPE::FLOAT
template <>
void TestStatisticsSortOrder<FloatType>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i] = static_cast<float>(i) -
                 5;  // {-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0};
  }

  // Write Float min/max values
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[0]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[9]), sizeof(T)));
}

// TYPE::DOUBLE
template <>
void TestStatisticsSortOrder<DoubleType>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i] = static_cast<float>(i) -
                 5;  // {-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0};
  }

  // Write Double min/max values
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[0]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[9]), sizeof(T)));
}

// TYPE::ByteArray
template <>
void TestStatisticsSortOrder<ByteArrayType>::AddNodes(std::string name) {
  // UTF8 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED,
                                                Type::BYTE_ARRAY, ConvertedType::UTF8));
}

template <>
void TestStatisticsSortOrder<ByteArrayType>::SetValues() {
  int max_byte_array_len = 10;
  size_t nbytes = NUM_VALUES * max_byte_array_len;
  values_buf_.resize(nbytes);
  std::vector<std::string> vals = {u8"c123", u8"b123", u8"a123", u8"d123", u8"e123",
                                   u8"f123", u8"g123", u8"h123", u8"i123", u8"ü123"};

  uint8_t* base = &values_buf_.data()[0];
  for (int i = 0; i < NUM_VALUES; i++) {
    memcpy(base, vals[i].c_str(), vals[i].length());
    values_[i].ptr = base;
    values_[i].len = static_cast<uint32_t>(vals[i].length());
    base += vals[i].length();
  }

  // Write String min/max values
  stats_[0]
      .set_min(
          std::string(reinterpret_cast<const char*>(vals[2].c_str()), vals[2].length()))
      .set_max(
          std::string(reinterpret_cast<const char*>(vals[9].c_str()), vals[9].length()));
}

// TYPE::FLBAArray
template <>
void TestStatisticsSortOrder<FLBAType>::AddNodes(std::string name) {
  // FLBA has only Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED,
                                                Type::FIXED_LEN_BYTE_ARRAY,
                                                ConvertedType::NONE, FLBA_LENGTH));
}

template <>
void TestStatisticsSortOrder<FLBAType>::SetValues() {
  size_t nbytes = NUM_VALUES * FLBA_LENGTH;
  values_buf_.resize(nbytes);
  char vals[NUM_VALUES][FLBA_LENGTH] = {"b12345", "a12345", "c12345", "d12345", "e12345",
                                        "f12345", "g12345", "h12345", "z12345", "a12345"};

  uint8_t* base = &values_buf_.data()[0];
  for (int i = 0; i < NUM_VALUES; i++) {
    memcpy(base, &vals[i][0], FLBA_LENGTH);
    values_[i].ptr = base;
    base += FLBA_LENGTH;
  }

  // Write FLBA min,max values
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&vals[1][0]), FLBA_LENGTH))
      .set_max(std::string(reinterpret_cast<const char*>(&vals[8][0]), FLBA_LENGTH));
}

TYPED_TEST_CASE(TestStatisticsSortOrder, CompareTestTypes);

TYPED_TEST(TestStatisticsSortOrder, MinMax) {
  this->AddNodes("Column ");
  this->SetUpSchema();
  this->WriteParquet();
  ASSERT_NO_FATAL_FAILURE(this->VerifyParquetStats());
}

TEST(TestByteArrayStatisticsFromArrow, Basics) {
  // Part of ARROW-3246. Replicating TestStatisticsSortOrder test but via Arrow

  auto values = ArrayFromJSON(::arrow::utf8(),
                              u8"[\"c123\", \"b123\", \"a123\", null, "
                              "null, \"f123\", \"g123\", \"h123\", \"i123\", \"ü123\"]");

  const auto& typed_values = static_cast<const ::arrow::BinaryArray&>(*values);

  NodePtr node = PrimitiveNode::Make("field", Repetition::REQUIRED, Type::BYTE_ARRAY,
                                     ConvertedType::UTF8);
  ColumnDescriptor descr(node, 0, 0);
  auto stats = MakeStatistics<ByteArrayType>(&descr);
  ASSERT_NO_FATAL_FAILURE(stats->Update(*values));

  ASSERT_EQ(ByteArray(typed_values.GetView(2)), stats->min());
  ASSERT_EQ(ByteArray(typed_values.GetView(9)), stats->max());
}

// Ensure UNKNOWN sort order is handled properly
using TestStatisticsSortOrderFLBA = TestStatisticsSortOrder<FLBAType>;

TEST_F(TestStatisticsSortOrderFLBA, UnknownSortOrder) {
  this->fields_.push_back(schema::PrimitiveNode::Make(
      "Column 0", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
      ConvertedType::INTERVAL, FLBA_LENGTH));
  this->SetUpSchema();
  this->WriteParquet();

  std::shared_ptr<Buffer> pbuffer;
  PARQUET_THROW_NOT_OK(parquet_sink_->Finish(&pbuffer));

  // Create a ParquetReader instance
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::Open(
          std::make_shared<arrow::io::BufferReader>(pbuffer));
  // Get the File MetaData
  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
  std::shared_ptr<parquet::RowGroupMetaData> rg_metadata = file_metadata->RowGroup(0);
  std::shared_ptr<parquet::ColumnChunkMetaData> cc_metadata = rg_metadata->ColumnChunk(0);

  // stats should not be set for UNKNOWN sort order
  ASSERT_FALSE(cc_metadata->is_stats_set());
}

// PARQUET-1225: Float NaN values may lead to incorrect filtering under certain
// circumstances
TEST(TestStatisticsSortOrderFloatNaN, NaNValues) {
  constexpr int NUM_VALUES = 10;
  NodePtr node = PrimitiveNode::Make("nan_float", Repetition::OPTIONAL, Type::FLOAT);
  ColumnDescriptor descr(node, 1, 1);
  float values[NUM_VALUES] = {std::nanf(""), -4.0f, -3.0f, -2.0f, -1.0f,
                              std::nanf(""), 1.0f,  2.0f,  3.0f,  std::nanf("")};
  float nan_values[NUM_VALUES];
  for (int i = 0; i < NUM_VALUES; i++) {
    nan_values[i] = std::nanf("");
  }

  // Test values
  auto nan_stats = MakeStatistics<FloatType>(&descr);
  nan_stats->Update(&values[0], NUM_VALUES, 0);
  float min = nan_stats->min();
  float max = nan_stats->max();
  ASSERT_EQ(min, -4.0f);
  ASSERT_EQ(max, 3.0f);

  // Test all NaNs
  auto all_nan_stats = MakeStatistics<FloatType>(&descr);
  all_nan_stats->Update(&nan_values[0], NUM_VALUES, 0);
  min = all_nan_stats->min();
  max = all_nan_stats->max();
  ASSERT_TRUE(std::isnan(min));
  ASSERT_TRUE(std::isnan(max));

  // Test values followed by all NaNs
  nan_stats->Update(&nan_values[0], NUM_VALUES, 0);
  min = nan_stats->min();
  max = nan_stats->max();
  ASSERT_EQ(min, -4.0f);
  ASSERT_EQ(max, 3.0f);

  // Test all NaNs followed by values
  all_nan_stats->Update(&values[0], NUM_VALUES, 0);
  min = all_nan_stats->min();
  max = all_nan_stats->max();
  ASSERT_EQ(min, -4.0f);
  ASSERT_EQ(max, 3.0f);

  // Test values followed by all NaNs followed by values
  nan_stats->Update(&values[0], NUM_VALUES, 0);
  min = nan_stats->min();
  max = nan_stats->max();
  ASSERT_EQ(min, -4.0f);
  ASSERT_EQ(max, 3.0f);
}

// PARQUET-1225: Float NaN values may lead to incorrect filtering under certain
// circumstances
TEST(TestStatisticsSortOrderFloatNaN, NaNValuesSpaced) {
  constexpr int NUM_VALUES = 10;
  NodePtr node = PrimitiveNode::Make("nan_float", Repetition::OPTIONAL, Type::FLOAT);
  ColumnDescriptor descr(node, 1, 1);
  float values[NUM_VALUES] = {std::nanf(""), -4.0f, -3.0f, -2.0f, -1.0f,
                              std::nanf(""), 1.0f,  2.0f,  3.0f,  std::nanf("")};
  float nan_values[NUM_VALUES];
  for (int i = 0; i < NUM_VALUES; i++) {
    nan_values[i] = std::nanf("");
  }
  std::vector<uint8_t> valid_bits(BitUtil::BytesForBits(NUM_VALUES) + 1, 255);

  // Test values
  auto nan_stats = MakeStatistics<FloatType>(&descr);
  nan_stats->UpdateSpaced(&values[0], valid_bits.data(), 0, NUM_VALUES, 0);
  float min = nan_stats->min();
  float max = nan_stats->max();
  ASSERT_EQ(min, -4.0f);
  ASSERT_EQ(max, 3.0f);

  // Test all NaNs
  auto all_nan_stats = MakeStatistics<FloatType>(&descr);
  all_nan_stats->UpdateSpaced(&nan_values[0], valid_bits.data(), 0, NUM_VALUES, 0);
  min = all_nan_stats->min();
  max = all_nan_stats->max();
  ASSERT_TRUE(std::isnan(min));
  ASSERT_TRUE(std::isnan(max));

  // Test values followed by all NaNs
  nan_stats->UpdateSpaced(&nan_values[0], valid_bits.data(), 0, NUM_VALUES, 0);
  min = nan_stats->min();
  max = nan_stats->max();
  ASSERT_EQ(min, -4.0f);
  ASSERT_EQ(max, 3.0f);

  // Test all NaNs followed by values
  all_nan_stats->UpdateSpaced(&values[0], valid_bits.data(), 0, NUM_VALUES, 0);
  min = all_nan_stats->min();
  max = all_nan_stats->max();
  ASSERT_EQ(min, -4.0f);
  ASSERT_EQ(max, 3.0f);

  // Test values followed by all NaNs followed by values
  nan_stats->UpdateSpaced(&values[0], valid_bits.data(), 0, NUM_VALUES, 0);
  min = nan_stats->min();
  max = nan_stats->max();
  ASSERT_EQ(min, -4.0f);
  ASSERT_EQ(max, 3.0f);
}

// NaN double values may lead to incorrect filtering under certain circumstances
TEST(TestStatisticsSortOrderDoubleNaN, NaNValues) {
  constexpr int NUM_VALUES = 10;
  NodePtr node = PrimitiveNode::Make("nan_double", Repetition::OPTIONAL, Type::DOUBLE);
  ColumnDescriptor descr(node, 1, 1);

  auto nan_stats = MakeStatistics<DoubleType>(&descr);
  double values[NUM_VALUES] = {std::nan(""), std::nan(""), -3.0, -2.0, -1.0,
                               0.0,          1.0,          2.0,  3.0,  4.0};
  double* values_ptr = &values[0];
  nan_stats->Update(values_ptr, NUM_VALUES, 0);
  double min = nan_stats->min();
  double max = nan_stats->max();

  ASSERT_EQ(min, -3.0);
  ASSERT_EQ(max, 4.0);
}

// Test statistics for binary column with UNSIGNED sort order
TEST(TestStatisticsSortOrderMinMax, Unsigned) {
  std::string dir_string(test::get_data_dir());
  std::stringstream ss;
  ss << dir_string << "/binary.parquet";
  auto path = ss.str();

  // The file is generated by parquet-mr 1.10.0, the first version that
  // supports correct statistics for binary data (see PARQUET-1025). It
  // contains a single column of binary type. Data is just single byte values
  // from 0x00 to 0x0B.
  auto file_reader = ParquetFileReader::OpenFile(path);
  auto rg_reader = file_reader->RowGroup(0);
  auto metadata = rg_reader->metadata();
  auto column_schema = metadata->schema()->Column(0);
  ASSERT_EQ(SortOrder::UNSIGNED, column_schema->sort_order());

  auto column_chunk = metadata->ColumnChunk(0);
  ASSERT_TRUE(column_chunk->is_stats_set());

  std::shared_ptr<Statistics> stats = column_chunk->statistics();
  ASSERT_TRUE(stats != NULL);
  ASSERT_EQ(0, stats->null_count());
  ASSERT_EQ(12, stats->num_values());
  ASSERT_EQ(0x00, stats->EncodeMin()[0]);
  ASSERT_EQ(0x0b, stats->EncodeMax()[0]);
}
}  // namespace test
}  // namespace parquet
