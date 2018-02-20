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

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/test-specialization.h"
#include "parquet/test-util.h"
#include "parquet/thrift.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"

using arrow::MemoryPool;
using arrow::default_memory_pool;

namespace parquet {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

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

    TypedStats statistics3(this->schema_.Column(0));
    std::vector<uint8_t> valid_bits(
        BitUtil::RoundUpNumBytes(static_cast<uint32_t>(this->values_.size())) + 1, 255);
    statistics3.UpdateSpaced(this->values_ptr_, valid_bits.data(), 0,
                             this->values_.size(), 0);
    std::string encoded_min_spaced = statistics3.EncodeMin();
    std::string encoded_max_spaced = statistics3.EncodeMax();

    ASSERT_EQ(encoded_min, statistics2.EncodeMin());
    ASSERT_EQ(encoded_max, statistics2.EncodeMax());
    ASSERT_EQ(statistics1.min(), statistics2.min());
    ASSERT_EQ(statistics1.max(), statistics2.max());
    ASSERT_EQ(encoded_min_spaced, statistics2.EncodeMin());
    ASSERT_EQ(encoded_max_spaced, statistics2.EncodeMax());
    ASSERT_EQ(statistics3.min(), statistics2.min());
    ASSERT_EQ(statistics3.max(), statistics2.max());
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
    statistics1.Update(this->values_ptr_, this->values_.size() - num_null[0],
                       num_null[0]);

    TypedStats statistics2(this->schema_.Column(0));
    this->GenerateData(1000);
    statistics2.Update(this->values_ptr_, this->values_.size() - num_null[1],
                       num_null[1]);

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

    auto buffer = sink->GetBuffer();
    auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
    auto file_reader = ParquetFileReader::Open(source);
    auto rg_reader = file_reader->RowGroup(0);
    auto column_chunk = rg_reader->metadata()->ColumnChunk(0);
    if (!column_chunk->is_stats_set()) return;
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
std::vector<ByteArray> TestRowGroupStatistics<ByteArrayType>::GetDeepCopy(
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
void TestRowGroupStatistics<TestType>::DeepFree(
    std::vector<typename TestType::c_type>& values) {}

template <>
void TestRowGroupStatistics<FLBAType>::DeepFree(std::vector<FLBA>& values) {
  MemoryPool* pool = default_memory_pool();
  for (FLBA& flba : values) {
    auto ptr = const_cast<uint8_t*>(flba.ptr);
    memset(ptr, 0, FLBA_LENGTH);
    pool->Free(ptr, FLBA_LENGTH);
  }
}

template <>
void TestRowGroupStatistics<ByteArrayType>::DeepFree(std::vector<ByteArray>& values) {
  MemoryPool* pool = default_memory_pool();
  for (ByteArray& ba : values) {
    auto ptr = const_cast<uint8_t*>(ba.ptr);
    memset(ptr, 0, ba.len);
    pool->Free(ptr, ba.len);
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
                                                     encoded_max, this->values_.size(), 0,
                                                     0, true);

  ASSERT_EQ(encoded_min, statistics2.EncodeMin());
  ASSERT_EQ(encoded_max, statistics2.EncodeMax());
  ASSERT_EQ(statistics1.min(), statistics2.min());
  ASSERT_EQ(statistics1.max(), statistics2.max());
}

using TestTypes = ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType,
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

// Statistics are restricted for few types in older parquet version
TEST(CorruptStatistics, Basics) {
  ApplicationVersion version("parquet-mr version 1.8.0");
  SchemaDescriptor schema;
  schema::NodePtr node;
  std::vector<schema::NodePtr> fields;
  // Test Physical Types
  fields.push_back(schema::PrimitiveNode::Make("col1", Repetition::OPTIONAL, Type::INT32,
                                               LogicalType::NONE));
  fields.push_back(schema::PrimitiveNode::Make("col2", Repetition::OPTIONAL,
                                               Type::BYTE_ARRAY, LogicalType::NONE));
  // Test Logical Types
  fields.push_back(schema::PrimitiveNode::Make("col3", Repetition::OPTIONAL, Type::INT32,
                                               LogicalType::DATE));
  fields.push_back(schema::PrimitiveNode::Make("col4", Repetition::OPTIONAL, Type::INT32,
                                               LogicalType::UINT_32));
  fields.push_back(schema::PrimitiveNode::Make("col5", Repetition::OPTIONAL,
                                               Type::FIXED_LEN_BYTE_ARRAY,
                                               LogicalType::INTERVAL, 12));
  fields.push_back(schema::PrimitiveNode::Make("col6", Repetition::OPTIONAL,
                                               Type::BYTE_ARRAY, LogicalType::UTF8));
  node = schema::GroupNode::Make("schema", Repetition::REQUIRED, fields);
  schema.Init(node);

  format::ColumnChunk col_chunk;
  col_chunk.meta_data.__isset.statistics = true;
  auto column_chunk1 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(0), &version);
  ASSERT_TRUE(column_chunk1->is_stats_set());
  auto column_chunk2 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(1), &version);
  ASSERT_FALSE(column_chunk2->is_stats_set());
  auto column_chunk3 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(2), &version);
  ASSERT_TRUE(column_chunk3->is_stats_set());
  auto column_chunk4 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(3), &version);
  ASSERT_FALSE(column_chunk4->is_stats_set());
  auto column_chunk5 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(4), &version);
  ASSERT_FALSE(column_chunk5->is_stats_set());
  auto column_chunk6 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(5), &version);
  ASSERT_FALSE(column_chunk6->is_stats_set());
}

// Statistics for all types have no restrictions in newer parquet version
TEST(CorrectStatistics, Basics) {
  ApplicationVersion version("parquet-cpp version 1.3.0");
  SchemaDescriptor schema;
  schema::NodePtr node;
  std::vector<schema::NodePtr> fields;
  // Test Physical Types
  fields.push_back(schema::PrimitiveNode::Make("col1", Repetition::OPTIONAL, Type::INT32,
                                               LogicalType::NONE));
  fields.push_back(schema::PrimitiveNode::Make("col2", Repetition::OPTIONAL,
                                               Type::BYTE_ARRAY, LogicalType::NONE));
  // Test Logical Types
  fields.push_back(schema::PrimitiveNode::Make("col3", Repetition::OPTIONAL, Type::INT32,
                                               LogicalType::DATE));
  fields.push_back(schema::PrimitiveNode::Make("col4", Repetition::OPTIONAL, Type::INT32,
                                               LogicalType::UINT_32));
  fields.push_back(schema::PrimitiveNode::Make("col5", Repetition::OPTIONAL,
                                               Type::FIXED_LEN_BYTE_ARRAY,
                                               LogicalType::INTERVAL, 12));
  fields.push_back(schema::PrimitiveNode::Make("col6", Repetition::OPTIONAL,
                                               Type::BYTE_ARRAY, LogicalType::UTF8));
  node = schema::GroupNode::Make("schema", Repetition::REQUIRED, fields);
  schema.Init(node);

  format::ColumnChunk col_chunk;
  col_chunk.meta_data.__isset.statistics = true;
  auto column_chunk1 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(0), &version);
  ASSERT_TRUE(column_chunk1->is_stats_set());
  auto column_chunk2 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(1), &version);
  ASSERT_TRUE(column_chunk2->is_stats_set());
  auto column_chunk3 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(2), &version);
  ASSERT_TRUE(column_chunk3->is_stats_set());
  auto column_chunk4 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(3), &version);
  ASSERT_TRUE(column_chunk4->is_stats_set());
  auto column_chunk5 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(4), &version);
  ASSERT_FALSE(column_chunk5->is_stats_set());
  auto column_chunk6 = ColumnChunkMetaData::Make(
      reinterpret_cast<const uint8_t*>(&col_chunk), schema.Column(5), &version);
  ASSERT_TRUE(column_chunk6->is_stats_set());
}

// Test SortOrder class
static const int NUM_VALUES = 10;

template <typename TestType>
class TestStatistics : public ::testing::Test {
 public:
  typedef typename TestType::c_type T;

  void AddNodes(std::string name) {
    fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED,
                                                  TestType::type_num, LogicalType::NONE));
  }

  void SetUpSchema() {
    stats_.resize(fields_.size());
    values_.resize(NUM_VALUES);
    schema_ = std::static_pointer_cast<GroupNode>(
        GroupNode::Make("Schema", Repetition::REQUIRED, fields_));

    parquet_sink_ = std::make_shared<InMemoryOutputStream>();
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
    auto pbuffer = parquet_sink_->GetBuffer();

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
  std::shared_ptr<InMemoryOutputStream> parquet_sink_;
  std::vector<EncodedStatistics> stats_;
};

using CompareTestTypes = ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType,
                                          ByteArrayType, FLBAType>;

// TYPE::INT32
template <>
void TestStatistics<Int32Type>::AddNodes(std::string name) {
  // UINT_32 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32,
                                                LogicalType::UINT_32));
  // INT_32 logical type to set Signed Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32,
                                                LogicalType::INT_32));
}

template <>
void TestStatistics<Int32Type>::SetValues() {
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
void TestStatistics<Int64Type>::AddNodes(std::string name) {
  // UINT_64 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT64,
                                                LogicalType::UINT_64));
  // INT_64 logical type to set Signed Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT64,
                                                LogicalType::INT_64));
}

template <>
void TestStatistics<Int64Type>::SetValues() {
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
void TestStatistics<FloatType>::SetValues() {
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
void TestStatistics<DoubleType>::SetValues() {
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
void TestStatistics<ByteArrayType>::AddNodes(std::string name) {
  // UTF8 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED,
                                                Type::BYTE_ARRAY, LogicalType::UTF8));
}

template <>
void TestStatistics<ByteArrayType>::SetValues() {
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
void TestStatistics<FLBAType>::AddNodes(std::string name) {
  // FLBA has only Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED,
                                                Type::FIXED_LEN_BYTE_ARRAY,
                                                LogicalType::NONE, FLBA_LENGTH));
}

template <>
void TestStatistics<FLBAType>::SetValues() {
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

TYPED_TEST_CASE(TestStatistics, CompareTestTypes);

TYPED_TEST(TestStatistics, MinMax) {
  this->AddNodes("Column ");
  this->SetUpSchema();
  this->WriteParquet();
  this->VerifyParquetStats();
}

// Ensure UNKNOWN sort order is handled properly
using TestStatisticsFLBA = TestStatistics<FLBAType>;

TEST_F(TestStatisticsFLBA, UnknownSortOrder) {
  this->fields_.push_back(schema::PrimitiveNode::Make(
      "Column 0", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL,
      FLBA_LENGTH));
  this->SetUpSchema();
  this->WriteParquet();

  auto pbuffer = parquet_sink_->GetBuffer();
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

}  // namespace test
}  // namespace parquet
