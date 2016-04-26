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

#include "parquet/file/reader-internal.h"
#include "parquet/file/writer-internal.h"
#include "parquet/column/reader.h"
#include "parquet/column/writer.h"
#include "parquet/util/input.h"
#include "parquet/util/output.h"
#include "parquet/types.h"

namespace parquet {

using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

class TestPrimitiveWriter : public ::testing::Test {
 public:
  void SetUpSchemaRequired() {
    node_ = PrimitiveNode::Make("int64", Repetition::REQUIRED, Type::INT64);
    schema_ = std::make_shared<ColumnDescriptor>(node_, 0, 0);
  }

  void SetUpSchemaOptional() {
    node_ = PrimitiveNode::Make("int64", Repetition::OPTIONAL, Type::INT64);
    schema_ = std::make_shared<ColumnDescriptor>(node_, 1, 0);
  }

  void SetUpSchemaRepeated() {
    node_ = PrimitiveNode::Make("int64", Repetition::REPEATED, Type::INT64);
    schema_ = std::make_shared<ColumnDescriptor>(node_, 1, 1);
  }

  void SetUp() {
    values_out_.resize(100);
    definition_levels_out_.resize(100);
    repetition_levels_out_.resize(100);

    SetUpSchemaRequired();
  }

  std::unique_ptr<Int64Reader> BuildReader() {
    auto buffer = sink_->GetBuffer();
    std::unique_ptr<InMemoryInputStream> source(new InMemoryInputStream(buffer));
    std::unique_ptr<SerializedPageReader> page_reader(
        new SerializedPageReader(std::move(source), Compression::UNCOMPRESSED));
    return std::unique_ptr<Int64Reader>(
        new Int64Reader(schema_.get(), std::move(page_reader)));
  }

  std::unique_ptr<Int64Writer> BuildWriter(int64_t output_size = 100) {
    sink_.reset(new InMemoryOutputStream());
    std::unique_ptr<SerializedPageWriter> pager(
        new SerializedPageWriter(sink_.get(), Compression::UNCOMPRESSED, &metadata_));
    return std::unique_ptr<Int64Writer>(new Int64Writer(schema_.get(), std::move(pager),
          output_size));
  }

  void ReadColumn() {
    auto reader = BuildReader();
    reader->ReadBatch(values_out_.size(), definition_levels_out_.data(),
        repetition_levels_out_.data(), values_out_.data(), &values_read_);
  }

 protected:
  int64_t values_read_;

  // Output buffers
  std::vector<int64_t> values_out_;
  std::vector<int16_t> definition_levels_out_;
  std::vector<int16_t> repetition_levels_out_;

 private:
  NodePtr node_;
  format::ColumnChunk metadata_;
  std::shared_ptr<ColumnDescriptor> schema_;
  std::unique_ptr<InMemoryOutputStream> sink_;
};

TEST_F(TestPrimitiveWriter, RequiredNonRepeated) {
  std::vector<int64_t> values(100, 128);

  // Test case 1: required and non-repeated, so no definition or repetition levels
  std::unique_ptr<Int64Writer> writer = BuildWriter();
  writer->WriteBatch(values.size(), nullptr, nullptr, values.data());
  writer->Close();

  ReadColumn();
  ASSERT_EQ(100, values_read_);
  ASSERT_EQ(values, values_out_);
}

TEST_F(TestPrimitiveWriter, OptionalNonRepeated) {
  // Optional and non-repeated, with definition levels
  // but no repetition levels
  SetUpSchemaOptional();

  std::vector<int64_t> values(100, 128);
  std::vector<int16_t> definition_levels(100, 1);
  definition_levels[1] = 0;

  auto writer = BuildWriter();
  writer->WriteBatch(values.size(), definition_levels.data(), nullptr, values.data());
  writer->Close();

  ReadColumn();
  ASSERT_EQ(99, values_read_);
  values_out_.resize(99);
  values.resize(99);
  ASSERT_EQ(values, values_out_);
}

TEST_F(TestPrimitiveWriter, OptionalRepeated) {
  // Optional and repeated, so definition and repetition levels
  SetUpSchemaRepeated();

  std::vector<int64_t> values(100, 128);
  std::vector<int16_t> definition_levels(100, 1);
  definition_levels[1] = 0;
  std::vector<int16_t> repetition_levels(100, 0);

  auto writer = BuildWriter();
  writer->WriteBatch(values.size(), definition_levels.data(),
      repetition_levels.data(), values.data());
  writer->Close();

  ReadColumn();
  ASSERT_EQ(99, values_read_);
  values_out_.resize(99);
  values.resize(99);
  ASSERT_EQ(values, values_out_);
}

TEST_F(TestPrimitiveWriter, RequiredTooFewRows) {
  std::vector<int64_t> values(99, 128);

  auto writer = BuildWriter();
  writer->WriteBatch(values.size(), nullptr, nullptr, values.data());
  ASSERT_THROW(writer->Close(), ParquetException);
}

TEST_F(TestPrimitiveWriter, RequiredTooMany) {
  std::vector<int64_t> values(200, 128);

  auto writer = BuildWriter();
  ASSERT_THROW(writer->WriteBatch(values.size(), nullptr, nullptr, values.data()),
      ParquetException);
}

TEST_F(TestPrimitiveWriter, OptionalRepeatedTooFewRows) {
  // Optional and repeated, so definition and repetition levels
  SetUpSchemaRepeated();

  std::vector<int64_t> values(100, 128);
  std::vector<int16_t> definition_levels(100, 1);
  definition_levels[1] = 0;
  std::vector<int16_t> repetition_levels(100, 0);
  repetition_levels[3] = 1;

  auto writer = BuildWriter();
  writer->WriteBatch(values.size(), definition_levels.data(),
      repetition_levels.data(), values.data());
  ASSERT_THROW(writer->Close(), ParquetException);
}

TEST_F(TestPrimitiveWriter, RequiredNonRepeatedLargeChunk) {
  std::vector<int64_t> values(10000, 128);

  // Test case 1: required and non-repeated, so no definition or repetition levels
  std::unique_ptr<Int64Writer> writer = BuildWriter(10000);
  writer->WriteBatch(values.size(), nullptr, nullptr, values.data());
  writer->Close();

  // Just read the first 100 to ensure we could read it back in
  ReadColumn();
  ASSERT_EQ(100, values_read_);
  values.resize(100);
  ASSERT_EQ(values, values_out_);
}

} // namespace test
} // namespace parquet


