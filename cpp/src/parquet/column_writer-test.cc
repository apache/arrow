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

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file/reader-internal.h"
#include "parquet/file/writer-internal.h"
#include "parquet/test-specialization.h"
#include "parquet/test-util.h"
#include "parquet/types.h"
#include "parquet/util/comparison.h"
#include "parquet/util/memory.h"

namespace parquet {

using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

// The default size used in most tests.
const int SMALL_SIZE = 100;
// Larger size to test some corner cases, only used in some specific cases.
const int LARGE_SIZE = 100000;
// Very large size to test dictionary fallback.
const int VERY_LARGE_SIZE = 400000;

template <typename TestType>
class TestPrimitiveWriter : public PrimitiveTypedTest<TestType> {
 public:
  typedef typename TestType::c_type T;

  void SetUp() {
    this->SetupValuesOut(SMALL_SIZE);
    writer_properties_ = default_writer_properties();
    definition_levels_out_.resize(SMALL_SIZE);
    repetition_levels_out_.resize(SMALL_SIZE);

    this->SetUpSchema(Repetition::REQUIRED);

    descr_ = this->schema_.Column(0);
  }

  Type::type type_num() { return TestType::type_num; }

  void BuildReader(int64_t num_rows,
                   Compression::type compression = Compression::UNCOMPRESSED) {
    auto buffer = sink_->GetBuffer();
    std::unique_ptr<InMemoryInputStream> source(new InMemoryInputStream(buffer));
    std::unique_ptr<SerializedPageReader> page_reader(
        new SerializedPageReader(std::move(source), num_rows, compression));
    reader_.reset(new TypedColumnReader<TestType>(this->descr_, std::move(page_reader)));
  }

  std::shared_ptr<TypedColumnWriter<TestType>> BuildWriter(
      int64_t output_size = SMALL_SIZE,
      const ColumnProperties& column_properties = ColumnProperties()) {
    sink_.reset(new InMemoryOutputStream());
    metadata_ = ColumnChunkMetaDataBuilder::Make(
        writer_properties_, this->descr_, reinterpret_cast<uint8_t*>(&thrift_metadata_));
    std::unique_ptr<SerializedPageWriter> pager(
        new SerializedPageWriter(sink_.get(), column_properties.codec, metadata_.get()));
    WriterProperties::Builder wp_builder;
    if (column_properties.encoding == Encoding::PLAIN_DICTIONARY ||
        column_properties.encoding == Encoding::RLE_DICTIONARY) {
      wp_builder.enable_dictionary();
    } else {
      wp_builder.disable_dictionary();
      wp_builder.encoding(column_properties.encoding);
    }
    writer_properties_ = wp_builder.build();
    std::shared_ptr<ColumnWriter> writer =
        ColumnWriter::Make(metadata_.get(), std::move(pager), writer_properties_.get());
    return std::static_pointer_cast<TypedColumnWriter<TestType>>(writer);
  }

  void ReadColumn(Compression::type compression = Compression::UNCOMPRESSED) {
    BuildReader(static_cast<int64_t>(this->values_out_.size()), compression);
    reader_->ReadBatch(static_cast<int>(this->values_out_.size()),
                       definition_levels_out_.data(), repetition_levels_out_.data(),
                       this->values_out_ptr_, &values_read_);
    this->SyncValuesOut();
  }

  void ReadColumnFully(Compression::type compression = Compression::UNCOMPRESSED);

  void TestRequiredWithEncoding(Encoding::type encoding) {
    return TestRequiredWithSettings(encoding, Compression::UNCOMPRESSED, false, false);
  }

  void TestRequiredWithSettings(Encoding::type encoding, Compression::type compression,
                                bool enable_dictionary, bool enable_statistics,
                                int64_t num_rows = SMALL_SIZE) {
    this->GenerateData(num_rows);

    this->WriteRequiredWithSettings(encoding, compression, enable_dictionary,
                                    enable_statistics, num_rows);
    this->ReadAndCompare(compression, num_rows);

    this->WriteRequiredWithSettingsSpaced(encoding, compression, enable_dictionary,
                                          enable_statistics, num_rows);
    this->ReadAndCompare(compression, num_rows);
  }

  void WriteRequiredWithSettings(Encoding::type encoding, Compression::type compression,
                                 bool enable_dictionary, bool enable_statistics,
                                 int64_t num_rows) {
    ColumnProperties column_properties(encoding, compression, enable_dictionary,
                                       enable_statistics);
    std::shared_ptr<TypedColumnWriter<TestType>> writer =
        this->BuildWriter(num_rows, column_properties);
    writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
    // The behaviour should be independent from the number of Close() calls
    writer->Close();
    writer->Close();
  }

  void WriteRequiredWithSettingsSpaced(Encoding::type encoding,
                                       Compression::type compression,
                                       bool enable_dictionary, bool enable_statistics,
                                       int64_t num_rows) {
    std::vector<uint8_t> valid_bits(
        BitUtil::RoundUpNumBytes(static_cast<uint32_t>(this->values_.size())) + 1, 255);
    ColumnProperties column_properties(encoding, compression, enable_dictionary,
                                       enable_statistics);
    std::shared_ptr<TypedColumnWriter<TestType>> writer =
        this->BuildWriter(num_rows, column_properties);
    writer->WriteBatchSpaced(this->values_.size(), nullptr, nullptr, valid_bits.data(), 0,
                             this->values_ptr_);
    // The behaviour should be independent from the number of Close() calls
    writer->Close();
    writer->Close();
  }

  void ReadAndCompare(Compression::type compression, int64_t num_rows) {
    this->SetupValuesOut(num_rows);
    this->ReadColumnFully(compression);
    std::shared_ptr<CompareDefault<TestType>> compare;
    compare = std::static_pointer_cast<CompareDefault<TestType>>(
        Comparator::Make(this->descr_));
    for (size_t i = 0; i < this->values_.size(); i++) {
      if ((*compare)(this->values_[i], this->values_out_[i]) ||
          (*compare)(this->values_out_[i], this->values_[i])) {
        std::cout << "Failed at " << i << std::endl;
      }
      ASSERT_FALSE((*compare)(this->values_[i], this->values_out_[i]));
      ASSERT_FALSE((*compare)(this->values_out_[i], this->values_[i]));
    }
    ASSERT_EQ(this->values_, this->values_out_);
  }

  int64_t metadata_num_values() {
    // Metadata accessor must be created lazily.
    // This is because the ColumnChunkMetaData semantics dictate the metadata object is
    // complete (no changes to the metadata buffer can be made after instantiation)
    auto metadata_accessor = ColumnChunkMetaData::Make(
        reinterpret_cast<const uint8_t*>(&thrift_metadata_), this->descr_);
    return metadata_accessor->num_values();
  }

  std::vector<Encoding::type> metadata_encodings() {
    // Metadata accessor must be created lazily.
    // This is because the ColumnChunkMetaData semantics dictate the metadata object is
    // complete (no changes to the metadata buffer can be made after instantiation)
    auto metadata_accessor = ColumnChunkMetaData::Make(
        reinterpret_cast<const uint8_t*>(&thrift_metadata_), this->descr_);
    return metadata_accessor->encodings();
  }

 protected:
  int64_t values_read_;
  // Keep the reader alive as for ByteArray the lifetime of the ByteArray
  // content is bound to the reader.
  std::unique_ptr<TypedColumnReader<TestType>> reader_;

  std::vector<int16_t> definition_levels_out_;
  std::vector<int16_t> repetition_levels_out_;

  const ColumnDescriptor* descr_;

 private:
  format::ColumnChunk thrift_metadata_;
  std::unique_ptr<ColumnChunkMetaDataBuilder> metadata_;
  std::unique_ptr<InMemoryOutputStream> sink_;
  std::shared_ptr<WriterProperties> writer_properties_;
  std::vector<std::vector<uint8_t>> data_buffer_;
};

template <typename TestType>
void TestPrimitiveWriter<TestType>::ReadColumnFully(Compression::type compression) {
  int64_t total_values = static_cast<int64_t>(this->values_out_.size());
  BuildReader(total_values, compression);
  values_read_ = 0;
  while (values_read_ < total_values) {
    int64_t values_read_recently = 0;
    reader_->ReadBatch(
        static_cast<int>(this->values_out_.size()) - static_cast<int>(values_read_),
        definition_levels_out_.data() + values_read_,
        repetition_levels_out_.data() + values_read_,
        this->values_out_ptr_ + values_read_, &values_read_recently);
    values_read_ += values_read_recently;
  }
  this->SyncValuesOut();
}

template <>
void TestPrimitiveWriter<FLBAType>::ReadColumnFully(Compression::type compression) {
  int64_t total_values = static_cast<int64_t>(this->values_out_.size());
  BuildReader(total_values, compression);
  this->data_buffer_.clear();

  values_read_ = 0;
  while (values_read_ < total_values) {
    int64_t values_read_recently = 0;
    reader_->ReadBatch(
        static_cast<int>(this->values_out_.size()) - static_cast<int>(values_read_),
        definition_levels_out_.data() + values_read_,
        repetition_levels_out_.data() + values_read_,
        this->values_out_ptr_ + values_read_, &values_read_recently);

    // Copy contents of the pointers
    std::vector<uint8_t> data(values_read_recently * this->descr_->type_length());
    uint8_t* data_ptr = data.data();
    for (int64_t i = 0; i < values_read_recently; i++) {
      memcpy(data_ptr + this->descr_->type_length() * i,
             this->values_out_[i + values_read_].ptr, this->descr_->type_length());
      this->values_out_[i + values_read_].ptr =
          data_ptr + this->descr_->type_length() * i;
    }
    data_buffer_.emplace_back(std::move(data));

    values_read_ += values_read_recently;
  }
  this->SyncValuesOut();
}

typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
                         BooleanType, ByteArrayType, FLBAType>
    TestTypes;

TYPED_TEST_CASE(TestPrimitiveWriter, TestTypes);

using TestNullValuesWriter = TestPrimitiveWriter<Int32Type>;

TYPED_TEST(TestPrimitiveWriter, RequiredPlain) {
  this->TestRequiredWithEncoding(Encoding::PLAIN);
}

TYPED_TEST(TestPrimitiveWriter, RequiredDictionary) {
  this->TestRequiredWithEncoding(Encoding::PLAIN_DICTIONARY);
}

/*
TYPED_TEST(TestPrimitiveWriter, RequiredRLE) {
  this->TestRequiredWithEncoding(Encoding::RLE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredBitPacked) {
  this->TestRequiredWithEncoding(Encoding::BIT_PACKED);
}

TYPED_TEST(TestPrimitiveWriter, RequiredDeltaBinaryPacked) {
  this->TestRequiredWithEncoding(Encoding::DELTA_BINARY_PACKED);
}

TYPED_TEST(TestPrimitiveWriter, RequiredDeltaLengthByteArray) {
  this->TestRequiredWithEncoding(Encoding::DELTA_LENGTH_BYTE_ARRAY);
}

TYPED_TEST(TestPrimitiveWriter, RequiredDeltaByteArray) {
  this->TestRequiredWithEncoding(Encoding::DELTA_BYTE_ARRAY);
}

TYPED_TEST(TestPrimitiveWriter, RequiredRLEDictionary) {
  this->TestRequiredWithEncoding(Encoding::RLE_DICTIONARY);
}
*/

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithSnappyCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::SNAPPY, false, false,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithBrotliCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::BROTLI, false, false,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithGzipCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::GZIP, false, false,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStats) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::UNCOMPRESSED, false, true,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStatsAndSnappyCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::SNAPPY, false, true,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStatsAndBrotliCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::BROTLI, false, true,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStatsAndGzipCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::GZIP, false, true,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, Optional) {
  // Optional and non-repeated, with definition levels
  // but no repetition levels
  this->SetUpSchema(Repetition::OPTIONAL);

  this->GenerateData(SMALL_SIZE);
  std::vector<int16_t> definition_levels(SMALL_SIZE, 1);
  definition_levels[1] = 0;

  auto writer = this->BuildWriter();
  writer->WriteBatch(this->values_.size(), definition_levels.data(), nullptr,
                     this->values_ptr_);
  writer->Close();

  // PARQUET-703
  ASSERT_EQ(100, this->metadata_num_values());

  this->ReadColumn();
  ASSERT_EQ(99, this->values_read_);
  this->values_out_.resize(99);
  this->values_.resize(99);
  ASSERT_EQ(this->values_, this->values_out_);
}

TYPED_TEST(TestPrimitiveWriter, OptionalSpaced) {
  // Optional and non-repeated, with definition levels
  // but no repetition levels
  this->SetUpSchema(Repetition::OPTIONAL);

  this->GenerateData(SMALL_SIZE);
  std::vector<int16_t> definition_levels(SMALL_SIZE, 1);
  std::vector<uint8_t> valid_bits(::arrow::BitUtil::BytesForBits(SMALL_SIZE), 255);

  definition_levels[SMALL_SIZE - 1] = 0;
  ::arrow::BitUtil::ClearBit(valid_bits.data(), SMALL_SIZE - 1);
  definition_levels[1] = 0;
  ::arrow::BitUtil::ClearBit(valid_bits.data(), 1);

  auto writer = this->BuildWriter();
  writer->WriteBatchSpaced(this->values_.size(), definition_levels.data(), nullptr,
                           valid_bits.data(), 0, this->values_ptr_);
  writer->Close();

  // PARQUET-703
  ASSERT_EQ(100, this->metadata_num_values());

  this->ReadColumn();
  ASSERT_EQ(98, this->values_read_);
  this->values_out_.resize(98);
  this->values_.resize(99);
  this->values_.erase(this->values_.begin() + 1);
  ASSERT_EQ(this->values_, this->values_out_);
}

TYPED_TEST(TestPrimitiveWriter, Repeated) {
  // Optional and repeated, so definition and repetition levels
  this->SetUpSchema(Repetition::REPEATED);

  this->GenerateData(SMALL_SIZE);
  std::vector<int16_t> definition_levels(SMALL_SIZE, 1);
  definition_levels[1] = 0;
  std::vector<int16_t> repetition_levels(SMALL_SIZE, 0);

  auto writer = this->BuildWriter();
  writer->WriteBatch(this->values_.size(), definition_levels.data(),
                     repetition_levels.data(), this->values_ptr_);
  writer->Close();

  this->ReadColumn();
  ASSERT_EQ(SMALL_SIZE - 1, this->values_read_);
  this->values_out_.resize(SMALL_SIZE - 1);
  this->values_.resize(SMALL_SIZE - 1);
  ASSERT_EQ(this->values_, this->values_out_);
}

TYPED_TEST(TestPrimitiveWriter, RequiredLargeChunk) {
  this->GenerateData(LARGE_SIZE);

  // Test case 1: required and non-repeated, so no definition or repetition levels
  auto writer = this->BuildWriter(LARGE_SIZE);
  writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
  writer->Close();

  // Just read the first SMALL_SIZE rows to ensure we could read it back in
  this->ReadColumn();
  ASSERT_EQ(SMALL_SIZE, this->values_read_);
  this->values_.resize(SMALL_SIZE);
  ASSERT_EQ(this->values_, this->values_out_);
}

// Test case for dictionary fallback encoding
TYPED_TEST(TestPrimitiveWriter, RequiredVeryLargeChunk) {
  this->GenerateData(VERY_LARGE_SIZE);

  auto writer = this->BuildWriter(VERY_LARGE_SIZE, Encoding::PLAIN_DICTIONARY);
  writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
  writer->Close();

  // Read all rows so we are sure that also the non-dictionary pages are read correctly
  this->SetupValuesOut(VERY_LARGE_SIZE);
  this->ReadColumnFully();
  ASSERT_EQ(VERY_LARGE_SIZE, this->values_read_);
  this->values_.resize(VERY_LARGE_SIZE);
  ASSERT_EQ(this->values_, this->values_out_);
  std::vector<Encoding::type> encodings = this->metadata_encodings();
  // There are 3 encodings (RLE, PLAIN_DICTIONARY, PLAIN) in a fallback case
  // Dictionary encoding is not allowed for boolean type
  // There are 2 encodings (RLE, PLAIN) in a non dictionary encoding case
  if (this->type_num() != Type::BOOLEAN) {
    ASSERT_EQ(Encoding::PLAIN_DICTIONARY, encodings[0]);
    ASSERT_EQ(Encoding::PLAIN, encodings[1]);
    ASSERT_EQ(Encoding::RLE, encodings[2]);
  } else {
    ASSERT_EQ(Encoding::PLAIN, encodings[0]);
    ASSERT_EQ(Encoding::RLE, encodings[1]);
  }
}

// PARQUET-719
// Test case for NULL values
TEST_F(TestNullValuesWriter, OptionalNullValueChunk) {
  this->SetUpSchema(Repetition::OPTIONAL);

  this->GenerateData(LARGE_SIZE);

  std::vector<int16_t> definition_levels(LARGE_SIZE, 0);
  std::vector<int16_t> repetition_levels(LARGE_SIZE, 0);

  auto writer = this->BuildWriter(LARGE_SIZE);
  // All values being written are NULL
  writer->WriteBatch(this->values_.size(), definition_levels.data(),
                     repetition_levels.data(), nullptr);
  writer->Close();

  // Just read the first SMALL_SIZE rows to ensure we could read it back in
  this->ReadColumn();
  ASSERT_EQ(0, this->values_read_);
}

// PARQUET-764
// Correct bitpacking for boolean write at non-byte boundaries
using TestBooleanValuesWriter = TestPrimitiveWriter<BooleanType>;
TEST_F(TestBooleanValuesWriter, AlternateBooleanValues) {
  this->SetUpSchema(Repetition::REQUIRED);
  auto writer = this->BuildWriter();
  for (int i = 0; i < SMALL_SIZE; i++) {
    bool value = (i % 2 == 0) ? true : false;
    writer->WriteBatch(1, nullptr, nullptr, &value);
  }
  writer->Close();
  this->ReadColumn();
  for (int i = 0; i < SMALL_SIZE; i++) {
    ASSERT_EQ((i % 2 == 0) ? true : false, this->values_out_[i]) << i;
  }
}

void GenerateLevels(int min_repeat_factor, int max_repeat_factor, int max_level,
                    std::vector<int16_t>& input_levels) {
  // for each repetition count upto max_repeat_factor
  for (int repeat = min_repeat_factor; repeat <= max_repeat_factor; repeat++) {
    // repeat count increases by a factor of 2 for every iteration
    int repeat_count = (1 << repeat);
    // generate levels for repetition count upto the maximum level
    int value = 0;
    int bwidth = 0;
    while (value <= max_level) {
      for (int i = 0; i < repeat_count; i++) {
        input_levels.push_back(value);
      }
      value = (2 << bwidth) - 1;
      bwidth++;
    }
  }
}

void EncodeLevels(Encoding::type encoding, int max_level, int num_levels,
                  const int16_t* input_levels, std::vector<uint8_t>& bytes) {
  LevelEncoder encoder;
  int levels_count = 0;
  bytes.resize(2 * num_levels);
  ASSERT_EQ(2 * num_levels, static_cast<int>(bytes.size()));
  // encode levels
  if (encoding == Encoding::RLE) {
    // leave space to write the rle length value
    encoder.Init(encoding, max_level, num_levels, bytes.data() + sizeof(int32_t),
                 static_cast<int>(bytes.size()));

    levels_count = encoder.Encode(num_levels, input_levels);
    (reinterpret_cast<int32_t*>(bytes.data()))[0] = encoder.len();
  } else {
    encoder.Init(encoding, max_level, num_levels, bytes.data(),
                 static_cast<int>(bytes.size()));
    levels_count = encoder.Encode(num_levels, input_levels);
  }
  ASSERT_EQ(num_levels, levels_count);
}

void VerifyDecodingLevels(Encoding::type encoding, int max_level,
                          std::vector<int16_t>& input_levels,
                          std::vector<uint8_t>& bytes) {
  LevelDecoder decoder;
  int levels_count = 0;
  std::vector<int16_t> output_levels;
  int num_levels = static_cast<int>(input_levels.size());

  output_levels.resize(num_levels);
  ASSERT_EQ(num_levels, static_cast<int>(output_levels.size()));

  // Decode levels and test with multiple decode calls
  decoder.SetData(encoding, max_level, num_levels, bytes.data());
  int decode_count = 4;
  int num_inner_levels = num_levels / decode_count;
  // Try multiple decoding on a single SetData call
  for (int ct = 0; ct < decode_count; ct++) {
    int offset = ct * num_inner_levels;
    levels_count = decoder.Decode(num_inner_levels, output_levels.data());
    ASSERT_EQ(num_inner_levels, levels_count);
    for (int i = 0; i < num_inner_levels; i++) {
      EXPECT_EQ(input_levels[i + offset], output_levels[i]);
    }
  }
  // check the remaining levels
  int num_levels_completed = decode_count * (num_levels / decode_count);
  int num_remaining_levels = num_levels - num_levels_completed;
  if (num_remaining_levels > 0) {
    levels_count = decoder.Decode(num_remaining_levels, output_levels.data());
    ASSERT_EQ(num_remaining_levels, levels_count);
    for (int i = 0; i < num_remaining_levels; i++) {
      EXPECT_EQ(input_levels[i + num_levels_completed], output_levels[i]);
    }
  }
  // Test zero Decode values
  ASSERT_EQ(0, decoder.Decode(1, output_levels.data()));
}

void VerifyDecodingMultipleSetData(Encoding::type encoding, int max_level,
                                   std::vector<int16_t>& input_levels,
                                   std::vector<std::vector<uint8_t>>& bytes) {
  LevelDecoder decoder;
  int levels_count = 0;
  std::vector<int16_t> output_levels;

  // Decode levels and test with multiple SetData calls
  int setdata_count = static_cast<int>(bytes.size());
  int num_levels = static_cast<int>(input_levels.size()) / setdata_count;
  output_levels.resize(num_levels);
  // Try multiple SetData
  for (int ct = 0; ct < setdata_count; ct++) {
    int offset = ct * num_levels;
    ASSERT_EQ(num_levels, static_cast<int>(output_levels.size()));
    decoder.SetData(encoding, max_level, num_levels, bytes[ct].data());
    levels_count = decoder.Decode(num_levels, output_levels.data());
    ASSERT_EQ(num_levels, levels_count);
    for (int i = 0; i < num_levels; i++) {
      EXPECT_EQ(input_levels[i + offset], output_levels[i]);
    }
  }
}

// Test levels with maximum bit-width from 1 to 8
// increase the repetition count for each iteration by a factor of 2
TEST(TestLevels, TestLevelsDecodeMultipleBitWidth) {
  int min_repeat_factor = 0;
  int max_repeat_factor = 7;  // 128
  int max_bit_width = 8;
  std::vector<int16_t> input_levels;
  std::vector<uint8_t> bytes;
  Encoding::type encodings[2] = {Encoding::RLE, Encoding::BIT_PACKED};

  // for each encoding
  for (int encode = 0; encode < 2; encode++) {
    Encoding::type encoding = encodings[encode];
    // BIT_PACKED requires a sequence of atleast 8
    if (encoding == Encoding::BIT_PACKED) min_repeat_factor = 3;
    // for each maximum bit-width
    for (int bit_width = 1; bit_width <= max_bit_width; bit_width++) {
      // find the maximum level for the current bit_width
      int max_level = (1 << bit_width) - 1;
      // Generate levels
      GenerateLevels(min_repeat_factor, max_repeat_factor, max_level, input_levels);
      EncodeLevels(encoding, max_level, static_cast<int>(input_levels.size()),
                   input_levels.data(), bytes);
      VerifyDecodingLevels(encoding, max_level, input_levels, bytes);
      input_levels.clear();
    }
  }
}

// Test multiple decoder SetData calls
TEST(TestLevels, TestLevelsDecodeMultipleSetData) {
  int min_repeat_factor = 3;
  int max_repeat_factor = 7;  // 128
  int bit_width = 8;
  int max_level = (1 << bit_width) - 1;
  std::vector<int16_t> input_levels;
  std::vector<std::vector<uint8_t>> bytes;
  Encoding::type encodings[2] = {Encoding::RLE, Encoding::BIT_PACKED};
  GenerateLevels(min_repeat_factor, max_repeat_factor, max_level, input_levels);
  int num_levels = static_cast<int>(input_levels.size());
  int setdata_factor = 8;
  int split_level_size = num_levels / setdata_factor;
  bytes.resize(setdata_factor);

  // for each encoding
  for (int encode = 0; encode < 2; encode++) {
    Encoding::type encoding = encodings[encode];
    for (int rf = 0; rf < setdata_factor; rf++) {
      int offset = rf * split_level_size;
      EncodeLevels(encoding, max_level, split_level_size,
                   reinterpret_cast<int16_t*>(input_levels.data()) + offset, bytes[rf]);
    }
    VerifyDecodingMultipleSetData(encoding, max_level, input_levels, bytes);
  }
}

TEST(TestLevelEncoder, MinimumBufferSize) {
  // PARQUET-676, PARQUET-698
  const int kNumToEncode = 1024;

  std::vector<int16_t> levels;
  for (int i = 0; i < kNumToEncode; ++i) {
    if (i % 9 == 0) {
      levels.push_back(0);
    } else {
      levels.push_back(1);
    }
  }

  std::vector<uint8_t> output(
      LevelEncoder::MaxBufferSize(Encoding::RLE, 1, kNumToEncode));

  LevelEncoder encoder;
  encoder.Init(Encoding::RLE, 1, kNumToEncode, output.data(),
               static_cast<int>(output.size()));
  int encode_count = encoder.Encode(kNumToEncode, levels.data());

  ASSERT_EQ(kNumToEncode, encode_count);
}

TEST(TestLevelEncoder, MinimumBufferSize2) {
  // PARQUET-708
  // Test the worst case for bit_width=2 consisting of
  // LiteralRun(size=8)
  // RepeatedRun(size=8)
  // LiteralRun(size=8)
  // ...
  const int kNumToEncode = 1024;

  std::vector<int16_t> levels;
  for (int i = 0; i < kNumToEncode; ++i) {
    // This forces a literal run of 00000001
    // followed by eight 1s
    if ((i % 16) < 7) {
      levels.push_back(0);
    } else {
      levels.push_back(1);
    }
  }

  for (int bit_width = 1; bit_width <= 8; bit_width++) {
    std::vector<uint8_t> output(
        LevelEncoder::MaxBufferSize(Encoding::RLE, bit_width, kNumToEncode));

    LevelEncoder encoder;
    encoder.Init(Encoding::RLE, bit_width, kNumToEncode, output.data(),
                 static_cast<int>(output.size()));
    int encode_count = encoder.Encode(kNumToEncode, levels.data());

    ASSERT_EQ(kNumToEncode, encode_count);
  }
}

}  // namespace test
}  // namespace parquet
