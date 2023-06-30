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

#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/io/buffered.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_builders.h"

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"
#include "parquet/types.h"

namespace bit_util = arrow::bit_util;

namespace parquet {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

// The default size used in most tests.
const int SMALL_SIZE = 100;
#ifdef PARQUET_VALGRIND
// Larger size to test some corner cases, only used in some specific cases.
const int LARGE_SIZE = 10000;
// Very large size to test dictionary fallback.
const int VERY_LARGE_SIZE = 40000;
// Reduced dictionary page size to use for testing dictionary fallback with valgrind
const int64_t DICTIONARY_PAGE_SIZE = 1024;
#else
// Larger size to test some corner cases, only used in some specific cases.
const int LARGE_SIZE = 100000;
// Very large size to test dictionary fallback.
const int VERY_LARGE_SIZE = 400000;
// Dictionary page size to use for testing dictionary fallback
const int64_t DICTIONARY_PAGE_SIZE = 1024 * 1024;
#endif

template <typename TestType>
class TestPrimitiveWriter : public PrimitiveTypedTest<TestType> {
 public:
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
                   Compression::type compression = Compression::UNCOMPRESSED,
                   bool page_checksum_verify = false) {
    ASSERT_OK_AND_ASSIGN(auto buffer, sink_->Finish());
    auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
    ReaderProperties readerProperties;
    readerProperties.set_page_checksum_verification(page_checksum_verify);
    std::unique_ptr<PageReader> page_reader =
        PageReader::Open(std::move(source), num_rows, compression, readerProperties);
    reader_ = std::static_pointer_cast<TypedColumnReader<TestType>>(
        ColumnReader::Make(this->descr_, std::move(page_reader)));
  }

  std::shared_ptr<TypedColumnWriter<TestType>> BuildWriter(
      int64_t output_size = SMALL_SIZE,
      const ColumnProperties& column_properties = ColumnProperties(),
      const ParquetVersion::type version = ParquetVersion::PARQUET_1_0,
      bool enable_checksum = false) {
    sink_ = CreateOutputStream();
    WriterProperties::Builder wp_builder;
    wp_builder.version(version);
    if (column_properties.encoding() == Encoding::PLAIN_DICTIONARY ||
        column_properties.encoding() == Encoding::RLE_DICTIONARY) {
      wp_builder.enable_dictionary();
      wp_builder.dictionary_pagesize_limit(DICTIONARY_PAGE_SIZE);
    } else {
      wp_builder.disable_dictionary();
      wp_builder.encoding(column_properties.encoding());
    }
    if (enable_checksum) {
      wp_builder.enable_page_checksum();
    }
    wp_builder.max_statistics_size(column_properties.max_statistics_size());
    writer_properties_ = wp_builder.build();

    metadata_ = ColumnChunkMetaDataBuilder::Make(writer_properties_, this->descr_);
    std::unique_ptr<PageWriter> pager = PageWriter::Open(
        sink_, column_properties.compression(), metadata_.get(),
        /* row_group_ordinal */ -1, /* column_chunk_ordinal*/ -1,
        ::arrow::default_memory_pool(), /* buffered_row_group */ false,
        /* header_encryptor */ NULLPTR, /* data_encryptor */ NULLPTR, enable_checksum);
    std::shared_ptr<ColumnWriter> writer =
        ColumnWriter::Make(metadata_.get(), std::move(pager), writer_properties_.get());
    return std::static_pointer_cast<TypedColumnWriter<TestType>>(writer);
  }

  void ReadColumn(Compression::type compression = Compression::UNCOMPRESSED,
                  bool page_checksum_verify = false) {
    BuildReader(static_cast<int64_t>(this->values_out_.size()), compression,
                page_checksum_verify);
    reader_->ReadBatch(static_cast<int>(this->values_out_.size()),
                       definition_levels_out_.data(), repetition_levels_out_.data(),
                       this->values_out_ptr_, &values_read_);
    this->SyncValuesOut();
  }

  void ReadColumnFully(Compression::type compression = Compression::UNCOMPRESSED,
                       bool page_checksum_verify = false);

  void TestRequiredWithEncoding(Encoding::type encoding) {
    return TestRequiredWithSettings(encoding, Compression::UNCOMPRESSED, false, false);
  }

  void TestRequiredWithSettings(
      Encoding::type encoding, Compression::type compression, bool enable_dictionary,
      bool enable_statistics, int64_t num_rows = SMALL_SIZE,
      int compression_level = Codec::UseDefaultCompressionLevel(),
      bool enable_checksum = false) {
    this->GenerateData(num_rows);

    this->WriteRequiredWithSettings(encoding, compression, enable_dictionary,
                                    enable_statistics, compression_level, num_rows,
                                    enable_checksum);
    ASSERT_NO_FATAL_FAILURE(this->ReadAndCompare(compression, num_rows, enable_checksum));

    this->WriteRequiredWithSettingsSpaced(encoding, compression, enable_dictionary,
                                          enable_statistics, num_rows, compression_level,
                                          enable_checksum);
    ASSERT_NO_FATAL_FAILURE(this->ReadAndCompare(compression, num_rows, enable_checksum));
  }

  void TestRequiredWithCodecOptions(
      Encoding::type encoding, Compression::type compression, bool enable_dictionary,
      bool enable_statistics, int64_t num_rows = SMALL_SIZE,
      const ::arrow::util::CodecOptions& codec_options = CodecOptions{},
      bool enable_checksum = false) {
    this->GenerateData(num_rows);

    this->WriteRequiredWithCodecOptions(encoding, compression, enable_dictionary,
                                        enable_statistics, codec_options, num_rows,
                                        enable_checksum);
    ASSERT_NO_FATAL_FAILURE(this->ReadAndCompare(compression, num_rows, enable_checksum));
  }

  void TestDictionaryFallbackEncoding(ParquetVersion::type version) {
    this->GenerateData(VERY_LARGE_SIZE);
    ColumnProperties column_properties;
    column_properties.set_dictionary_enabled(true);

    if (version == ParquetVersion::PARQUET_1_0) {
      column_properties.set_encoding(Encoding::PLAIN_DICTIONARY);
    } else {
      column_properties.set_encoding(Encoding::RLE_DICTIONARY);
    }

    auto writer = this->BuildWriter(VERY_LARGE_SIZE, column_properties, version);

    writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
    writer->Close();

    // Read all rows so we are sure that also the non-dictionary pages are read correctly
    this->SetupValuesOut(VERY_LARGE_SIZE);
    this->ReadColumnFully();
    ASSERT_EQ(VERY_LARGE_SIZE, this->values_read_);
    this->values_.resize(VERY_LARGE_SIZE);
    ASSERT_EQ(this->values_, this->values_out_);
    std::vector<Encoding::type> encodings_vector = this->metadata_encodings();
    std::set<Encoding::type> encodings(encodings_vector.begin(), encodings_vector.end());

    if (this->type_num() == Type::BOOLEAN) {
      // Dictionary encoding is not allowed for boolean type
      // There are 2 encodings (PLAIN, RLE) in a non dictionary encoding case
      std::set<Encoding::type> expected({Encoding::PLAIN, Encoding::RLE});
      ASSERT_EQ(encodings, expected);
    } else if (version == ParquetVersion::PARQUET_1_0) {
      // There are 3 encodings (PLAIN_DICTIONARY, PLAIN, RLE) in a fallback case
      // for version 1.0
      std::set<Encoding::type> expected(
          {Encoding::PLAIN_DICTIONARY, Encoding::PLAIN, Encoding::RLE});
      ASSERT_EQ(encodings, expected);
    } else {
      // There are 3 encodings (RLE_DICTIONARY, PLAIN, RLE) in a fallback case for
      // version 2.0
      std::set<Encoding::type> expected(
          {Encoding::RLE_DICTIONARY, Encoding::PLAIN, Encoding::RLE});
      ASSERT_EQ(encodings, expected);
    }

    std::vector<parquet::PageEncodingStats> encoding_stats =
        this->metadata_encoding_stats();
    if (this->type_num() == Type::BOOLEAN) {
      ASSERT_EQ(encoding_stats[0].encoding, Encoding::PLAIN);
      ASSERT_EQ(encoding_stats[0].page_type, PageType::DATA_PAGE);
    } else if (version == ParquetVersion::PARQUET_1_0) {
      std::vector<Encoding::type> expected(
          {Encoding::PLAIN_DICTIONARY, Encoding::PLAIN, Encoding::PLAIN_DICTIONARY});
      ASSERT_EQ(encoding_stats[0].encoding, expected[0]);
      ASSERT_EQ(encoding_stats[0].page_type, PageType::DICTIONARY_PAGE);
      for (size_t i = 1; i < encoding_stats.size(); i++) {
        ASSERT_EQ(encoding_stats[i].encoding, expected[i]);
        ASSERT_EQ(encoding_stats[i].page_type, PageType::DATA_PAGE);
      }
    } else {
      std::vector<Encoding::type> expected(
          {Encoding::PLAIN, Encoding::PLAIN, Encoding::RLE_DICTIONARY});
      ASSERT_EQ(encoding_stats[0].encoding, expected[0]);
      ASSERT_EQ(encoding_stats[0].page_type, PageType::DICTIONARY_PAGE);
      for (size_t i = 1; i < encoding_stats.size(); i++) {
        ASSERT_EQ(encoding_stats[i].encoding, expected[i]);
        ASSERT_EQ(encoding_stats[i].page_type, PageType::DATA_PAGE);
      }
    }
  }

  void WriteRequiredWithSettings(Encoding::type encoding, Compression::type compression,
                                 bool enable_dictionary, bool enable_statistics,
                                 int compression_level, int64_t num_rows,
                                 bool enable_checksum) {
    ColumnProperties column_properties(encoding, compression, enable_dictionary,
                                       enable_statistics);
    column_properties.set_codec_options(
        std::make_shared<CodecOptions>(compression_level));
    std::shared_ptr<TypedColumnWriter<TestType>> writer = this->BuildWriter(
        num_rows, column_properties, ParquetVersion::PARQUET_1_0, enable_checksum);
    writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
    // The behaviour should be independent from the number of Close() calls
    writer->Close();
    writer->Close();
  }

  void WriteRequiredWithSettingsSpaced(Encoding::type encoding,
                                       Compression::type compression,
                                       bool enable_dictionary, bool enable_statistics,
                                       int64_t num_rows, int compression_level,
                                       bool enable_checksum) {
    std::vector<uint8_t> valid_bits(
        bit_util::BytesForBits(static_cast<uint32_t>(this->values_.size())) + 1, 255);
    ColumnProperties column_properties(encoding, compression, enable_dictionary,
                                       enable_statistics);
    column_properties.set_codec_options(
        std::make_shared<CodecOptions>(compression_level));
    std::shared_ptr<TypedColumnWriter<TestType>> writer = this->BuildWriter(
        num_rows, column_properties, ParquetVersion::PARQUET_1_0, enable_checksum);
    writer->WriteBatchSpaced(this->values_.size(), nullptr, nullptr, valid_bits.data(), 0,
                             this->values_ptr_);
    // The behaviour should be independent from the number of Close() calls
    writer->Close();
    writer->Close();
  }

  void WriteRequiredWithCodecOptions(Encoding::type encoding,
                                     Compression::type compression,
                                     bool enable_dictionary, bool enable_statistics,
                                     const std::shared_ptr<CodecOptions>& codec_options,
                                     int64_t num_rows, bool enable_checksum) {
    ColumnProperties column_properties(encoding, compression, enable_dictionary,
                                       enable_statistics);
    column_properties.set_codec_options(codec_options);
    std::shared_ptr<TypedColumnWriter<TestType>> writer = this->BuildWriter(
        num_rows, column_properties, ParquetVersion::PARQUET_1_0, enable_checksum);
    writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
    // The behaviour should be independent from the number of Close() calls
    writer->Close();
    writer->Close();
  }

  void ReadAndCompare(Compression::type compression, int64_t num_rows,
                      bool page_checksum_verify) {
    this->SetupValuesOut(num_rows);
    this->ReadColumnFully(compression, page_checksum_verify);
    auto comparator = MakeComparator<TestType>(this->descr_);
    for (size_t i = 0; i < this->values_.size(); i++) {
      if (comparator->Compare(this->values_[i], this->values_out_[i]) ||
          comparator->Compare(this->values_out_[i], this->values_[i])) {
        ARROW_SCOPED_TRACE("i = ", i);
      }
      ASSERT_FALSE(comparator->Compare(this->values_[i], this->values_out_[i]));
      ASSERT_FALSE(comparator->Compare(this->values_out_[i], this->values_[i]));
    }
    ASSERT_EQ(this->values_, this->values_out_);
  }

  int64_t metadata_num_values() {
    // Metadata accessor must be created lazily.
    // This is because the ColumnChunkMetaData semantics dictate the metadata object is
    // complete (no changes to the metadata buffer can be made after instantiation)
    auto metadata_accessor =
        ColumnChunkMetaData::Make(metadata_->contents(), this->descr_);
    return metadata_accessor->num_values();
  }

  bool metadata_is_stats_set() {
    // Metadata accessor must be created lazily.
    // This is because the ColumnChunkMetaData semantics dictate the metadata object is
    // complete (no changes to the metadata buffer can be made after instantiation)
    ApplicationVersion app_version(this->writer_properties_->created_by());
    auto metadata_accessor = ColumnChunkMetaData::Make(
        metadata_->contents(), this->descr_, default_reader_properties(), &app_version);
    return metadata_accessor->is_stats_set();
  }

  std::pair<bool, bool> metadata_stats_has_min_max() {
    // Metadata accessor must be created lazily.
    // This is because the ColumnChunkMetaData semantics dictate the metadata object is
    // complete (no changes to the metadata buffer can be made after instantiation)
    ApplicationVersion app_version(this->writer_properties_->created_by());
    auto metadata_accessor = ColumnChunkMetaData::Make(
        metadata_->contents(), this->descr_, default_reader_properties(), &app_version);
    auto encoded_stats = metadata_accessor->statistics()->Encode();
    return {encoded_stats.has_min, encoded_stats.has_max};
  }

  std::vector<Encoding::type> metadata_encodings() {
    // Metadata accessor must be created lazily.
    // This is because the ColumnChunkMetaData semantics dictate the metadata object is
    // complete (no changes to the metadata buffer can be made after instantiation)
    auto metadata_accessor =
        ColumnChunkMetaData::Make(metadata_->contents(), this->descr_);
    return metadata_accessor->encodings();
  }

  std::vector<parquet::PageEncodingStats> metadata_encoding_stats() {
    // Metadata accessor must be created lazily.
    // This is because the ColumnChunkMetaData semantics dictate the metadata object is
    // complete (no changes to the metadata buffer can be made after instantiation)
    auto metadata_accessor =
        ColumnChunkMetaData::Make(metadata_->contents(), this->descr_);
    return metadata_accessor->encoding_stats();
  }

 protected:
  int64_t values_read_;
  // Keep the reader alive as for ByteArray the lifetime of the ByteArray
  // content is bound to the reader.
  std::shared_ptr<TypedColumnReader<TestType>> reader_;

  std::vector<int16_t> definition_levels_out_;
  std::vector<int16_t> repetition_levels_out_;

  const ColumnDescriptor* descr_;

 private:
  std::unique_ptr<ColumnChunkMetaDataBuilder> metadata_;
  std::shared_ptr<::arrow::io::BufferOutputStream> sink_;
  std::shared_ptr<WriterProperties> writer_properties_;
  std::vector<std::vector<uint8_t>> data_buffer_;
};

template <typename TestType>
void TestPrimitiveWriter<TestType>::ReadColumnFully(Compression::type compression,
                                                    bool page_checksum_verify) {
  int64_t total_values = static_cast<int64_t>(this->values_out_.size());
  BuildReader(total_values, compression, page_checksum_verify);
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
void TestPrimitiveWriter<Int96Type>::ReadAndCompare(Compression::type compression,
                                                    int64_t num_rows,
                                                    bool page_checksum_verify) {
  this->SetupValuesOut(num_rows);
  this->ReadColumnFully(compression, page_checksum_verify);

  auto comparator = MakeComparator<Int96Type>(Type::INT96, SortOrder::SIGNED);
  for (size_t i = 0; i < this->values_.size(); i++) {
    if (comparator->Compare(this->values_[i], this->values_out_[i]) ||
        comparator->Compare(this->values_out_[i], this->values_[i])) {
      ARROW_SCOPED_TRACE("i = ", i);
    }
    ASSERT_FALSE(comparator->Compare(this->values_[i], this->values_out_[i]));
    ASSERT_FALSE(comparator->Compare(this->values_out_[i], this->values_[i]));
  }
  ASSERT_EQ(this->values_, this->values_out_);
}

template <>
void TestPrimitiveWriter<FLBAType>::ReadColumnFully(Compression::type compression,
                                                    bool page_checksum_verify) {
  int64_t total_values = static_cast<int64_t>(this->values_out_.size());
  BuildReader(total_values, compression, page_checksum_verify);
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

TYPED_TEST_SUITE(TestPrimitiveWriter, TestTypes);

using TestValuesWriterInt32Type = TestPrimitiveWriter<Int32Type>;
using TestValuesWriterInt64Type = TestPrimitiveWriter<Int64Type>;
using TestByteArrayValuesWriter = TestPrimitiveWriter<ByteArrayType>;
using TestFixedLengthByteArrayValuesWriter = TestPrimitiveWriter<FLBAType>;

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
*/

TEST_F(TestValuesWriterInt32Type, RequiredDeltaBinaryPacked) {
  this->TestRequiredWithEncoding(Encoding::DELTA_BINARY_PACKED);
}

TEST_F(TestValuesWriterInt64Type, RequiredDeltaBinaryPacked) {
  this->TestRequiredWithEncoding(Encoding::DELTA_BINARY_PACKED);
}

TEST_F(TestByteArrayValuesWriter, RequiredDeltaLengthByteArray) {
  this->TestRequiredWithEncoding(Encoding::DELTA_LENGTH_BYTE_ARRAY);
}

/*
TYPED_TEST(TestByteArrayValuesWriter, RequiredDeltaByteArray) {
  this->TestRequiredWithEncoding(Encoding::DELTA_BYTE_ARRAY);
}

TEST_F(TestFixedLengthByteArrayValuesWriter, RequiredDeltaByteArray) {
  this->TestRequiredWithEncoding(Encoding::DELTA_BYTE_ARRAY);
}
*/

TYPED_TEST(TestPrimitiveWriter, RequiredRLEDictionary) {
  this->TestRequiredWithEncoding(Encoding::RLE_DICTIONARY);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStats) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::UNCOMPRESSED, false, true,
                                 LARGE_SIZE);
}

#ifdef ARROW_WITH_SNAPPY
TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithSnappyCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::SNAPPY, false, false,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStatsAndSnappyCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::SNAPPY, false, true,
                                 LARGE_SIZE);
}
#endif

#ifdef ARROW_WITH_BROTLI
TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithBrotliCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::BROTLI, false, false,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithBrotliCompressionAndLevel) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::BROTLI, false, false,
                                 LARGE_SIZE, 10);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStatsAndBrotliCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::BROTLI, false, true,
                                 LARGE_SIZE);
}

#endif

#ifdef ARROW_WITH_GZIP
TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithGzipCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::GZIP, false, false,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithGzipCompressionAndLevel) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::GZIP, false, false,
                                 LARGE_SIZE, 10);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStatsAndGzipCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::GZIP, false, true,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithGzipCodecOptions) {
  auto codec_options = std::make_shared<::arrow::util::GZipCodecOptions>(10);
  codec_options->gzip_format = ::arrow::util::GZipFormat::GZIP;
  codec_options->window_bits = 12;
  this->TestRequiredWithCodecOptions(Encoding::PLAIN, Compression::GZIP, false, false,
                                     LARGE_SIZE, codec_options);
}
#endif

#ifdef ARROW_WITH_LZ4
TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithLz4Compression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::LZ4, false, false,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStatsAndLz4Compression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::LZ4, false, true,
                                 LARGE_SIZE);
}
#endif

#ifdef ARROW_WITH_ZSTD
TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithZstdCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::ZSTD, false, false,
                                 LARGE_SIZE);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithZstdCompressionAndLevel) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::ZSTD, false, false,
                                 LARGE_SIZE, 6);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainWithStatsAndZstdCompression) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::ZSTD, false, true,
                                 LARGE_SIZE);
}
#endif

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
  std::vector<uint8_t> valid_bits(::arrow::bit_util::BytesForBits(SMALL_SIZE), 255);

  definition_levels[SMALL_SIZE - 1] = 0;
  ::arrow::bit_util::ClearBit(valid_bits.data(), SMALL_SIZE - 1);
  definition_levels[1] = 0;
  ::arrow::bit_util::ClearBit(valid_bits.data(), 1);

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

// Test cases for dictionary fallback encoding
TYPED_TEST(TestPrimitiveWriter, DictionaryFallbackVersion1_0) {
  this->TestDictionaryFallbackEncoding(ParquetVersion::PARQUET_1_0);
}

TYPED_TEST(TestPrimitiveWriter, DictionaryFallbackVersion2_0) {
  this->TestDictionaryFallbackEncoding(ParquetVersion::PARQUET_2_4);
}

TEST(TestWriter, NullValuesBuffer) {
  std::shared_ptr<::arrow::io::BufferOutputStream> sink = CreateOutputStream();

  const auto item_node = schema::PrimitiveNode::Make(
      "item", Repetition::REQUIRED, LogicalType::Int(32, true), Type::INT32);
  const auto list_node =
      schema::GroupNode::Make("list", Repetition::REPEATED, {item_node});
  const auto column_node = schema::GroupNode::Make(
      "array_of_ints_column", Repetition::OPTIONAL, {list_node}, LogicalType::List());
  const auto schema_node =
      schema::GroupNode::Make("schema", Repetition::REQUIRED, {column_node});

  auto file_writer = ParquetFileWriter::Open(
      sink, std::dynamic_pointer_cast<schema::GroupNode>(schema_node));
  auto group_writer = file_writer->AppendRowGroup();
  auto column_writer = group_writer->NextColumn();
  auto typed_writer = dynamic_cast<Int32Writer*>(column_writer);

  const int64_t num_values = 1;
  const int16_t def_levels[] = {0};
  const int16_t rep_levels[] = {0};
  const uint8_t valid_bits[] = {0};
  const int64_t valid_bits_offset = 0;
  const int32_t* values = nullptr;

  typed_writer->WriteBatchSpaced(num_values, def_levels, rep_levels, valid_bits,
                                 valid_bits_offset, values);
}

TYPED_TEST(TestPrimitiveWriter, RequiredPlainChecksum) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::UNCOMPRESSED,
                                 /* enable_dictionary */ false, false, SMALL_SIZE,
                                 Codec::UseDefaultCompressionLevel(),
                                 /* enable_checksum */ true);
}

TYPED_TEST(TestPrimitiveWriter, RequiredDictChecksum) {
  this->TestRequiredWithSettings(Encoding::PLAIN, Compression::UNCOMPRESSED,
                                 /* enable_dictionary */ true, false, SMALL_SIZE,
                                 Codec::UseDefaultCompressionLevel(),
                                 /* enable_checksum */ true);
}

// PARQUET-719
// Test case for NULL values
TEST_F(TestValuesWriterInt32Type, OptionalNullValueChunk) {
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

// PARQUET-979
// Prevent writing large MIN, MAX stats
TEST_F(TestByteArrayValuesWriter, OmitStats) {
  int min_len = 1024 * 4;
  int max_len = 1024 * 8;
  this->SetUpSchema(Repetition::REQUIRED);
  auto writer = this->BuildWriter();

  values_.resize(SMALL_SIZE);
  InitWideByteArrayValues(SMALL_SIZE, this->values_, this->buffer_, min_len, max_len);
  writer->WriteBatch(SMALL_SIZE, nullptr, nullptr, this->values_.data());
  writer->Close();

  auto has_min_max = this->metadata_stats_has_min_max();
  ASSERT_FALSE(has_min_max.first);
  ASSERT_FALSE(has_min_max.second);
}

// PARQUET-1405
// Prevent writing large stats in the DataPageHeader
TEST_F(TestByteArrayValuesWriter, OmitDataPageStats) {
  int min_len = static_cast<int>(std::pow(10, 7));
  int max_len = static_cast<int>(std::pow(10, 7));
  this->SetUpSchema(Repetition::REQUIRED);
  ColumnProperties column_properties;
  column_properties.set_statistics_enabled(false);
  auto writer = this->BuildWriter(SMALL_SIZE, column_properties);

  values_.resize(1);
  InitWideByteArrayValues(1, this->values_, this->buffer_, min_len, max_len);
  writer->WriteBatch(1, nullptr, nullptr, this->values_.data());
  writer->Close();

  ASSERT_NO_THROW(this->ReadColumn());
}

TEST_F(TestByteArrayValuesWriter, LimitStats) {
  int min_len = 1024 * 4;
  int max_len = 1024 * 8;
  this->SetUpSchema(Repetition::REQUIRED);
  ColumnProperties column_properties;
  column_properties.set_max_statistics_size(static_cast<size_t>(max_len));
  auto writer = this->BuildWriter(SMALL_SIZE, column_properties);

  values_.resize(SMALL_SIZE);
  InitWideByteArrayValues(SMALL_SIZE, this->values_, this->buffer_, min_len, max_len);
  writer->WriteBatch(SMALL_SIZE, nullptr, nullptr, this->values_.data());
  writer->Close();

  ASSERT_TRUE(this->metadata_is_stats_set());
}

TEST_F(TestByteArrayValuesWriter, CheckDefaultStats) {
  this->SetUpSchema(Repetition::REQUIRED);
  auto writer = this->BuildWriter();
  this->GenerateData(SMALL_SIZE);

  writer->WriteBatch(SMALL_SIZE, nullptr, nullptr, this->values_ptr_);
  writer->Close();

  ASSERT_TRUE(this->metadata_is_stats_set());
}

TEST(TestColumnWriter, RepeatedListsUpdateSpacedBug) {
  // In ARROW-3930 we discovered a bug when writing from Arrow when we had data
  // that looks like this:
  //
  // [null, [0, 1, null, 2, 3, 4, null]]

  // Create schema
  NodePtr item = schema::Int32("item");  // optional item
  NodePtr list(GroupNode::Make("b", Repetition::REPEATED, {item}, ConvertedType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));  // optional list
  std::vector<NodePtr> fields = {bag};
  NodePtr root = GroupNode::Make("schema", Repetition::REPEATED, fields);

  SchemaDescriptor schema;
  schema.Init(root);

  auto sink = CreateOutputStream();
  auto props = WriterProperties::Builder().build();

  auto metadata = ColumnChunkMetaDataBuilder::Make(props, schema.Column(0));
  std::unique_ptr<PageWriter> pager =
      PageWriter::Open(sink, Compression::UNCOMPRESSED, metadata.get());
  std::shared_ptr<ColumnWriter> writer =
      ColumnWriter::Make(metadata.get(), std::move(pager), props.get());
  auto typed_writer = std::static_pointer_cast<TypedColumnWriter<Int32Type>>(writer);

  std::vector<int16_t> def_levels = {1, 3, 3, 2, 3, 3, 3, 2, 3, 3, 3, 2, 3, 3};
  std::vector<int16_t> rep_levels = {0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  std::vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

  // Write the values into uninitialized memory
  ASSERT_OK_AND_ASSIGN(auto values_buffer, ::arrow::AllocateBuffer(64));
  memcpy(values_buffer->mutable_data(), values.data(), 13 * sizeof(int32_t));
  auto values_data = reinterpret_cast<const int32_t*>(values_buffer->data());

  std::shared_ptr<Buffer> valid_bits;
  ASSERT_OK_AND_ASSIGN(valid_bits, ::arrow::internal::BytesToBits(
                                       {1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1}));

  // valgrind will warn about out of bounds access into def_levels_data
  typed_writer->WriteBatchSpaced(14, def_levels.data(), rep_levels.data(),
                                 valid_bits->data(), 0, values_data);
  writer->Close();
}

void GenerateLevels(int min_repeat_factor, int max_repeat_factor, int max_level,
                    std::vector<int16_t>& input_levels) {
  // for each repetition count up to max_repeat_factor
  for (int repeat = min_repeat_factor; repeat <= max_repeat_factor; repeat++) {
    // repeat count increases by a factor of 2 for every iteration
    int repeat_count = (1 << repeat);
    // generate levels for repetition count up to the maximum level
    int16_t value = 0;
    int bwidth = 0;
    while (value <= max_level) {
      for (int i = 0; i < repeat_count; i++) {
        input_levels.push_back(value);
      }
      value = static_cast<int16_t>((2 << bwidth) - 1);
      bwidth++;
    }
  }
}

void EncodeLevels(Encoding::type encoding, int16_t max_level, int num_levels,
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

void VerifyDecodingLevels(Encoding::type encoding, int16_t max_level,
                          std::vector<int16_t>& input_levels,
                          std::vector<uint8_t>& bytes) {
  LevelDecoder decoder;
  int levels_count = 0;
  std::vector<int16_t> output_levels;
  int num_levels = static_cast<int>(input_levels.size());

  output_levels.resize(num_levels);
  ASSERT_EQ(num_levels, static_cast<int>(output_levels.size()));

  // Decode levels and test with multiple decode calls
  decoder.SetData(encoding, max_level, num_levels, bytes.data(),
                  static_cast<int32_t>(bytes.size()));
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

void VerifyDecodingMultipleSetData(Encoding::type encoding, int16_t max_level,
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
    decoder.SetData(encoding, max_level, num_levels, bytes[ct].data(),
                    static_cast<int32_t>(bytes[ct].size()));
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
    // BIT_PACKED requires a sequence of at least 8
    if (encoding == Encoding::BIT_PACKED) min_repeat_factor = 3;
    // for each maximum bit-width
    for (int bit_width = 1; bit_width <= max_bit_width; bit_width++) {
      // find the maximum level for the current bit_width
      int16_t max_level = static_cast<int16_t>((1 << bit_width) - 1);
      // Generate levels
      GenerateLevels(min_repeat_factor, max_repeat_factor, max_level, input_levels);
      ASSERT_NO_FATAL_FAILURE(EncodeLevels(encoding, max_level,
                                           static_cast<int>(input_levels.size()),
                                           input_levels.data(), bytes));
      ASSERT_NO_FATAL_FAILURE(
          VerifyDecodingLevels(encoding, max_level, input_levels, bytes));
      input_levels.clear();
    }
  }
}

// Test multiple decoder SetData calls
TEST(TestLevels, TestLevelsDecodeMultipleSetData) {
  int min_repeat_factor = 3;
  int max_repeat_factor = 7;  // 128
  int bit_width = 8;
  int16_t max_level = static_cast<int16_t>((1 << bit_width) - 1);
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
      ASSERT_NO_FATAL_FAILURE(EncodeLevels(
          encoding, max_level, split_level_size,
          reinterpret_cast<int16_t*>(input_levels.data()) + offset, bytes[rf]));
    }
    ASSERT_NO_FATAL_FAILURE(
        VerifyDecodingMultipleSetData(encoding, max_level, input_levels, bytes));
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

  for (int16_t bit_width = 1; bit_width <= 8; bit_width++) {
    std::vector<uint8_t> output(
        LevelEncoder::MaxBufferSize(Encoding::RLE, bit_width, kNumToEncode));

    LevelEncoder encoder;
    encoder.Init(Encoding::RLE, bit_width, kNumToEncode, output.data(),
                 static_cast<int>(output.size()));
    int encode_count = encoder.Encode(kNumToEncode, levels.data());

    ASSERT_EQ(kNumToEncode, encode_count);
  }
}

TEST(TestColumnWriter, WriteDataPageV2Header) {
  auto sink = CreateOutputStream();
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED,
                      {
                          schema::Int32("required", Repetition::REQUIRED),
                          schema::Int32("optional", Repetition::OPTIONAL),
                          schema::Int32("repeated", Repetition::REPEATED),
                      }));
  auto properties = WriterProperties::Builder()
                        .disable_dictionary()
                        ->data_page_version(ParquetDataPageVersion::V2)
                        ->build();
  auto file_writer = ParquetFileWriter::Open(sink, schema, properties);
  auto rg_writer = file_writer->AppendRowGroup();

  constexpr int32_t num_rows = 100;

  auto required_writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
  for (int32_t i = 0; i < num_rows; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &i);
  }

  // Write a null value at every other row.
  auto optional_writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
  for (int32_t i = 0; i < num_rows; i++) {
    int16_t definition_level = i % 2 == 0 ? 1 : 0;
    optional_writer->WriteBatch(1, &definition_level, nullptr, &i);
  }

  // Each row has repeated twice.
  auto repeated_writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
  for (int i = 0; i < 2 * num_rows; i++) {
    int32_t value = i * 1000;
    int16_t definition_level = 1;
    int16_t repetition_level = i % 2 == 0 ? 1 : 0;
    repeated_writer->WriteBatch(1, &definition_level, &repetition_level, &value);
  }

  ASSERT_NO_THROW(file_writer->Close());
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(
      std::make_shared<::arrow::io::BufferReader>(buffer), default_reader_properties());
  auto metadata = file_reader->metadata();
  ASSERT_EQ(1, metadata->num_row_groups());
  auto row_group_reader = file_reader->RowGroup(0);

  // Verify required column.
  {
    auto page_reader = row_group_reader->GetColumnPageReader(0);
    auto page = page_reader->NextPage();
    ASSERT_NE(page, nullptr);
    auto data_page = std::static_pointer_cast<DataPageV2>(page);
    EXPECT_EQ(num_rows, data_page->num_rows());
    EXPECT_EQ(num_rows, data_page->num_values());
    EXPECT_EQ(0, data_page->num_nulls());
    EXPECT_EQ(page_reader->NextPage(), nullptr);
  }

  // Verify optional column.
  {
    auto page_reader = row_group_reader->GetColumnPageReader(1);
    auto page = page_reader->NextPage();
    ASSERT_NE(page, nullptr);
    auto data_page = std::static_pointer_cast<DataPageV2>(page);
    EXPECT_EQ(num_rows, data_page->num_rows());
    EXPECT_EQ(num_rows, data_page->num_values());
    EXPECT_EQ(num_rows / 2, data_page->num_nulls());
    EXPECT_EQ(page_reader->NextPage(), nullptr);
  }

  // Verify repeated column.
  {
    auto page_reader = row_group_reader->GetColumnPageReader(2);
    auto page = page_reader->NextPage();
    ASSERT_NE(page, nullptr);
    auto data_page = std::static_pointer_cast<DataPageV2>(page);
    EXPECT_EQ(num_rows, data_page->num_rows());
    EXPECT_EQ(num_rows * 2, data_page->num_values());
    EXPECT_EQ(0, data_page->num_nulls());
    EXPECT_EQ(page_reader->NextPage(), nullptr);
  }
}

// The test below checks that data page v2 changes on record boundaries for
// all repetition types (i.e. required, optional, and repeated)
TEST(TestColumnWriter, WriteDataPagesChangeOnRecordBoundaries) {
  auto sink = CreateOutputStream();
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED,
                      {schema::Int32("required", Repetition::REQUIRED),
                       schema::Int32("optional", Repetition::OPTIONAL),
                       schema::Int32("repeated", Repetition::REPEATED)}));
  // Write at most 11 levels per batch.
  constexpr int64_t batch_size = 11;
  auto properties = WriterProperties::Builder()
                        .disable_dictionary()
                        ->data_page_version(ParquetDataPageVersion::V2)
                        ->write_batch_size(batch_size)
                        ->data_pagesize(1) /* every page size check creates a new page */
                        ->build();
  auto file_writer = ParquetFileWriter::Open(sink, schema, properties);
  auto rg_writer = file_writer->AppendRowGroup();

  constexpr int32_t num_levels = 100;
  const std::vector<int32_t> values(num_levels, 1024);
  std::array<int16_t, num_levels> def_levels;
  std::array<int16_t, num_levels> rep_levels;
  for (int32_t i = 0; i < num_levels; i++) {
    def_levels[i] = i % 2 == 0 ? 1 : 0;
    rep_levels[i] = i % 2 == 0 ? 0 : 1;
  }

  auto required_writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
  required_writer->WriteBatch(num_levels, nullptr, nullptr, values.data());

  // Write a null value at every other row.
  auto optional_writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
  optional_writer->WriteBatch(num_levels, def_levels.data(), nullptr, values.data());

  // Each row has repeated twice.
  auto repeated_writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
  repeated_writer->WriteBatch(num_levels, def_levels.data(), rep_levels.data(),
                              values.data());
  repeated_writer->WriteBatch(num_levels, def_levels.data(), rep_levels.data(),
                              values.data());

  ASSERT_NO_THROW(file_writer->Close());
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(
      std::make_shared<::arrow::io::BufferReader>(buffer), default_reader_properties());
  auto metadata = file_reader->metadata();
  ASSERT_EQ(1, metadata->num_row_groups());
  auto row_group_reader = file_reader->RowGroup(0);

  // Check if pages are changed on record boundaries.
  constexpr int num_columns = 3;
  const std::array<int64_t, num_columns> expected_num_pages = {10, 10, 19};
  for (int i = 0; i < num_columns; ++i) {
    auto page_reader = row_group_reader->GetColumnPageReader(i);
    int64_t num_rows = 0;
    int64_t num_pages = 0;
    std::shared_ptr<Page> page;
    while ((page = page_reader->NextPage()) != nullptr) {
      auto data_page = std::static_pointer_cast<DataPageV2>(page);
      if (i < 2) {
        EXPECT_EQ(data_page->num_values(), data_page->num_rows());
      } else {
        // Make sure repeated column has 2 values per row and not span multiple pages.
        EXPECT_EQ(data_page->num_values(), 2 * data_page->num_rows());
      }
      num_rows += data_page->num_rows();
      num_pages++;
    }
    EXPECT_EQ(num_levels, num_rows);
    EXPECT_EQ(expected_num_pages[i], num_pages);
  }
}

// The test below checks that data page v2 changes on record boundaries for
// repeated columns with small batches.
TEST(TestColumnWriter, WriteDataPagesChangeOnRecordBoundariesWithSmallBatches) {
  auto sink = CreateOutputStream();
  auto schema = std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED,
                      {schema::Int32("tiny_repeat", Repetition::REPEATED),
                       schema::Int32("small_repeat", Repetition::REPEATED),
                       schema::Int32("medium_repeat", Repetition::REPEATED),
                       schema::Int32("large_repeat", Repetition::REPEATED)}));

  // The batch_size is large enough so each WriteBatch call checks page size at most once.
  constexpr int64_t batch_size = std::numeric_limits<int64_t>::max();
  auto properties = WriterProperties::Builder()
                        .disable_dictionary()
                        ->data_page_version(ParquetDataPageVersion::V2)
                        ->write_batch_size(batch_size)
                        ->data_pagesize(1) /* every page size check creates a new page */
                        ->build();
  auto file_writer = ParquetFileWriter::Open(sink, schema, properties);
  auto rg_writer = file_writer->AppendRowGroup();

  constexpr int32_t num_cols = 4;
  constexpr int64_t num_rows = 400;
  constexpr int64_t num_levels = 100;
  constexpr std::array<int64_t, num_cols> num_levels_per_row_by_col = {1, 50, 99, 150};

  // All values are not null and fixed to 1024 for simplicity.
  const std::vector<int32_t> values(num_levels, 1024);
  const std::vector<int16_t> def_levels(num_levels, 1);
  std::vector<int16_t> rep_levels(num_levels, 0);

  for (int32_t i = 0; i < num_cols; ++i) {
    auto writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
    const auto num_levels_per_row = num_levels_per_row_by_col[i];
    int64_t num_rows_written = 0;
    int64_t num_levels_written_curr_row = 0;
    while (num_rows_written < num_rows) {
      int32_t num_levels_to_write = 0;
      while (num_levels_to_write < num_levels) {
        if (num_levels_written_curr_row == 0) {
          // A new record.
          rep_levels[num_levels_to_write++] = 0;
        } else {
          rep_levels[num_levels_to_write++] = 1;
        }

        if (++num_levels_written_curr_row == num_levels_per_row) {
          // Current row has enough levels.
          num_levels_written_curr_row = 0;
          if (++num_rows_written == num_rows) {
            // Enough rows have been written.
            break;
          }
        }
      }

      writer->WriteBatch(num_levels_to_write, def_levels.data(), rep_levels.data(),
                         values.data());
    }
  }

  ASSERT_NO_THROW(file_writer->Close());
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(
      std::make_shared<::arrow::io::BufferReader>(buffer), default_reader_properties());
  auto metadata = file_reader->metadata();
  ASSERT_EQ(1, metadata->num_row_groups());
  auto row_group_reader = file_reader->RowGroup(0);

  // Check if pages are changed on record boundaries.
  const std::array<int64_t, num_cols> expect_num_pages_by_col = {5, 201, 397, 201};
  const std::array<int64_t, num_cols> expect_num_rows_1st_page_by_col = {99, 1, 1, 1};
  const std::array<int64_t, num_cols> expect_num_vals_1st_page_by_col = {99, 50, 99, 150};
  for (int32_t i = 0; i < num_cols; ++i) {
    auto page_reader = row_group_reader->GetColumnPageReader(i);
    int64_t num_rows_read = 0;
    int64_t num_pages_read = 0;
    int64_t num_values_read = 0;
    std::shared_ptr<Page> page;
    while ((page = page_reader->NextPage()) != nullptr) {
      auto data_page = std::static_pointer_cast<DataPageV2>(page);
      num_values_read += data_page->num_values();
      num_rows_read += data_page->num_rows();
      if (num_pages_read++ == 0) {
        EXPECT_EQ(expect_num_rows_1st_page_by_col[i], data_page->num_rows());
        EXPECT_EQ(expect_num_vals_1st_page_by_col[i], data_page->num_values());
      }
    }
    EXPECT_EQ(num_rows, num_rows_read);
    EXPECT_EQ(expect_num_pages_by_col[i], num_pages_read);
    EXPECT_EQ(num_levels_per_row_by_col[i] * num_rows, num_values_read);
  }
}

class ColumnWriterTestSizeEstimated : public ::testing::Test {
 public:
  void SetUp() {
    sink_ = CreateOutputStream();
    node_ = std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED,
                        {
                            schema::Int32("required", Repetition::REQUIRED),
                        }));
    std::vector<schema::NodePtr> fields;
    schema_descriptor_ = std::make_unique<SchemaDescriptor>();
    schema_descriptor_->Init(node_);
  }

  std::shared_ptr<parquet::Int32Writer> BuildWriter(Compression::type compression,
                                                    bool buffered,
                                                    bool enable_dictionary = false) {
    auto builder = WriterProperties::Builder();
    builder.disable_dictionary()
        ->compression(compression)
        ->data_pagesize(100 * sizeof(int));
    if (enable_dictionary) {
      builder.enable_dictionary();
    } else {
      builder.disable_dictionary();
    }
    writer_properties_ = builder.build();
    metadata_ = ColumnChunkMetaDataBuilder::Make(writer_properties_,
                                                 schema_descriptor_->Column(0));

    std::unique_ptr<PageWriter> pager = PageWriter::Open(
        sink_, compression, metadata_.get(),
        /* row_group_ordinal */ -1, /* column_chunk_ordinal*/ -1,
        ::arrow::default_memory_pool(), /* buffered_row_group */ buffered,
        /* header_encryptor */ NULLPTR, /* data_encryptor */ NULLPTR,
        /* enable_checksum */ false);
    return std::static_pointer_cast<parquet::Int32Writer>(
        ColumnWriter::Make(metadata_.get(), std::move(pager), writer_properties_.get()));
  }

  std::shared_ptr<::arrow::io::BufferOutputStream> sink_;
  std::shared_ptr<GroupNode> node_;
  std::unique_ptr<SchemaDescriptor> schema_descriptor_;

  std::shared_ptr<WriterProperties> writer_properties_;
  std::unique_ptr<ColumnChunkMetaDataBuilder> metadata_;
};

TEST_F(ColumnWriterTestSizeEstimated, NonBuffered) {
  auto required_writer =
      this->BuildWriter(Compression::UNCOMPRESSED, /* buffered*/ false);
  // Write half page, page will not be flushed after loop
  for (int32_t i = 0; i < 50; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &i);
  }
  // Page not flushed, check size
  EXPECT_EQ(0, required_writer->total_bytes_written());
  EXPECT_EQ(0, required_writer->total_compressed_bytes());  // unbuffered
  EXPECT_EQ(0, required_writer->total_compressed_bytes_written());
  // Write half page, page be flushed after loop
  for (int32_t i = 0; i < 50; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &i);
  }
  // Page flushed, check size
  EXPECT_LT(400, required_writer->total_bytes_written());
  EXPECT_EQ(0, required_writer->total_compressed_bytes());
  EXPECT_LT(400, required_writer->total_compressed_bytes_written());

  // Test after closed
  int64_t written_size = required_writer->Close();
  EXPECT_EQ(0, required_writer->total_compressed_bytes());
  EXPECT_EQ(written_size, required_writer->total_bytes_written());
  // uncompressed writer should be equal
  EXPECT_EQ(written_size, required_writer->total_compressed_bytes_written());
}

TEST_F(ColumnWriterTestSizeEstimated, Buffered) {
  auto required_writer = this->BuildWriter(Compression::UNCOMPRESSED, /* buffered*/ true);
  // Write half page, page will not be flushed after loop
  for (int32_t i = 0; i < 50; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &i);
  }
  // Page not flushed, check size
  EXPECT_EQ(0, required_writer->total_bytes_written());
  EXPECT_EQ(0, required_writer->total_compressed_bytes());  // buffered
  EXPECT_EQ(0, required_writer->total_compressed_bytes_written());
  // Write half page, page be flushed after loop
  for (int32_t i = 0; i < 50; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &i);
  }
  // Page flushed, check size
  EXPECT_LT(400, required_writer->total_bytes_written());
  EXPECT_EQ(0, required_writer->total_compressed_bytes());
  EXPECT_LT(400, required_writer->total_compressed_bytes_written());

  // Test after closed
  int64_t written_size = required_writer->Close();
  EXPECT_EQ(0, required_writer->total_compressed_bytes());
  EXPECT_EQ(written_size, required_writer->total_bytes_written());
  // uncompressed writer should be equal
  EXPECT_EQ(written_size, required_writer->total_compressed_bytes_written());
}

TEST_F(ColumnWriterTestSizeEstimated, NonBufferedDictionary) {
  auto required_writer =
      this->BuildWriter(Compression::UNCOMPRESSED, /* buffered*/ false, true);
  // for dict, keep all values equal
  int32_t dict_value = 1;
  for (int32_t i = 0; i < 50; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &dict_value);
  }
  // Page not flushed, check size
  EXPECT_EQ(0, required_writer->total_bytes_written());
  EXPECT_EQ(0, required_writer->total_compressed_bytes());
  EXPECT_EQ(0, required_writer->total_compressed_bytes_written());
  // write a huge batch to trigger page flush
  for (int32_t i = 0; i < 50000; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &dict_value);
  }
  // Page flushed, check size
  EXPECT_EQ(0, required_writer->total_bytes_written());
  EXPECT_LT(400, required_writer->total_compressed_bytes());
  EXPECT_EQ(0, required_writer->total_compressed_bytes_written());

  required_writer->Close();

  // Test after closed
  int64_t written_size = required_writer->Close();
  EXPECT_EQ(0, required_writer->total_compressed_bytes());
  EXPECT_EQ(written_size, required_writer->total_bytes_written());
  // uncompressed writer should be equal
  EXPECT_EQ(written_size, required_writer->total_compressed_bytes_written());
}

TEST_F(ColumnWriterTestSizeEstimated, BufferedCompression) {
#ifndef ARROW_WITH_SNAPPY
  GTEST_SKIP() << "Test requires snappy compression";
#endif
  auto required_writer = this->BuildWriter(Compression::SNAPPY, true);

  // Write half page
  for (int32_t i = 0; i < 50; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &i);
  }
  // Page not flushed, check size
  EXPECT_EQ(0, required_writer->total_bytes_written());
  EXPECT_EQ(0, required_writer->total_compressed_bytes());  // buffered
  EXPECT_EQ(0, required_writer->total_compressed_bytes_written());
  for (int32_t i = 0; i < 50; i++) {
    required_writer->WriteBatch(1, nullptr, nullptr, &i);
  }
  // Page flushed, check size
  EXPECT_LT(400, required_writer->total_bytes_written());
  EXPECT_EQ(0, required_writer->total_compressed_bytes());
  EXPECT_LT(required_writer->total_compressed_bytes_written(),
            required_writer->total_bytes_written());

  // Test after closed
  int64_t written_size = required_writer->Close();
  EXPECT_EQ(0, required_writer->total_compressed_bytes());
  EXPECT_EQ(written_size, required_writer->total_bytes_written());
  EXPECT_GT(written_size, required_writer->total_compressed_bytes_written());
}

TEST(TestColumnWriter, WriteDataPageV2HeaderNullCount) {
  auto sink = CreateOutputStream();
  auto list_type = GroupNode::Make("list", Repetition::REPEATED,
                                   {schema::Int32("elem", Repetition::OPTIONAL)});
  auto schema = std::static_pointer_cast<GroupNode>(GroupNode::Make(
      "schema", Repetition::REQUIRED,
      {
          schema::Int32("non_null", Repetition::OPTIONAL),
          schema::Int32("half_null", Repetition::OPTIONAL),
          schema::Int32("all_null", Repetition::OPTIONAL),
          GroupNode::Make("half_null_list", Repetition::OPTIONAL, {list_type}),
          GroupNode::Make("half_empty_list", Repetition::OPTIONAL, {list_type}),
          GroupNode::Make("half_list_of_null", Repetition::OPTIONAL, {list_type}),
          GroupNode::Make("all_single_list", Repetition::OPTIONAL, {list_type}),
      }));
  auto properties = WriterProperties::Builder()
                        /* Use V2 data page to read null_count from header */
                        .data_page_version(ParquetDataPageVersion::V2)
                        /* Disable stats to test null_count is properly set */
                        ->disable_statistics()
                        ->disable_dictionary()
                        ->build();
  auto file_writer = ParquetFileWriter::Open(sink, schema, properties);
  auto rg_writer = file_writer->AppendRowGroup();

  constexpr int32_t num_rows = 10;
  constexpr int32_t num_cols = 7;
  const std::vector<std::vector<int16_t>> def_levels_by_col = {
      {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, {1, 0, 1, 0, 1, 0, 1, 0, 1, 0},
      {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {0, 3, 0, 3, 0, 3, 0, 3, 0, 3},
      {1, 3, 1, 3, 1, 3, 1, 3, 1, 3}, {2, 3, 2, 3, 2, 3, 2, 3, 2, 3},
      {3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
  };
  const std::vector<int16_t> ref_levels(num_rows, 0);
  const std::vector<int32_t> values(num_rows, 123);
  const std::vector<int64_t> expect_null_count_by_col = {0, 5, 10, 5, 5, 5, 0};

  for (int32_t i = 0; i < num_cols; ++i) {
    auto writer = static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
    writer->WriteBatch(num_rows, def_levels_by_col[i].data(),
                       i >= 3 ? ref_levels.data() : nullptr, values.data());
  }

  ASSERT_NO_THROW(file_writer->Close());
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(
      std::make_shared<::arrow::io::BufferReader>(buffer), default_reader_properties());
  auto metadata = file_reader->metadata();
  ASSERT_EQ(1, metadata->num_row_groups());
  auto row_group_reader = file_reader->RowGroup(0);

  std::shared_ptr<Page> page;
  for (int32_t i = 0; i < num_cols; ++i) {
    auto page_reader = row_group_reader->GetColumnPageReader(i);
    int64_t num_nulls_read = 0;
    int64_t num_rows_read = 0;
    int64_t num_values_read = 0;
    while ((page = page_reader->NextPage()) != nullptr) {
      auto data_page = std::static_pointer_cast<DataPageV2>(page);
      num_nulls_read += data_page->num_nulls();
      num_rows_read += data_page->num_rows();
      num_values_read += data_page->num_values();
    }
    EXPECT_EQ(expect_null_count_by_col[i], num_nulls_read);
    EXPECT_EQ(num_rows, num_rows_read);
    EXPECT_EQ(num_rows, num_values_read);
  }
}

}  // namespace test
}  // namespace parquet
