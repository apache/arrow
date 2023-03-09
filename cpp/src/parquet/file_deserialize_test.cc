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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"
#include "parquet/types.h"

#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/compression.h"
#include "arrow/util/crc32.h"

namespace parquet {

using ::arrow::io::BufferReader;
using ::parquet::DataPageStats;

// Adds page statistics occupying a certain amount of bytes (for testing very
// large page headers)
template <typename H>
static inline void AddDummyStats(int stat_size, H& header, bool fill_all_stats = false) {
  std::vector<uint8_t> stat_bytes(stat_size);
  // Some non-zero value
  std::fill(stat_bytes.begin(), stat_bytes.end(), 1);
  header.statistics.__set_max(
      std::string(reinterpret_cast<const char*>(stat_bytes.data()), stat_size));

  if (fill_all_stats) {
    header.statistics.__set_min(
        std::string(reinterpret_cast<const char*>(stat_bytes.data()), stat_size));
    header.statistics.__set_null_count(42);
    header.statistics.__set_distinct_count(1);
  }

  header.__isset.statistics = true;
}

template <typename H>
static inline void CheckStatistics(const H& expected, const EncodedStatistics& actual) {
  if (expected.statistics.__isset.max) {
    ASSERT_EQ(expected.statistics.max, actual.max());
  }
  if (expected.statistics.__isset.min) {
    ASSERT_EQ(expected.statistics.min, actual.min());
  }
  if (expected.statistics.__isset.null_count) {
    ASSERT_EQ(expected.statistics.null_count, actual.null_count);
  }
  if (expected.statistics.__isset.distinct_count) {
    ASSERT_EQ(expected.statistics.distinct_count, actual.distinct_count);
  }
}

static std::vector<Compression::type> GetSupportedCodecTypes() {
  std::vector<Compression::type> codec_types;

#ifdef ARROW_WITH_SNAPPY
  codec_types.push_back(Compression::SNAPPY);
#endif

#ifdef ARROW_WITH_BROTLI
  codec_types.push_back(Compression::BROTLI);
#endif

#ifdef ARROW_WITH_GZIP
  codec_types.push_back(Compression::GZIP);
#endif

#ifdef ARROW_WITH_LZ4
  codec_types.push_back(Compression::LZ4);
  codec_types.push_back(Compression::LZ4_HADOOP);
#endif

#ifdef ARROW_WITH_ZSTD
  codec_types.push_back(Compression::ZSTD);
#endif
  return codec_types;
}

class TestPageSerde : public ::testing::Test {
 public:
  void SetUp() {
    data_page_header_.encoding = format::Encoding::PLAIN;
    data_page_header_.definition_level_encoding = format::Encoding::RLE;
    data_page_header_.repetition_level_encoding = format::Encoding::RLE;

    ResetStream();
  }

  void InitSerializedPageReader(int64_t num_rows,
                                Compression::type codec = Compression::UNCOMPRESSED,
                                const ReaderProperties& properties = ReaderProperties()) {
    EndStream();

    auto stream = std::make_shared<::arrow::io::BufferReader>(out_buffer_);
    page_reader_ = PageReader::Open(stream, num_rows, codec, properties);
  }

  void WriteDataPageHeader(int max_serialized_len = 1024, int32_t uncompressed_size = 0,
                           int32_t compressed_size = 0,
                           std::optional<int32_t> checksum = std::nullopt) {
    // Simplifying writing serialized data page headers which may or may not
    // have meaningful data associated with them

    // Serialize the Page header
    page_header_.__set_data_page_header(data_page_header_);
    page_header_.uncompressed_page_size = uncompressed_size;
    page_header_.compressed_page_size = compressed_size;
    page_header_.type = format::PageType::DATA_PAGE;
    if (checksum.has_value()) {
      page_header_.__set_crc(checksum.value());
    }

    ThriftSerializer serializer;
    ASSERT_NO_THROW(serializer.Serialize(&page_header_, out_stream_.get()));
  }

  void WriteDataPageHeaderV2(int max_serialized_len = 1024, int32_t uncompressed_size = 0,
                             int32_t compressed_size = 0) {
    // Simplifying writing serialized data page V2 headers which may or may not
    // have meaningful data associated with them

    // Serialize the Page header
    page_header_.__set_data_page_header_v2(data_page_header_v2_);
    page_header_.uncompressed_page_size = uncompressed_size;
    page_header_.compressed_page_size = compressed_size;
    page_header_.type = format::PageType::DATA_PAGE_V2;

    ThriftSerializer serializer;
    ASSERT_NO_THROW(serializer.Serialize(&page_header_, out_stream_.get()));
  }

  void WriteDictionaryPageHeader(int32_t uncompressed_size = 0,
                                 int32_t compressed_size = 0,
                                 std::optional<int32_t> checksum = std::nullopt) {
    page_header_.__set_dictionary_page_header(dictionary_page_header_);
    page_header_.uncompressed_page_size = uncompressed_size;
    page_header_.compressed_page_size = compressed_size;
    page_header_.type = format::PageType::DICTIONARY_PAGE;
    if (checksum.has_value()) {
      page_header_.__set_crc(checksum.value());
    }

    ThriftSerializer serializer;
    ASSERT_NO_THROW(serializer.Serialize(&page_header_, out_stream_.get()));
  }

  void WriteIndexPageHeader(int32_t uncompressed_size = 0, int32_t compressed_size = 0) {
    page_header_.__set_index_page_header(index_page_header_);
    page_header_.uncompressed_page_size = uncompressed_size;
    page_header_.compressed_page_size = compressed_size;
    page_header_.type = format::PageType::INDEX_PAGE;

    ThriftSerializer serializer;
    ASSERT_NO_THROW(serializer.Serialize(&page_header_, out_stream_.get()));
  }

  void ResetStream() { out_stream_ = CreateOutputStream(); }

  void EndStream() { PARQUET_ASSIGN_OR_THROW(out_buffer_, out_stream_->Finish()); }

  void TestPageSerdeCrc(bool write_checksum, bool write_page_corrupt,
                        bool verification_checksum, bool has_dictionary = false);

 protected:
  std::shared_ptr<::arrow::io::BufferOutputStream> out_stream_;
  std::shared_ptr<Buffer> out_buffer_;

  std::unique_ptr<PageReader> page_reader_;
  format::PageHeader page_header_;
  format::DataPageHeader data_page_header_;
  format::DataPageHeaderV2 data_page_header_v2_;
  format::IndexPageHeader index_page_header_;
  format::DictionaryPageHeader dictionary_page_header_;
};

void TestPageSerde::TestPageSerdeCrc(bool write_checksum, bool write_page_corrupt,
                                     bool verification_checksum, bool has_dictionary) {
  auto codec_types = GetSupportedCodecTypes();
  codec_types.push_back(Compression::UNCOMPRESSED);
  const int32_t num_rows = 32;  // dummy value
  data_page_header_.num_values = num_rows;
  dictionary_page_header_.num_values = num_rows;

  const int num_pages = 10;

  std::vector<std::vector<uint8_t>> faux_data;
  faux_data.resize(num_pages);
  for (int i = 0; i < num_pages; ++i) {
    // The pages keep getting larger
    int page_size = (i + 1) * 64;
    test::random_bytes(page_size, 0, &faux_data[i]);
  }
  for (auto codec_type : codec_types) {
    auto codec = GetCodec(codec_type);

    std::vector<uint8_t> buffer;
    for (int i = 0; i < num_pages; ++i) {
      const uint8_t* data = faux_data[i].data();
      int data_size = static_cast<int>(faux_data[i].size());
      int64_t actual_size;
      if (codec == nullptr) {
        buffer = faux_data[i];
        actual_size = data_size;
      } else {
        int64_t max_compressed_size = codec->MaxCompressedLen(data_size, data);
        buffer.resize(max_compressed_size);

        ASSERT_OK_AND_ASSIGN(
            actual_size,
            codec->Compress(data_size, data, max_compressed_size, &buffer[0]));
      }
      std::optional<uint32_t> checksum_opt;
      if (write_checksum) {
        uint32_t checksum =
            ::arrow::internal::crc32(/* prev */ 0, buffer.data(), actual_size);
        if (write_page_corrupt) {
          checksum += 1;  // write a bad checksum
        }
        checksum_opt = checksum;
      }
      if (has_dictionary && i == 0) {
        ASSERT_NO_FATAL_FAILURE(WriteDictionaryPageHeader(
            data_size, static_cast<int32_t>(actual_size), checksum_opt));
      } else {
        ASSERT_NO_FATAL_FAILURE(WriteDataPageHeader(
            1024, data_size, static_cast<int32_t>(actual_size), checksum_opt));
      }
      ASSERT_OK(out_stream_->Write(buffer.data(), actual_size));
    }
    ReaderProperties readerProperties;
    readerProperties.set_page_checksum_verification(verification_checksum);
    InitSerializedPageReader(num_rows * num_pages, codec_type, readerProperties);

    for (int i = 0; i < num_pages; ++i) {
      if (write_checksum && write_page_corrupt && verification_checksum) {
        EXPECT_THROW_THAT([&]() { page_reader_->NextPage(); }, ParquetException,
                          ::testing::Property(
                              &ParquetException::what,
                              ::testing::HasSubstr("CRC checksum verification failed")));
      } else {
        const auto page = page_reader_->NextPage();
        const int data_size = static_cast<int>(faux_data[i].size());
        if (has_dictionary && i == 0) {
          ASSERT_EQ(PageType::DICTIONARY_PAGE, page->type());
          const auto dict_page = static_cast<const DictionaryPage*>(page.get());
          ASSERT_EQ(data_size, dict_page->size());
          ASSERT_EQ(0, memcmp(faux_data[i].data(), dict_page->data(), data_size));
        } else {
          ASSERT_EQ(PageType::DATA_PAGE, page->type());
          const auto data_page = static_cast<const DataPageV1*>(page.get());
          ASSERT_EQ(data_size, data_page->size());
          ASSERT_EQ(0, memcmp(faux_data[i].data(), data_page->data(), data_size));
        }
      }
    }

    ResetStream();
  }
}

void CheckDataPageHeader(const format::DataPageHeader& expected, const Page* page) {
  ASSERT_EQ(PageType::DATA_PAGE, page->type());

  const DataPageV1* data_page = static_cast<const DataPageV1*>(page);
  ASSERT_EQ(expected.num_values, data_page->num_values());
  ASSERT_EQ(expected.encoding, data_page->encoding());
  ASSERT_EQ(expected.definition_level_encoding, data_page->definition_level_encoding());
  ASSERT_EQ(expected.repetition_level_encoding, data_page->repetition_level_encoding());
  CheckStatistics(expected, data_page->statistics());
}

// Overload for DataPageV2 tests.
void CheckDataPageHeader(const format::DataPageHeaderV2& expected, const Page* page) {
  ASSERT_EQ(PageType::DATA_PAGE_V2, page->type());

  const DataPageV2* data_page = static_cast<const DataPageV2*>(page);
  ASSERT_EQ(expected.num_values, data_page->num_values());
  ASSERT_EQ(expected.num_nulls, data_page->num_nulls());
  ASSERT_EQ(expected.num_rows, data_page->num_rows());
  ASSERT_EQ(expected.encoding, data_page->encoding());
  ASSERT_EQ(expected.definition_levels_byte_length,
            data_page->definition_levels_byte_length());
  ASSERT_EQ(expected.repetition_levels_byte_length,
            data_page->repetition_levels_byte_length());
  ASSERT_EQ(expected.is_compressed, data_page->is_compressed());
  CheckStatistics(expected, data_page->statistics());
}

TEST_F(TestPageSerde, DataPageV1) {
  int stats_size = 512;
  const int32_t num_rows = 4444;
  AddDummyStats(stats_size, data_page_header_, /* fill_all_stats = */ true);
  data_page_header_.num_values = num_rows;

  ASSERT_NO_FATAL_FAILURE(WriteDataPageHeader());
  InitSerializedPageReader(num_rows);
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  ASSERT_NO_FATAL_FAILURE(CheckDataPageHeader(data_page_header_, current_page.get()));
}

// Templated test class to test page filtering for both format::DataPageHeader
// and format::DataPageHeaderV2.
template <typename T>
class PageFilterTest : public TestPageSerde {
 public:
  const int kNumPages = 10;
  void WriteStream();
  void WritePageWithoutStats();
  void CheckNumRows(std::optional<int32_t> num_rows, const T& header);

 protected:
  std::vector<T> data_page_headers_;
  int total_rows_ = 0;
};

template <>
void PageFilterTest<format::DataPageHeader>::WriteStream() {
  for (int i = 0; i < kNumPages; ++i) {
    // Vary the number of rows to produce different headers.
    int32_t num_rows = i + 100;
    total_rows_ += num_rows;
    int data_size = i + 1024;
    this->data_page_header_.__set_num_values(num_rows);
    this->data_page_header_.statistics.__set_min_value("A" + std::to_string(i));
    this->data_page_header_.statistics.__set_max_value("Z" + std::to_string(i));
    this->data_page_header_.statistics.__set_null_count(0);
    this->data_page_header_.statistics.__set_distinct_count(num_rows);
    this->data_page_header_.__isset.statistics = true;
    ASSERT_NO_FATAL_FAILURE(
        this->WriteDataPageHeader(/*max_serialized_len=*/1024, data_size, data_size));
    data_page_headers_.push_back(this->data_page_header_);
    // Also write data, to make sure we skip the data correctly.
    std::vector<uint8_t> faux_data(data_size);
    ASSERT_OK(this->out_stream_->Write(faux_data.data(), data_size));
  }
  this->EndStream();
}

template <>
void PageFilterTest<format::DataPageHeaderV2>::WriteStream() {
  for (int i = 0; i < kNumPages; ++i) {
    // Vary the number of rows to produce different headers.
    int32_t num_rows = i + 100;
    total_rows_ += num_rows;
    int data_size = i + 1024;
    this->data_page_header_v2_.__set_num_values(num_rows);
    this->data_page_header_v2_.__set_num_rows(num_rows);
    this->data_page_header_v2_.statistics.__set_min_value("A" + std::to_string(i));
    this->data_page_header_v2_.statistics.__set_max_value("Z" + std::to_string(i));
    this->data_page_header_v2_.statistics.__set_null_count(0);
    this->data_page_header_v2_.statistics.__set_distinct_count(num_rows);
    this->data_page_header_v2_.__isset.statistics = true;
    ASSERT_NO_FATAL_FAILURE(
        this->WriteDataPageHeaderV2(/*max_serialized_len=*/1024, data_size, data_size));
    data_page_headers_.push_back(this->data_page_header_v2_);
    // Also write data, to make sure we skip the data correctly.
    std::vector<uint8_t> faux_data(data_size);
    ASSERT_OK(this->out_stream_->Write(faux_data.data(), data_size));
  }
  this->EndStream();
}

template <>
void PageFilterTest<format::DataPageHeader>::WritePageWithoutStats() {
  int32_t num_rows = 100;
  total_rows_ += num_rows;
  int data_size = 1024;
  this->data_page_header_.__set_num_values(num_rows);
  ASSERT_NO_FATAL_FAILURE(
      this->WriteDataPageHeader(/*max_serialized_len=*/1024, data_size, data_size));
  data_page_headers_.push_back(this->data_page_header_);
  std::vector<uint8_t> faux_data(data_size);
  ASSERT_OK(this->out_stream_->Write(faux_data.data(), data_size));
  this->EndStream();
}

template <>
void PageFilterTest<format::DataPageHeaderV2>::WritePageWithoutStats() {
  int32_t num_rows = 100;
  total_rows_ += num_rows;
  int data_size = 1024;
  this->data_page_header_v2_.__set_num_values(num_rows);
  this->data_page_header_v2_.__set_num_rows(num_rows);
  ASSERT_NO_FATAL_FAILURE(
      this->WriteDataPageHeaderV2(/*max_serialized_len=*/1024, data_size, data_size));
  data_page_headers_.push_back(this->data_page_header_v2_);
  std::vector<uint8_t> faux_data(data_size);
  ASSERT_OK(this->out_stream_->Write(faux_data.data(), data_size));
  this->EndStream();
}

template <>
void PageFilterTest<format::DataPageHeader>::CheckNumRows(
    std::optional<int32_t> num_rows, const format::DataPageHeader& header) {
  ASSERT_EQ(num_rows, std::nullopt);
}

template <>
void PageFilterTest<format::DataPageHeaderV2>::CheckNumRows(
    std::optional<int32_t> num_rows, const format::DataPageHeaderV2& header) {
  ASSERT_EQ(*num_rows, header.num_rows);
}

using DataPageHeaderTypes =
    ::testing::Types<format::DataPageHeader, format::DataPageHeaderV2>;
TYPED_TEST_SUITE(PageFilterTest, DataPageHeaderTypes);

// Test that the returned encoded_statistics is nullptr when there are no
// statistics in the page header.
TYPED_TEST(PageFilterTest, TestPageWithoutStatistics) {
  this->WritePageWithoutStats();

  auto stream = std::make_shared<::arrow::io::BufferReader>(this->out_buffer_);
  this->page_reader_ =
      PageReader::Open(stream, this->total_rows_, Compression::UNCOMPRESSED);

  int num_pages = 0;
  bool is_stats_null = false;
  auto read_all_pages = [&](const DataPageStats& stats) -> bool {
    is_stats_null = stats.encoded_statistics == nullptr;
    ++num_pages;
    return false;
  };

  this->page_reader_->set_data_page_filter(read_all_pages);
  std::shared_ptr<Page> current_page = this->page_reader_->NextPage();
  ASSERT_EQ(num_pages, 1);
  ASSERT_EQ(is_stats_null, true);
  ASSERT_EQ(this->page_reader_->NextPage(), nullptr);
}

// Creates a number of pages and skips some of them with the page filter callback.
TYPED_TEST(PageFilterTest, TestPageFilterCallback) {
  this->WriteStream();

  {  // Read all pages.
     // Also check that the encoded statistics passed to the callback function
     // are right.
    auto stream = std::make_shared<::arrow::io::BufferReader>(this->out_buffer_);
    this->page_reader_ =
        PageReader::Open(stream, this->total_rows_, Compression::UNCOMPRESSED);

    std::vector<EncodedStatistics> read_stats;
    std::vector<int64_t> read_num_values;
    std::vector<std::optional<int32_t>> read_num_rows;
    auto read_all_pages = [&](const DataPageStats& stats) -> bool {
      DCHECK_NE(stats.encoded_statistics, nullptr);
      read_stats.push_back(*stats.encoded_statistics);
      read_num_values.push_back(stats.num_values);
      read_num_rows.push_back(stats.num_rows);
      return false;
    };

    this->page_reader_->set_data_page_filter(read_all_pages);
    for (int i = 0; i < this->kNumPages; ++i) {
      std::shared_ptr<Page> current_page = this->page_reader_->NextPage();
      ASSERT_NE(current_page, nullptr);
      ASSERT_NO_FATAL_FAILURE(
          CheckDataPageHeader(this->data_page_headers_[i], current_page.get()));
      auto data_page = static_cast<const DataPage*>(current_page.get());
      const EncodedStatistics encoded_statistics = data_page->statistics();
      ASSERT_EQ(read_stats[i].max(), encoded_statistics.max());
      ASSERT_EQ(read_stats[i].min(), encoded_statistics.min());
      ASSERT_EQ(read_stats[i].null_count, encoded_statistics.null_count);
      ASSERT_EQ(read_stats[i].distinct_count, encoded_statistics.distinct_count);
      ASSERT_EQ(read_num_values[i], this->data_page_headers_[i].num_values);
      this->CheckNumRows(read_num_rows[i], this->data_page_headers_[i]);
    }
    ASSERT_EQ(this->page_reader_->NextPage(), nullptr);
  }

  {  // Skip all pages.
    auto stream = std::make_shared<::arrow::io::BufferReader>(this->out_buffer_);
    this->page_reader_ =
        PageReader::Open(stream, this->total_rows_, Compression::UNCOMPRESSED);

    auto skip_all_pages = [](const DataPageStats& stats) -> bool { return true; };

    this->page_reader_->set_data_page_filter(skip_all_pages);
    std::shared_ptr<Page> current_page = this->page_reader_->NextPage();
    ASSERT_EQ(this->page_reader_->NextPage(), nullptr);
  }

  {  // Skip every other page.
    auto stream = std::make_shared<::arrow::io::BufferReader>(this->out_buffer_);
    this->page_reader_ =
        PageReader::Open(stream, this->total_rows_, Compression::UNCOMPRESSED);

    // Skip pages with even number of values.
    auto skip_even_pages = [](const DataPageStats& stats) -> bool {
      if (stats.num_values % 2 == 0) return true;
      return false;
    };

    this->page_reader_->set_data_page_filter(skip_even_pages);

    for (int i = 0; i < this->kNumPages; ++i) {
      // Only pages with odd number of values are read.
      if (i % 2 != 0) {
        std::shared_ptr<Page> current_page = this->page_reader_->NextPage();
        ASSERT_NE(current_page, nullptr);
        ASSERT_NO_FATAL_FAILURE(
            CheckDataPageHeader(this->data_page_headers_[i], current_page.get()));
      }
    }
    // We should have exhausted reading the pages by reading the odd pages only.
    ASSERT_EQ(this->page_reader_->NextPage(), nullptr);
  }
}

// Set the page filter more than once. The new filter should be effective
// on the next NextPage() call.
TYPED_TEST(PageFilterTest, TestChangingPageFilter) {
  this->WriteStream();

  auto stream = std::make_shared<::arrow::io::BufferReader>(this->out_buffer_);
  this->page_reader_ =
      PageReader::Open(stream, this->total_rows_, Compression::UNCOMPRESSED);

  // This callback will always return false.
  auto read_all_pages = [](const DataPageStats& stats) -> bool { return false; };
  this->page_reader_->set_data_page_filter(read_all_pages);
  std::shared_ptr<Page> current_page = this->page_reader_->NextPage();
  ASSERT_NE(current_page, nullptr);
  ASSERT_NO_FATAL_FAILURE(
      CheckDataPageHeader(this->data_page_headers_[0], current_page.get()));

  // This callback will skip all pages.
  auto skip_all_pages = [](const DataPageStats& stats) -> bool { return true; };
  this->page_reader_->set_data_page_filter(skip_all_pages);
  ASSERT_EQ(this->page_reader_->NextPage(), nullptr);
}

// Test that we do not skip dictionary pages.
TEST_F(TestPageSerde, DoesNotFilterDictionaryPages) {
  int data_size = 1024;
  std::vector<uint8_t> faux_data(data_size);

  ASSERT_NO_FATAL_FAILURE(
      WriteDataPageHeader(/*max_serialized_len=*/1024, data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));

  ASSERT_NO_FATAL_FAILURE(WriteDictionaryPageHeader(data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));

  ASSERT_NO_FATAL_FAILURE(
      WriteDataPageHeader(/*max_serialized_len=*/1024, data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));
  EndStream();

  // Try to read it back while asking for all data pages to be skipped.
  auto stream = std::make_shared<::arrow::io::BufferReader>(out_buffer_);
  page_reader_ = PageReader::Open(stream, /*num_rows=*/100, Compression::UNCOMPRESSED);

  auto skip_all_pages = [](const DataPageStats& stats) -> bool { return true; };

  page_reader_->set_data_page_filter(skip_all_pages);
  // The first data page is skipped, so we are now at the dictionary page.
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  ASSERT_NE(current_page, nullptr);
  ASSERT_EQ(current_page->type(), PageType::DICTIONARY_PAGE);
  // The data page after dictionary page is skipped.
  ASSERT_EQ(page_reader_->NextPage(), nullptr);
}

// Tests that we successfully skip non-data pages.
TEST_F(TestPageSerde, SkipsNonDataPages) {
  int data_size = 1024;
  std::vector<uint8_t> faux_data(data_size);
  ASSERT_NO_FATAL_FAILURE(WriteIndexPageHeader(data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));

  ASSERT_NO_FATAL_FAILURE(
      WriteDataPageHeader(/*max_serialized_len=*/1024, data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));

  ASSERT_NO_FATAL_FAILURE(WriteIndexPageHeader(data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));
  ASSERT_NO_FATAL_FAILURE(WriteIndexPageHeader(data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));

  ASSERT_NO_FATAL_FAILURE(
      WriteDataPageHeader(/*max_serialized_len=*/1024, data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));
  ASSERT_NO_FATAL_FAILURE(WriteIndexPageHeader(data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));
  EndStream();

  auto stream = std::make_shared<::arrow::io::BufferReader>(out_buffer_);
  page_reader_ = PageReader::Open(stream, /*num_rows=*/100, Compression::UNCOMPRESSED);

  // Only the two data pages are returned.
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  ASSERT_EQ(current_page->type(), PageType::DATA_PAGE);
  current_page = page_reader_->NextPage();
  ASSERT_EQ(current_page->type(), PageType::DATA_PAGE);
  ASSERT_EQ(page_reader_->NextPage(), nullptr);
}

TEST_F(TestPageSerde, DataPageV2) {
  int stats_size = 512;
  const int32_t num_rows = 4444;
  AddDummyStats(stats_size, data_page_header_v2_, /* fill_all_stats = */ true);
  data_page_header_v2_.num_values = num_rows;

  ASSERT_NO_FATAL_FAILURE(WriteDataPageHeaderV2());
  InitSerializedPageReader(num_rows);
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  ASSERT_NO_FATAL_FAILURE(CheckDataPageHeader(data_page_header_v2_, current_page.get()));
}

TEST_F(TestPageSerde, TestLargePageHeaders) {
  int stats_size = 256 * 1024;  // 256 KB
  AddDummyStats(stats_size, data_page_header_);

  // Any number to verify metadata roundtrip
  const int32_t num_rows = 4141;
  data_page_header_.num_values = num_rows;

  int max_header_size = 512 * 1024;  // 512 KB
  ASSERT_NO_FATAL_FAILURE(WriteDataPageHeader(max_header_size));

  ASSERT_OK_AND_ASSIGN(int64_t position, out_stream_->Tell());
  ASSERT_GE(max_header_size, position);

  // check header size is between 256 KB to 16 MB
  ASSERT_LE(stats_size, position);
  ASSERT_GE(kDefaultMaxPageHeaderSize, position);

  InitSerializedPageReader(num_rows);
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  ASSERT_NO_FATAL_FAILURE(CheckDataPageHeader(data_page_header_, current_page.get()));
}

TEST_F(TestPageSerde, TestFailLargePageHeaders) {
  const int32_t num_rows = 1337;  // dummy value

  int stats_size = 256 * 1024;  // 256 KB
  AddDummyStats(stats_size, data_page_header_);

  // Serialize the Page header
  int max_header_size = 512 * 1024;  // 512 KB
  ASSERT_NO_FATAL_FAILURE(WriteDataPageHeader(max_header_size));
  ASSERT_OK_AND_ASSIGN(int64_t position, out_stream_->Tell());
  ASSERT_GE(max_header_size, position);

  int smaller_max_size = 128 * 1024;
  ASSERT_LE(smaller_max_size, position);
  InitSerializedPageReader(num_rows);

  // Set the max page header size to 128 KB, which is less than the current
  // header size
  page_reader_->set_max_page_header_size(smaller_max_size);
  ASSERT_THROW(page_reader_->NextPage(), ParquetException);
}

TEST_F(TestPageSerde, Compression) {
  auto codec_types = GetSupportedCodecTypes();

  const int32_t num_rows = 32;  // dummy value
  data_page_header_.num_values = num_rows;

  const int num_pages = 10;

  std::vector<std::vector<uint8_t>> faux_data;
  faux_data.resize(num_pages);
  for (int i = 0; i < num_pages; ++i) {
    // The pages keep getting larger
    int page_size = (i + 1) * 64;
    test::random_bytes(page_size, 0, &faux_data[i]);
  }
  for (auto codec_type : codec_types) {
    auto codec = GetCodec(codec_type);

    std::vector<uint8_t> buffer;
    for (int i = 0; i < num_pages; ++i) {
      const uint8_t* data = faux_data[i].data();
      int data_size = static_cast<int>(faux_data[i].size());

      int64_t max_compressed_size = codec->MaxCompressedLen(data_size, data);
      buffer.resize(max_compressed_size);

      int64_t actual_size;
      ASSERT_OK_AND_ASSIGN(
          actual_size, codec->Compress(data_size, data, max_compressed_size, &buffer[0]));

      ASSERT_NO_FATAL_FAILURE(
          WriteDataPageHeader(1024, data_size, static_cast<int32_t>(actual_size)));
      ASSERT_OK(out_stream_->Write(buffer.data(), actual_size));
    }

    InitSerializedPageReader(num_rows * num_pages, codec_type);

    std::shared_ptr<Page> page;
    const DataPageV1* data_page;
    for (int i = 0; i < num_pages; ++i) {
      int data_size = static_cast<int>(faux_data[i].size());
      page = page_reader_->NextPage();
      data_page = static_cast<const DataPageV1*>(page.get());
      ASSERT_EQ(data_size, data_page->size());
      ASSERT_EQ(0, memcmp(faux_data[i].data(), data_page->data(), data_size));
    }

    ResetStream();
  }
}  // namespace parquet

TEST_F(TestPageSerde, LZONotSupported) {
  // Must await PARQUET-530
  int data_size = 1024;
  std::vector<uint8_t> faux_data(data_size);
  ASSERT_NO_FATAL_FAILURE(WriteDataPageHeader(1024, data_size, data_size));
  ASSERT_OK(out_stream_->Write(faux_data.data(), data_size));
  ASSERT_THROW(InitSerializedPageReader(data_size, Compression::LZO), ParquetException);
}

TEST_F(TestPageSerde, NoCrc) {
  int stats_size = 512;
  const int32_t num_rows = 4444;
  AddDummyStats(stats_size, data_page_header_, /*fill_all_stats=*/true);
  data_page_header_.num_values = num_rows;

  ASSERT_NO_FATAL_FAILURE(WriteDataPageHeader());
  ReaderProperties readerProperties;
  readerProperties.set_page_checksum_verification(true);
  InitSerializedPageReader(num_rows, Compression::UNCOMPRESSED, readerProperties);
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  ASSERT_NO_FATAL_FAILURE(CheckDataPageHeader(data_page_header_, current_page.get()));
}

TEST_F(TestPageSerde, NoCrcDict) {
  const int32_t num_rows = 4444;
  dictionary_page_header_.num_values = num_rows;

  ASSERT_NO_FATAL_FAILURE(WriteDictionaryPageHeader());
  ReaderProperties readerProperties;
  readerProperties.set_page_checksum_verification(true);
  InitSerializedPageReader(num_rows, Compression::UNCOMPRESSED, readerProperties);
  std::shared_ptr<Page> current_page = page_reader_->NextPage();

  ASSERT_EQ(PageType::DICTIONARY_PAGE, current_page->type());

  const auto* dict_page = static_cast<const DictionaryPage*>(current_page.get());
  EXPECT_EQ(num_rows, dict_page->num_values());
}

TEST_F(TestPageSerde, CrcCheckSuccessful) {
  this->TestPageSerdeCrc(/* write_checksum */ true, /* write_page_corrupt */ false,
                         /* verification_checksum */ true);
}

TEST_F(TestPageSerde, CrcCheckFail) {
  this->TestPageSerdeCrc(/* write_checksum */ true, /* write_page_corrupt */ true,
                         /* verification_checksum */ true);
}

TEST_F(TestPageSerde, CrcCorruptNotChecked) {
  this->TestPageSerdeCrc(/* write_checksum */ true, /* write_page_corrupt */ true,
                         /* verification_checksum */ false);
}

TEST_F(TestPageSerde, CrcCheckNonExistent) {
  this->TestPageSerdeCrc(/* write_checksum */ false, /* write_page_corrupt */ false,
                         /* verification_checksum */ true);
}

TEST_F(TestPageSerde, DictCrcCheckSuccessful) {
  this->TestPageSerdeCrc(/* write_checksum */ true, /* write_page_corrupt */ false,
                         /* verification_checksum */ true, /* has_dictionary */ true);
}

TEST_F(TestPageSerde, DictCrcCheckFail) {
  this->TestPageSerdeCrc(/* write_checksum */ true, /* write_page_corrupt */ true,
                         /* verification_checksum */ true, /* has_dictionary */ true);
}

TEST_F(TestPageSerde, DictCrcCorruptNotChecked) {
  this->TestPageSerdeCrc(/* write_checksum */ true, /* write_page_corrupt */ true,
                         /* verification_checksum */ false, /* has_dictionary */ true);
}

TEST_F(TestPageSerde, DictCrcCheckNonExistent) {
  this->TestPageSerdeCrc(/* write_checksum */ false, /* write_page_corrupt */ false,
                         /* verification_checksum */ true, /* has_dictionary */ true);
}

// ----------------------------------------------------------------------
// File structure tests

class TestParquetFileReader : public ::testing::Test {
 public:
  void AssertInvalidFileThrows(const std::shared_ptr<Buffer>& buffer) {
    reader_.reset(new ParquetFileReader());

    auto reader = std::make_shared<BufferReader>(buffer);

    ASSERT_THROW(reader_->Open(ParquetFileReader::Contents::Open(reader)),
                 ParquetException);
  }

 protected:
  std::unique_ptr<ParquetFileReader> reader_;
};

TEST_F(TestParquetFileReader, InvalidHeader) {
  const char* bad_header = "PAR2";

  auto buffer = Buffer::Wrap(bad_header, strlen(bad_header));
  ASSERT_NO_FATAL_FAILURE(AssertInvalidFileThrows(buffer));
}

TEST_F(TestParquetFileReader, InvalidFooter) {
  // File is smaller than FOOTER_SIZE
  const char* bad_file = "PAR1PAR";
  auto buffer = Buffer::Wrap(bad_file, strlen(bad_file));
  ASSERT_NO_FATAL_FAILURE(AssertInvalidFileThrows(buffer));

  // Magic number incorrect
  const char* bad_file2 = "PAR1PAR2";
  buffer = Buffer::Wrap(bad_file2, strlen(bad_file2));
  ASSERT_NO_FATAL_FAILURE(AssertInvalidFileThrows(buffer));
}

TEST_F(TestParquetFileReader, IncompleteMetadata) {
  auto stream = CreateOutputStream();

  const char* magic = "PAR1";

  ASSERT_OK(stream->Write(reinterpret_cast<const uint8_t*>(magic), strlen(magic)));
  std::vector<uint8_t> bytes(10);
  ASSERT_OK(stream->Write(bytes.data(), bytes.size()));
  uint32_t metadata_len = 24;
  ASSERT_OK(
      stream->Write(reinterpret_cast<const uint8_t*>(&metadata_len), sizeof(uint32_t)));
  ASSERT_OK(stream->Write(reinterpret_cast<const uint8_t*>(magic), strlen(magic)));

  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Finish());
  ASSERT_NO_FATAL_FAILURE(AssertInvalidFileThrows(buffer));
}

}  // namespace parquet
