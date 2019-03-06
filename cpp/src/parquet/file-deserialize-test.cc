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

#include <cstdint>
#include <cstring>
#include <memory>

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/thrift.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"
#include "parquet/util/test-common.h"

#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/util/compression.h"

namespace parquet {

#define ASSERT_OK(expr)         \
  do {                          \
    ::arrow::Status s = (expr); \
    if (!s.ok()) {              \
      FAIL() << s.ToString();   \
    }                           \
  } while (0)

using ::arrow::io::BufferReader;

// Adds page statistics occupying a certain amount of bytes (for testing very
// large page headers)
static inline void AddDummyStats(int stat_size, format::DataPageHeader& data_page) {
  std::vector<uint8_t> stat_bytes(stat_size);
  // Some non-zero value
  std::fill(stat_bytes.begin(), stat_bytes.end(), 1);
  data_page.statistics.__set_max(
      std::string(reinterpret_cast<const char*>(stat_bytes.data()), stat_size));
  data_page.__isset.statistics = true;
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
                                Compression::type codec = Compression::UNCOMPRESSED) {
    EndStream();
    std::unique_ptr<InputStream> stream;
    stream.reset(new InMemoryInputStream(out_buffer_));
    page_reader_ = PageReader::Open(std::move(stream), num_rows, codec);
  }

  void WriteDataPageHeader(int max_serialized_len = 1024, int32_t uncompressed_size = 0,
                           int32_t compressed_size = 0) {
    // Simplifying writing serialized data page headers which may or may not
    // have meaningful data associated with them

    // Serialize the Page header
    page_header_.__set_data_page_header(data_page_header_);
    page_header_.uncompressed_page_size = uncompressed_size;
    page_header_.compressed_page_size = compressed_size;
    page_header_.type = format::PageType::DATA_PAGE;

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

  void ResetStream() { out_stream_.reset(new InMemoryOutputStream); }

  void EndStream() { out_buffer_ = out_stream_->GetBuffer(); }

 protected:
  std::unique_ptr<InMemoryOutputStream> out_stream_;
  std::shared_ptr<Buffer> out_buffer_;

  std::unique_ptr<PageReader> page_reader_;
  format::PageHeader page_header_;
  format::DataPageHeader data_page_header_;
  format::DataPageHeaderV2 data_page_header_v2_;
};

void CheckDataPageHeader(const format::DataPageHeader expected, const Page* page) {
  ASSERT_EQ(PageType::DATA_PAGE, page->type());

  const DataPageV1* data_page = static_cast<const DataPageV1*>(page);
  ASSERT_EQ(expected.num_values, data_page->num_values());
  ASSERT_EQ(expected.encoding, data_page->encoding());
  ASSERT_EQ(expected.definition_level_encoding, data_page->definition_level_encoding());
  ASSERT_EQ(expected.repetition_level_encoding, data_page->repetition_level_encoding());

  if (expected.statistics.__isset.max) {
    ASSERT_EQ(expected.statistics.max, data_page->statistics().max());
  }
  if (expected.statistics.__isset.min) {
    ASSERT_EQ(expected.statistics.min, data_page->statistics().min());
  }
}

// Overload for DataPageV2 tests.
void CheckDataPageHeader(const format::DataPageHeaderV2 expected, const Page* page) {
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

  // TODO: Tests for DataPageHeaderV2 statistics.
}

TEST_F(TestPageSerde, DataPageV1) {
  format::PageHeader out_page_header;

  int stats_size = 512;
  const int32_t num_rows = 4444;
  AddDummyStats(stats_size, data_page_header_);
  data_page_header_.num_values = num_rows;

  ASSERT_NO_FATAL_FAILURE(WriteDataPageHeader());
  InitSerializedPageReader(num_rows);
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  ASSERT_NO_FATAL_FAILURE(CheckDataPageHeader(data_page_header_, current_page.get()));
}

TEST_F(TestPageSerde, DataPageV2) {
  format::PageHeader out_page_header;

  const int32_t num_rows = 4444;
  data_page_header_.num_values = num_rows;

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
  ASSERT_GE(max_header_size, out_stream_->Tell());

  // check header size is between 256 KB to 16 MB
  ASSERT_LE(stats_size, out_stream_->Tell());
  ASSERT_GE(kDefaultMaxPageHeaderSize, out_stream_->Tell());

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
  ASSERT_GE(max_header_size, out_stream_->Tell());

  int smaller_max_size = 128 * 1024;
  ASSERT_LE(smaller_max_size, out_stream_->Tell());
  InitSerializedPageReader(num_rows);

  // Set the max page header size to 128 KB, which is less than the current
  // header size
  page_reader_->set_max_page_header_size(smaller_max_size);
  ASSERT_THROW(page_reader_->NextPage(), ParquetException);
}

TEST_F(TestPageSerde, Compression) {
  std::vector<Compression::type> codec_types = {Compression::GZIP, Compression::SNAPPY,
                                                Compression::BROTLI, Compression::LZ4};
#ifdef ARROW_WITH_ZSTD
  codec_types.push_back(Compression::ZSTD);
#endif

  const int32_t num_rows = 32;  // dummy value
  data_page_header_.num_values = num_rows;

  int num_pages = 10;

  std::vector<std::vector<uint8_t>> faux_data;
  faux_data.resize(num_pages);
  for (int i = 0; i < num_pages; ++i) {
    // The pages keep getting larger
    int page_size = (i + 1) * 64;
    test::random_bytes(page_size, 0, &faux_data[i]);
  }
  for (auto codec_type : codec_types) {
    auto codec = GetCodecFromArrow(codec_type);

    std::vector<uint8_t> buffer;
    for (int i = 0; i < num_pages; ++i) {
      const uint8_t* data = faux_data[i].data();
      int data_size = static_cast<int>(faux_data[i].size());

      int64_t max_compressed_size = codec->MaxCompressedLen(data_size, data);
      buffer.resize(max_compressed_size);

      int64_t actual_size;
      ASSERT_OK(codec->Compress(data_size, data, max_compressed_size, &buffer[0],
                                &actual_size));

      ASSERT_NO_FATAL_FAILURE(
          WriteDataPageHeader(1024, data_size, static_cast<int32_t>(actual_size)));
      out_stream_->Write(buffer.data(), actual_size);
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
}

TEST_F(TestPageSerde, LZONotSupported) {
  // Must await PARQUET-530
  int data_size = 1024;
  std::vector<uint8_t> faux_data(data_size);
  ASSERT_NO_FATAL_FAILURE(WriteDataPageHeader(1024, data_size, data_size));
  out_stream_->Write(faux_data.data(), data_size);
  ASSERT_THROW(InitSerializedPageReader(data_size, Compression::LZO), ParquetException);
}

// ----------------------------------------------------------------------
// File structure tests

class TestParquetFileReader : public ::testing::Test {
 public:
  void AssertInvalidFileThrows(const std::shared_ptr<Buffer>& buffer) {
    reader_.reset(new ParquetFileReader());

    auto reader = std::make_shared<BufferReader>(buffer);
    auto wrapper = std::unique_ptr<ArrowInputFile>(new ArrowInputFile(reader));

    ASSERT_THROW(reader_->Open(ParquetFileReader::Contents::Open(std::move(wrapper))),
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
  InMemoryOutputStream stream;

  const char* magic = "PAR1";

  stream.Write(reinterpret_cast<const uint8_t*>(magic), strlen(magic));
  std::vector<uint8_t> bytes(10);
  stream.Write(bytes.data(), bytes.size());
  uint32_t metadata_len = 24;
  stream.Write(reinterpret_cast<const uint8_t*>(&metadata_len), sizeof(uint32_t));
  stream.Write(reinterpret_cast<const uint8_t*>(magic), strlen(magic));

  auto buffer = stream.GetBuffer();
  ASSERT_NO_FATAL_FAILURE(AssertInvalidFileThrows(buffer));
}

}  // namespace parquet
