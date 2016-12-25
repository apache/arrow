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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/compression/codec.h"
#include "parquet/exception.h"
#include "parquet/file/reader-internal.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/thrift/util.h"
#include "parquet/types.h"
#include "parquet/util/input.h"
#include "parquet/util/output.h"
#include "parquet/util/test-common.h"

namespace parquet {

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

  void InitSerializedPageReader(
      int64_t num_rows, Compression::type codec = Compression::UNCOMPRESSED) {
    EndStream();
    std::unique_ptr<InputStream> stream;
    stream.reset(new InMemoryInputStream(out_buffer_));
    page_reader_.reset(new SerializedPageReader(std::move(stream), num_rows, codec));
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

    ASSERT_NO_THROW(
        SerializeThriftMsg(&page_header_, max_serialized_len, out_stream_.get()));
  }

  void ResetStream() { out_stream_.reset(new InMemoryOutputStream); }

  void EndStream() { out_buffer_ = out_stream_->GetBuffer(); }

 protected:
  std::unique_ptr<InMemoryOutputStream> out_stream_;
  std::shared_ptr<Buffer> out_buffer_;

  std::unique_ptr<SerializedPageReader> page_reader_;
  format::PageHeader page_header_;
  format::DataPageHeader data_page_header_;
};

void CheckDataPageHeader(const format::DataPageHeader expected, const Page* page) {
  ASSERT_EQ(PageType::DATA_PAGE, page->type());

  const DataPage* data_page = static_cast<const DataPage*>(page);
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

TEST_F(TestPageSerde, DataPage) {
  format::PageHeader out_page_header;

  int stats_size = 512;
  const int32_t num_rows = 4444;
  AddDummyStats(stats_size, data_page_header_);
  data_page_header_.num_values = num_rows;

  WriteDataPageHeader();
  InitSerializedPageReader(num_rows);
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  CheckDataPageHeader(data_page_header_, current_page.get());
}

TEST_F(TestPageSerde, TestLargePageHeaders) {
  int stats_size = 256 * 1024;  // 256 KB
  AddDummyStats(stats_size, data_page_header_);

  // Any number to verify metadata roundtrip
  const int32_t num_rows = 4141;
  data_page_header_.num_values = num_rows;

  int max_header_size = 512 * 1024;  // 512 KB
  WriteDataPageHeader(max_header_size);
  ASSERT_GE(max_header_size, out_stream_->Tell());

  // check header size is between 256 KB to 16 MB
  ASSERT_LE(stats_size, out_stream_->Tell());
  ASSERT_GE(DEFAULT_MAX_PAGE_HEADER_SIZE, out_stream_->Tell());

  InitSerializedPageReader(num_rows);
  std::shared_ptr<Page> current_page = page_reader_->NextPage();
  CheckDataPageHeader(data_page_header_, current_page.get());
}

TEST_F(TestPageSerde, TestFailLargePageHeaders) {
  const int32_t num_rows = 1337;  // dummy value

  int stats_size = 256 * 1024;  // 256 KB
  AddDummyStats(stats_size, data_page_header_);

  // Serialize the Page header
  int max_header_size = 512 * 1024;  // 512 KB
  WriteDataPageHeader(max_header_size);
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
  Compression::type codec_types[3] = {
      Compression::GZIP, Compression::SNAPPY, Compression::BROTLI};

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
    std::unique_ptr<Codec> codec = Codec::Create(codec_type);

    std::vector<uint8_t> buffer;
    for (int i = 0; i < num_pages; ++i) {
      const uint8_t* data = faux_data[i].data();
      int data_size = faux_data[i].size();

      int64_t max_compressed_size = codec->MaxCompressedLen(data_size, data);
      buffer.resize(max_compressed_size);

      int64_t actual_size =
          codec->Compress(data_size, data, max_compressed_size, &buffer[0]);

      WriteDataPageHeader(1024, data_size, actual_size);
      out_stream_->Write(buffer.data(), actual_size);
    }

    InitSerializedPageReader(num_rows * num_pages, codec_type);

    std::shared_ptr<Page> page;
    const DataPage* data_page;
    for (int i = 0; i < num_pages; ++i) {
      int data_size = faux_data[i].size();
      page = page_reader_->NextPage();
      data_page = static_cast<const DataPage*>(page.get());
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
  WriteDataPageHeader(1024, data_size, data_size);
  out_stream_->Write(faux_data.data(), data_size);
  ASSERT_THROW(InitSerializedPageReader(data_size, Compression::LZO), ParquetException);
}

// ----------------------------------------------------------------------
// File structure tests

class TestParquetFileReader : public ::testing::Test {
 public:
  void AssertInvalidFileThrows(const std::shared_ptr<Buffer>& buffer) {
    std::unique_ptr<BufferReader> reader(new BufferReader(buffer));
    reader_.reset(new ParquetFileReader());

    ASSERT_THROW(
        reader_->Open(SerializedFile::Open(std::move(reader))), ParquetException);
  }

 protected:
  std::unique_ptr<ParquetFileReader> reader_;
};

TEST_F(TestParquetFileReader, InvalidHeader) {
  const char* bad_header = "PAR2";

  auto buffer = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(bad_header), strlen(bad_header));
  AssertInvalidFileThrows(buffer);
}

TEST_F(TestParquetFileReader, InvalidFooter) {
  // File is smaller than FOOTER_SIZE
  const char* bad_file = "PAR1PAR";
  auto buffer = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(bad_file), strlen(bad_file));
  AssertInvalidFileThrows(buffer);

  // Magic number incorrect
  const char* bad_file2 = "PAR1PAR2";
  buffer = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(bad_file2), strlen(bad_file2));
  AssertInvalidFileThrows(buffer);
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
  AssertInvalidFileThrows(buffer);
}

}  // namespace parquet
