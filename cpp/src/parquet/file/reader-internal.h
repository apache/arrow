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

#ifndef PARQUET_FILE_READER_INTERNAL_H
#define PARQUET_FILE_READER_INTERNAL_H

#include <cstdint>
#include <memory>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/compression/codec.h"
#include "parquet/file/reader.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/types.h"
#include "parquet/util/input.h"

namespace parquet_cpp {

class SchemaDescriptor;

// 16 MB is the default maximum page header size
static constexpr uint32_t DEFAULT_MAX_PAGE_HEADER_SIZE = 16 * 1024 * 1024;

// 16 KB is the default expected page header size
static constexpr uint32_t DEFAULT_PAGE_HEADER_SIZE = 16 * 1024;

// This subclass delimits pages appearing in a serialized stream, each preceded
// by a serialized Thrift parquet::PageHeader indicating the type of each page
// and the page metadata.
class SerializedPageReader : public PageReader {
 public:
  SerializedPageReader(std::unique_ptr<InputStream> stream,
      Compression::type codec);

  virtual ~SerializedPageReader() {}

  // Implement the PageReader interface
  virtual std::shared_ptr<Page> NextPage();

  void set_max_page_header_size(uint32_t size) {
    max_page_header_size_ = size;
  }

 private:
  std::unique_ptr<InputStream> stream_;

  parquet::PageHeader current_page_header_;
  std::shared_ptr<Page> current_page_;

  // Compression codec to use.
  std::unique_ptr<Codec> decompressor_;
  std::vector<uint8_t> decompression_buffer_;
  // Maximum allowed page size
  uint32_t max_page_header_size_;
};

// RowGroupReader::Contents implementation for the Parquet file specification
class SerializedRowGroup : public RowGroupReader::Contents {
 public:
  SerializedRowGroup(RandomAccessSource* source,
      const parquet::RowGroup* metadata) :
      source_(source),
      metadata_(metadata) {}

  virtual int num_columns() const;
  virtual std::unique_ptr<PageReader> GetColumnPageReader(int i);
  virtual RowGroupStatistics GetColumnStats(int i);

 private:
  RandomAccessSource* source_;
  const parquet::RowGroup* metadata_;
};

// An implementation of ParquetFileReader::Contents that deals with the Parquet
// file structure, Thrift deserialization, and other internal matters

class SerializedFile : public ParquetFileReader::Contents {
 public:
  // Open the valid and validate the header, footer, and parse the Thrift metadata
  //
  // This class does _not_ take ownership of the data source. You must manage its
  // lifetime separately
  static std::unique_ptr<ParquetFileReader::Contents> Open(
      std::unique_ptr<RandomAccessSource> source);
  virtual void Close();
  virtual std::shared_ptr<RowGroupReader> GetRowGroup(int i);
  virtual int64_t num_rows() const;
  virtual int num_columns() const;
  virtual int num_row_groups() const;

 private:
  // This class takes ownership of the provided data source
  explicit SerializedFile(std::unique_ptr<RandomAccessSource> source);

  std::unique_ptr<RandomAccessSource> source_;
  parquet::FileMetaData metadata_;

  void ParseMetaData();
};

} // namespace parquet_cpp

#endif // PARQUET_FILE_READER_INTERNAL_H
