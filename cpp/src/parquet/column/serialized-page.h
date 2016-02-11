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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#ifndef PARQUET_COLUMN_SERIALIZED_PAGE_H
#define PARQUET_COLUMN_SERIALIZED_PAGE_H

#include <memory>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/compression/codec.h"
#include "parquet/util/input.h"
#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

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
      parquet::CompressionCodec::type codec);

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

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_SERIALIZED_PAGE_H
