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

#include "parquet/column/serialized-page.h"

#include <memory>

#include "parquet/exception.h"
#include "parquet/thrift/util.h"

using parquet::PageType;

namespace parquet_cpp {

// ----------------------------------------------------------------------
// SerializedPageReader deserializes Thrift metadata and pages that have been
// assembled in a serialized stream for storing in a Parquet files

SerializedPageReader::SerializedPageReader(std::unique_ptr<InputStream> stream,
    parquet::CompressionCodec::type codec) :
    stream_(std::move(stream)) {
  max_page_header_size_ = DEFAULT_MAX_PAGE_HEADER_SIZE;
  switch (codec) {
    case parquet::CompressionCodec::UNCOMPRESSED:
      break;
    case parquet::CompressionCodec::SNAPPY:
      decompressor_.reset(new SnappyCodec());
      break;
    default:
      ParquetException::NYI("Reading compressed data");
  }
}


std::shared_ptr<Page> SerializedPageReader::NextPage() {
  // Loop here because there may be unhandled page types that we skip until
  // finding a page that we do know what to do with
  while (true) {
    int64_t bytes_read = 0;
    int64_t bytes_available = 0;
    uint32_t header_size = 0;
    const uint8_t* buffer;
    uint32_t allowed_page_size = DEFAULT_PAGE_HEADER_SIZE;
    std::stringstream ss;

    // Page headers can be very large because of page statistics
    // We try to deserialize a larger buffer progressively
    // until a maximum allowed header limit
    while (true) {
      buffer = stream_->Peek(allowed_page_size, &bytes_available);
      if (bytes_available == 0) {
        return std::shared_ptr<Page>(nullptr);
      }

      // This gets used, then set by DeserializeThriftMsg
      header_size = bytes_available;
      try {
        DeserializeThriftMsg(buffer, &header_size, &current_page_header_);
        break;
      } catch (std::exception& e) {
        // Failed to deserialize. Double the allowed page header size and try again
        ss << e.what();
        allowed_page_size *= 2;
        if (allowed_page_size > max_page_header_size_) {
          ss << "Deserializing page header failed.\n";
          throw ParquetException(ss.str());
        }
      }
    }
    // Advance the stream offset
    stream_->Read(header_size, &bytes_read);

    int compressed_len = current_page_header_.compressed_page_size;
    int uncompressed_len = current_page_header_.uncompressed_page_size;

    // Read the compressed data page.
    buffer = stream_->Read(compressed_len, &bytes_read);
    if (bytes_read != compressed_len) ParquetException::EofException();

    // Uncompress it if we need to
    if (decompressor_ != NULL) {
      // Grow the uncompressed buffer if we need to.
      if (uncompressed_len > decompression_buffer_.size()) {
        decompression_buffer_.resize(uncompressed_len);
      }
      decompressor_->Decompress(compressed_len, buffer, uncompressed_len,
          &decompression_buffer_[0]);
      buffer = &decompression_buffer_[0];
    }

    if (current_page_header_.type == PageType::DICTIONARY_PAGE) {
      return std::make_shared<DictionaryPage>(buffer, uncompressed_len,
          current_page_header_.dictionary_page_header);
    } else if (current_page_header_.type == PageType::DATA_PAGE) {
      return std::make_shared<DataPage>(buffer, uncompressed_len,
          current_page_header_.data_page_header);
    } else if (current_page_header_.type == PageType::DATA_PAGE_V2) {
      ParquetException::NYI("data page v2");
    } else {
      // We don't know what this page type is. We're allowed to skip non-data
      // pages.
      continue;
    }
  }
  return std::shared_ptr<Page>(nullptr);
}

} // namespace parquet_cpp
