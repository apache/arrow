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

#include <sstream>
#include <utility>

#include "parquet/properties.h"

#include "arrow/io/buffered.h"
#include "arrow/util/logging.h"

namespace parquet {

std::shared_ptr<ArrowInputStream> ReaderProperties::GetStream(
    std::shared_ptr<ArrowInputFile> source, int64_t start, int64_t num_bytes) {
  if (buffered_stream_enabled_) {
    // ARROW-6180 / PARQUET-1636 Create isolated reader that references segment
    // of source
    std::shared_ptr<::arrow::io::InputStream> safe_stream =
        ::arrow::io::RandomAccessFile::GetStream(source, start, num_bytes);
    std::shared_ptr<::arrow::io::BufferedInputStream> stream;
    PARQUET_THROW_NOT_OK(::arrow::io::BufferedInputStream::Create(
        buffer_size_, pool_, safe_stream, &stream, num_bytes));
    return std::move(stream);
  } else {
    std::shared_ptr<Buffer> data;
    PARQUET_THROW_NOT_OK(source->ReadAt(start, num_bytes, &data));

    if (data->size() != num_bytes) {
      std::stringstream ss;
      ss << "Tried reading " << num_bytes << " bytes starting at position " << start
         << " from file but only got " << data->size();
      throw ParquetException(ss.str());
    }
    return std::make_shared<::arrow::io::BufferReader>(data);
  }
}

ArrowReaderProperties default_arrow_reader_properties() {
  static ArrowReaderProperties default_reader_props;
  return default_reader_props;
}

std::shared_ptr<ArrowWriterProperties> default_arrow_writer_properties() {
  static std::shared_ptr<ArrowWriterProperties> default_writer_properties =
      ArrowWriterProperties::Builder().build();
  return default_writer_properties;
}

}  // namespace parquet
