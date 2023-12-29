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
#include "arrow/io/memory.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

namespace parquet {

namespace {

static void CheckReadPosition(int64_t offset, int64_t length,
                              const std::shared_ptr<ArrowInputFile>& source) {
  PARQUET_ASSIGN_OR_THROW(auto size, source->GetSize());

  if (offset < 0) {
    throw ParquetException("Invalid read  offset ", offset);
  }

  if (length < 0) {
    throw ParquetException("Invalid read  length ", length);
  }

  if (offset + length > size) {
    std::stringstream ss;
    ss << "Tried reading " << length << " bytes starting at position " << offset
       << " from file but only got " << (size - offset);
    throw ParquetException(ss.str());
  }
}

}  // namespace

std::shared_ptr<ArrowInputStream> ReaderProperties::GetStream(
    std::shared_ptr<ArrowInputFile> source, int64_t start, int64_t num_bytes,
    const ReadRanges* read_ranges) {
  if (read_ranges != nullptr) {
    CheckReadPosition(start, num_bytes, source);

    // Prefetch data
    PARQUET_THROW_NOT_OK(source->WillNeed(*read_ranges));
    PARQUET_ASSIGN_OR_THROW(auto input, ::arrow::io::ChunkBufferedInputStream::Create(
                                            start, num_bytes, source, *read_ranges,
                                            buffer_size_, io_merge_threshold_, pool_));
    return std::move(input);
  }

  if (buffered_stream_enabled_) {
    // ARROW-6180 / PARQUET-1636 Create isolated reader that references segment
    // of source
    PARQUET_ASSIGN_OR_THROW(
        std::shared_ptr<::arrow::io::InputStream> safe_stream,
        ::arrow::io::RandomAccessFile::GetStream(source, start, num_bytes));
    PARQUET_ASSIGN_OR_THROW(
        auto stream, ::arrow::io::BufferedInputStream::Create(buffer_size_, pool_,
                                                              safe_stream, num_bytes));
    return std::move(stream);
  } else {
    PARQUET_ASSIGN_OR_THROW(auto data, source->ReadAt(start, num_bytes));

    if (data->size() != num_bytes) {
      std::stringstream ss;
      ss << "Tried reading " << num_bytes << " bytes starting at position " << start
         << " from file but only got " << data->size();
      throw ParquetException(ss.str());
    }
    return std::make_shared<::arrow::io::BufferReader>(data);
  }
}

::arrow::internal::Executor* ArrowWriterProperties::executor() const {
  return executor_ != nullptr ? executor_ : ::arrow::internal::GetCpuThreadPool();
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
