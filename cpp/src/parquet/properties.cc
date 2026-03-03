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

std::shared_ptr<ArrowInputStream> ReaderProperties::GetStream(
    std::shared_ptr<ArrowInputFile> source, int64_t start, int64_t num_bytes) {
  if (buffered_stream_enabled_) {
    // ARROW-6180 / PARQUET-1636 Create isolated reader that references segment
    // of source
    PARQUET_ASSIGN_OR_THROW(
        std::shared_ptr<::arrow::io::InputStream> safe_stream,
        ::arrow::io::RandomAccessFile::GetStream(source, start, num_bytes));
    PARQUET_ASSIGN_OR_THROW(
        auto stream, ::arrow::io::BufferedInputStream::Create(buffer_size_, pool_,
                                                              safe_stream, num_bytes));
    return stream;
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

void WriterProperties::Builder::CopyColumnSpecificProperties(
    const WriterProperties& properties) {
  for (const auto& [col_path, col_props] : properties.column_properties_) {
    if (col_props.statistics_enabled() !=
        default_column_properties_.statistics_enabled()) {
      if (col_props.statistics_enabled()) {
        this->enable_statistics(col_path);
      } else {
        this->disable_statistics(col_path);
      }
    }

    if (col_props.dictionary_enabled() !=
        default_column_properties_.dictionary_enabled()) {
      if (col_props.dictionary_enabled()) {
        this->enable_dictionary(col_path);
      } else {
        this->disable_dictionary(col_path);
      }
    }

    if (col_props.page_index_enabled() !=
        default_column_properties_.page_index_enabled()) {
      if (col_props.page_index_enabled()) {
        this->enable_write_page_index(col_path);
      } else {
        this->disable_write_page_index(col_path);
      }
    }

    if (col_props.compression() != default_column_properties_.compression()) {
      this->compression(col_path, col_props.compression());
    }

    if (col_props.compression_level() != default_column_properties_.compression_level()) {
      this->compression_level(col_path, col_props.compression_level());
    }

    if (col_props.encoding() != default_column_properties_.encoding()) {
      this->encoding(col_path, col_props.encoding());
    }

    if (col_props.bloom_filter_options().has_value()) {
      this->enable_bloom_filter(col_path, col_props.bloom_filter_options().value());
    }
  }
}

}  // namespace parquet
