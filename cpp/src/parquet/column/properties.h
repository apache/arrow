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

#ifndef PARQUET_COLUMN_PROPERTIES_H
#define PARQUET_COLUMN_PROPERTIES_H

#include <memory>
#include <string>

#include "parquet/util/input.h"
#include "parquet/util/mem-allocator.h"

namespace parquet {

static int64_t DEFAULT_BUFFER_SIZE = 0;
static bool DEFAULT_USE_BUFFERED_STREAM = false;

class ReaderProperties {
 public:
  explicit ReaderProperties(MemoryAllocator* allocator = default_allocator())
      : allocator_(allocator) {
    buffered_stream_enabled_ = DEFAULT_USE_BUFFERED_STREAM;
    buffer_size_ = DEFAULT_BUFFER_SIZE;
  }

  MemoryAllocator* allocator() { return allocator_; }

  std::unique_ptr<InputStream> GetStream(
      RandomAccessSource* source, int64_t start, int64_t num_bytes) {
    std::unique_ptr<InputStream> stream;
    if (buffered_stream_enabled_) {
      stream.reset(
          new BufferedInputStream(allocator_, buffer_size_, source, start, num_bytes));
    } else {
      stream.reset(new InMemoryInputStream(source, start, num_bytes));
    }
    return stream;
  }

  bool is_buffered_stream_enabled() const { return buffered_stream_enabled_; }

  void enable_buffered_stream() { buffered_stream_enabled_ = true; }

  void disable_buffered_stream() { buffered_stream_enabled_ = false; }

  void set_buffer_size(int64_t buf_size) { buffer_size_ = buf_size; }

  int64_t buffer_size() const { return buffer_size_; }

 private:
  MemoryAllocator* allocator_;
  int64_t buffer_size_;
  bool buffered_stream_enabled_;
};

ReaderProperties default_reader_properties();

static int64_t DEFAULT_PAGE_SIZE = 1024 * 1024;
static int64_t DEFAULT_DICTIONARY_PAGE_SIZE = DEFAULT_PAGE_SIZE;
static bool DEFAULT_IS_DICTIONARY_ENABLED = true;

class WriterProperties {
 public:
  explicit WriterProperties(MemoryAllocator* allocator = default_allocator())
      : allocator_(allocator) {
    pagesize_ = DEFAULT_PAGE_SIZE;
    dictionary_pagesize_ = DEFAULT_DICTIONARY_PAGE_SIZE;
    dictionary_enabled_ = DEFAULT_IS_DICTIONARY_ENABLED;
  }

  int64_t dictionary_pagesize() const { return dictionary_pagesize_; }

  void set_dictionary_pagesize(int64_t dictionary_psize) {
    dictionary_pagesize_ = dictionary_psize;
  }

  int64_t data_pagesize() const { return pagesize_; }

  void set_data_pagesize(int64_t pg_size) { pagesize_ = pg_size; }

  void enable_dictionary() { dictionary_enabled_ = true; }

  void disable_dictionary() { dictionary_enabled_ = false; }

  bool is_dictionary_enabled() const { return dictionary_enabled_; }

  MemoryAllocator* allocator() { return allocator_; }

 private:
  int64_t pagesize_;
  int64_t dictionary_pagesize_;
  bool dictionary_enabled_;
  MemoryAllocator* allocator_;
};

WriterProperties default_writer_properties();

}  // namespace parquet

#endif  // PARQUET_COLUMN_PROPERTIES_H
