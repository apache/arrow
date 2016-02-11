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

#ifndef PARQUET_COLUMN_PAGE_H
#define PARQUET_COLUMN_PAGE_H

#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

// Note: Copying the specific page header Thrift metadata to the Page object
// (instead of using a pointer) presently so that data pages can be
// decompressed and processed in parallel. We can turn the header members of
// these classes into pointers at some point, but the downside is that
// applications materializing multiple data pages at once will have to have a
// data container that manages the lifetime of the deserialized
// parquet::PageHeader structs.
//
// TODO: Parallel processing is not yet safe because of memory-ownership
// semantics (the PageReader may or may not own the memory referenced by a
// page)
class Page {
  // TODO(wesm): In the future Parquet implementations may store the crc code
  // in parquet::PageHeader. parquet-mr currently does not, so we also skip it
  // here, both on the read and write path
 public:
  Page(const uint8_t* buffer, size_t buffer_size, parquet::PageType::type type) :
      buffer_(buffer),
      buffer_size_(buffer_size),
      type_(type) {}

  parquet::PageType::type type() const {
    return type_;
  }

  // @returns: a pointer to the page's data
  const uint8_t* data() const {
    return buffer_;
  }

  // @returns: the total size in bytes of the page's data buffer
  size_t size() const {
    return buffer_size_;
  }

 private:
  const uint8_t* buffer_;
  size_t buffer_size_;

  parquet::PageType::type type_;
};


class DataPage : public Page {
 public:
  DataPage(const uint8_t* buffer, size_t buffer_size,
      const parquet::DataPageHeader& header) :
      Page(buffer, buffer_size, parquet::PageType::DATA_PAGE),
      header_(header) {}

  size_t num_values() const {
    return header_.num_values;
  }

  parquet::Encoding::type encoding() const {
    return header_.encoding;
  }

  parquet::Encoding::type repetition_level_encoding() const {
    return header_.repetition_level_encoding;
  }

  parquet::Encoding::type definition_level_encoding() const {
    return header_.definition_level_encoding;
  }

 private:
  parquet::DataPageHeader header_;
};


class DataPageV2 : public Page {
 public:
  DataPageV2(const uint8_t* buffer, size_t buffer_size,
      const parquet::DataPageHeaderV2& header) :
      Page(buffer, buffer_size, parquet::PageType::DATA_PAGE_V2),
      header_(header) {}

 private:
  parquet::DataPageHeaderV2 header_;
};


class DictionaryPage : public Page {
 public:
  DictionaryPage(const uint8_t* buffer, size_t buffer_size,
      const parquet::DictionaryPageHeader& header) :
      Page(buffer, buffer_size, parquet::PageType::DICTIONARY_PAGE),
      header_(header) {}

  size_t num_values() const {
    return header_.num_values;
  }

 private:
  parquet::DictionaryPageHeader header_;
};

// Abstract page iterator interface. This way, we can feed column pages to the
// ColumnReader through whatever mechanism we choose
class PageReader {
 public:
  virtual ~PageReader() {}

  // @returns: shared_ptr<Page>(nullptr) on EOS, std::shared_ptr<Page>
  // containing new Page otherwise
  virtual std::shared_ptr<Page> NextPage() = 0;
};

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_PAGE_H
