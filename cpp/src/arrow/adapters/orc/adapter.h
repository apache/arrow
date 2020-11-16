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

#pragma once

#include <cstdint>
#include <memory>
#include <sstream>
#include <vector>

#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"
#include "orc/OrcFile.hh"

namespace liborc = orc;

#define ORC_THROW_NOT_OK(s)                   \
  do {                                        \
    Status _s = (s);                          \
    if (!_s.ok()) {                           \
      std::stringstream ss;                   \
      ss << "Arrow error: " << _s.ToString(); \
      throw liborc::ParseError(ss.str());     \
    }                                         \
  } while (0)

#define ORC_ASSIGN_OR_THROW_IMPL(status_name, lhs, rexpr) \
  auto status_name = (rexpr);                             \
  ORC_THROW_NOT_OK(status_name.status());                 \
  lhs = std::move(status_name).ValueOrDie();

#define ORC_ASSIGN_OR_THROW(lhs, rexpr)                                              \
  ORC_ASSIGN_OR_THROW_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                           lhs, rexpr);

namespace arrow {

namespace adapters {

namespace orc {

/// \class ORCFileReader
/// \brief Read an Arrow Table or RecordBatch from an ORC file.
class ARROW_EXPORT ORCFileReader {
 public:
  ~ORCFileReader();

  /// \brief Creates a new ORC reader.
  ///
  /// \param[in] file the data source
  /// \param[in] pool a MemoryPool to use for buffer allocations
  /// \param[out] reader the returned reader object
  /// \return Status
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& file, MemoryPool* pool,
                     std::unique_ptr<ORCFileReader>* reader);

  /// \brief Return the schema read from the ORC file
  ///
  /// \param[out] out the returned Schema object
  Status ReadSchema(std::shared_ptr<Schema>* out);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[out] out the returned Table
  Status Read(std::shared_ptr<Table>* out);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] schema the Table schema
  /// \param[out] out the returned Table
  Status Read(const std::shared_ptr<Schema>& schema, std::shared_ptr<Table>* out);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] include_indices the selected field indices to read
  /// \param[out] out the returned Table
  Status Read(const std::vector<int>& include_indices, std::shared_ptr<Table>* out);

  /// \brief Read the file as a Table
  ///
  /// The table will be composed of one record batch per stripe.
  ///
  /// \param[in] schema the Table schema
  /// \param[in] include_indices the selected field indices to read
  /// \param[out] out the returned Table
  Status Read(const std::shared_ptr<Schema>& schema,
              const std::vector<int>& include_indices, std::shared_ptr<Table>* out);

  /// \brief Read a single stripe as a RecordBatch
  ///
  /// \param[in] stripe the stripe index
  /// \param[out] out the returned RecordBatch
  Status ReadStripe(int64_t stripe, std::shared_ptr<RecordBatch>* out);

  /// \brief Read a single stripe as a RecordBatch
  ///
  /// \param[in] stripe the stripe index
  /// \param[in] include_indices the selected field indices to read
  /// \param[out] out the returned RecordBatch
  Status ReadStripe(int64_t stripe, const std::vector<int>& include_indices,
                    std::shared_ptr<RecordBatch>* out);

  /// \brief Seek to designated row. Invoke NextStripeReader() after seek
  ///        will return stripe reader starting from designated row.
  ///
  /// \param[in] row_number the rows number to seek
  Status Seek(int64_t row_number);

  /// \brief Get a stripe level record batch iterator with specified row count
  ///         in each record batch. NextStripeReader serves as a fine grain
  ///         alternative to ReadStripe which may cause OOM issue by loading
  ///         the whole stripes into memory.
  ///
  /// \param[in] batch_size the number of rows each record batch contains in
  ///            record batch iteration.
  /// \param[out] out the returned stripe reader
  Status NextStripeReader(int64_t batch_size, std::shared_ptr<RecordBatchReader>* out);

  /// \brief Get a stripe level record batch iterator with specified row count
  ///         in each record batch. NextStripeReader serves as a fine grain
  ///         alternative to ReadStripe which may cause OOM issue by loading
  ///         the whole stripes into memory.
  ///
  /// \param[in] batch_size Get a stripe level record batch iterator with specified row
  /// count in each record batch.
  ///
  /// \param[in] include_indices the selected field indices to read
  /// \param[out] out the returned stripe reader
  Status NextStripeReader(int64_t batch_size, const std::vector<int>& include_indices,
                          std::shared_ptr<RecordBatchReader>* out);

  /// \brief The number of stripes in the file
  int64_t NumberOfStripes();

  /// \brief The number of rows in the file
  int64_t NumberOfRows();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  ORCFileReader();
};

class ARROW_EXPORT ArrowWriterOptions {
 public:
  uint64_t batch_size_;
  explicit ArrowWriterOptions(uint64_t batch_size) : batch_size_(batch_size) {}
  Status set_batch_size(uint64_t batch_size) {
    batch_size_ = batch_size;
    return Status::OK();
  }
  uint64_t get_batch_size() { return batch_size_; }
};

class ArrowOutputFile : public liborc::OutputStream {
 public:
  explicit ArrowOutputFile(const std::shared_ptr<io::FileOutputStream>& file)
      : file_(file), length_(0) {}

  uint64_t getLength() const override { return length_; }

  uint64_t getNaturalWriteSize() const override { return 128 * 1024; }

  void write(const void* buf, size_t length) override {
    ORC_THROW_NOT_OK(file_->Write(buf, static_cast<int64_t>(length)));
    length_ += static_cast<int64_t>(length);
  }

  const std::string& getName() const override {
    static const std::string filename("ArrowOutputFile");
    return filename;
  }

  void close() override {
    if (!file_->closed()) {
      ORC_THROW_NOT_OK(file_->Close());
    }
  }

  int64_t get_length() { return length_; }

  void set_length(int64_t length) { length_ = length; }

 private:
  std::shared_ptr<io::FileOutputStream> file_;
  int64_t length_;
};

/// \class ORCFileWriter
/// \brief Write an Arrow Table or RecordBatch to an ORC file.
class ARROW_EXPORT ORCFileWriter {
 public:
  ~ORCFileWriter();
  /// \brief Creates a new ORC writer.
  ///
  /// \param[in] schema of the Arrow table
  /// \param[in] file the file to write into
  /// \param[in] options ORC writer options
  /// \param[in] arrow_options ORC writer options
  /// \param[out] writer the returned writer object
  /// \return Status
  static Status Open(Schema* schema, const std::shared_ptr<io::FileOutputStream>& file,
                     std::shared_ptr<liborc::WriterOptions> options,
                     std::shared_ptr<ArrowWriterOptions> arrow_options,
                     std::unique_ptr<ORCFileWriter>* writer);

  /// \brief Write a table
  ///
  /// \param[in] table the Arrow table from which data is extracted
  /// \return Status
  Status Write(const std::shared_ptr<Table> table);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  ORCFileWriter();
};

}  // namespace orc

}  // namespace adapters

}  // namespace arrow
