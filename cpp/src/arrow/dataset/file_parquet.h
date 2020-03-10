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

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace parquet {
class ParquetFileReader;
class RowGroupMetaData;
class FileMetaData;
class FileDecryptionProperties;
class ReaderProperties;
class ArrowReaderProperties;
}  // namespace parquet

namespace arrow {
namespace dataset {

/// \brief A FileFormat implementation that reads from Parquet files
class ARROW_DS_EXPORT ParquetFileFormat : public FileFormat {
 public:
  ParquetFileFormat() = default;

  /// Convenience constructor which copies properties from a parquet::ReaderProperties.
  /// memory_pool will be ignored.
  explicit ParquetFileFormat(const parquet::ReaderProperties& reader_properties);

  struct ReaderOptions {
    /// \defgroup parquet-file-format-reader-properties properties which correspond to
    /// members of parquet::ReaderProperties.
    ///
    /// We don't embed parquet::ReaderProperties directly because we get memory_pool from
    /// ScanContext at scan time and provide differing defaults.
    ///
    /// @{
    bool use_buffered_stream = false;
    int64_t buffer_size = 1 << 13;
    std::shared_ptr<parquet::FileDecryptionProperties> file_decryption_properties;
    /// @}

    /// \defgroup parquet-file-format-arrow-reader-properties properties which correspond
    /// to members of parquet::ArrowReaderProperties.
    ///
    /// We don't embed parquet::ReaderProperties directly because we get batch_size from
    /// ScanOptions at scan time, and we will never pass use_threads == true (since we
    /// defer parallelization of the scan). Additionally column names (rather than
    /// indices) are used to indicate dictionary columns.
    ///
    /// @{
    std::unordered_set<std::string> dict_columns;
    /// @}
  } reader_options;

  std::string type_name() const override { return "parquet"; }

  bool splittable() const override { return true; }

  Result<bool> IsSupported(const FileSource& source) const override;

  /// \brief Return the schema of the file if possible.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  /// \brief Open a file for scanning
  Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                    std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context) const override;
};

}  // namespace dataset
}  // namespace arrow
