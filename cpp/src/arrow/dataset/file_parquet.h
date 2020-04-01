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
#include <vector>

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

  std::string type_name() const override { return "parquet"; }

  bool splittable() const override { return true; }

  // Note: the default values are exposed in the python bindings and documented
  //       in the docstrings, if any of the default values gets changed please
  //       update there as well.
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

  Result<bool> IsSupported(const FileSource& source) const override;

  /// \brief Return the schema of the file if possible.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  /// \brief Open a file for scanning
  Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                    std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context) const override;

  /// \brief Open a file for scanning, restricted to the specified row groups.
  Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                    std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context,
                                    const std::vector<int>& row_groups) const;

  using FileFormat::MakeFragment;

  Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, std::shared_ptr<ScanOptions> options,
      std::shared_ptr<Expression> partition_expression) override;

  /// \brief Create a Fragment, restricted to the specified row groups.
  Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, std::shared_ptr<ScanOptions> options,
      std::shared_ptr<Expression> partition_expression, std::vector<int> row_groups);

  /// \brief Split a ParquetFileFragment into a Fragment for each row group.
  /// Row groups whose metadata contradicts the fragment's filter or the extra_filter
  /// will be excluded.
  Result<FragmentIterator> GetRowGroupFragments(
      const ParquetFileFragment& fragment,
      std::shared_ptr<Expression> extra_filter = scalar(true));
};

class ARROW_DS_EXPORT ParquetFileFragment : public FileFragment {
 public:
  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanContext> context) override;

  /// \brief The row groups viewed by this Fragment. This may be empty which signifies all
  /// row groups are selected.
  const std::vector<int>& row_groups() const { return row_groups_; }

 private:
  ParquetFileFragment(FileSource source, std::shared_ptr<FileFormat> format,
                      std::shared_ptr<ScanOptions> scan_options,
                      std::shared_ptr<Expression> partition_expression,
                      std::vector<int> row_groups)
      : FileFragment(std::move(source), std::move(format), std::move(scan_options),
                     std::move(partition_expression)),
        row_groups_(std::move(row_groups)) {}

  const ParquetFileFormat& parquet_format() const;

  std::vector<int> row_groups_;

  friend class ParquetFileFormat;
};

}  // namespace dataset
}  // namespace arrow
