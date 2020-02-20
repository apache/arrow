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
#include "parquet/properties.h"

namespace parquet {
class ParquetFileReader;
class RowGroupMetaData;
class FileMetaData;
}  // namespace parquet

namespace arrow {
namespace dataset {

/// \brief A FileFormat implementation that reads from Parquet files
class ARROW_DS_EXPORT ParquetFileFormat : public FileFormat {
 public:
  static std::shared_ptr<ParquetFileFormat> Make() {
    std::shared_ptr<ParquetFileFormat> out{new ParquetFileFormat};
    out->weak_this_ = out;
    return out;
  }

  struct ReaderOptions {
    /// \defgroup parquet-file-format-reader-properties properties which correspond to
    /// members of parquet::ReaderProperties.
    ///
    /// @{
    int64_t buffer_size = parquet::DEFAULT_BUFFER_SIZE;
    std::shared_ptr<parquet::FileDecryptionProperties> file_decryption_properties;
    /// @}

    /// \defgroup parquet-file-format-arrow-reader-properties properties which correspond
    /// to members of parquet::ArrowReaderProperties.
    ///
    /// @{
    std::unordered_set<std::string> dict_columns;
    /// @}
  } reader_options;

  std::string type_name() const override { return "parquet"; }

  Result<bool> IsSupported(const FileSource& source) const override;

  /// \brief Return the schema of the file if possible.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  /// \brief Open a file for scanning
  Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                    std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context) const override;

  Result<std::shared_ptr<Fragment>> MakeFragment(
      FileSource source, std::shared_ptr<ScanOptions> options) override;

 private:
  ParquetFileFormat() = default;

  std::weak_ptr<ParquetFileFormat> weak_this_;
};

class ARROW_DS_EXPORT ParquetFragment : public FileFragment {
 public:
  ParquetFragment(FileSource source, std::shared_ptr<ParquetFileFormat> format,
                  std::shared_ptr<ScanOptions> options)
      : FileFragment(std::move(source), std::move(format), std::move(options)) {}

  bool splittable() const override { return true; }
};

}  // namespace dataset
}  // namespace arrow
