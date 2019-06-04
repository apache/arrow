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

#include "arrow/csv/options.h"
#include "arrow/dataset/file_base.h"
#include "arrow/util/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {

namespace fs {

class FileSystem;

}  // namespace fs

namespace dataset {

class ARROW_EXPORT CsvScanOptions : public FileScanOptions {
 public:
  ///
  std::string file_type() const override;

 private:
  csv::ParseOptions parse_options_;
  csv::ConvertOptions convert_options_;
  csv::ReadOptions read_options_;
};

class ARROW_EXPORT CsvWriteOptions : public FileWriteOptions {
 public:
  virtual ~FileWriteOptions() = default;

  ///
  virtual file_type() const = 0;
};

/// \brief A FileFormat implementation that reads from CSV files
class ARROW_EXPORT CsvFileFormat : public FileFormat {
 public:
  std::string name() const override;

  /// \brief Return true if the given file extension
  bool IsKnownExtension(const std::string& ext) const override;

  /// \brief Open a file for scanning
  Status ScanFile(const std::string& path, const FileScanOptions& options,
                  fs::FileSystem* filesystem,
                  std::unique_ptr<ScanTaskIterator>* out) const override;
};

}  // namespace dataset
}  // namespace arrow
