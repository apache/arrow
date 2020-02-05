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

#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

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
  std::string type_name() const override { return "parquet"; }

  Result<bool> IsSupported(const FileSource& source) const override;

  /// \brief Return the schema of the file if possible.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  /// \brief Open a file for scanning
  Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                    std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context) const override;

  Result<std::shared_ptr<Fragment>> MakeFragment(
      const FileSource& source, std::shared_ptr<ScanOptions> options) override;

 private:
  Result<std::unique_ptr<::parquet::ParquetFileReader>> OpenReader(
      const FileSource& source, MemoryPool* pool) const;
};

class ARROW_DS_EXPORT ParquetFragment : public FileFragment {
 public:
  ParquetFragment(const FileSource& source, std::shared_ptr<ScanOptions> options)
      : FileFragment(source, std::make_shared<ParquetFileFormat>(), options) {}

  bool splittable() const override { return true; }
};

Result<std::shared_ptr<Expression>> RowGroupStatisticsAsExpression(
    const parquet::RowGroupMetaData& metadata);

}  // namespace dataset
}  // namespace arrow
