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

// This API is EXPERIMENTAL.

#pragma once

#include <memory>
#include <string>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/io/type_fwd.h"
#include "arrow/result.h"
#include "arrow/util/mutex.h"

namespace arrow {
namespace dataset {

/// \addtogroup dataset-file-formats
///
/// @{

constexpr char kOrcTypeName[] = "orc";

/// \brief A FileFormat implementation that reads from and writes to ORC files
class ARROW_DS_EXPORT OrcFileFormat : public FileFormat {
 public:
  OrcFileFormat();

  std::string type_name() const override { return kOrcTypeName; }

  bool Equals(const FileFormat& other) const override {
    return type_name() == other.type_name();
  }

  Result<bool> IsSupported(const FileSource& source) const override;

  /// \brief Return the schema of the file if possible.
  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override;

  Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, compute::Expression partition_expression,
      std::shared_ptr<Schema> physical_schema) override;

  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<FileFragment>& file) const override;

  Future<std::optional<int64_t>> CountRows(
      const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
      const std::shared_ptr<ScanOptions>& options) override;

  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options,
      fs::FileLocator destination_locator) const override;

  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override;
};

/// \brief A FileFragment implementation for ORC files with predicate pushdown
class ARROW_DS_EXPORT OrcFileFragment : public FileFragment {
 public:
  /// \brief Filter stripes based on predicate using stripe statistics
  ///
  /// Returns indices of stripes where the predicate may be satisfied.
  /// Currently supports INT64 columns with greater-than operator only.
  ///
  /// \param predicate Arrow compute expression to evaluate
  /// \return Vector of stripe indices to read (0-based)
  Result<std::vector<int>> FilterStripes(const compute::Expression& predicate);

  /// \brief Ensure metadata is cached
  Status EnsureMetadataCached();

 private:
  OrcFileFragment(FileSource source, std::shared_ptr<FileFormat> format,
                  compute::Expression partition_expression,
                  std::shared_ptr<Schema> physical_schema);

  /// \brief Test each stripe against predicate
  ///
  /// Returns simplified expressions (one per stripe) after applying
  /// stripe statistics as guarantees.
  ///
  /// \param predicate Arrow compute expression to test
  /// \return Vector of simplified expressions
  Result<std::vector<compute::Expression>> TestStripes(
      const compute::Expression& predicate);

  // Cached metadata to avoid repeated I/O
  mutable util::Mutex metadata_mutex_;
  mutable std::shared_ptr<Schema> cached_schema_;
  mutable std::vector<int64_t> stripe_num_rows_;
  mutable bool metadata_cached_ = false;

  // Lazy evaluation structures for predicate pushdown
  // Each stripe starts with literal(true) and gets refined as fields are processed
  mutable std::vector<compute::Expression> statistics_expressions_;

  // Track which fields have been processed to avoid duplicate work
  mutable std::vector<bool> statistics_expressions_complete_;

  // Cached ORC reader for accessing stripe statistics
  mutable std::unique_ptr<adapters::orc::ORCFileReader> cached_reader_;

  friend class OrcFileFormat;
};

/// @}

}  // namespace dataset
}  // namespace arrow
