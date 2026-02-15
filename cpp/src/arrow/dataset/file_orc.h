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
#include <optional>
#include <string>
#include <vector>

#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/io/type_fwd.h"
#include "arrow/result.h"

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

  using FileFormat::MakeFragment;

  Result<std::shared_ptr<FileFragment>> MakeFragment(
      FileSource source, compute::Expression partition_expression,
      std::shared_ptr<Schema> physical_schema) override;

  Result<std::shared_ptr<OrcFileFragment>> MakeFragment(
      FileSource source, compute::Expression partition_expression,
      std::shared_ptr<Schema> physical_schema, std::vector<int> stripe_ids);
};

/// \brief A FileFragment with ORC-specific logic for stripe-level subsetting.
///
/// OrcFileFragment provides the ability to scan ORC files at stripe granularity,
/// enabling parallel processing of sub-file splits. The caller can provide an
/// optional list of selected stripe IDs to limit the scan to specific stripes.
class ARROW_DS_EXPORT OrcFileFragment : public FileFragment {
 public:
  /// \brief Return the stripe IDs selected by this fragment.
  /// Empty vector means all stripes.
  const std::vector<int>& stripe_ids() const {
    if (stripe_ids_) return *stripe_ids_;
    static std::vector<int> empty;
    return empty;
  }

  /// \brief Return fragment which selects a subset of this fragment's stripes.
  Result<std::shared_ptr<Fragment>> Subset(std::vector<int> stripe_ids);

 private:
  OrcFileFragment(FileSource source, std::shared_ptr<FileFormat> format,
                  compute::Expression partition_expression,
                  std::shared_ptr<Schema> physical_schema,
                  std::optional<std::vector<int>> stripe_ids);

  std::optional<std::vector<int>> stripe_ids_;

  friend class OrcFileFormat;
};

/// @}

}  // namespace dataset
}  // namespace arrow
