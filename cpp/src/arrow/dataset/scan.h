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

#include "arrow/util/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace dataset {

class Dataset;

class ARROW_EXPORT ScanTask {
 public:
  RecordBatchIterator
};

class ARROW_EXPORT Scanner {
 public:
 protected:
  friend class ScannerBuilder;
};

class ARROW_EXPORT ScannerBuilder {
 public:
  /// \brief Set
  ScannerBuilder* Project(const std::vector<std::string>& columns) const;

  ScannerBuilder* AddFilter(const std::shared_ptr<Filter>& filter) const;

  /// \brief If true (default), add
  ScannerBuilder* IncludePartitionKeys(bool include = true) const;

  /// \brief Return the constructed now-immutable Scanner object
  std::unique_ptr<Scanner> Finish() const;

 private:
  std::shared_ptr<Dataset> dataset_;
  std::vector<std::string> project_columns_;
  std::vector<std::shared_ptr<Filter>> filters_;
  bool include_partition_keys_;
};

}  // namespace dataset
}  // namespace arrow
