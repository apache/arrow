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
#include <vector>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace arrow {
namespace dataset {

/// \brief A granular piece of a Dataset, such as an individual file,
/// which can be read/scanned separately from other fragments
class ARROW_DS_EXPORT DataFragment {
 public:
  /// \brief Return true if the fragment can benefit from parallel
  /// scanning
  virtual bool splittable() const = 0;
};

/// \brief A basic component of a Dataset which yields zero or more
/// DataFragments
class ARROW_DS_EXPORT DataSource {
 public:
  virtual ~DataSource() = default;

  virtual std::unique_ptr<DataFragmentIterator> GetFragments() = 0;
};

/// \brief A DataSource consisting of a flat sequence of DataFragments
class ARROW_DS_EXPORT SimpleDataSource : public DataSource {
 public:
  std::unique_ptr<DataFragmentIterator> GetFragments() override;

 private:
  DataFragmentVector fragments_;
};

/// \brief Top-level interface for a Dataset with fragments coming
/// from possibly multiple sources
class ARROW_DS_EXPORT Dataset : public std::enable_shared_from_this<Dataset> {
 public:
  explicit Dataset(const std::shared_ptr<DataSource>& source);
  explicit Dataset(const std::vector<std::shared_ptr<DataSource>>& sources);

  virtual ~Dataset() = default;

  /// \brief Begin to build a new Scan operation against this Dataset
  ScannerBuilder NewScan() const;

  const std::vector<std::shared_ptr<DataSource>>& sources() const { return sources_; }

 protected:
  std::vector<std::shared_ptr<DataSource>> sources_;
};

}  // namespace dataset
}  // namespace arrow
