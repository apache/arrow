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
#include <utility>
#include <vector>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace dataset {

/// \brief A granular piece of a Dataset, such as an individual file,
/// which can be read/scanned separately from other fragments.
///
/// A DataFragment yields a collection of RecordBatch, encapsulated in one or
/// more ScanTasks.
class ARROW_DS_EXPORT DataFragment {
 public:
  /// \brief Scan returns an iterator of ScanTasks, each of which yields
  /// RecordBatches from this DataFragment.
  virtual Result<ScanTaskIterator> Scan(ScanContextPtr context) = 0;

  /// \brief Return true if the fragment can benefit from parallel
  /// scanning
  virtual bool splittable() const = 0;

  /// \brief Filtering, schema reconciliation, and partition options to use when
  /// scanning this fragment. May be nullptr, which indicates that no filtering
  /// or schema reconciliation will be performed and all partitions will be
  /// scanned.
  ScanOptionsPtr scan_options() const { return scan_options_; }

  virtual ~DataFragment() = default;

  /// \brief An expression which evaluates to true for all data viewed by this
  /// DataFragment. May be null, which indicates no information is available.
  const std::shared_ptr<Expression>& partition_expression() const {
    return partition_expression_;
  }

 protected:
  explicit DataFragment(ScanOptionsPtr scan_options);

  DataFragment(ScanOptionsPtr scan_options, ExpressionPtr partition_expression)
      : scan_options_(std::move(scan_options)),
        partition_expression_(std::move(partition_expression)) {}

  ScanOptionsPtr scan_options_;
  ExpressionPtr partition_expression_;
};

/// \brief A trivial DataFragment that yields ScanTask out of a fixed set of
/// RecordBatch.
class ARROW_DS_EXPORT SimpleDataFragment : public DataFragment {
 public:
  SimpleDataFragment(std::vector<std::shared_ptr<RecordBatch>> record_batches,
                     ScanOptionsPtr scan_options);

  Result<ScanTaskIterator> Scan(ScanContextPtr context) override;

  bool splittable() const override { return false; }

 protected:
  std::vector<std::shared_ptr<RecordBatch>> record_batches_;
};

/// \brief A basic component of a Dataset which yields zero or more
/// DataFragments. A DataSource acts as a discovery mechanism of DataFragments
/// and partitions, e.g. files deeply nested in a directory.
class ARROW_DS_EXPORT DataSource {
 public:
  /// \brief GetFragments returns an iterator of DataFragments. The ScanOptions
  /// controls filtering and schema inference.
  DataFragmentIterator GetFragments(ScanOptionsPtr options);

  /// \brief An expression which evaluates to true for all data viewed by this DataSource.
  /// May be null, which indicates no information is available.
  const ExpressionPtr& partition_expression() const { return partition_expression_; }

  virtual std::string type() const = 0;

  virtual ~DataSource() = default;

 protected:
  DataSource() = default;
  explicit DataSource(ExpressionPtr c) : partition_expression_(std::move(c)) {}

  virtual DataFragmentIterator GetFragmentsImpl(ScanOptionsPtr options) = 0;

  /// Mutates a ScanOptions by assuming partition_expression_ holds for all yielded
  /// fragments. Returns false if the selector is not satisfiable in this DataSource.
  virtual bool AssumePartitionExpression(const ScanOptionsPtr& scan_options,
                                         ScanOptionsPtr* simplified_scan_options) const;

  ExpressionPtr partition_expression_;
};

/// \brief A DataSource consisting of a flat sequence of DataFragments
class ARROW_DS_EXPORT SimpleDataSource : public DataSource {
 public:
  explicit SimpleDataSource(DataFragmentVector fragments)
      : fragments_(std::move(fragments)) {}

  DataFragmentIterator GetFragmentsImpl(ScanOptionsPtr options) override;

  std::string type() const override { return "simple_data_source"; }

 private:
  DataFragmentVector fragments_;
};

/// \brief A recursive DataSource with child DataSources.
class ARROW_DS_EXPORT TreeDataSource : public DataSource {
 public:
  explicit TreeDataSource(DataSourceVector children) : children_(std::move(children)) {}

  DataFragmentIterator GetFragmentsImpl(ScanOptionsPtr options) override;

  std::string type() const override { return "tree_data_source"; }

 private:
  DataSourceVector children_;
};

/// \brief Top-level interface for a Dataset with fragments coming
/// from possibly multiple sources.
class ARROW_DS_EXPORT Dataset : public std::enable_shared_from_this<Dataset> {
 public:
  /// \brief Build a Dataset from uniform sources.
  //
  /// \param[in] sources one or more input data sources
  /// \param[in] schema a known schema to conform to, may be nullptr
  static Result<DatasetPtr> Make(DataSourceVector sources,
                                 std::shared_ptr<Schema> schema);

  /// \brief Begin to build a new Scan operation against this Dataset
  Result<ScannerBuilderPtr> NewScan(ScanContextPtr context);
  Result<ScannerBuilderPtr> NewScan();

  const DataSourceVector& sources() const { return sources_; }

  std::shared_ptr<Schema> schema() const { return schema_; }

 protected:
  explicit Dataset(DataSourceVector sources, std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)), sources_(std::move(sources)) {}

  // The data sources must conform their output to this schema (with
  // projections and filters taken into account)
  std::shared_ptr<Schema> schema_;

  DataSourceVector sources_;
};

}  // namespace dataset
}  // namespace arrow
