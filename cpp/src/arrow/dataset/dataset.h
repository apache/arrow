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

/// \brief A granular piece of a Dataset, such as an individual file, which can be
/// read/scanned separately from other fragments.
///
/// A Fragment yields a collection of RecordBatch, encapsulated in one or more ScanTasks.
class ARROW_DS_EXPORT Fragment {
 public:
  /// \brief Scan returns an iterator of ScanTasks, each of which yields
  /// RecordBatches from this Fragment.
  virtual Result<ScanTaskIterator> Scan(std::shared_ptr<ScanContext> context) = 0;

  /// \brief Return true if the fragment can benefit from parallel
  /// scanning
  virtual bool splittable() const = 0;

  /// \brief Filtering, schema reconciliation, and partition options to use when
  /// scanning this fragment. May be nullptr, which indicates that no filtering
  /// or schema reconciliation will be performed and all partitions will be
  /// scanned.
  std::shared_ptr<ScanOptions> scan_options() const { return scan_options_; }

  virtual ~Fragment() = default;

  /// \brief An expression which evaluates to true for all data viewed by this
  /// Fragment. May be null, which indicates no information is available.
  const std::shared_ptr<Expression>& partition_expression() const {
    return partition_expression_;
  }

 protected:
  explicit Fragment(std::shared_ptr<ScanOptions> scan_options);

  Fragment(std::shared_ptr<ScanOptions> scan_options,
           std::shared_ptr<Expression> partition_expression)
      : scan_options_(std::move(scan_options)),
        partition_expression_(std::move(partition_expression)) {}

  std::shared_ptr<ScanOptions> scan_options_;
  std::shared_ptr<Expression> partition_expression_;
};

/// \brief A trivial Fragment that yields ScanTask out of a fixed set of
/// RecordBatch.
class ARROW_DS_EXPORT InMemoryFragment : public Fragment {
 public:
  InMemoryFragment(std::vector<std::shared_ptr<RecordBatch>> record_batches,
                   std::shared_ptr<ScanOptions> scan_options);

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanContext> context) override;

  bool splittable() const override { return false; }

 protected:
  std::vector<std::shared_ptr<RecordBatch>> record_batches_;
};

/// \brief A basic component of a Dataset which yields zero or more
/// Fragments. A Source acts as a discovery mechanism of Fragments
/// and partitions, e.g. files deeply nested in a directory.
class ARROW_DS_EXPORT Source {
 public:
  /// \brief GetFragments returns an iterator of Fragments. The ScanOptions
  /// controls filtering and schema inference.
  FragmentIterator GetFragments(std::shared_ptr<ScanOptions> options);

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  /// \brief An expression which evaluates to true for all data viewed by this Source.
  /// May be null, which indicates no information is available.
  const std::shared_ptr<Expression>& partition_expression() const {
    return partition_expression_;
  }

  /// \brief The name identifying the kind of source
  virtual std::string type_name() const = 0;

  virtual ~Source() = default;

 protected:
  explicit Source(std::shared_ptr<Schema> schema) : schema_(std::move(schema)) {}

  Source(std::shared_ptr<Schema> schema, std::shared_ptr<Expression> e)
      : schema_(std::move(schema)), partition_expression_(std::move(e)) {}
  Source() = default;

  virtual FragmentIterator GetFragmentsImpl(std::shared_ptr<ScanOptions> options) = 0;

  /// Mutates a ScanOptions by assuming partition_expression_ holds for all yielded
  /// fragments. Returns false if the selector is not satisfiable in this Source.
  virtual bool AssumePartitionExpression(
      const std::shared_ptr<ScanOptions>& scan_options,
      std::shared_ptr<ScanOptions>* simplified_scan_options) const;

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Expression> partition_expression_;
};

/// \brief A Source consisting of a flat sequence of Fragments
class ARROW_DS_EXPORT InMemorySource : public Source {
 public:
  explicit InMemorySource(std::shared_ptr<Schema> schema, FragmentVector fragments)
      : Source(std::move(schema)), fragments_(std::move(fragments)) {}

  FragmentIterator GetFragmentsImpl(std::shared_ptr<ScanOptions> options) override;

  std::string type_name() const override { return "in-memory"; }

 private:
  FragmentVector fragments_;
};

/// \brief A recursive Source with child Sources.
class ARROW_DS_EXPORT TreeSource : public Source {
 public:
  explicit TreeSource(std::shared_ptr<Schema> schema, SourceVector children)
      : Source(std::move(schema)), children_(std::move(children)) {}

  FragmentIterator GetFragmentsImpl(std::shared_ptr<ScanOptions> options) override;

  std::string type_name() const override { return "tree"; }

 private:
  SourceVector children_;
};

/// \brief Top-level interface for a Dataset with fragments coming
/// from possibly multiple sources.
class ARROW_DS_EXPORT Dataset : public std::enable_shared_from_this<Dataset> {
 public:
  /// \brief Build a Dataset from uniform sources.
  //
  /// \param[in] sources one or more input sources
  /// \param[in] schema a known schema to conform to
  static Result<std::shared_ptr<Dataset>> Make(SourceVector sources,
                                               std::shared_ptr<Schema> schema);

  /// \brief Begin to build a new Scan operation against this Dataset
  Result<std::shared_ptr<ScannerBuilder>> NewScan(std::shared_ptr<ScanContext> context);
  Result<std::shared_ptr<ScannerBuilder>> NewScan();

  const SourceVector& sources() const { return sources_; }

  std::shared_ptr<Schema> schema() const { return schema_; }

 protected:
  explicit Dataset(SourceVector sources, std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)), sources_(std::move(sources)) {}

  // The sources must conform their output to this schema (with
  // projections and filters taken into account)
  std::shared_ptr<Schema> schema_;

  SourceVector sources_;
};

}  // namespace dataset
}  // namespace arrow
