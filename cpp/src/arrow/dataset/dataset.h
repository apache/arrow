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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/macros.h"
#include "arrow/util/mutex.h"

namespace arrow {
namespace dataset {

/// \brief A granular piece of a Dataset, such as an individual file.
///
/// A Fragment can be read/scanned separately from other fragments. It yields a
/// collection of RecordBatches when scanned, encapsulated in one or more
/// ScanTasks.
///
/// Note that Fragments have well defined physical schemas which are reconciled by
/// the Datasets which contain them; these physical schemas may differ from a parent
/// Dataset's schema and the physical schemas of sibling Fragments.
class ARROW_DS_EXPORT Fragment {
 public:
  /// \brief Return the physical schema of the Fragment.
  ///
  /// The physical schema is also called the writer schema.
  /// This method is blocking and may suffer from high latency filesystem.
  /// The schema is cached after being read once, or may be specified at construction.
  Result<std::shared_ptr<Schema>> ReadPhysicalSchema();

  /// \brief Scan returns an iterator of ScanTasks, each of which yields
  /// RecordBatches from this Fragment.
  ///
  /// Note that batches yielded using this method will not be filtered and may not align
  /// with the Fragment's schema. In particular, note that columns referenced by the
  /// filter may be present in yielded batches even if they are not projected (so that
  /// they are available when a filter is applied). Additionally, explicitly projected
  /// columns may be absent if they were not present in this fragment.
  ///
  /// To receive a record batch stream which is fully filtered and projected, use Scanner.
  virtual Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                        std::shared_ptr<ScanContext> context) = 0;

  /// \brief Return true if the fragment can benefit from parallel scanning.
  virtual bool splittable() const = 0;

  virtual std::string type_name() const = 0;

  /// \brief An expression which evaluates to true for all data viewed by this
  /// Fragment.
  const std::shared_ptr<Expression>& partition_expression() const {
    return partition_expression_;
  }

  virtual ~Fragment() = default;

 protected:
  Fragment() = default;
  explicit Fragment(std::shared_ptr<Expression> partition_expression,
                    std::shared_ptr<Schema> physical_schema);

  virtual Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() = 0;

  util::Mutex physical_schema_mutex_;
  std::shared_ptr<Expression> partition_expression_ = scalar(true);
  std::shared_ptr<Schema> physical_schema_;
};

/// \brief A trivial Fragment that yields ScanTask out of a fixed set of
/// RecordBatch.
class ARROW_DS_EXPORT InMemoryFragment : public Fragment {
 public:
  InMemoryFragment(std::shared_ptr<Schema> schema, RecordBatchVector record_batches,
                   std::shared_ptr<Expression> = scalar(true));
  explicit InMemoryFragment(RecordBatchVector record_batches,
                            std::shared_ptr<Expression> = scalar(true));

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                std::shared_ptr<ScanContext> context) override;

  bool splittable() const override { return false; }

  std::string type_name() const override { return "in-memory"; }

 protected:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override;

  RecordBatchVector record_batches_;
};

/// \brief A container of zero or more Fragments.
///
/// A Dataset acts as a union of Fragments, e.g. files deeply nested in a
/// directory. A Dataset has a schema to which Fragments must align during a
/// scan operation. This is analogous to Avro's reader and writer schema.
class ARROW_DS_EXPORT Dataset : public std::enable_shared_from_this<Dataset> {
 public:
  /// \brief Begin to build a new Scan operation against this Dataset
  Result<std::shared_ptr<ScannerBuilder>> NewScan(std::shared_ptr<ScanContext> context);
  Result<std::shared_ptr<ScannerBuilder>> NewScan();

  /// \brief GetFragments returns an iterator of Fragments given a predicate.
  FragmentIterator GetFragments(std::shared_ptr<Expression> predicate = scalar(true));

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  /// \brief An expression which evaluates to true for all data viewed by this Dataset.
  /// May be null, which indicates no information is available.
  const std::shared_ptr<Expression>& partition_expression() const {
    return partition_expression_;
  }

  /// \brief The name identifying the kind of Dataset
  virtual std::string type_name() const = 0;

  /// \brief Return a copy of this Dataset with a different schema.
  ///
  /// The copy will view the same Fragments. If the new schema is not compatible with the
  /// original dataset's schema then an error will be raised.
  virtual Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const = 0;

  virtual ~Dataset() = default;

 protected:
  explicit Dataset(std::shared_ptr<Schema> schema) : schema_(std::move(schema)) {}

  Dataset(std::shared_ptr<Schema> schema,
          std::shared_ptr<Expression> partition_expression);

  virtual FragmentIterator GetFragmentsImpl(std::shared_ptr<Expression> predicate) = 0;

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Expression> partition_expression_ = scalar(true);
};

/// \brief A Source which yields fragments wrapping a stream of record batches.
///
/// The record batches must match the schema provided to the source at construction.
class ARROW_DS_EXPORT InMemoryDataset : public Dataset {
 public:
  class RecordBatchGenerator {
   public:
    virtual ~RecordBatchGenerator() = default;
    virtual RecordBatchIterator Get() const = 0;
  };

  InMemoryDataset(std::shared_ptr<Schema> schema,
                  std::shared_ptr<RecordBatchGenerator> get_batches)
      : Dataset(std::move(schema)), get_batches_(std::move(get_batches)) {}

  // Convenience constructor taking a fixed list of batches
  InMemoryDataset(std::shared_ptr<Schema> schema, RecordBatchVector batches);

  explicit InMemoryDataset(std::shared_ptr<Table> table);

  std::string type_name() const override { return "in-memory"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

 protected:
  FragmentIterator GetFragmentsImpl(std::shared_ptr<Expression> predicate) override;

  std::shared_ptr<RecordBatchGenerator> get_batches_;
};

/// \brief A Dataset wrapping child Datasets.
class ARROW_DS_EXPORT UnionDataset : public Dataset {
 public:
  /// \brief Construct a UnionDataset wrapping child Datasets.
  ///
  /// \param[in] schema the schema of the resulting dataset.
  /// \param[in] children one or more child Datasets. Their schemas must be identical to
  /// schema.
  static Result<std::shared_ptr<UnionDataset>> Make(std::shared_ptr<Schema> schema,
                                                    DatasetVector children);

  const DatasetVector& children() const { return children_; }

  std::string type_name() const override { return "union"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

 protected:
  FragmentIterator GetFragmentsImpl(std::shared_ptr<Expression> predicate) override;

  explicit UnionDataset(std::shared_ptr<Schema> schema, DatasetVector children)
      : Dataset(std::move(schema)), children_(std::move(children)) {}

  DatasetVector children_;

  friend class UnionDatasetFactory;
};

}  // namespace dataset
}  // namespace arrow
