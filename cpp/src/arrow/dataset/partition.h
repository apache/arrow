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

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace dataset {

// ----------------------------------------------------------------------
// Computing partition values

// TODO(wesm): API for computing partition keys derived from raw
// values. For example, year(value) or hash_function(value) instead of
// simply value, so a dataset with a timestamp column might group all
// data with year 2009 in the same partition

// /// \brief
// class ScalarTransform {
//  public:
//   virtual Status Transform(const std::shared_ptr<Scalar>& input,
//                            std::shared_ptr<Scalar>* output) const = 0;
// };

// class PartitionField {
//  public:

//  private:
//   std::string field_name_;
// };

// ----------------------------------------------------------------------
// Partition schemes

/// \brief Interface for parsing partition expressions from string partition
/// identifiers.
///
/// For example, the identifier "foo=5" might be parsed to an equality expression
/// between the "foo" field and the value 5.
///
/// Some partition schemes may store the field names in a metadata
/// store instead of in file paths, for example
/// dataset_root/2009/11/... could be used when the partition fields
/// are "year" and "month"
class ARROW_DS_EXPORT PartitionScheme {
 public:
  virtual ~PartitionScheme() = default;

  /// \brief The name identifying the kind of partition scheme
  virtual std::string name() const = 0;

  /// \brief Return true if path can be parsed
  virtual bool CanParse(util::string_view path) const = 0;

  /// \brief Parse a path into a partition expression
  virtual Status Parse(util::string_view path,
                       std::shared_ptr<Expression>* out) const = 0;
};

/// \brief Trivial partition scheme which does not actually parse paths.
/// Instead it returns an expression provided on construction whatever the path.
class ARROW_DS_EXPORT SimplePartitionScheme : public PartitionScheme {
 public:
  explicit SimplePartitionScheme(std::shared_ptr<Expression> expr)
      : partition_expression_(std::move(expr)) {}

  std::string name() const override { return "simple_partition_scheme"; }

  bool CanParse(util::string_view path) const override { return true; }

  Status Parse(util::string_view path, std::shared_ptr<Expression>* out) const override {
    *out = partition_expression_;
    return Status::OK();
  }

 private:
  std::shared_ptr<Expression> partition_expression_;
};

/// \brief Multi-level, directory based partitioning scheme
/// originating from Apache Hive with all data files stored in the
/// leaf directories. Data is partitioned by static values of a
/// particular column in the schema. Partition keys are represented in
/// the form $key=$value in directory names
class ARROW_DS_EXPORT HivePartitionScheme : public PartitionScheme {
 public:
  explicit HivePartitionScheme(std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)) {}

  std::string name() const override { return "hive_partition_scheme"; }

  bool CanParse(util::string_view path) const override;

  Status Parse(util::string_view path, std::shared_ptr<Expression>* out) const override;

 protected:
  /// XXX do we have a schema when constructing partition schemes?
  /// If so: where do we get that?
  /// If not: we don't know what the types of RHSs are; they must stay strings
  /// for later conversion. This means among other things that some errors are
  /// deferred until the schema is given, including some parse errors.
  std::shared_ptr<Schema> schema_;
};

// ----------------------------------------------------------------------
//

// Partitioned datasets come in different forms. Here is an example of
// a Hive-style partitioned dataset:
//
// dataset_root/
//   key1=$k1_v1/
//     key2=$k2_v1/
//       0.parquet
//       1.parquet
//       2.parquet
//       3.parquet
//     key2=$k2_v2/
//       0.parquet
//       1.parquet
//   key1=$k1_v2/
//     key2=$k2_v1/
//       0.parquet
//       1.parquet
//     key2=$k2_v2/
//       0.parquet
//       1.parquet
//       2.parquet
//
// In this case, the dataset has 11 fragments (11 files) to be
// scanned, or potentially more if it is configured to split Parquet
// files at the row group level

}  // namespace dataset
}  // namespace arrow
