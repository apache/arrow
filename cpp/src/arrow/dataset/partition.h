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
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace arrow {
namespace dataset {

// ----------------------------------------------------------------------
// Partition schemes

struct ARROW_DS_EXPORT UnconvertedKey {
  std::string name, value;
};

/// \brief Helper function for the common case of combining partition information
/// consisting of equality expressions into a single conjunction expression
ARROW_DS_EXPORT
Status ConvertPartitionKeys(const std::vector<UnconvertedKey>& keys, const Schema& schema,
                            std::shared_ptr<Expression>* out);

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
///
/// Paths are consumed from left to right. Paths must be relative to
/// the root of a partition; path prefixes must be removed before passing
/// the path to a scheme for parsing.
class ARROW_DS_EXPORT PartitionScheme {
 public:
  virtual ~PartitionScheme() = default;

  /// \brief The name identifying the kind of partition scheme
  virtual std::string name() const = 0;

  /// \brief Parse a path into a partition expression
  ///
  /// \param[in] path the partition identifier to parse
  /// \param[out] unconsumed a suffix of path which was not consumed
  /// \param[out] out the parsed expression
  virtual Status Parse(const std::string& path, std::string* unconsumed,
                       std::shared_ptr<Expression>* out) const = 0;
};

/// \brief Trivial partition scheme which only consumes a specified prefix (empty by
/// default) from a path, yielding an expression provided on construction.
class ARROW_DS_EXPORT SimplePartitionScheme : public PartitionScheme {
 public:
  explicit SimplePartitionScheme(std::shared_ptr<Expression> expr,
                                 std::string ignored = "")
      : partition_expression_(std::move(expr)), ignored_(std::move(ignored)) {}

  std::string name() const override { return "simple_partition_scheme"; }

  Status Parse(const std::string& path, std::string* unconsumed,
               std::shared_ptr<Expression>* out) const override;

 private:
  std::shared_ptr<Expression> partition_expression_;
  std::string ignored_;
};

/// \brief Combine partition schemes
class ARROW_DS_EXPORT ChainPartitionScheme : public PartitionScheme {
 public:
  explicit ChainPartitionScheme(std::vector<std::unique_ptr<PartitionScheme>> schemes)
      : schemes_(std::move(schemes)) {}

  std::string name() const override { return "chain_partition_scheme"; }

  Status Parse(const std::string& path, std::string* unconsumed,
               std::shared_ptr<Expression>* out) const override;

 protected:
  std::vector<std::unique_ptr<PartitionScheme>> schemes_;
};

/// \brief Parse a single field from a single path segment
class ARROW_DS_EXPORT FieldPartitionScheme : public PartitionScheme {
 public:
  FieldPartitionScheme(std::shared_ptr<Field> field) : field_(std::move(field)) {}

  std::string name() const override { return "field_partition_scheme"; }

  Status Parse(const std::string& path, std::string* unconsumed,
               std::shared_ptr<Expression>* out) const override;

 protected:
  std::shared_ptr<Field> field_;
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

  Status Parse(const std::string& path, std::string* unconsumed,
               std::shared_ptr<Expression>* out) const override;

  std::vector<UnconvertedKey> GetUnconvertedKeys(const std::string& path,
                                                 std::string* unconsumed) const;

 protected:
  /// XXX do we have a schema when constructing partition schemes?
  /// If so: where do we get that?
  /// If not: we don't know what the types of RHSs are; they must stay strings
  /// for later conversion. This means among other things that some errors are
  /// deferred until the schema is given, including some parse errors.
  /// Also: how do we know what fields belong to this partition scheme?
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
